import json
import logging
import time
import signal
import sys
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

from ..common import config
from ..common import gitlab_client

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Kafka Client Initialization ---

producer = None
consumer = None

def initialize_kafka_clients(retry_delay=5):
    """Initializes Kafka consumer and producer, with retry logic."""
    global producer, consumer
    while True:
        try:
            logger.info(f"Attempting to connect to Kafka brokers: {config.KAFKA_BROKERS}")
            consumer = KafkaConsumer(
                config.KAFKA_TOPIC_REPOSITORIES,
                bootstrap_servers=config.KAFKA_BROKERS,
                group_id=config.KAFKA_CONSUMER_GROUP_REPOS, # Use config variable
                auto_offset_reset='earliest', # Start from the beginning if no offset
                value_deserializer=lambda v: json.loads(v.decode('utf-8'))
            )
            logger.info(f"Kafka Consumer connected to topic '{config.KAFKA_TOPIC_REPOSITORIES}'.")

            producer = KafkaProducer(
                bootstrap_servers=config.KAFKA_BROKERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            logger.info(f"Kafka Producer connected. Target topic: '{config.KAFKA_TOPIC_FILES}'.")
            return # Success

        except NoBrokersAvailable:
            logger.error(f"Kafka brokers not available at {config.KAFKA_BROKERS}. Retrying in {retry_delay} seconds...")
            time.sleep(retry_delay)
        except Exception as e:
            logger.error(f"Failed to initialize Kafka clients: {e}. Retrying in {retry_delay} seconds...")
            time.sleep(retry_delay)

# --- Message Processing Logic ---

def process_repository(message):
    """Processes a single repository message from Kafka."""
    repo_data = message.value
    project_id = repo_data.get('project_id')
    commit_sha = repo_data.get('branch_commit_sha')
    project_name = repo_data.get('project_name', 'N/A') # Get project name for logging

    if not project_id or not commit_sha:
        logger.error(f"Missing project_id or branch_commit_sha in message: {repo_data}")
        return

    logger.info(f"Processing repository: {project_name} (ID: {project_id}), Commit: {commit_sha[:8]}")

    try:
        file_count = 0
        for item in gitlab_client.get_repository_tree(project_id, commit_sha):
            if item.get('type') == 'blob': # Process only files
                file_path = item.get('path')
                file_id = item.get('id') # Blob ID
                file_mode = item.get('mode')

                if not file_path or not file_id:
                    logger.warning(f"Skipping item due to missing path or id in project {project_id}, ref {commit_sha}: {item}")
                    continue

                # Construct the output message
                file_message = {
                    'project_id': project_id,
                    'project_name': repo_data.get('project_name'),
                    'project_path': repo_data.get('project_path'),
                    'commit_sha': commit_sha,
                    'file_path': file_path,
                    'file_id': file_id,
                    'file_mode': file_mode,
                    # Pass through other relevant repo data if needed
                    'project_last_activity_at': repo_data.get('project_last_activity_at'),
                    'project_web_url': repo_data.get('project_web_url'),
                    'branch_name': repo_data.get('branch_name'),
                }

                # Send to output Kafka topic
                if producer:
                    try:
                        future = producer.send(config.KAFKA_TOPIC_FILES, value=file_message)
                        # Optional: Wait for confirmation (can slow down processing)
                        # record_metadata = future.get(timeout=10)
                        # logger.debug(f"Sent file '{file_path}' to Kafka topic {record_metadata.topic} partition {record_metadata.partition}")
                        file_count += 1
                    except Exception as e:
                        logger.error(f"Failed to send file message for '{file_path}' (Project {project_id}) to Kafka: {e}")
                        # Decide on error handling: retry, dead-letter queue, skip?
                else:
                    logger.error("Kafka Producer is not initialized. Cannot send message.")
                    # Potentially re-initialize or exit

        logger.info(f"Finished processing repository {project_name} (ID: {project_id}). Found and enqueued {file_count} files for commit {commit_sha[:8]}.")

    except Exception as e:
        logger.error(f"Error processing repository {project_id} (Commit: {commit_sha[:8]}): {e}", exc_info=True)
        # Handle errors from get_repository_tree (e.g., API errors, timeouts)

# --- Main Loop and Shutdown Handling ---

running = True

def handle_shutdown(signum, frame):
    """Gracefully shuts down the application."""
    global running
    logger.info(f"Received signal {signum}. Shutting down...")
    running = False

signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)

def main():
    """Main function to run the repository processing worker."""
    global running, consumer, producer

    # Validate essential configuration
    if not config.GITLAB_URL or not config.GITLAB_PRIVATE_TOKEN:
        logger.critical("GitLab URL or Private Token not configured. Exiting.")
        sys.exit(1)
    if not config.KAFKA_BROKERS:
        logger.critical("Kafka Brokers not configured. Exiting.")
        sys.exit(1)

    initialize_kafka_clients()

    if not consumer or not producer:
        logger.critical("Failed to initialize Kafka clients after retries. Exiting.")
        sys.exit(1)

    logger.info("Repository Processing Worker started. Waiting for messages...")

    try:
        while running:
            # Poll for messages with a timeout
            # Adjust poll timeout as needed
            messages = consumer.poll(timeout_ms=1000) # 1 second timeout

            if not messages:
                # logger.debug("No new messages, continuing poll...")
                continue # No messages, loop back to poll

            for topic_partition, batch in messages.items():
                logger.debug(f"Received batch for {topic_partition} with {len(batch)} messages.")
                for message in batch:
                    if not running:
                        break # Exit inner loop if shutdown requested
                    try:
                        process_repository(message)
                        # Commit offsets after successfully processing the message
                        # Consider committing in batches for better performance
                        consumer.commit()
                        logger.debug(f"Committed offset for partition {message.partition}, offset {message.offset}")
                    except Exception as e:
                        logger.error(f"Unhandled exception during message processing: {e}", exc_info=True)
                        # Decide: Skip message? Retry? Stop worker?
                if not running:
                    break # Exit outer loop if shutdown requested

    except Exception as e:
        logger.error(f"An unexpected error occurred in the main loop: {e}", exc_info=True)
    finally:
        logger.info("Shutting down Kafka clients...")
        if consumer:
            consumer.close()
            logger.info("Kafka Consumer closed.")
        if producer:
            producer.flush() # Ensure all buffered messages are sent
            producer.close()
            logger.info("Kafka Producer closed.")
        logger.info("Repository Processing Worker stopped.")

if __name__ == "__main__":
    main()
