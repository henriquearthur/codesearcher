"""Repository Discovery Service.

This service fetches project information from a GitLab instance,
identifies specified target branches (e.g., development, master),
and publishes details about these branches to a Kafka topic.

It uses a KafkaProducer for asynchronous message sending and a ThreadPoolExecutor
to process projects concurrently.
"""
import logging
import json
import time # For timing operations
from kafka import KafkaProducer
from kafka.errors import KafkaError
from concurrent.futures import ThreadPoolExecutor, as_completed, Future
from typing import List, Dict, Any, Optional, Union # Added typing imports

from . import gitlab_client
from . import config

# Configure logging
# Using a basic config here, but consider external configuration (e.g., file, dictConfig)
# for more complex applications.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - [%(threadName)s] - %(message)s"
)
logger = logging.getLogger(__name__)

# --- Constants ---
# List of branch names to check for in each project.
TARGET_BRANCHES: List[str] = ["desenvolvimento", "homologacao", "master"]
# Maximum number of worker threads to use for processing projects concurrently.
MAX_WORKERS: int = 100
# Timeout in seconds for flushing the Kafka producer buffer before closing.
KAFKA_FLUSH_TIMEOUT: int = 120
# ----------------


def process_project(project: Dict[str, Any], producer: KafkaProducer) -> int:
    """Processes a single GitLab project.

    Fetches details for target branches within the project.
    If a target branch exists, constructs a message containing project and branch
    details and sends it to the configured Kafka topic using the provided producer.

    Args:
        project: Dictionary representing a GitLab project (from GitLab API).
        producer: The initialized KafkaProducer instance.

    Returns:
        The number of messages successfully queued for sending for this project.
        Returns 0 if the project ID is missing or no target branches are found.
    """
    messages_queued_count = 0
    project_id = project.get('id')
    project_name_ns = project.get('name_with_namespace', 'N/A') # Default for logging

    if not project_id:
        logger.warning(f"Skipping project with missing ID: {project_name_ns}")
        return messages_queued_count

    # Optional: Log start of processing for a project if needed (can be verbose)
    # logger.debug(f"Processing project {project_id} ('{project_name_ns}')")

    for branch_name in TARGET_BRANCHES:
        try:
            # Fetch branch details using the GitLab client
            branch_details = gitlab_client.get_project_branch(project_id, branch_name)

            if branch_details:
                logger.debug(f"Found branch '{branch_name}' for project {project_id}. Preparing message.")
                # Construct the message payload
                message_data = {
                    # Project Details
                    'project_id': project_id,
                    'project_name': project.get('name_with_namespace'),
                    'project_path': project.get('path_with_namespace'),
                    'project_last_activity_at': project.get('last_activity_at'),
                    'project_web_url': project.get('web_url'),
                    'project_ssh_url': project.get('ssh_url_to_repo'),
                    'project_http_url': project.get('http_url_to_repo'),
                    # Branch Details
                    'branch_name': branch_details.get('name'),
                    'branch_commit_sha': branch_details.get('commit', {}).get('id'),
                    'branch_commit_authored_date': branch_details.get('commit', {}).get('authored_date'),
                    'branch_commit_message': branch_details.get('commit', {}).get('message'),
                    'branch_web_url': branch_details.get('web_url'),
                }
                # Define a unique key for the Kafka message (useful for partitioning/compaction)
                message_key = f"{project_id}-{branch_name}"

                # Send the message asynchronously via the Kafka producer.
                # The producer handles batching and sending in the background.
                try:
                    # producer.send returns a Future - can be used for detailed result tracking
                    future: Future = producer.send(
                        config.KAFKA_TOPIC_REPOSITORIES,
                        value=message_data,
                        key=message_key.encode('utf-8') # Keys should be bytes
                    )
                    messages_queued_count += 1
                    # Optional: Add callbacks for async success/error handling per message
                    # future.add_callback(on_send_success, message_key)
                    # future.add_errback(on_send_error, message_key)
                    logger.debug(f"Queued message for project {project_id}, branch '{branch_name}'. Key: {message_key}")
                except KafkaError as e:
                    # Error queuing the message (e.g., buffer full, serialization error)
                    logger.error(f"Kafka queuing error for project {project_id}, branch '{branch_name}': {e}")
                except Exception as e:
                    # Catch unexpected errors during message preparation or queuing
                    logger.error(f"Unexpected error queuing message for project {project_id}, branch '{branch_name}': {e}", exc_info=True)

            # else: Branch not found, log implicitly via gitlab_client debug logs if enabled

        except Exception as e:
            # Catch errors during branch fetching for a specific branch
            logger.error(f"Error processing branch '{branch_name}' for project {project_id} ('{project_name_ns}'): {e}", exc_info=False) # Set exc_info=True for traceback

    if messages_queued_count > 0:
         logger.info(f"Finished processing project {project_id} ('{project_name_ns}'). Queued {messages_queued_count} messages.")

    return messages_queued_count

# Optional callback functions for detailed per-message status
# def on_send_success(record_metadata, message_key):
#     logger.info(f"Successfully sent message with key {message_key} to topic {record_metadata.topic} partition {record_metadata.partition} offset {record_metadata.offset}")
#
# def on_send_error(exc, message_key):
#     logger.error(f"Error sending message with key {message_key}: {exc}")

def main():
    """Main execution function."""
    logger.info("Starting Repository Discovery Service...")

    producer: Optional[KafkaProducer] = None
    start_time_main = time.time()
    total_messages_queued_count = 0

    try:
        # Initialize Kafka Producer
        logger.info(f"Initializing Kafka producer for brokers: {config.KAFKA_BROKERS}")
        producer = KafkaProducer(
            bootstrap_servers=config.KAFKA_BROKERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=None, # Keys are manually encoded in send()
            # How many acknowledgments the leader must receive before considering a request complete.
            # 'all': Broker waits for all in-sync replicas to acknowledge.
            # '1': Broker waits for leader acknowledgment.
            # '0': Producer does not wait for acknowledgment.
            acks='all',
            # Number of retries if a send fails.
            retries=5,
            # Consider adding linger_ms and batch_size for tuning throughput/latency
            # linger_ms=5, # Max time (ms) to buffer data before sending batch
            # batch_size=16384, # Max bytes per batch
            # Optional: Compression type (e.g., 'gzip', 'snappy', 'lz4')
            # compression_type='gzip',
        )
        logger.info("Kafka producer initialized successfully.")

        # Fetch Projects from GitLab
        logger.info("Fetching projects from GitLab...")
        # Use max_pages=N during testing to limit API calls
        projects: List[Dict[str, Any]] = gitlab_client.get_all_projects(max_pages=None)
        # projects = projects[:10] # DEBUG: Limit projects for testing

        if not projects:
            logger.warning("No projects retrieved from GitLab. Check token permissions, API URL, and network access.")
            # Exit gracefully if no projects found
            return

        logger.info(f"Retrieved {len(projects)} projects. Processing target branches concurrently (max_workers={MAX_WORKERS})...")

        # Process Projects Concurrently
        start_time_processing = time.time()
        with ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix='RepoWorker') as executor:
            # Submit tasks: process each project using the shared producer instance
            future_to_project: Dict[Future, Dict[str, Any]] = {
                executor.submit(process_project, project, producer): project
                for project in projects
            }

            # Collect results as they complete
            for future in as_completed(future_to_project):
                project = future_to_project[future]
                project_id: Union[int, str] = project.get('id', 'unknown_id')
                project_name_ns = project.get('name_with_namespace', 'unknown_project')
                try:
                    # Get the count of messages queued by this worker
                    messages_queued_by_worker: int = future.result()
                    total_messages_queued_count += messages_queued_by_worker
                    # Log progress periodically if needed
                    # if total_messages_queued_count % 100 == 0:
                    #     logger.info(f"Queued {total_messages_queued_count} messages so far...")
                except Exception as exc:
                    # Log exceptions raised *within* the process_project task execution
                    logger.error(f"Project {project_id} ('{project_name_ns}') generated an exception during processing task: {exc}", exc_info=True)

        processing_time = time.time() - start_time_processing
        logger.info(f"Project processing and message queuing completed in {processing_time:.2f} seconds.")
        logger.info(f"Total messages queued across all projects: {total_messages_queued_count}")

    except KafkaError as e:
        # Errors during producer initialization or potentially fatal connection issues
        logger.exception(f"Fatal Kafka error: {e}")
    except Exception as e:
        # Catch-all for other unexpected errors in the main block
        logger.exception(f"An unexpected error occurred in the main execution: {e}")
    finally:
        # Ensure Kafka producer resources are released
        if producer:
            try:
                logger.info(f"Flushing Kafka producer buffer (timeout={KAFKA_FLUSH_TIMEOUT}s). This may take time...")
                # Wait for all outstanding messages to be delivered or timeout.
                producer.flush(timeout=KAFKA_FLUSH_TIMEOUT)
                logger.info("Kafka producer flushed successfully.")
            except KafkaError as e:
                logger.error(f"Error flushing Kafka producer: {e}")
            except Exception as e:
                 logger.error(f"Unexpected error during producer flush: {e}")
            finally:
                logger.info("Closing Kafka producer.")
                producer.close()

    end_time_main = time.time()
    total_duration = end_time_main - start_time_main
    logger.info(f"Repository Discovery Service finished in {total_duration:.2f} seconds.")

if __name__ == "__main__":
    main() # Call the main function 