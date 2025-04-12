import logging
import json
from kafka import KafkaProducer
from kafka.errors import KafkaError
from concurrent.futures import ThreadPoolExecutor, as_completed
import time # For potential backoff/sleep if needed

from . import gitlab_client
from . import config # Import config module

logger = logging.getLogger(__name__)

# --- Constants ---
TARGET_BRANCHES = ["desenvolvimento", "homologacao", "master"]
MAX_WORKERS = 100 # Increased max workers
# ----------------

def process_project(project, producer): # Added producer argument
    """Processes a single project: finds target branches and sends Kafka messages.

    Args:
        project (dict): Dictionary representing a GitLab project.
        producer (KafkaProducer): The Kafka producer instance.

    Returns:
        int: The number of messages successfully queued for sending for this project.
    """
    messages_sent_count = 0 # Renamed from messages_to_send
    project_id = project.get('id')

    if not project_id:
        logger.warning(f"Skipping project with missing ID: {project.get('name_with_namespace')}")
        return messages_sent_count # Return 0

    # logger.debug(f"Processing project {project_id} ('{project.get('name_with_namespace')}')") # Reduced verbosity

    for branch_name in TARGET_BRANCHES:
        try:
            branch_details = gitlab_client.get_project_branch(project_id, branch_name)

            if branch_details:
                # logger.info(f"Found branch '{branch_name}' for project {project_id}. Preparing and sending message.") # Reduced verbosity
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
                message_key = f"{project_id}-{branch_name}"

                # Send message immediately within the worker thread
                try:
                    future = producer.send(
                        config.KAFKA_TOPIC_REPOSITORIES,
                        value=message_data,
                        key=message_key.encode('utf-8')
                    )
                    messages_sent_count += 1
                    # Optional: Add callbacks for async handling if needed
                    # future.add_callback(on_send_success, message_key)
                    # future.add_errback(on_send_error, message_key)
                    logger.debug(f"Queued message for project {project_id}, branch '{branch_name}'.")
                except KafkaError as e:
                    logger.error(f"Kafka send error for project {project_id}, branch '{branch_name}': {e}")
                except Exception as e:
                    logger.error(f"Unexpected error sending message for project {project_id}, branch '{branch_name}': {e}")

            # else: Branch not found, do nothing
        except Exception as e:
            logger.error(f"Error processing branch '{branch_name}' for project {project_id}: {e}", exc_info=False)

    if messages_sent_count > 0:
         logger.info(f"Finished processing project {project_id}. Queued {messages_sent_count} messages.")

    return messages_sent_count


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(threadName)s - %(message)s')
    logger.info("Starting Repository Discovery Service...")

    producer = None
    try:
        logger.info(f"Initializing Kafka producer for brokers: {config.KAFKA_BROKERS}")
        producer = KafkaProducer(
            bootstrap_servers=config.KAFKA_BROKERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            retries=5,
            acks='all' # Consider '1' or '0' if throughput > durability and latency is critical with many workers
        )
        logger.info("Kafka producer initialized successfully.")

        logger.info("Fetching projects from GitLab...")
        projects = gitlab_client.get_all_projects()
        # projects = projects[:200] # Optional: Limit for testing

        if projects:
            logger.info(f"Successfully retrieved {len(projects)} projects. Processing branches in parallel (max_workers={MAX_WORKERS})...")
            total_messages_sent_count = 0 # Renamed from messages_sent_count
            # all_messages_to_send = [] # Removed intermediate list

            start_time = time.time()
            with ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix='RepoWorker') as executor:
                # Pass producer instance to each worker
                future_to_project = {executor.submit(process_project, project, producer): project for project in projects}

                for future in as_completed(future_to_project):
                    project = future_to_project[future]
                    project_id = project.get('id', 'unknown')
                    try:
                        # Get the count of messages sent by this worker
                        messages_sent_by_worker = future.result()
                        total_messages_sent_count += messages_sent_by_worker
                        # No longer need to extend all_messages_to_send
                        # if project_messages:
                        #    all_messages_to_send.extend(project_messages)
                    except Exception as exc:
                        # Log exceptions raised during process_project execution (including Kafka errors if not caught internally)
                        logger.error(f"Project {project_id} ('{project.get('name_with_namespace')}') generated an exception during processing: {exc}", exc_info=True)

            processing_time = time.time() - start_time
            logger.info(f"Branch checking and message queuing completed in {processing_time:.2f} seconds.")
            logger.info(f"Total messages successfully queued across all projects: {total_messages_sent_count}")

            # Removed the sequential sending loop as messages are sent within workers
            # if all_messages_to_send:
            #    logger.info(f"Sending {len(all_messages_to_send)} messages...")
            #    ...
            # else:
            #     logger.info("No messages to send to Kafka.")

        else:
            logger.warning("No projects were retrieved. Check token permissions and GitLab API access.")

    except KafkaError as e:
        logger.exception(f"Kafka producer connection error: {e}")
    except Exception as e:
        logger.exception(f"An unexpected error occurred: {e}")
    finally:
        if producer:
            logger.info("Flushing Kafka producer (may take time if buffer is large)...")
            producer.flush(timeout=120) # Increased timeout further for 100 workers
            logger.info("Closing Kafka producer.")
            producer.close()

    logger.info("Repository Discovery Service finished.") 