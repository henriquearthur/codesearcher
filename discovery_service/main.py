import logging
import json
from kafka import KafkaProducer
from kafka.errors import KafkaError

from . import gitlab_client
from . import config # Import config module

logger = logging.getLogger(__name__)

if __name__ == "__main__":
    logger.info("Starting Repository Discovery Service...")

    producer = None  # Initialize producer outside try
    try:
        # Initialize Kafka Producer
        logger.info(f"Initializing Kafka producer for brokers: {config.KAFKA_BROKERS}") # Use config
        producer = KafkaProducer(
            bootstrap_servers=config.KAFKA_BROKERS, # Use config
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            retries=5, # Add some resilience
            acks='all' # Ensure messages are acknowledged by leader and replicas
        )
        logger.info("Kafka producer initialized successfully.")

        # For testing, you might want to limit the pages:
        # projects = gitlab_client.get_all_projects(max_pages=2)
        projects = gitlab_client.get_all_projects()

        if projects:
            logger.info(f"Successfully retrieved {len(projects)} projects.")
            project_count_sent = 0
            for project in projects:
                # Prepare data for Kafka
                repo_data = {
                    'id': project.get('id'),
                    'name': project.get('name_with_namespace'),
                    'path': project.get('path_with_namespace'),
                    'last_activity_at': project.get('last_activity_at'),
                    'web_url': project.get('web_url'),
                    'ssh_url_to_repo': project.get('ssh_url_to_repo'),
                    'http_url_to_repo': project.get('http_url_to_repo'),
                    # Add any other relevant fields
                }
                try:
                    # Send data to Kafka topic
                    future = producer.send(config.KAFKA_TOPIC_REPOSITORIES, value=repo_data, key=str(repo_data['id']).encode('utf-8')) # Use config
                    # Optional: Block for synchronous send confirmation (remove for higher throughput)
                    # record_metadata = future.get(timeout=10)
                    # logger.debug(f"Sent project {repo_data['id']} to topic {record_metadata.topic} partition {record_metadata.partition} offset {record_metadata.offset}")
                    project_count_sent += 1
                except KafkaError as e:
                    logger.error(f"Failed to send project {repo_data.get('id')} to Kafka: {e}")
                except Exception as e: # Catch other potential serialization errors etc.
                     logger.error(f"An unexpected error occurred sending project {repo_data.get('id')} to Kafka: {e}")


            logger.info(f"Attempted to send {project_count_sent}/{len(projects)} projects to Kafka topic '{config.KAFKA_TOPIC_REPOSITORIES}'.") # Use config
            # Optionally, save to a file
            # with open("projects.json", "w") as f:
            #     json.dump(projects, f, indent=2)
            # logger.info("Project list saved to projects.json")
        else:
            logger.warning("No projects were retrieved. Check token permissions and GitLab API access.")

    except KafkaError as e:
        logger.exception(f"Kafka producer connection error: {e}")
    except Exception as e:
        logger.exception(f"An error occurred during repository discovery or Kafka publishing: {e}")
    finally:
        if producer:
            logger.info("Flushing Kafka producer...")
            producer.flush(timeout=30) # Wait for all messages to be sent
            logger.info("Closing Kafka producer.")
            producer.close()


    logger.info("Repository Discovery Service finished.") 