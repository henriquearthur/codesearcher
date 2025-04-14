"""Worker module for processing individual files from GitLab repositories.

This module consumes file tasks from Kafka, retrieves file content from GitLab,
processes it line by line, and publishes the results to another Kafka topic.
"""
import json
import logging
from typing import Dict, Any, Optional
from confluent_kafka import Consumer, Producer, KafkaError
from confluent_kafka.admin import AdminClient
import requests
from urllib.parse import urljoin, quote

from ..common import config
from ..common.gitlab_client import _make_gitlab_request

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_file_content(project_id: int, file_path: str, ref: str) -> Optional[str]:
    """Retrieves the raw content of a file from GitLab.
    
    Args:
        project_id (int): The ID of the GitLab project
        file_path (str): The path of the file in the repository
        ref (str): The commit hash or branch name
        
    Returns:
        Optional[str]: The raw content of the file if successful, None otherwise
    """
    try:
        # URL-encode the file path to handle special characters like '/'
        encoded_file_path = quote(file_path, safe='')
        
        # Construct the API endpoint for raw file content using the encoded path
        endpoint = f"/projects/{project_id}/repository/files/{encoded_file_path}/raw"
        params = {"ref": ref}
        
        # Make the request using the common GitLab client helper
        api_url = urljoin(config.GITLAB_URL, f"/api/v4{endpoint}")
        headers = {"PRIVATE-TOKEN": config.GITLAB_PRIVATE_TOKEN}
        
        response = requests.get(api_url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        
        # Decode the content assuming UTF-8, handle potential errors
        try:
            return response.content.decode('utf-8')
        except UnicodeDecodeError:            
            try:
                return response.content.decode('latin-1')
            except UnicodeDecodeError:
                logger.error(f"Could not decode file {file_path} in project {project_id} with UTF-8 or latin-1.")
                return None # Or return raw bytes if needed downstream?

    except requests.exceptions.RequestException as e:
        # Log the specific URL that failed
        failed_url = api_url if 'api_url' in locals() else f"URL construction failed for project {project_id}, path {file_path}"
        logger.error(f"Error fetching file content for {file_path} in project {project_id} from {failed_url}: {e}")
        return None

def process_file(file_info: Dict[str, Any], producer: Producer) -> None:
    """Process a single file and publish its content line by line to Kafka.
    
    Args:
        file_info (Dict[str, Any]): Information about the file to process
        producer (Producer): Kafka producer instance for publishing results
    """
    project_id = file_info.get('project_id')
    file_path = file_info.get('file_path')
    commit_sha = file_info.get('commit_sha')
    repository_name = file_info.get('project_name', file_info.get('project_path', ''))
    
    if not all([project_id, file_path, commit_sha]):
        logger.error(f"Missing required file information: {file_info}")
        return

    # Log the file being processed
    logger.info(f"Processing file: project_id={project_id}, file_path='{file_path}', commit_sha={commit_sha}")
    
    # Get file content
    content = get_file_content(project_id, file_path, commit_sha)
    if content is None:
        return
    
    # Process file content line by line
    for line_number, line in enumerate(content.splitlines(), 1):
        # Create base document by copying all input metadata
        document = file_info.copy()
        
        # Update/add line-specific information
        document.update({
            'line_number': line_number,
            'line_content': line
        })
        
        # Optionally remove fields that might be redundant or unwanted in the final doc
        # document.pop('some_input_field', None)

        # Publish to Kafka
        try:
            producer.produce(
                config.KAFKA_TOPIC_FILES + '.content',
                key="",
                value=json.dumps(document)
            )
            producer.poll(0)  # Trigger delivery reports
        except Exception as e:
            logger.error(f"Error publishing line {line_number} of {file_path}: {e}")

def setup_kafka_consumer() -> Consumer:
    """Sets up and returns a Kafka consumer configured for file processing."""
    consumer_conf = {
        'bootstrap.servers': ','.join(config.KAFKA_BROKERS),
        'group.id': config.KAFKA_CONSUMER_GROUP_REPOS + '.files',
        'auto.offset.reset': 'earliest'
    }
    
    consumer = Consumer(consumer_conf)
    consumer.subscribe([config.KAFKA_TOPIC_FILES])
    return consumer

def setup_kafka_producer() -> Producer:
    """Sets up and returns a Kafka producer for publishing processed lines."""
    producer_conf = {
        'bootstrap.servers': ','.join(config.KAFKA_BROKERS)
    }
    return Producer(producer_conf)

def main():
    """Main entry point for the file processing worker."""
    logger.info("Starting file processing worker...")
    
    # Set up Kafka connections
    consumer = setup_kafka_consumer()
    producer = setup_kafka_producer()
    
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                continue
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.debug("Reached end of partition")
                else:
                    logger.error(f"Kafka error: {msg.error()}")
                continue
            
            try:
                file_info = json.loads(msg.value())
                process_file(file_info, producer)
            except json.JSONDecodeError as e:
                logger.error(f"Error decoding message: {e}")
            except Exception as e:
                logger.error(f"Error processing file: {e}", exc_info=True)
            
            # Flush the producer periodically
            producer.flush()
    
    except KeyboardInterrupt:
        logger.info("Shutting down file processing worker...")
    finally:
        # Clean up
        producer.flush()
        consumer.close()

if __name__ == "__main__":
    main() 