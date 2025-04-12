"""Configuration loading for the Repository Processor Service.

Reads essential configuration parameters from environment variables.
Uses python-dotenv to load variables from a .env file if present.
"""
import os
import shlex # Import shlex for safe parsing of broker list
from dotenv import load_dotenv

load_dotenv()  # Load environment variables from .env file, if present

# --- GitLab Configuration ---
# URL of the GitLab instance (e.g., "https://gitlab.com")
GITLAB_URL = os.getenv("GITLAB_URL", "YOUR_GITLAB_INSTANCE_URL")
# Personal Access Token with API scope for accessing GitLab projects
GITLAB_PRIVATE_TOKEN = os.getenv("GITLAB_PRIVATE_TOKEN", "YOUR_API_TOKEN")
# Optional: Group path to filter projects within a specific group (e.g., "my-group/my-subgroup")
# Currently not used by the client but defined here for potential future use.
GITLAB_GROUP_PATH = os.getenv("GITLAB_GROUP_PATH")

# --- Kafka Configuration ---
# Comma-separated list of Kafka broker addresses (e.g., "kafka1:9092,kafka2:9092")
KAFKA_BROKERS_STRING = os.getenv("KAFKA_BROKERS", "localhost:29092") # Default if not set
# Parse the broker string into a list, handling potential spaces and quotes safely
KAFKA_BROKERS = shlex.split(KAFKA_BROKERS_STRING.replace(',', ' '))
# Kafka topic where repository/branch information will be published
KAFKA_TOPIC_REPOSITORIES = os.getenv("KAFKA_TOPIC_REPOSITORIES", "codesearcher.repositories") # Default if not set
# Kafka topic where individual file details will be published
KAFKA_TOPIC_FILES = os.getenv("KAFKA_TOPIC_FILES", "codesearcher.files") # Default if not set
# Kafka consumer group ID for the repository processor
KAFKA_CONSUMER_GROUP_REPOS = os.getenv("KAFKA_CONSUMER_GROUP_REPOS", "codesearcher") # Default if not set