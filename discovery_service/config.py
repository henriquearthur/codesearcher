import os
import shlex # Import shlex for safe parsing of broker list
from dotenv import load_dotenv

load_dotenv()  # Load environment variables from .env file, if present

GITLAB_URL = os.getenv("GITLAB_URL")
GITLAB_PRIVATE_TOKEN = os.getenv("GITLAB_PRIVATE_TOKEN")
GITLAB_GROUP_PATH = os.getenv("GITLAB_GROUP_PATH")

# Kafka Configuration
KAFKA_BROKERS_STRING = os.getenv("KAFKA_BROKERS", "localhost:29092")
KAFKA_BROKERS = shlex.split(KAFKA_BROKERS_STRING.replace(',', ' '))
KAFKA_TOPIC_REPOSITORIES = os.getenv("KAFKA_TOPIC_REPOSITORIES", "codesearch.repositories")