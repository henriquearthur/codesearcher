import os
from dotenv import load_dotenv

load_dotenv()  # Load environment variables from .env file, if present

GITLAB_URL = os.getenv("GITLAB_URL", "https://gitlab.com")
GITLAB_PRIVATE_TOKEN = os.getenv("GITLAB_PRIVATE_TOKEN")
GITLAB_GROUP_PATH = os.getenv("GITLAB_GROUP_PATH")

if not GITLAB_PRIVATE_TOKEN:
    raise ValueError("GITLAB_PRIVATE_TOKEN environment variable is not set.") 