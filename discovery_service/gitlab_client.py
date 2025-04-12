import requests
import logging
from urllib.parse import urljoin

from . import config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Consider adding more robust error handling, retry logic, and rate limit handling

def get_all_projects(max_pages=None):
    """Fetches all accessible projects from the GitLab API, handling pagination."""
    projects = []
    page = 1
    per_page = 100  # Max allowed by GitLab API v4
    api_url = urljoin(config.GITLAB_URL, "/api/v4/projects")
    headers = {"PRIVATE-TOKEN": config.GITLAB_PRIVATE_TOKEN}

    while True:
        params = {
            "page": page,
            "per_page": per_page,
        }
        try:
            logger.info(f"Fetching projects page {page} from {config.GITLAB_URL}")
            response = requests.get(api_url, headers=headers, params=params, timeout=30)
            response.raise_for_status()  # Raise an exception for bad status codes (4xx or 5xx)
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching projects from GitLab: {e}")
            # Implement retry logic or specific error handling here
            break # Exit loop on error for now

        current_page_projects = response.json()
        if not current_page_projects:
            logger.info("No more projects found.")
            break  # No more projects

        projects.extend(current_page_projects)
        logger.info(f"Fetched {len(current_page_projects)} projects on page {page}. Total: {len(projects)}")

        # Check pagination headers
        if 'X-Next-Page' in response.headers and response.headers['X-Next-Page']:
            page = int(response.headers['X-Next-Page'])
            if max_pages is not None and page > max_pages:
                logger.warning(f"Reached max_pages limit ({max_pages}). Stopping pagination.")
                break
        else:
            logger.info("No next page indicated by GitLab API.")
            break # Last page reached

    logger.info(f"Finished fetching. Total projects retrieved: {len(projects)}")
    return projects

def get_project_branch(project_id, branch_name):
    """Fetches details for a specific branch within a project."""
    api_url = urljoin(config.GITLAB_URL, f"/api/v4/projects/{project_id}/repository/branches/{branch_name}")
    headers = {"PRIVATE-TOKEN": config.GITLAB_PRIVATE_TOKEN}
    try:
        # logger.debug(f"Fetching branch '{branch_name}' for project {project_id} from {config.GITLAB_URL}")
        response = requests.get(api_url, headers=headers, timeout=15)
        if response.status_code == 404:
            # logger.debug(f"Branch '{branch_name}' not found for project {project_id}.")
            return None # Branch doesn't exist
        response.raise_for_status() # Raise an exception for other bad status codes
        branch_data = response.json()
        # logger.debug(f"Successfully fetched branch '{branch_name}' for project {project_id}.")
        return branch_data
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching branch '{branch_name}' for project {project_id}: {e}")
        # Decide how to handle: return None, raise exception, etc.
        return None # Return None on error for now 