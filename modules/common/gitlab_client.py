"""Handles communication with the GitLab API to fetch project and branch information."""
import requests
import logging
from urllib.parse import urljoin
from typing import List, Dict, Any, Optional, Iterator, Union # Added Iterator and Union
from concurrent.futures import ThreadPoolExecutor, as_completed # Added imports

from . import config

logging.basicConfig(level=logging.INFO) # Consider configuring logging more globally if needed
logger = logging.getLogger(__name__)

# GitLab API base path
GITLAB_API_BASE = "/api/v4"

# --- Helper Functions ---

def _make_gitlab_request(
    method: str,
    endpoint: str,
    params: Optional[Dict[str, Any]] = None,
    timeout: int = 30
) -> Optional[Any]:
    """Makes a request to the GitLab API and handles common errors.

    Args:
        method (str): HTTP method (e.g., 'GET', 'POST').
        endpoint (str): API endpoint path (e.g., '/projects').
        params (Optional[Dict[str, Any]], optional): Query parameters. Defaults to None.
        timeout (int, optional): Request timeout in seconds. Defaults to 30.

    Returns:
        Optional[Any]: The JSON response from GitLab, or None if an error occurs
                       or the resource is not found (for GET requests).
    """
    api_url = urljoin(config.GITLAB_URL, GITLAB_API_BASE + endpoint)
    headers = {"PRIVATE-TOKEN": config.GITLAB_PRIVATE_TOKEN}
    try:
        logger.debug(f"Making {method} request to {api_url} with params: {params}")
        response = requests.request(method, api_url, headers=headers, params=params, timeout=timeout)

        # Handle 404 specifically for GET requests (resource not found)
        if method.upper() == 'GET' and response.status_code == 404:
            logger.debug(f"Resource not found (404) at {api_url}")
            return None

        response.raise_for_status() # Raise HTTPError for other bad status codes (4xx or 5xx)
        logger.debug(f"Request to {api_url} successful ({response.status_code})")
        # Handle cases where response might be empty (e.g., 204 No Content)
        if response.status_code == 204 or not response.content:
            return None
        return response.json()

    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP error during GitLab API request to {api_url}: {e.response.status_code} {e.response.reason}")
        # Log response body for more context if available and useful (be careful with sensitive data)
        # if e.response is not None:
        #     logger.error(f"Response body: {e.response.text}")
        return None # Or re-raise a custom exception
    except requests.exceptions.RequestException as e:
        logger.error(f"Error during GitLab API request to {api_url}: {e}")
        # Consider implementing retry logic here for transient network issues
        return None # Or re-raise a custom exception
    except Exception as e:
        logger.error(f"An unexpected error occurred during GitLab API request to {api_url}: {e}", exc_info=True)
        return None


# --- Main Client Functions ---

# Consider adding more robust error handling, retry logic, and rate limit handling if needed

def get_all_projects(max_pages: Optional[int] = None) -> List[Dict[str, Any]]:
    """Fetches all accessible projects from the GitLab API, handling pagination.

    Args:
        max_pages (Optional[int], optional): Maximum number of pages to fetch.
                                            Defaults to None (fetch all pages).

    Returns:
        List[Dict[str, Any]]: A list of dictionaries, each representing a GitLab project.
                              Returns an empty list if an error occurs during fetching
                              or no projects are found.
    """
    projects: List[Dict[str, Any]] = []
    page = 1
    per_page = 100  # Max allowed by GitLab API v4

    while True:
        if max_pages is not None and page > max_pages:
            logger.warning(f"Reached max_pages limit ({max_pages}). Stopping pagination.")
            break

        endpoint = "/projects"
        params = {
            "page": page,
            "per_page": per_page,
            # Add other parameters like 'membership=true' or 'owned=true' if needed
            # "archived": "false", # Example: Exclude archived projects
        }

        logger.info(f"Fetching projects page {page} from {config.GITLAB_URL}")
        # Reuse the helper for the actual request
        # We need the full response object here to check headers, so call requests directly
        api_url = urljoin(config.GITLAB_URL, GITLAB_API_BASE + endpoint)
        headers = {"PRIVATE-TOKEN": config.GITLAB_PRIVATE_TOKEN}
        try:
            response = requests.get(api_url, headers=headers, params=params, timeout=30)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching projects page {page} from GitLab: {e}")
            # Decide on behavior: break, return partial list, raise exception?
            break # Exit loop on error for now

        current_page_projects = response.json()
        if not current_page_projects:
            logger.info("No more projects found on this page.")
            break

        projects.extend(current_page_projects)
        logger.info(f"Fetched {len(current_page_projects)} projects on page {page}. Total: {len(projects)}")

        # Check pagination headers efficiently
        next_page_header = response.headers.get('X-Next-Page')
        if next_page_header and next_page_header.isdigit():
            page = int(next_page_header)
        else:
            logger.info("No next page indicated by GitLab API headers.")
            break # Last page reached or header missing/invalid

    logger.info(f"Finished fetching projects. Total projects retrieved: {len(projects)}")
    return projects


def get_group_projects(group_path: str, include_subgroups: bool = True, max_pages: Optional[int] = None, max_workers: int = 10) -> List[Dict[str, Any]]:
    """Fetches projects belonging to a specific GitLab group, optionally including subgroups.

    Handles pagination, fetching pages concurrently after the first page.

    Args:
        group_path (str): The URL-encoded path of the GitLab group (e.g., 'my-group%2Fmy-subgroup').
        include_subgroups (bool, optional): Whether to include projects from subgroups.
                                            Defaults to True.
        max_pages (Optional[int], optional): Maximum number of pages to fetch.
                                            Defaults to None (fetch all pages based on X-Total-Pages).
        max_workers (int, optional): Maximum number of threads for concurrent page fetching. Defaults to 10.

    Returns:
        List[Dict[str, Any]]: A list of dictionaries, each representing a GitLab project.
                              Returns an empty list if an error occurs or no projects are found.
    """
    projects: List[Dict[str, Any]] = []
    per_page = 100 # Max allowed by GitLab API v4
    endpoint = f"/groups/{group_path}/projects"
    api_url = urljoin(config.GITLAB_URL, GITLAB_API_BASE + endpoint)
    headers = {"PRIVATE-TOKEN": config.GITLAB_PRIVATE_TOKEN}

    # --- Fetch First Page and Total Pages ---
    logger.info(f"Fetching initial project page for group \'{group_path}\' to determine total pages...")
    initial_params = {
        "page": 1,
        "per_page": per_page,
        "include_subgroups": str(include_subgroups).lower(),
        # "archived": "false", # Example
    }
    try:
        response = requests.get(api_url, headers=headers, params=initial_params, timeout=45)
        response.raise_for_status()
        initial_projects = response.json()
        projects.extend(initial_projects)

        # Get total pages from header
        total_pages_header = response.headers.get('X-Total-Pages')
        if total_pages_header and total_pages_header.isdigit():
            total_pages = int(total_pages_header)
            logger.info(f"GitLab indicates {total_pages} total pages for group \'{group_path}\'. Fetched page 1 ({len(initial_projects)} projects).")
        else:
            logger.warning(f"Could not determine total pages from X-Total-Pages header for group \'{group_path}\'. Only page 1 fetched.")
            total_pages = 1 # Assume only one page if header is missing

        # Apply max_pages limit if specified
        if max_pages is not None and total_pages > max_pages:
            logger.warning(f"Limiting fetch to {max_pages} pages due to max_pages setting (total was {total_pages}).")
            total_pages = max_pages

    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching initial project page for group \'{group_path}\' from GitLab: {e}")
        return [] # Return empty list on initial fetch error

    if not initial_projects and total_pages <= 1:
         logger.info(f"No projects found on the first page for group \'{group_path}\'.")
         return []


    # --- Fetch Remaining Pages Concurrently ---
    if total_pages > 1:
        logger.info(f"Fetching remaining {total_pages - 1} pages concurrently (max_workers={max_workers})...")

        def _fetch_page(page_num: int) -> Optional[List[Dict[str, Any]]]:
            """Fetches a single page of projects."""
            page_params = {
                "page": page_num,
                "per_page": per_page,
                "include_subgroups": str(include_subgroups).lower(),
            }
            try:
                # logger.debug(f"Fetching page {page_num} for group {group_path}")
                page_response = requests.get(api_url, headers=headers, params=page_params, timeout=45)
                page_response.raise_for_status()
                page_data = page_response.json()
                # logger.debug(f"Successfully fetched page {page_num} ({len(page_data)} projects)")
                return page_data
            except requests.exceptions.RequestException as e:
                logger.error(f"Error fetching projects page {page_num} for group \'{group_path}\': {e}")
                return None # Indicate error for this page

        with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix='GitLabPageFetch') as executor:
            future_to_page = {
                executor.submit(_fetch_page, page): page
                for page in range(2, total_pages + 1)
            }

            for future in as_completed(future_to_page):
                page_num = future_to_page[future]
                try:
                    page_result = future.result()
                    if page_result is not None:
                        projects.extend(page_result)
                        logger.info(f"Successfully processed results for page {page_num} ({len(page_result)} projects). Current total: {len(projects)}")
                    else:
                        # Error already logged in _fetch_page
                        logger.warning(f"Skipping results for page {page_num} due to fetch error.")
                except Exception as exc:
                    logger.error(f"Page {page_num} generated an exception during future processing: {exc}", exc_info=True)


    logger.info(f"Finished fetching projects for group \'{group_path}\'. Total projects retrieved: {len(projects)}")
    return projects


def get_project_branch(project_id: int, branch_name: str) -> Optional[Dict[str, Any]]:
    """Fetches details for a specific branch within a project.

    Args:
        project_id (int): The ID of the GitLab project.
        branch_name (str): The name of the branch to fetch.

    Returns:
        Optional[Dict[str, Any]]: A dictionary containing branch details if found,
                                  otherwise None (if branch doesn't exist or an error occurs).
    """
    endpoint = f"/projects/{project_id}/repository/branches/{branch_name}"
    logger.debug(f"Fetching branch '{branch_name}' for project {project_id}")
    branch_data = _make_gitlab_request('GET', endpoint, timeout=15)

    if branch_data:
        logger.debug(f"Successfully fetched branch '{branch_name}' for project {project_id}.")
    # else: # Handled by _make_gitlab_request returning None for 404 or other errors
    #     logger.debug(f"Branch '{branch_name}' not found or error occurred for project {project_id}.")

    return branch_data


def get_repository_tree(project_id: int, ref: str) -> Iterator[Dict[str, Any]]:
    """Fetches the repository tree (list of files and directories) recursively for a specific ref (commit SHA, branch, tag).

    Uses pagination to handle potentially large trees.

    Args:
        project_id (int): The ID of the GitLab project.
        ref (str): The commit SHA, branch name, or tag name.

    Yields:
        Iterator[Dict[str, Any]]: An iterator yielding dictionaries, each representing a file or directory entry.
                                   Yields nothing if the tree is empty or an error occurs.
    """
    page = 1
    per_page = 100 # Max allowed by GitLab API v4

    while True:
        endpoint = f"/projects/{project_id}/repository/tree"
        params = {
            "ref": ref,
            "recursive": "true",
            "per_page": per_page,
            "page": page
        }
        logger.info(f"Fetching repository tree page {page} for project {project_id}, ref {ref}")

        # We need the full response object here to check headers, so call requests directly
        api_url = urljoin(config.GITLAB_URL, GITLAB_API_BASE + endpoint)
        headers = {"PRIVATE-TOKEN": config.GITLAB_PRIVATE_TOKEN}
        try:
            response = requests.get(api_url, headers=headers, params=params, timeout=60) # Increased timeout for potentially large trees

            # Check for 404 specifically (repo or ref might not exist)
            if response.status_code == 404:
                logger.warning(f"Repository tree not found (404) for project {project_id}, ref {ref}. Might be an empty repo or invalid ref.")
                break # Stop pagination

            response.raise_for_status() # Raise HTTPError for other bad status codes

        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching repository tree page {page} for project {project_id}, ref {ref}: {e}")
            break # Exit loop on error

        tree_items = response.json()
        if not tree_items:
            logger.info(f"No more tree items found on page {page} for project {project_id}, ref {ref}.")
            break # No more items

        logger.debug(f"Fetched {len(tree_items)} tree items on page {page}.")
        for item in tree_items:
            yield item

        # Check pagination headers
        next_page_header = response.headers.get('X-Next-Page')
        if next_page_header and next_page_header.isdigit():
            page = int(next_page_header)
        else:
            logger.info("No next page indicated by GitLab API headers for tree.")
            break # Last page reached

    logger.info(f"Finished fetching repository tree for project {project_id}, ref {ref}.") 