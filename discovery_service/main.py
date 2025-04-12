import logging
import json

from . import gitlab_client

logger = logging.getLogger(__name__)

if __name__ == "__main__":
    logger.info("Starting Repository Discovery Service...")
    try:
        # For testing, you might want to limit the pages:
        # projects = gitlab_client.get_all_projects(max_pages=2)
        projects = gitlab_client.get_all_projects()

        if projects:
            logger.info(f"Successfully retrieved {len(projects)} projects.")
            # In a real scenario, here you would enqueue tasks based on these projects
            # For now, just print basic info
            for project in projects:
                print(f"- Project ID: {project.get('id')}, Name: {project.get('name_with_namespace')}, Path: {project.get('path_with_namespace')}, Last Activity: {project.get('last_activity_at')}")
            # Optionally, save to a file
            # with open("projects.json", "w") as f:
            #     json.dump(projects, f, indent=2)
            # logger.info("Project list saved to projects.json")
        else:
            logger.warning("No projects were retrieved. Check token permissions and GitLab API access.")

    except Exception as e:
        logger.exception(f"An error occurred during repository discovery: {e}")

    logger.info("Repository Discovery Service finished.") 