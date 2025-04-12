# Code Searcher

This project aims to index code from GitLab repositories into Elasticsearch.

## Components

*   **Discovery Service:** Finds repositories via the GitLab API.

## Setup

1.  Install requirements:
    ```bash
    pip install -r requirements.txt
    ```
2.  Set environment variables:
    ```bash
    export GITLAB_URL="https://your.gitlab.instance" # Optional, defaults to gitlab.com
    export GITLAB_PRIVATE_TOKEN="your_gitlab_api_token"
    ```

## Running

### Discovery Service

```bash
python -m discovery_service.main
``` 