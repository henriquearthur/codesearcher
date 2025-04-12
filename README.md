# Code Searcher

This project aims to index code from GitLab repositories into Elasticsearch. It currently consists of a Discovery Service that finds repositories and publishes information about specific branches to a Kafka topic.

## Components

*   **Discovery Service:** Connects to a GitLab instance, fetches all accessible projects, checks for the existence of predefined branches (e.g., `desenvolvimento`, `homologacao`, `master`), and publishes details about found project/branch combinations to a Kafka topic.

## Setup

1.  Install requirements:
    ```bash
    pip install -r requirements.txt
    ```
2.  Create a `.env` file in the project root or set the following environment variables:
    ```bash
    # GitLab Configuration
    export GITLAB_URL="https://your.gitlab.instance" # Optional, defaults to https://gitlab.com
    export GITLAB_PRIVATE_TOKEN="your_gitlab_api_token" # Required, needs 'api' scope

    # Kafka Configuration (used by Discovery Service)
    export KAFKA_BROKERS="kafka1:9092,kafka2:9092" # Required, comma-separated list of Kafka brokers
    export KAFKA_TOPIC_REPOSITORIES="codesearch.repositories" # Optional, defaults to 'codesearch.repositories'
    ```

## Running

### Discovery Service

The Discovery Service scans GitLab projects and publishes information to Kafka.

```bash
python -m discovery_service.main
``` 