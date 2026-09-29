"""spark-submit entry point for ingestion."""
from github_analytics.runner import main

if __name__ == "__main__":
    main("ingest")
