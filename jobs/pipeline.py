"""spark-submit entry point for the full batch chain."""
from github_analytics.runner import main

if __name__ == "__main__":
    main("pipeline")
