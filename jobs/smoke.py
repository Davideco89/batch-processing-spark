"""Exercise Spark and Parquet using one synthetic row, without raw data."""

from pyspark.sql import functions as F

from github_analytics.config import parse_config
from github_analytics.session import create_session
from github_analytics.storage import write_partitioned


def main() -> None:
    config = parse_config()
    spark = create_session("github-events-smoke")
    try:
        frame = spark.range(1).select(
            F.lit("synthetic").alias("event_type"),
            F.lit(config.event_date).alias("event_date"),
        )
        write_partitioned(frame, config.output)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
