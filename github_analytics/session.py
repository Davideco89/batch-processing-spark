"""Shared Spark session settings; the launcher selects the master."""

from pyspark.sql import SparkSession


def create_session(app_name: str) -> SparkSession:
    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    # Launcher properties reach the JVM when the context starts. A Python
    # SparkConf created before that point may be empty and hide explicit flags.
    if not spark.sparkContext.getConf().contains("spark.sql.shuffle.partitions"):
        spark.conf.set("spark.sql.shuffle.partitions", "2")
    return spark
