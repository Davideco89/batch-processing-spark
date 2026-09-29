"""Read hourly GH Archive gzip files using an explicit nested schema.

The official crawler writes one encoded event per line:
https://github.com/igrigorik/gharchive.org/blob/master/crawler/crawler.rb
"""

from pathlib import Path
from pyspark.sql import functions as F, types as T


EVENT_SCHEMA = T.StructType([
    T.StructField("id", T.StringType()),
    T.StructField("type", T.StringType()),
    T.StructField("created_at", T.StringType()),
    T.StructField("repo", T.StructType([T.StructField("name", T.StringType())])),
    T.StructField("actor", T.StructType([T.StructField("login", T.StringType())])),
    T.StructField("org", T.StructType([T.StructField("login", T.StringType())])),
    T.StructField("payload", T.StructType([
        T.StructField("action", T.StringType()),
        T.StructField("number", T.LongType()),
        T.StructField("pull_request", T.StructType([T.StructField("number", T.LongType())])),
        T.StructField("issue", T.StructType([
            T.StructField("number", T.LongType()),
            T.StructField("labels", T.ArrayType(T.StructType([
                T.StructField("name", T.StringType())
            ]))),
        ])),
    ])),
    T.StructField("_corrupt_record", T.StringType()),
])


def ingest(spark, raw_root, event_date):
    paths = sorted(str(p) for p in Path(raw_root).glob(f"{event_date}-*.json.gz"))
    if not paths:
        raise FileNotFoundError(f"No hourly archives for {event_date} in {raw_root}")
    raw = (spark.read.schema(EVENT_SCHEMA).option("multiLine", "false")
           .option("mode", "PERMISSIVE").json(paths))
    return raw.select(
        F.col("id").alias("event_id"), F.col("type").alias("event_type"),
        "created_at", F.col("repo.name").alias("repo_name"),
        F.col("actor.login").alias("actor_login"),
        F.col("org.login").alias("org_login"),
        F.col("payload.action").alias("payload_action"),
        F.when(F.col("type") == "PullRequestEvent",
               F.coalesce("payload.pull_request.number", "payload.number")).alias("pr_number"),
        F.col("payload.issue.number").alias("issue_number"),
        F.col("payload.issue.labels.name").alias("issue_labels"),
        F.col("_corrupt_record").alias("corrupt_record"),
        F.input_file_name().alias("source_file"),
        F.lit(event_date).alias("event_date"),
    )
