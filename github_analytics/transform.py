"""Validate events and derive UTC analytics dimensions without Python UDFs."""

from pyspark.sql import Window, functions as F

CATEGORIES = {"PushEvent": "content", "PullRequestEvent": "collaboration",
              "IssuesEvent": "collaboration", "WatchEvent": "passive"}

# Preserve every normalized business field, including the source timestamp text
# and ordered labels. Source-file provenance does not make a business conflict.
BUSINESS_FIELDS = ("event_type", "created_at", "repo_name", "actor_login", "org_login",
                   "payload_action", "pr_number", "issue_number", "issue_labels", "event_timestamp")


def deduplicate_valid(frame):
    """One valid event ID per requested date; quarantine conflicting contents.

    All conflicting copies are rejected. For identical copies, retain the smallest
    source URI; ties have identical persisted rows, so physical row order is immaterial.
    Invalid copies never enter this function and retain their quality reasons.
    """
    group = Window.partitionBy("event_date", "event_id")
    business = F.struct(*BUSINESS_FIELDS)
    tagged = frame.withColumn("_hash", F.xxhash64(business))
    tagged = (tagged.withColumn("_copies", F.count(F.lit(1)).over(group))
              .withColumn("_hash_min", F.min("_hash").over(group))
              .withColumn("_hash_max", F.max("_hash").over(group)))
    # Hash inequality proves a conflict. Equality never proves business equality:
    # serialize only the duplicate candidate groups requiring an exact check.
    candidates = (F.col("_copies") > 1) & (F.col("_hash_min") == F.col("_hash_max"))
    tagged = tagged.withColumn("_business", F.when(
        candidates, F.to_json(business, {"ignoreNullFields": "false"})))
    conflict = ((F.col("_hash_min") != F.col("_hash_max")) |
                (F.min("_business").over(group) != F.max("_business").over(group)))
    tagged = (tagged.withColumn("_conflict", F.coalesce(conflict, F.lit(False)))
              .withColumn("_copy", F.row_number().over(group.orderBy(F.col("source_file").asc_nulls_last()))))
    return (tagged.withColumn("rejection_reason",
                             F.when(F.col("_conflict"), "conflicting_event_id")
                              .when(F.col("_copy") > 1, "duplicate_event_id"))
            .drop("_hash", "_copies", "_hash_min", "_hash_max", "_business", "_conflict", "_copy"))


def transform(frame, event_date):
    for name in ("event_id", "event_type", "repo_name", "actor_login"):
        frame = frame.withColumn(name, F.trim(F.col(name)))
    frame = frame.withColumn("event_timestamp", F.try_to_timestamp(
        "created_at", F.lit("yyyy-MM-dd'T'HH:mm:ss[.SSSSSS]XXX")))
    reason = F.when(F.col("corrupt_record").isNotNull(), "corrupt_json")
    for name in ("event_id", "event_type", "repo_name", "actor_login"):
        reason = reason.when(F.col(name).isNull() | (F.col(name) == ""), f"missing_{name}")
    reason = (reason.when(F.col("event_timestamp").isNull(), "invalid_timestamp")
              .when(~F.col("event_type").isin(list(CATEGORIES)), "unsupported_event_type")
              .when(F.to_date("event_timestamp") != F.lit(event_date), "outside_date"))
    classified = frame.withColumn("rejection_reason", reason)
    quality_rejected = classified.filter("rejection_reason IS NOT NULL")
    valid = deduplicate_valid(classified.filter("rejection_reason IS NULL"))
    rejected = quality_rejected.unionByName(valid.filter("rejection_reason IS NOT NULL"))
    mapping = F.create_map(*[F.lit(v) for pair in CATEGORIES.items() for v in pair])
    clean = (valid.filter("rejection_reason IS NULL")
             .drop("rejection_reason", "corrupt_record")
             .withColumn("event_date", F.to_date("event_timestamp"))
             .withColumn("event_hour", F.hour("event_timestamp"))
             .withColumn("is_bot", F.lower("actor_login").endswith("[bot]")))
    return clean.withColumn("event_category", mapping[F.col("event_type")]), rejected
