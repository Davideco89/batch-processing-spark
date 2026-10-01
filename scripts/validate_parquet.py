"""Distributed exact full-row comparisons and compact Parquet quality reports."""

import argparse
from datetime import date
import json
from pathlib import Path
import time

from pyspark import StorageLevel
from pyspark.sql import functions as F, types as T
from github_analytics.session import create_session
from github_analytics.paths import PARQUET_ROOT
from github_analytics.storage import read_date
from github_analytics.publication import published_partition
from scripts.profile_archive import DATASETS, SCHEMAS


def read_partition(spark, root, name, day):
    return read_date(spark, Path(root) / name, day)


def equal_rows(left, right, name):
    left_types = {field.name: field.dataType.simpleString() for field in left.schema}
    right_types = {field.name: field.dataType.simpleString() for field in right.schema}
    assert left_types == right_types == SCHEMAS[name], (name, "schema")
    columns = sorted(left.columns)
    left, right = left.select(columns), right.select(columns)
    assert left.exceptAll(right).limit(1).count() == 0, (name, "unexpected rows or multiplicity")
    assert right.exceptAll(left).limit(1).count() == 0, (name, "missing rows or multiplicity")


def validate_sources(oracle_report, manifest):
    assert manifest["complete"] and manifest["date"] == oracle_report["date"], "Incomplete or wrong-date acquisition"
    actual = {item["filename"]: (item["compressed_bytes"], item["sha256"]) for item in oracle_report["inputs"]}
    planned = {item["url"].rsplit("/", 1)[1]: (item["compressed_bytes"], item["sha256"]) for item in manifest["completed"]}
    assert actual == planned, "Source coverage, sizes or hashes differ from acquisition"
    assert len(actual) == len(manifest["hours"]), "Acquisition hour count differs"


def validate(spark, root, day, oracle=None, compare_root=None, manifest=None):
    started, frames, result = time.monotonic(), {}, {"date": str(day), "datasets": {}}
    try:
        for name in DATASETS:
            frame = read_partition(spark, root, name, day).persist(StorageLevel.DISK_ONLY)
            frames[name] = frame
            assert {field.name: field.dataType.simpleString() for field in frame.schema} == SCHEMAS[name], (name, "schema")
            assert not frame.filter(F.col("event_date").isNull() | (F.col("event_date") != F.lit(day))).limit(1).count(), (name, "partition date")
            files = [Path(path) for path in published_partition(Path(root) / name, day)[0]]
            result["datasets"][name] = {"rows": frame.count(), "schema": frame.schema.simpleString(),
                                        "files": len(files), "bytes": sum(path.stat().st_size for path in files),
                                        "minimum_file_bytes": min(path.stat().st_size for path in files),
                                        "maximum_file_bytes": max(path.stat().st_size for path in files)}
            if oracle:
                fields = [T.StructField(key, T._parse_datatype_string(kind)) for key, kind in SCHEMAS[name].items()]
                expected = spark.read.schema(T.StructType(fields)).option("timestampFormat", "yyyy-MM-dd HH:mm:ss[.SSSSSS]").json(str(Path(oracle) / f"{name}.json"))
                equal_rows(frame, expected, name)
            if compare_root:
                equal_rows(frame, read_partition(spark, compare_root, name, day), name)
        counts = {name: value["rows"] for name, value in result["datasets"].items()}
        assert counts["ingested"] == counts["clean"] + counts["rejected"]
        clean = frames["clean"]
        critical = ("event_id", "event_type", "repo_name", "actor_login", "event_timestamp", "event_date", "event_hour", "is_bot", "event_category")
        nulls = clean.agg(*[F.coalesce(F.sum(F.col(key).isNull().cast("long")), F.lit(0)).alias(key) for key in critical]).first().asDict()
        assert not any(value for value in nulls.values()), nulls
        result["critical_nulls"] = nulls
        assert clean.select("event_id").distinct().count() == counts["clean"], "Clean ID duplicates"
        result["rejection_reasons"] = {row.rejection_reason: row["count"] for row in frames["rejected"].groupBy("rejection_reason").count().collect()}
        result["accepted_types"] = {row.event_type: row["count"] for row in clean.groupBy("event_type").count().collect()}
        result["event_hours"] = {str(row.event_hour): row["count"] for row in clean.groupBy("event_hour").count().collect()}
        result["bots"] = {str(row.is_bot): row["count"] for row in clean.groupBy("is_bot").count().collect()}
        result["optional_nulls"] = clean.agg(*[F.sum(F.col(key).isNull().cast("long")).alias(key) for key in ("org_login", "pr_number", "issue_number", "issue_labels")]).first().asDict()
        assert (frames["daily_volume"].agg(F.sum("event_count")).first()[0] or 0) == counts["clean"]
        assert (frames["event_counts"].agg(F.sum("event_count")).first()[0] or 0) == counts["clean"]
        if oracle:
            expected = json.loads((Path(oracle) / "report.json").read_text())
            if manifest:
                validate_sources(expected, json.loads(Path(manifest).read_text()))
                result["input_coverage"] = {"files": len(expected["inputs"]), "hours": json.loads(Path(manifest).read_text())["hours"],
                                            "verification": "PASS: every archive size and SHA256 matches the completed acquisition"}
            assert expected["raw_lines"] == counts["ingested"] and expected["accepted"] == counts["clean"] and expected["rejected"] == counts["rejected"]
            assert expected["rejection_reasons"] == result["rejection_reasons"]
        result.update(elapsed_seconds=time.monotonic() - started,
                      shuffle_partitions=spark.conf.get("spark.sql.shuffle.partitions"),
                      verification="PASS: schemas, dates, unique clean IDs, nulls, counts, exact full-row multiset comparisons in both directions")
        return result
    finally:
        for frame in frames.values():
            frame.unpersist()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True, type=date.fromisoformat)
    parser.add_argument("--output-root", default=PARQUET_ROOT)
    parser.add_argument("--oracle")
    parser.add_argument("--compare-root")
    parser.add_argument("--manifest", help="Require independent oracle source coverage/hashes to match completed acquisition")
    parser.add_argument("--report", required=True)
    args = parser.parse_args()
    if not args.oracle and not args.compare_root:
        parser.error("Choose an independent oracle or comparison root")
    spark = create_session("validate-complete-parquet")
    spark.sparkContext.setLogLevel("WARN")
    try:
        if args.manifest and not args.oracle:
            parser.error("--manifest requires --oracle")
        result = validate(spark, args.output_root, args.date, args.oracle, args.compare_root, args.manifest)
        report = Path(args.report)
        report.parent.mkdir(parents=True, exist_ok=True)
        report.write_text(json.dumps(result, indent=2) + "\n")
        print(json.dumps(result, indent=2), flush=True)
    finally:
        spark.stop()
