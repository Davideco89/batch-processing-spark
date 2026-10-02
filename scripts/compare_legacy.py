"""Read-only, exact comparison with an explicit pre-publication Parquet baseline.

This tool does not import or attest legacy data. Normal production readers keep
refusing legacy partitions. Only direct files in the named historical date are
read for a distributed comparison; source_file is compared without rewriting.
"""

import argparse
from datetime import date
import json
from pathlib import Path

from pyspark.sql import functions as F

from github_analytics.session import create_session
from github_analytics.storage import read_date
from scripts.profile_archive import DATASETS
from scripts.validate_parquet import equal_rows


def compare(spark, legacy_root, published_root, day):
    result = {}
    for name in DATASETS:
        if name == "rejection_counts":
            continue  # Introduced later; validated against the extended raw oracle.
        directory = Path(legacy_root) / name / f"event_date={day}"
        if (directory / "_publication.json").exists() or any(p.is_dir() for p in directory.iterdir()):
            raise ValueError("Expected an explicit direct-file legacy partition")
        files = sorted(directory.glob("*.parquet"))
        if not files or not (directory / "_SUCCESS").is_file():
            raise ValueError("Legacy baseline is missing files or its completion marker")
        old = spark.read.parquet(*map(str, files)).withColumn("event_date", F.lit(day))
        current = read_date(spark, Path(published_root) / name, day)
        equal_rows(old, current, name)
        result[name] = {"rows": current.count(), "schema": current.schema.simpleString(),
                        "legacy_files": len(files), "legacy_bytes": sum(p.stat().st_size for p in files),
                        "comparison": "Exact schema and bidirectional full-row multiset; provenance unchanged"}
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True, type=date.fromisoformat)
    parser.add_argument("--legacy-root", required=True)
    parser.add_argument("--published-root", required=True)
    parser.add_argument("--report", required=True)
    args = parser.parse_args()
    if Path(args.legacy_root).resolve() == Path(args.published_root).resolve():
        parser.error("Legacy and regenerated output roots must differ")
    spark = create_session("compare-explicit-legacy-baseline")
    spark.sparkContext.setLogLevel("WARN")
    try:
        result = compare(spark, args.legacy_root, args.published_root, args.date)
        Path(args.report).write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
        print("PASS: all seven legacy schemas and complete rows; no import, attestation or source URI rewrite", flush=True)
    finally:
        spark.stop()
