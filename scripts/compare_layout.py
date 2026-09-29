"""Compare regenerated outputs with explicit read-only historical baselines.

Only the two exact archive URIs may differ in source_file. Every other field,
schema, multiplicity, partition, SQL type/row and CSV byte must remain equal.
"""

import argparse
import hashlib
import json
from pathlib import Path
from datetime import date

from pyspark.sql import functions as F
from github_analytics.paths import RAW_ROOT, PARQUET_ROOT, CSV_ROOT, DATABASE
from github_analytics.session import create_session
from scripts.profile_archive import DATASETS
from scripts.verify_exports import verify_exports


def historical_source_uri(recorded, supplied_uri):
    """Physical relocation must not change the provenance recorded in rows."""
    samples = [row for rows in recorded["samples"].values() for row in rows]
    samples += recorded["rejected_samples"]
    observed = {row["source_file"] for row in samples}
    assert observed == {supplied_uri}, (observed, "historical URI metadata")
    return supplied_uri


def compare(spark, old_root, new_root, old_archive, new_archive, old_uri, day):
    expected_hash = "50f14cf2e96873bb6e3c2b66253c9715dd6f70f5dde6aa2199096f517e54a0a8"
    for path in (Path(old_archive), Path(new_archive)):
        assert path.stat().st_size == 63874867
        assert hashlib.sha256(path.read_bytes()).hexdigest() == expected_hash
    new_uri = Path(new_archive).resolve().as_uri()
    results = {}
    for name in DATASETS:
        old_path, new_path = Path(old_root) / name, Path(new_root) / name
        old = spark.read.option("basePath", str(old_path)).parquet(str(old_path / f"event_date={day}"))
        new = spark.read.option("basePath", str(new_path)).parquet(str(new_path / f"event_date={day}"))
        assert old.schema == new.schema, (name, "schema")
        assert sorted(p.name for p in old_path.glob("event_date=*")) == sorted(p.name for p in new_path.glob("event_date=*")), (name, "partitions")
        if "source_file" in new.columns:
            assert {r.source_file for r in old.select("source_file").distinct().collect()} == {old_uri}
            assert {r.source_file for r in new.select("source_file").distinct().collect()} == {new_uri}
            new = new.withColumn("source_file", F.when(F.col("source_file") == new_uri, F.lit(old_uri)).otherwise(F.col("source_file")))
        old_rows, new_rows = sorted(old.toJSON().collect()), sorted(new.toJSON().collect())
        assert old_rows == new_rows, (name, "full row multiset")
        results[name] = {"rows": len(new_rows), "schema": old.schema.simpleString(),
                         "sha256_rows": hashlib.sha256("\n".join(old_rows).encode()).hexdigest()}
    return {"archive_sha256": expected_hash, "source_uri_mapping": {new_uri: old_uri}, "datasets": results}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True, type=date.fromisoformat)
    parser.add_argument("--historical-parquet", required=True)
    parser.add_argument("--historical-archive", required=True)
    parser.add_argument("--historical-source-uri", required=True,
                        help="Original URI recorded in historical rows, independent of the relocated archive path")
    parser.add_argument("--historical-csv", required=True)
    parser.add_argument("--historical-database", required=True)
    parser.add_argument("--historical-profile", required=True)
    parser.add_argument("--historical-exports", required=True)
    parser.add_argument("--parquet-root", default=PARQUET_ROOT)
    parser.add_argument("--archive", default=f"{RAW_ROOT}/2025-06-01-0.json.gz")
    parser.add_argument("--csv-root", default=CSV_ROOT)
    parser.add_argument("--database", default=DATABASE)
    parser.add_argument("--report", required=True)
    args = parser.parse_args()
    spark = create_session("compare-functional-layout")
    spark.sparkContext.setLogLevel("WARN")
    try:
        recorded = json.loads(Path(args.historical_profile).read_text())
        old_uri = historical_source_uri(recorded, args.historical_source_uri)
        result = compare(spark, args.historical_parquet, args.parquet_root, args.historical_archive, args.archive, old_uri, args.date)
        for name, dataset in result["datasets"].items():
            assert all(dataset[key] == recorded["datasets"][name][key] for key in dataset), (name, "recorded historical report")
        old = verify_exports(args.historical_parquet, args.historical_csv, args.historical_database, args.date)
        new = verify_exports(args.parquet_root, args.csv_root, args.database, args.date)
        recorded_exports = json.loads(Path(args.historical_exports).read_text())
        for key in ("tables", "csv_files"):
            assert old[key] == new[key] == recorded_exports[key], key
        result["exports"] = new
        result["verification"] = "PASS: exact provenance mapping; complete logical contents and recorded reports preserved"
        report = Path(args.report)
        report.parent.mkdir(parents=True, exist_ok=True)
        report.write_text(json.dumps(result, indent=2) + "\n")
        print(result["verification"], flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
