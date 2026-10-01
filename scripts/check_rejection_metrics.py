"""Isolated bind-mount rejection metric gates, restart and empty-date checks.

This autonomous experiment uses generated fixtures only. It never changes
historical outputs and requires a fresh directory under the named test root.
"""

import argparse
from datetime import date
import json
from pathlib import Path

from github_analytics.export import export_date
from github_analytics.publication import PublicationError, publish, recover_partition, sha256
from github_analytics.runner import run_stage
from github_analytics.session import create_session
from github_analytics.storage import read_date, read_dates
from scripts.profile_archive import DATASETS, profile
from scripts.stream_oracle import build_oracle
from scripts.validate_parquet import validate
from scripts.verify_exports import verify_exports
from scripts.verify_fixture import verify
from tests.fixture_data import event, write_archive, write_fixture

DAY, OTHER = date(2025, 6, 1), date(2025, 6, 2)


def snapshot(paths, root):
    return {str(path.relative_to(root)): sha256(path)
            for parent in paths for path in parent.rglob("*") if path.is_file()}


def interrupted(stage):
    stage.mkdir()
    (stage / "part-incomplete.parquet").write_bytes(b"not a completed parquet file")
    raise RuntimeError("Injected interrupted rejection checkpoint publication")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True)
    args = parser.parse_args()
    root = Path(args.root).resolve()
    allowed = Path("/data/test/rejection-metrics").resolve()
    if root == allowed or allowed not in root.parents or (root.exists() and any(root.iterdir())):
        parser.error("Use a fresh isolated directory under /data/test/rejection-metrics")
    root.mkdir(parents=True, exist_ok=True)
    raw, parquet, csv, database = [root / name for name in ("raw", "parquet", "csv", "analytics.duckdb")]
    spark = create_session("verify-rejection-metrics")
    spark.sparkContext.setLogLevel("WARN")
    report = {"runtime": {"spark": spark.version, "hadoop": spark._jvm.org.apache.hadoop.util.VersionInfo.getVersion(),
                          "master": spark.sparkContext.master, "timezone": spark.conf.get("spark.sql.session.timeZone"),
                          "shuffle_partitions": spark.conf.get("spark.sql.shuffle.partitions")}}
    try:
        write_fixture(raw)
        for day in (DAY, OTHER):
            run_stage(spark, "pipeline", day, raw, parquet)
            export_date(parquet, day, csv, database)
        original = profile(spark, raw, parquet, DAY, 10)
        report["baseline"] = original
        verify(spark, parquet)
        other_paths = [parquet / name / f"event_date={OTHER}" for name in DATASETS]
        other_paths.append(csv / f"event_date={OTHER}")
        other = snapshot(other_paths, root)
        csv_before = snapshot([csv], root)
        exports_before = verify_exports(parquet, csv, database, DAY)

        # Interrupt the new metric while keeping all other fixture inputs valid:
        # each consumer must reject this exact dataset, not a missing dependency.
        target = parquet / "rejection_counts"
        try:
            publish(target, DAY, interrupted)
        except RuntimeError as error:
            report["injection"] = str(error)
        consumers = {
            "Spark date reader": lambda: read_date(spark, target, DAY).count(),
            "Spark dataset reader": lambda: read_dates(spark, target).count(),
            "DuckDB export": lambda: export_date(parquet, DAY, csv, database),
            "export verifier": lambda: verify_exports(parquet, csv, database, DAY),
            "exact Parquet verifier": lambda: validate(spark, parquet, DAY),
            "independent raw oracle": lambda: profile(spark, raw, parquet, DAY, 10),
            "golden fixture verifier": lambda: verify(spark, parquet),
        }
        gates = {}
        assert (target / "_transactions" / str(DAY)).is_dir()
        for name, consumer in consumers.items():
            try:
                consumer()
            except PublicationError as error:
                gates[name] = {"dataset": str(target), "exception": type(error).__name__, "message": str(error)}
            else:
                raise AssertionError(name + " accepted incomplete rejection_counts")
        report["incomplete_metric_gates"] = gates
        assert snapshot([csv], root) == csv_before, "Failed late export changed ranking CSV"
        report["metric_recovery"] = recover_partition(target, DAY)
        assert verify_exports(parquet, csv, database, DAY) == exports_before, "Late export changed committed tables"

        # Aggregate must gate both durable inputs before any metric publication.
        metric_paths = [parquet / name for name in DATASETS[3:]]
        before = snapshot(metric_paths, root)
        try:
            publish(parquet / "rejected", DAY, interrupted)
        except RuntimeError:
            pass
        try:
            run_stage(spark, "aggregate", DAY, "/not-used-no-raw", parquet)
        except PublicationError as error:
            report["rejected_checkpoint_gate"] = str(error)
        else:
            raise AssertionError("Aggregate accepted incomplete rejected checkpoint")
        assert snapshot(metric_paths, root) == before, "Aggregate wrote metrics before both inputs were gated"
        report["checkpoint_recovery"] = recover_partition(parquet / "rejected", DAY)
        run_stage(spark, "aggregate", DAY, "/not-used-no-raw", parquet)
        after = profile(spark, raw, parquet, DAY, 10)
        for name in DATASETS:
            for key in ("rows", "schema", "sha256_rows"):
                assert after["datasets"][name][key] == original["datasets"][name][key], (name, key)
        export_date(parquet, DAY, csv, database)
        assert verify_exports(parquet, csv, database, DAY) == exports_before
        assert snapshot([csv], root) == csv_before
        report["aggregate_only_restart"] = "PASS: all eight business datasets and ranking CSV bytes unchanged"

        # Exercise changed reasons, all rejected, zero rejected, and a true
        # zero-input date using the same date's existing publications.
        cases = [
            ("all_rejected", [event(30, "ForkEvent", "alice", "acme/repo", "2025-06-01T00:00:00Z"),
                               event(31, "WatchEvent", "alice", "acme/repo", "2025-06-02T00:00:00Z")],
             {"unsupported_event_type": 1, "outside_date": 1}),
            ("no_rejected", [event(32, "PushEvent", "alice", "acme/repo", "2025-06-01T00:00:00Z")], {}),
            ("empty_input", [], {}),
        ]
        report["rerun_cases"] = {}
        for label, rows, reasons in cases:
            case_raw = root / "case-input" / label
            write_archive(case_raw / "2025-06-01-0.json.gz", rows)
            write_archive(case_raw / "2025-06-01-1.json.gz", [])
            run_stage(spark, "pipeline", DAY, case_raw, parquet)
            oracle = root / "oracle" / label
            build_oracle(case_raw, DAY, oracle)
            result = validate(spark, parquet, DAY, oracle=oracle)
            assert result["published_rejection_counts"] == reasons
            export_date(parquet, DAY, csv, database)
            result["exports"] = verify_exports(parquet, csv, database, DAY)
            assert snapshot(other_paths, root) == other, "Other date's physical bytes changed"
            report["rerun_cases"][label] = result
        report["other_date_files_unchanged"] = len(other)
        report["verification"] = "PASS: reason metrics, seven consumer gates, rollback, recovery, checkpoint restart, eight schemas, independent oracles and other-date bytes"
        (root / "report.json").write_text(json.dumps(report, indent=2, default=str) + "\n")
        print(json.dumps(report, indent=2, default=str), flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
