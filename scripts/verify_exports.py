"""Independently compare Parquet metrics with CSV and read-only DuckDB rows."""

from github_analytics.paths import PARQUET_ROOT, CSV_ROOT, DATABASE

import argparse
from collections import Counter
import csv
from datetime import date
import hashlib
import json
from pathlib import Path

import duckdb
from github_analytics.publication import published_partition

EXPECTED = {
    "event_counts": [("event_date", "DATE"), ("event_type", "VARCHAR"), ("event_hour", "INTEGER"), ("is_bot", "BOOLEAN"), ("event_count", "BIGINT")],
    "daily_volume": [("event_date", "DATE"), ("event_count", "BIGINT")],
    "top_repositories": [("event_date", "DATE"), ("rank", "INTEGER"), ("repo_name", "VARCHAR"), ("event_count", "BIGINT")],
    "top_actors": [("event_date", "DATE"), ("rank", "INTEGER"), ("actor_login", "VARCHAR"), ("event_count", "BIGINT")],
}


def canonical(row):
    return json.dumps(row, default=str, separators=(",", ":"))


def row_hash(rows):
    return hashlib.sha256("\n".join(sorted(map(canonical, rows))).encode()).hexdigest()


def verify_exports(parquet_root, csv_root, database, day):
    result = {"date": str(day), "duckdb_version": duckdb.__version__, "tables": {}, "csv_files": {}}
    with duckdb.connect(config={"threads": 2}) as source, duckdb.connect(str(database), read_only=True, config={"threads": 2}) as target:
        for name, fields in EXPECTED.items():
            files, _ = published_partition(Path(parquet_root) / name, day)
            columns = ",".join(column for column, _ in fields)
            relation = "read_parquet(?, hive_partitioning=true)"
            physical = {row[0]: row[1] for row in source.execute(f"DESCRIBE SELECT * FROM {relation}", [files]).fetchall()}
            assert physical == dict(fields), (name, "Parquet schema", physical)
            expected = source.execute(f"SELECT {columns} FROM {relation}", [files]).fetchall()
            schema = [(row[0], row[1]) for row in target.execute(f"DESCRIBE {name}").fetchall()]
            assert schema == fields, (name, "DuckDB schema", schema)
            actual = target.execute(f"SELECT {columns} FROM {name} WHERE event_date = ?", [day]).fetchall()
            assert Counter(map(canonical, actual)) == Counter(map(canonical, expected)), (name, "SQL rows differ")
            assert all(row[0] == day and all(value is not None for value in row) for row in actual), name
            all_rows = target.execute(f"SELECT {columns} FROM {name}").fetchall()
            result["tables"][name] = {"schema": [[column, kind] for column, kind in fields], "rows": len(actual), "sha256_rows": row_hash(actual),
                                      "all_dates_rows": len(all_rows), "sha256_all_dates_rows": row_hash(all_rows)}
            if name in ("top_repositories", "top_actors"):
                path = Path(csv_root) / f"event_date={day}" / f"{name}.csv"
                with path.open(encoding="utf-8", newline="") as stream:
                    reader = csv.reader(stream)
                    assert next(reader) == [column for column, _ in fields], (name, "CSV header")
                    csv_rows = list(reader)
                wanted = [[str(value) for value in row] for row in sorted(expected, key=lambda row: row[1])]
                assert csv_rows == wanted, (name, "CSV rows/order differ")
        for path in sorted(Path(csv_root).glob("event_date=*/*.csv")):
            result["csv_files"][str(path.relative_to(csv_root))] = hashlib.sha256(path.read_bytes()).hexdigest()
    result["verification"] = "PASS: Parquet, SQL schemas/full rows and CSV headers/values/order"
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True, type=date.fromisoformat)
    parser.add_argument("--parquet-root", default=PARQUET_ROOT)
    parser.add_argument("--csv-root", default=CSV_ROOT)
    parser.add_argument("--database", default=DATABASE)
    parser.add_argument("--report", required=True)
    parser.add_argument("--compare")
    args = parser.parse_args()
    result = verify_exports(args.parquet_root, args.csv_root, args.database, args.date)
    if args.compare:
        previous = json.loads(Path(args.compare).read_text())
        # Database binary bytes may change; compare logical full rows for every date.
        assert result["tables"] == previous["tables"], "SQL schemas/rows or other dates changed"
        assert result["csv_files"] == previous["csv_files"], "CSV files or other dates changed"
        result["rerun"] = "PASS: logical SQL contents/types and CSV bytes unchanged, including other dates"
    report = Path(args.report)
    report.parent.mkdir(parents=True, exist_ok=True)
    report.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2), flush=True)


if __name__ == "__main__":
    main()
