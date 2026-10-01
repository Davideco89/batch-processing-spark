"""Export existing aggregate Parquet; do not recompute pipeline metrics.

DuckDB replaces all five date slices in one transaction. CSV replacement is
atomic per file, not across both files or the database; rerun after interruption.
Local single-writer usage only.
"""

from github_analytics.paths import PARQUET_ROOT, CSV_ROOT, DATABASE

import argparse
import csv
from datetime import date
import os
from pathlib import Path
import tempfile

import duckdb
from github_analytics.publication import published_partition
from github_analytics.quality import REJECTION_REASONS

SCHEMAS = {
    "event_counts": {"event_date": "DATE", "event_type": "VARCHAR", "event_hour": "INTEGER", "is_bot": "BOOLEAN", "event_count": "BIGINT"},
    "daily_volume": {"event_date": "DATE", "event_count": "BIGINT"},
    "top_repositories": {"event_date": "DATE", "rank": "INTEGER", "repo_name": "VARCHAR", "event_count": "BIGINT"},
    "top_actors": {"event_date": "DATE", "rank": "INTEGER", "actor_login": "VARCHAR", "event_count": "BIGINT"},
    "rejection_counts": {"event_date": "DATE", "rejection_reason": "VARCHAR", "event_count": "BIGINT"},
}
GRAINS = {"event_counts": "event_date,event_type,event_hour,is_bot", "daily_volume": "event_date",
          "top_repositories": "event_date,rank", "top_actors": "event_date,rank",
          "rejection_counts": "event_date,rejection_reason"}
RANKINGS = ("top_repositories", "top_actors")


def load_partition(connection, name, parquet_root, day):
    """Validate physical schemas before reading a requested-date partition."""
    files, _ = published_partition(Path(parquet_root) / name, day)
    expected = SCHEMAS[name]
    physical = None
    for path in files:
        fields = {row[0]: row[1] for row in connection.execute(
            "DESCRIBE SELECT * FROM read_parquet(?, hive_partitioning=false)", [path]).fetchall()}
        if fields not in (expected, {k: v for k, v in expected.items() if k != "event_date"}):
            raise ValueError(f"Unexpected Parquet schema for {name}: {fields}")
        if physical is not None and fields != physical:
            raise ValueError(f"Mixed physical schemas for {name}")
        physical = fields
    columns = ",".join(expected)
    projection = columns if "event_date" in physical else "CAST(? AS DATE) AS event_date," + ",".join(k for k in expected if k != "event_date")
    parameters = [files] if "event_date" in physical else [day, files]
    table = f"incoming_{name}"
    connection.execute(f"CREATE OR REPLACE TEMP TABLE {table} AS SELECT {projection} FROM read_parquet(?, hive_partitioning=false)", parameters)
    nulls = " OR ".join(f"{column} IS NULL" for column in expected)
    invalid = connection.execute(f"SELECT count(*) FROM {table} WHERE {nulls} OR event_date <> ? OR event_count <= 0", [day]).fetchone()[0]
    if invalid:
        raise ValueError(f"Invalid date, null or event_count in {name}")
    if connection.execute(f"SELECT count(*) FROM (SELECT {GRAINS[name]} FROM {table} GROUP BY {GRAINS[name]} HAVING count(*) > 1)").fetchone()[0]:
        raise ValueError(f"Duplicate aggregate grain in {name}")
    if name == "event_counts" and connection.execute(f"SELECT count(*) FROM {table} WHERE event_hour NOT BETWEEN 0 AND 23 OR trim(event_type) = ''").fetchone()[0]:
        raise ValueError("Invalid event dimensions")
    if name == "rejection_counts":
        placeholders = ",".join("?" for _ in REJECTION_REASONS)
        if connection.execute(f"SELECT count(*) FROM {table} WHERE rejection_reason NOT IN ({placeholders})", REJECTION_REASONS).fetchone()[0]:
            raise ValueError("Invalid rejection reason")
    if name in RANKINGS:
        entity = "repo_name" if name == "top_repositories" else "actor_login"
        rows = connection.execute(f"SELECT rank,{entity},event_count FROM {table} ORDER BY rank").fetchall()
        if ([row[0] for row in rows] != list(range(1, len(rows) + 1))
                or len({row[1] for row in rows}) != len(rows)
                or any(not row[1].strip() for row in rows)
                or rows != sorted(rows, key=lambda row: (-row[2], row[1]))):
            raise ValueError(f"Invalid deterministic ranking in {name}")
    return table


def write_csv(target, columns, rows):
    target = Path(target)
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", newline="", dir=target.parent,
                                         prefix=f".{target.name}.", suffix=".tmp", delete=False) as stream:
            temporary = Path(stream.name)
            writer = csv.writer(stream, lineterminator="\n")
            writer.writerow(columns)
            writer.writerows(rows)
        os.replace(temporary, target)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def export_date(parquet_root, day, csv_root=None, database=None):
    if csv_root is None and database is None:
        raise ValueError("Choose a CSV or DuckDB destination")
    if database is not None:
        Path(database).parent.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(str(database) if database is not None else ":memory:", config={"threads": 2})
    pending_csv, counts = {}, {}
    try:
        connection.execute("BEGIN TRANSACTION")
        try:
            for name, fields in SCHEMAS.items():
                incoming = load_partition(connection, name, parquet_root, day)
                counts[name] = connection.execute(f"SELECT count(*) FROM {incoming}").fetchone()[0]
                if database is not None:
                    definition = ",".join(f"{column} {kind} NOT NULL" for column, kind in fields.items())
                    connection.execute(f"CREATE TABLE IF NOT EXISTS {name} ({definition}, PRIMARY KEY ({GRAINS[name]}))")
                    actual = [(row[0], row[1]) for row in connection.execute(f"DESCRIBE {name}").fetchall()]
                    if actual != list(fields.items()):
                        raise ValueError(f"Unexpected target schema for {name}: {actual}")
                    connection.execute(f"DELETE FROM {name} WHERE event_date = ?", [day])
                    connection.execute(f"INSERT INTO {name} SELECT * FROM {incoming}")
                if csv_root is not None and name in RANKINGS:
                    pending_csv[name] = connection.execute(f"SELECT * FROM {incoming} ORDER BY rank").fetchall()
            connection.execute("COMMIT")
        except Exception:
            connection.execute("ROLLBACK")
            raise
    finally:
        connection.close()
    for name, rows in pending_csv.items():
        write_csv(Path(csv_root) / f"event_date={day}" / f"{name}.csv", list(SCHEMAS[name]), rows)
    return counts


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True, type=date.fromisoformat)
    parser.add_argument("--parquet-root", default=PARQUET_ROOT)
    parser.add_argument("--mode", choices=("csv", "duckdb", "both"), default="both")
    parser.add_argument("--csv-root")
    parser.add_argument("--database")
    args = parser.parse_args()
    if not args.parquet_root.strip() or args.csv_root == "" or args.database == "":
        parser.error("Paths must not be empty")
    csv_root = (args.csv_root or CSV_ROOT) if args.mode in ("csv", "both") else None
    database = (args.database or DATABASE) if args.mode in ("duckdb", "both") else None
    counts = export_date(args.parquet_root, args.date, csv_root, database)
    print(f"Exported {args.date}: {counts}; csv={csv_root}; database={database}; DuckDB={duckdb.__version__}", flush=True)
