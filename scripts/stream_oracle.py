"""Independent complete raw oracle: bounded Python memory, SQLite disk grouping.

No pipeline transformation/aggregation is called. Expected full rows are JSON
lines for distributed Spark multiset reconciliation, not sampled records.
"""

import argparse
from collections import Counter
from datetime import date, datetime, timezone
import gzip
import json
from pathlib import Path
import sqlite3
import tempfile
import time

from scripts.acquire_archive import file_hash
from scripts.profile_archive import normalized, CATEGORIES

BUSINESS = ("event_type", "created_at", "repo_name", "actor_login", "org_login", "payload_action",
            "pr_number", "issue_number", "issue_labels", "event_timestamp")


def encoded(row):
    return json.dumps(row, sort_keys=True, separators=(",", ":"))


def build_oracle(raw_root, day, destination):
    destination = Path(destination)
    destination.mkdir(parents=True, exist_ok=True)
    # SQLite random page I/O on a Windows bind is costly. Scratch is disposable;
    # every expected row and the source-hash report remain on the data bind.
    scratch = tempfile.TemporaryDirectory(prefix="gharchive-oracle-")
    database = Path(scratch.name) / "grouping.sqlite"
    connection = sqlite3.connect(database)
    # Derived scratch state is rebuilt after interruption; batched transactions
    # and WAL reduce synchronization without changing the source archives.
    connection.executescript("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA temp_store=FILE; PRAGMA cache_size=-65536; DROP TABLE IF EXISTS candidates; DROP TABLE IF EXISTS metrics; CREATE TABLE candidates(id TEXT, source TEXT, signature TEXT, row TEXT); CREATE TABLE metrics(repo TEXT, actor TEXT, type TEXT, hour INTEGER, bot INTEGER);")
    reasons, types, groups = Counter(), Counter(), Counter()
    total, accepted, minimum, maximum = 0, 0, None, None
    inputs = []
    started = time.monotonic()
    try:
        with (destination / "ingested.json").open("w", buffering=1024 * 1024) as ingested, (destination / "rejected.json").open("w", buffering=1024 * 1024) as rejected:
            batch = []
            for archive in sorted(Path(raw_root).glob(f"{day}-*.json.gz")):
                source = archive.resolve().as_uri()
                lines = 0
                with gzip.open(archive, "rt", encoding="utf-8") as stream:
                    for line in stream:
                        if not line.strip():
                            continue
                        corrupt = None
                        try:
                            event = json.loads(line)
                        except json.JSONDecodeError:
                            corrupt, event = line.rstrip("\n"), {}
                        row = normalized(event, source, day)
                        row["corrupt_record"] = corrupt
                        ingested.write(encoded(row) + "\n")
                        total += 1
                        lines += 1
                        if corrupt is None:
                            types[row["event_type"]] += 1
                        for key in ("event_id", "event_type", "repo_name", "actor_login"):
                            row[key] = row[key].strip() if row[key] is not None else None
                        try:
                            stamp = datetime.fromisoformat(row["created_at"].replace("Z", "+00:00"))
                            if stamp.tzinfo is None:
                                raise ValueError("Timezone required")
                            stamp = stamp.astimezone(timezone.utc)
                            text = stamp.isoformat()
                            minimum = min(minimum, text) if minimum else text
                            maximum = max(maximum, text) if maximum else text
                        except (ValueError, TypeError, AttributeError):
                            stamp = None
                        row["event_timestamp"] = str(stamp.replace(tzinfo=None)) if stamp else None
                        missing = next((key for key in ("event_id", "event_type", "repo_name", "actor_login") if not row[key]), None)
                        reason = ("corrupt_json" if corrupt is not None else f"missing_{missing}" if missing
                                  else "invalid_timestamp" if stamp is None else "unsupported_event_type" if row["event_type"] not in CATEGORIES
                                  else "outside_date" if stamp.date() != day else None)
                        if reason:
                            reasons[reason] += 1
                            rejected.write(encoded(dict(row, rejection_reason=reason)) + "\n")
                        else:
                            batch.append((row["event_id"], source, encoded({key: row[key] for key in BUSINESS}), encoded(row)))
                            if len(batch) >= 5000:
                                connection.executemany("INSERT INTO candidates VALUES (?,?,?,?)", batch)
                                connection.commit()
                                batch.clear()
                inputs.append({"filename": archive.name, "compressed_bytes": archive.stat().st_size,
                               "sha256": file_hash(archive), "rows": lines, "source_file": source})
                print(f"Oracle parsed {archive.name}: rows={lines}; elapsed={time.monotonic() - started:.3f}s", flush=True)
            if not inputs:
                raise ValueError("No matching hourly archives")
            connection.executemany("INSERT INTO candidates VALUES (?,?,?,?)", batch)
            connection.commit()
            connection.execute("CREATE INDEX candidates_by_id_source ON candidates(id,source)")
            query = "SELECT row, min(signature) OVER(PARTITION BY id), max(signature) OVER(PARTITION BY id), row_number() OVER(PARTITION BY id ORDER BY source) FROM candidates"
            metric_batch = []
            with (destination / "clean.json").open("w", buffering=1024 * 1024) as clean:
                for serialized, least, greatest, copy in connection.execute(query):
                    row = json.loads(serialized)
                    reason = "conflicting_event_id" if least != greatest else "duplicate_event_id" if copy > 1 else None
                    if reason:
                        reasons[reason] += 1
                        rejected.write(encoded(dict(row, rejection_reason=reason)) + "\n")
                    else:
                        row.pop("corrupt_record")
                        hour = datetime.fromisoformat(row["event_timestamp"]).hour
                        bot = row["actor_login"].lower().endswith("[bot]")
                        row.update(event_hour=hour, is_bot=bot, event_category=CATEGORIES[row["event_type"]])
                        clean.write(encoded(row) + "\n")
                        accepted += 1
                        groups[(row["event_type"], hour, bot)] += 1
                        metric_batch.append((row["repo_name"], row["actor_login"], row["event_type"], hour, int(bot)))
                        if len(metric_batch) >= 5000:
                            connection.executemany("INSERT INTO metrics VALUES (?,?,?,?,?)", metric_batch)
                            metric_batch.clear()
                connection.executemany("INSERT INTO metrics VALUES (?,?,?,?,?)", metric_batch)
                connection.commit()
        metrics = {"event_counts": [dict(event_date=str(day), event_type=kind, event_hour=hour, is_bot=bot, event_count=count)
                                    for (kind, hour, bot), count in groups.items()],
                   "daily_volume": [dict(event_date=str(day), event_count=accepted)] if accepted else [],
                   "rejection_counts": [dict(event_date=str(day), rejection_reason=reason, event_count=count)
                                        for reason, count in sorted(reasons.items())]}
        for dataset, key, column in (("top_repositories", "repo_name", "repo"), ("top_actors", "actor_login", "actor")):
            rankings = connection.execute(f"SELECT {column},count(*) AS n FROM metrics GROUP BY {column} ORDER BY n DESC,{column} LIMIT 10")
            metrics[dataset] = [dict(event_date=str(day), **{key: entity}, event_count=count, rank=rank)
                                for rank, (entity, count) in enumerate(rankings, 1)]
        for dataset, rows in metrics.items():
            (destination / f"{dataset}.json").write_text("".join(encoded(row) + "\n" for row in rows))
        connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        result = {"date": str(day), "inputs": inputs, "raw_lines": total, "accepted": accepted,
                  "rejected": sum(reasons.values()), "rejection_reasons": dict(reasons), "raw_types": dict(types),
                  "raw_timestamp_min_utc": minimum, "raw_timestamp_max_utc": maximum,
                  "elapsed_seconds": time.monotonic() - started, "sqlite_bytes": database.stat().st_size,
                  "sqlite_storage": "Disposable container filesystem; persistent full expected rows and report on data bind",
                  "verification": "Independent streaming JSON/datetime oracle; disk-backed dedup and rankings; all rows represented"}
        assert total == accepted + result["rejected"]
        (destination / "report.json").write_text(json.dumps(result, indent=2) + "\n")
        print(json.dumps({key: result[key] for key in ("raw_lines", "accepted", "rejected", "rejection_reasons", "elapsed_seconds")}), flush=True)
        return result
    finally:
        connection.close()
        scratch.cleanup()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True, type=date.fromisoformat)
    parser.add_argument("--raw-root", default="/data/raw/gharchive")
    parser.add_argument("--destination", required=True)
    args = parser.parse_args()
    build_oracle(args.raw_root, args.date, args.destination)
