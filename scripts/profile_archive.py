"""Independently reconcile a bounded raw archive sample with all seven outputs.

Run with spark-submit. The raw oracle uses Python's JSON and datetime parsers;
it does not call the pipeline's transformations or aggregations. Reports and
full-row multiset fingerprints are generated artifacts under the data mount.
"""

import argparse
from github_analytics.paths import RAW_ROOT, PARQUET_ROOT
from collections import Counter
from datetime import date, datetime, timezone
import gzip
import hashlib
import json
from pathlib import Path

from github_analytics.session import create_session
from github_analytics.storage import read_date
from github_analytics.publication import published_partition

DATASETS = ("ingested", "clean", "rejected", "event_counts", "daily_volume",
            "top_repositories", "top_actors")
BASE_FIELDS = {name: "string" for name in ("event_id", "event_type", "created_at", "repo_name",
               "actor_login", "org_login", "payload_action", "source_file")}
BASE_FIELDS.update(pr_number="bigint", issue_number="bigint", issue_labels="array<string>", event_date="date")
SCHEMAS = {
    "ingested": dict(BASE_FIELDS, corrupt_record="string"),
    "clean": dict(BASE_FIELDS, event_timestamp="timestamp", event_hour="int", is_bot="boolean", event_category="string"),
    "rejected": dict(BASE_FIELDS, corrupt_record="string", event_timestamp="timestamp", rejection_reason="string"),
    "event_counts": {"event_date": "date", "event_type": "string", "event_hour": "int", "is_bot": "boolean", "event_count": "bigint"},
    "daily_volume": {"event_date": "date", "event_count": "bigint"},
    "top_repositories": {"event_date": "date", "repo_name": "string", "event_count": "bigint", "rank": "int"},
    "top_actors": {"event_date": "date", "actor_login": "string", "event_count": "bigint", "rank": "int"},
}
CATEGORIES = {"PushEvent": "content", "PullRequestEvent": "collaboration",
              "IssuesEvent": "collaboration", "WatchEvent": "passive"}


def normalized(event, source_uri, day):
    payload = event.get("payload") or {}
    issue, pr = payload.get("issue") or {}, payload.get("pull_request") or {}
    return {"event_id": event.get("id"), "event_type": event.get("type"),
            "created_at": event.get("created_at"),
            "repo_name": (event.get("repo") or {}).get("name"),
            "actor_login": (event.get("actor") or {}).get("login"),
            "org_login": (event.get("org") or {}).get("login"),
            "payload_action": payload.get("action"),
            "pr_number": (pr.get("number") if pr.get("number") is not None else payload.get("number"))
                         if event.get("type") == "PullRequestEvent" else None,
            "issue_number": issue.get("number"),
            "issue_labels": [label.get("name") for label in issue["labels"]]
                            if issue.get("labels") is not None else None,
            "corrupt_record": None, "source_file": source_uri,
            "event_date": str(day)}


def raw_oracle(raw_root, day):
    inputs, expected, reasons, all_types = [], [], Counter(), Counter()
    ingested, timestamps, rejected_rows, candidates = [], [], [], {}
    for path in sorted(Path(raw_root).glob(f"{day}-*.json.gz")):
        source_uri = path.resolve().as_uri()
        inputs.append({"url": f"https://data.gharchive.org/{path.name}",
                       "filename": path.name, "compressed_bytes": path.stat().st_size,
                       "sha256": hashlib.sha256(path.read_bytes()).hexdigest()})
        with gzip.open(path, "rt", encoding="utf-8") as stream:
            for line in stream:
                if not line.strip():
                    continue
                corrupt = None
                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    # Observed Spark syntax-error recovery clears projected fields.
                    # Other unsupported malformed shapes fail full-row reconciliation.
                    corrupt, event = line.rstrip("\n"), {}
                row = normalized(event, source_uri, day)
                row["corrupt_record"] = corrupt
                ingested.append(row)
                if not corrupt:
                    all_types[row["event_type"]] += 1
                row = dict(row)
                for key in ("event_id", "event_type", "repo_name", "actor_login"):
                    row[key] = row[key].strip() if row[key] is not None else None
                stamp = None
                try:
                    stamp = datetime.fromisoformat(row["created_at"].replace("Z", "+00:00"))
                    if stamp.tzinfo is None:
                        raise ValueError("Timezone required")
                    stamp = stamp.astimezone(timezone.utc)
                    timestamps.append(stamp.isoformat())
                except (ValueError, TypeError, AttributeError):
                    stamp = None
                row["event_timestamp"] = str(stamp.replace(tzinfo=None)) if stamp else None
                missing = next((key for key in ("event_id", "event_type", "repo_name", "actor_login") if not row[key]), None)
                reason = ("corrupt_json" if corrupt is not None else f"missing_{missing}" if missing
                          else "invalid_timestamp" if stamp is None
                          else "unsupported_event_type" if row["event_type"] not in CATEGORIES
                          else "outside_date" if stamp.date() != day else None)
                if reason:
                    rejected_rows.append(dict(row, rejection_reason=reason))
                else:
                    candidates.setdefault(row["event_id"], []).append((row, stamp))
    if not inputs:
        raise ValueError("No matching hourly archives")
    for copies in candidates.values():
        # Independent Python grouping, without using pipeline code or Spark.
        signatures = {canonical({k: v for k, v in row.items()
                                 if k not in ("event_id", "event_date", "source_file", "corrupt_record")})
                      for row, _ in copies}
        for index, (row, stamp) in enumerate(sorted(copies, key=lambda pair: pair[0]["source_file"] or "\uffff")):
            reason = "conflicting_event_id" if len(signatures) > 1 else "duplicate_event_id" if index else None
            if reason:
                rejected_rows.append(dict(row, rejection_reason=reason))
            else:
                row = {k: v for k, v in row.items() if k != "corrupt_record"}
                row.update(event_hour=stamp.hour, is_bot=row["actor_login"].lower().endswith("[bot]"),
                           event_category=CATEGORIES[row["event_type"]])
                expected.append(row)
    reasons.update(row["rejection_reason"] for row in rejected_rows)
    return inputs, len(ingested), expected, reasons, all_types, ingested, timestamps, rejected_rows


def canonical(value):
    return json.dumps(value, sort_keys=True, default=str, separators=(",", ":"))


def profile(spark, raw_root, output_root, day, top_n):
    inputs, raw_lines, expected, reasons, all_types, raw_ingested, timestamps, expected_rejected = raw_oracle(raw_root, day)
    frames, datasets = {}, {}
    for name in DATASETS:
        root = Path(output_root) / name
        partition = root / f"event_date={day}"
        frame = read_date(spark, root, day).cache()
        frames[name] = frame
        assert {field.name: field.dataType.simpleString() for field in frame.schema} == SCHEMAS[name], (name, frame.schema)
        serialized = sorted(frame.toJSON().collect())
        datasets[name] = {"rows": len(serialized), "schema": frame.schema.simpleString(),
                          "sha256_rows": hashlib.sha256("\n".join(serialized).encode()).hexdigest(),
                          "partitions": sorted(p.name for p in root.glob("event_date=*")),
                          "parquet_files": len(published_partition(root, day)[0])}
        assert f"event_date={day}" in datasets[name]["partitions"], name
        assert datasets[name]["parquet_files"] > 0, name
        assert all(str(r.event_date) == str(day) for r in frame.select("event_date").distinct().collect()), name
    try:
        assert datasets["ingested"]["rows"] == raw_lines
        actual_ingested = [r.asDict(recursive=True) for r in frames["ingested"].collect()]
        assert Counter(map(canonical, actual_ingested)) == Counter(map(canonical, raw_ingested)), "Ingested normalized fields differ"
        actual_clean = [r.asDict(recursive=True) for r in frames["clean"].collect()]
        for row in actual_clean:
            row["event_timestamp"] = str(row["event_timestamp"])
        if expected:
            assert Counter(canonical({k: r[k] for k in expected[0]}) for r in actual_clean) == Counter(map(canonical, expected)), "Clean fields differ from raw oracle"
        else:
            assert not actual_clean
        assert datasets["clean"]["rows"] + datasets["rejected"]["rows"] == raw_lines
        actual_reasons = Counter({r.rejection_reason: r["count"] for r in frames["rejected"].groupBy("rejection_reason").count().collect()})
        assert actual_reasons == reasons, (actual_reasons, reasons)
        rejected_rows = [r.asDict(recursive=True) for r in frames["rejected"].collect()]
        for row in rejected_rows:
            row["event_timestamp"] = str(row["event_timestamp"]) if row["event_timestamp"] is not None else None
        assert Counter(map(canonical, rejected_rows)) == Counter(map(canonical, expected_rejected)), "Full rejected row multiset differs"
        assert len({r["event_id"] for r in actual_clean}) == len(actual_clean), "Clean event IDs are not unique"
        groups = Counter((r["event_type"], r["event_hour"], r["is_bot"]) for r in expected)
        actual_groups = {(r.event_type, r.event_hour, r.is_bot): r.event_count for r in frames["event_counts"].collect()}
        assert len(actual_groups) == datasets["event_counts"]["rows"]
        assert actual_groups == groups, "Aggregated dimensions/counts differ"
        assert sum(actual_groups.values()) == len(expected)
        volume = frames["daily_volume"].collect()
        assert ((len(volume) == 1 and volume[0].event_count == len(expected))
                if expected else not volume)
        rankings = {}
        for dataset, key in (("top_repositories", "repo_name"), ("top_actors", "actor_login")):
            counts = Counter(r[key] for r in expected)
            wanted = [(entity, count, rank) for rank, (entity, count) in
                      enumerate(sorted(counts.items(), key=lambda pair: (-pair[1], pair[0]))[:top_n], 1)]
            actual = [(r[key], r.event_count, r.rank) for r in frames[dataset].orderBy("rank").collect()]
            assert actual == wanted, (dataset, actual, wanted)
            rankings[dataset] = actual
        samples = {}
        for kind in CATEGORIES:
            selected = sorted((r for r in actual_clean if r["event_type"] == kind), key=lambda r: r["event_id"])
            samples[kind] = selected[:1]
        return {"date": str(day), "coverage": "Selected hourly files only; not a complete daily total",
                "inputs": inputs, "raw_lines": raw_lines, "raw_types": dict(all_types),
                "accepted": len(expected), "rejected": sum(reasons.values()), "rejection_reasons": dict(reasons),
                "selected_types": dict(Counter(r["event_type"] for r in expected)),
                "event_hours": dict(Counter(r["event_hour"] for r in expected)),
                "bots": dict(Counter(str(r["is_bot"]) for r in expected)),
                "categories": dict(Counter(r["event_category"] for r in expected)),
                "unique_event_ids": len({r["event_id"] for r in expected}),
                "duplicate_event_ids": len(expected) - len({r["event_id"] for r in expected}),
                "raw_timestamp_min_utc": min(timestamps) if timestamps else None,
                "raw_timestamp_max_utc": max(timestamps) if timestamps else None,
                "critical_nulls": {k: sum(r[k] is None or r[k] == "" for r in actual_clean)
                                   for k in ("event_id", "event_type", "repo_name", "actor_login", "event_timestamp", "event_date", "event_hour", "is_bot", "event_category")},
                "null_optional_fields": {k: sum(r[k] is None for r in expected)
                                         for k in ("org_login", "pr_number", "issue_number", "issue_labels")},
                "datasets": datasets, "rankings": rankings, "samples": samples,
                "rejected_samples": [r.asDict(recursive=True) for r in frames["rejected"].orderBy("event_id").limit(3).collect()],
                "reconciliation": "PASS: raw oracle, full clean/rejected row multisets, unique clean IDs, counts, ranks and partitions"}
    finally:
        for frame in frames.values():
            frame.unpersist()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True, type=date.fromisoformat)
    parser.add_argument("--raw-root", default=RAW_ROOT)
    parser.add_argument("--output-root", default=PARQUET_ROOT)
    parser.add_argument("--report", required=True)
    parser.add_argument("--compare")
    parser.add_argument("--top-n", type=int, default=10)
    args = parser.parse_args()
    spark = create_session("profile-real-gh-archive")
    spark.sparkContext.setLogLevel("WARN")
    try:
        result = profile(spark, args.raw_root, args.output_root, args.date, args.top_n)
        if args.compare:
            previous = json.loads(Path(args.compare).read_text())
            assert result["inputs"] == previous["inputs"], "Raw sample changed"
            # File names/counts may change; full-row multiset hashes and schemas must not.
            for name in DATASETS:
                for key in ("rows", "schema", "sha256_rows", "partitions"):
                    assert result["datasets"][name][key] == previous["datasets"][name][key], (name, key)
            result["rerun"] = "PASS: row counts, schemas, partitions and full-row multiset hashes unchanged for seven datasets"
        report = Path(args.report)
        report.parent.mkdir(parents=True, exist_ok=True)
        report.write_text(json.dumps(result, indent=2, default=str) + "\n")
        print(json.dumps({k: result[k] for k in ("raw_lines", "accepted", "rejected", "selected_types", "rejection_reasons", "reconciliation")}, indent=2), flush=True)
        print(result.get("rerun", "Baseline saved") + f"; report={report}", flush=True)
    finally:
        spark.stop()
