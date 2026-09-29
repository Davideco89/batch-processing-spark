"""Assert golden fixture counts and compare full Parquet contents across reruns."""

from github_analytics.paths import FIXTURE_PARQUET_ROOT

import argparse
from datetime import date
import json
from pathlib import Path

from github_analytics.session import create_session
from github_analytics.storage import read_date


def verify(spark, root):
    day = date(2025, 6, 1)
    expected = {"ingested": 13, "clean": 8, "rejected": 5, "event_counts": 6,
                "daily_volume": 1, "top_repositories": 2, "top_actors": 3}
    snapshot = {}
    for name, count in expected.items():
        frame = read_date(spark, Path(root) / name, day)
        assert frame.count() == count, (name, frame.count(), count)
        snapshot[name] = sorted(spark.read.parquet(str(Path(root) / name)).toJSON().collect())
        print(f"{name}: {count} rows; schema={frame.schema.simpleString()}", flush=True)
    clean = read_date(spark, Path(root) / "clean", day)
    assert clean.select("event_id").distinct().count() == 8
    types = {r.event_type: r["count"] for r in clean.groupBy("event_type").count().collect()}
    assert types == {"PushEvent": 3, "PullRequestEvent": 2, "IssuesEvent": 1, "WatchEvent": 2}, types
    bots = {r.is_bot: r["count"] for r in clean.groupBy("is_bot").count().collect()}
    assert bots == {False: 6, True: 2}, bots
    reasons = {r.rejection_reason: r["count"] for r in read_date(spark, Path(root) / "rejected", day).groupBy("rejection_reason").count().collect()}
    assert reasons == {"corrupt_json": 1, "missing_actor_login": 1, "invalid_timestamp": 1,
                       "unsupported_event_type": 1, "outside_date": 1}, reasons
    assert read_date(spark, Path(root) / "daily_volume", day).first().event_count == 8
    metrics = read_date(spark, Path(root) / "event_counts", day)
    assert sum(r.event_count for r in metrics.collect()) == 8
    for name, key, expected_ranks in [
        ("top_repositories", "repo_name", [("acme/alpha", 5, 1), ("acme/beta", 3, 2)]),
        ("top_actors", "actor_login", [("alice", 4, 1), ("bob", 2, 2), ("ci[bot]", 2, 3)]),
    ]:
        actual = [(r[key], r.event_count, r.rank) for r in read_date(spark, Path(root) / name, day).orderBy("rank").collect()]
        assert actual == expected_ranks, actual
        print(f"{name}: {actual}", flush=True)
    print(f"Types={types}; bots={bots}; rejection_reasons={reasons}", flush=True)
    return snapshot


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-root", default=FIXTURE_PARQUET_ROOT)
    parser.add_argument("--snapshot", required=True)
    parser.add_argument("--compare", action="store_true")
    args = parser.parse_args()
    spark = create_session("verify-github-events-fixture")
    spark.sparkContext.setLogLevel("WARN")
    try:
        snapshot = verify(spark, args.output_root)
        path = Path(args.snapshot)
        if args.compare:
            assert snapshot == json.loads(path.read_text()), "Parquet contents changed on rerun"
            print("Rerun verified: all seven datasets unchanged, including other dates; 8 unique first-day event IDs.", flush=True)
        else:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(snapshot, indent=2))
            print(f"Verified snapshot saved: {path}", flush=True)
    finally:
        spark.stop()
