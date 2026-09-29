"""Generate small GH Archive-shaped gzip JSON Lines files without downloads."""

import argparse
from copy import deepcopy
import gzip
import json
from pathlib import Path


def event(identifier, kind, actor, repo, timestamp, payload=None):
    return {"id": str(identifier), "type": kind, "actor": {"id": 1, "login": actor},
            "repo": {"id": 10, "name": repo}, "org": {"login": "acme"},
            "created_at": timestamp, "public": True, "payload": payload or {}}


def first_day_records():
    records = [
        event(1, "PushEvent", "alice", "acme/alpha", "2025-06-01T00:10:00Z", {"size": 2}),
        event(2, "PushEvent", "alice", "acme/alpha", "2025-06-01T00:20:00Z"),
        event(3, "PushEvent", "ci[bot]", "acme/beta", "2025-06-01T01:10:00Z"),
        event(4, "PullRequestEvent", "alice", "acme/alpha", "2025-06-01T01:20:00Z",
              {"action": "opened", "number": 42, "pull_request": {"number": 42}}),
        event(5, "PullRequestEvent", "ci[bot]", "acme/beta", "2025-06-01T01:30:00Z",
              {"action": "closed", "number": 43}),
        event(6, "IssuesEvent", "bob", "acme/alpha", "2025-06-01T02:10:00Z",
              {"action": "opened", "issue": {"number": 7, "labels": [{"name": "bug"}, {"name": "help wanted"}]}}),
        event(7, "WatchEvent", "alice", "acme/alpha", "2025-06-01T02:20:00Z", {"action": "started"}),
        event(8, "WatchEvent", "bob", "acme/beta", "2025-06-01T02:30:00Z"),
        event(9, "ForkEvent", "alice", "acme/alpha", "2025-06-01T03:00:00Z"),
        event(10, "PushEvent", None, "acme/alpha", "2025-06-01T03:10:00Z"),
        event(11, "PushEvent", "alice", "acme/alpha", "not-a-timestamp"),
        event(12, "WatchEvent", "alice", "acme/alpha", "2025-06-02T00:00:00Z"),
    ]
    # Optional org is valid, unlike the missing required actor above.
    records[7].pop("org")
    return deepcopy(records)


def write_archive(path, records, malformed=False):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wt", encoding="utf-8", newline="\n") as stream:
        for record in records:
            stream.write(json.dumps(record) + "\n")
        if malformed:
            stream.write('{"id": "broken", "type":\n')


def write_fixture(root):
    root = Path(root)
    records = first_day_records()
    write_archive(root / "2025-06-01-0.json.gz", records[:4])
    write_archive(root / "2025-06-01-1.json.gz", records[4:], malformed=True)
    write_archive(root / "2025-06-02-0.json.gz", [
        event(20, "PushEvent", "robot", "acme/gamma", "2025-06-02T00:10:00Z"),
        event(21, "WatchEvent", "bob", "acme/gamma", "2025-06-02T01:00:00Z"),
    ])
    print(f"Fixture written to {root}: 2025-06-01=13 lines; 2025-06-02=2 lines", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True)
    write_fixture(parser.parse_args().output)
