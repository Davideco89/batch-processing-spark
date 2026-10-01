"""Autonomous crash-window, inventory, locking and recovery checks."""

from datetime import date
import json
from pathlib import Path
import tempfile
import unittest

import duckdb

from github_analytics.export import load_partition
from github_analytics.publication import (PublicationError, partition_lock, publish,
                                         published_dates, published_partition,
                                         recover_partition, sha256)
from github_analytics.storage import read_date
from scripts.validate_parquet import read_partition

DAY, OTHER = date(2025, 6, 1), date(2025, 6, 2)
PHASES = ("transaction_created", "journal_written", "stage_written", "validated",
          "manifest_written", "prepared", "generation_renamed", "pointer_temp_written",
          "pointer_replaced", "committed", "cleaned")


def fixture_writer(value=None):
    def writer(stage):
        stage.mkdir()
        with duckdb.connect(config={"threads": 2}) as connection:
            connection.execute("CREATE TABLE fixture(event_count BIGINT)")
            if value is not None:
                connection.execute("INSERT INTO fixture VALUES (?)", [value])
            connection.execute("COPY fixture TO ? (FORMAT PARQUET)", [str(stage / "part.parquet")])
        (stage / "_SUCCESS").touch()
        return int(value is not None), {"event_count": "bigint"}
    return writer


def fail_at(target):
    def hook(phase):
        if phase == target:
            raise RuntimeError("injected interruption: " + phase)
    return hook


def file_snapshot(directory):
    return {str(path.relative_to(directory)): sha256(path) for path in directory.rglob("*") if path.is_file()}


class PublicationTests(unittest.TestCase):
    def test_all_fault_windows_new_existing_empty_and_other_date_preservation(self):
        observations = []
        for existing, empty in ((False, False), (True, False), (True, True)):
            for phase in PHASES:
                with self.subTest(existing=existing, empty=empty, phase=phase), tempfile.TemporaryDirectory() as directory:
                    root = Path(directory) / "daily_volume"
                    publish(root, OTHER, fixture_writer(7))
                    other = root / f"event_date={OTHER}"
                    before = file_snapshot(other)
                    if existing:
                        publish(root, DAY, fixture_writer(3))
                    with self.assertRaisesRegex(RuntimeError, phase):
                        publish(root, DAY, fixture_writer(None if empty else 9), fail_at(phase))
                    if phase != "cleaned":
                        # Spark construction, DuckDB and exact checker share the
                        # gate; no Spark action can be built from partial files.
                        with self.assertRaises(PublicationError):
                            published_partition(root, DAY)
                        with self.assertRaises(PublicationError):
                            read_date(None, root, DAY)
                        with self.assertRaises(PublicationError):
                            read_partition(None, Path(directory), "daily_volume", DAY)
                        with duckdb.connect(config={"threads": 2}) as connection:
                            with self.assertRaises(PublicationError):
                                load_partition(connection, "daily_volume", Path(directory), DAY)
                        with self.assertRaises(PublicationError):
                            published_dates(root)
                    outcome = recover_partition(root, DAY)
                    committed = phase in ("pointer_replaced", "committed", "cleaned")
                    if committed or existing:
                        files, manifest = published_partition(root, DAY)
                        expected = (None if empty else 9) if committed else 3
                        with duckdb.connect(config={"threads": 2}) as connection:
                            rows = connection.execute("SELECT * FROM read_parquet(?, hive_partitioning=false)", [files]).fetchall()
                        self.assertEqual(rows, [] if expected is None else [(expected,)])
                        self.assertEqual(manifest["rows"], len(rows))
                    else:
                        with self.assertRaises(PublicationError):
                            published_partition(root, DAY)
                    self.assertEqual(file_snapshot(other), before)
                    self.assertEqual(recover_partition(root, DAY), "no transaction")
                    observations.append((existing, empty, phase, outcome))
        print("Publication fault matrix: " + json.dumps(observations), flush=True)

    def test_explicit_commit_only_accepts_valid_prepared_generation(self):
        for phase in ("prepared", "generation_renamed", "pointer_temp_written"):
            with self.subTest(phase=phase), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                publish(root, DAY, fixture_writer(3))
                with self.assertRaises(RuntimeError):
                    publish(root, DAY, fixture_writer(9), fail_at(phase))
                self.assertEqual(recover_partition(root, DAY, "commit"), "prepared generation committed")
                self.assertEqual(published_partition(root, DAY)[1]["rows"], 1)
                self.assertEqual(len(list((root / f"event_date={DAY}" / "_generations").iterdir())), 2)
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(RuntimeError):
                publish(directory, DAY, fixture_writer(9), fail_at("stage_written"))
            with self.assertRaisesRegex(PublicationError, "No validated"):
                recover_partition(directory, DAY, "commit")
            recover_partition(directory, DAY)

    def test_inventory_completion_pointer_journal_and_content_tampering_fail_closed(self):
        for alteration in ("missing", "extra", "content", "success", "manifest", "pointer", "partition-extra", "pointer-residue"):
            with self.subTest(alteration=alteration), tempfile.TemporaryDirectory() as directory:
                publish(directory, DAY, fixture_writer(3))
                files, _ = published_partition(directory, DAY)
                generation = Path(files[0]).parent
                if alteration == "missing":
                    Path(files[0]).unlink()
                elif alteration == "extra":
                    (generation / "stale.parquet").write_bytes(Path(files[0]).read_bytes())
                elif alteration == "content":
                    data = bytearray(Path(files[0]).read_bytes())
                    data[len(data) // 2] ^= 1  # Same size is not sufficient proof.
                    Path(files[0]).write_bytes(data)
                elif alteration == "success":
                    (generation / "_SUCCESS").unlink()
                elif alteration == "manifest":
                    (generation / "_manifest.json").write_text("{}")
                elif alteration == "pointer":
                    (Path(directory) / f"event_date={DAY}" / "_publication.json").write_text("{}")
                elif alteration == "partition-extra":
                    (generation.parent.parent / "stale.parquet").write_bytes(Path(files[0]).read_bytes())
                else:
                    (generation.parent.parent / "_publication.json.tmp").write_text("{}")
                with self.assertRaises(PublicationError):
                    published_partition(directory, DAY)
        with tempfile.TemporaryDirectory() as directory:
            publish(directory, DAY, fixture_writer(3))
            with self.assertRaises(RuntimeError):
                publish(directory, DAY, fixture_writer(9), fail_at("prepared"))
            journal = Path(directory) / "_transactions" / str(DAY) / "journal.json"
            journal.write_text("{}")
            before = file_snapshot(Path(directory))
            with self.assertRaises(PublicationError):
                recover_partition(directory, DAY)
            self.assertEqual(file_snapshot(Path(directory)), before)

    def test_active_writer_cannot_be_recovered_or_read_and_other_date_remains_available(self):
        with tempfile.TemporaryDirectory() as directory:
            publish(directory, DAY, fixture_writer(3))
            publish(directory, OTHER, fixture_writer(7))
            with partition_lock(directory, DAY, exclusive=True):
                with self.assertRaisesRegex(PublicationError, "busy"):
                    recover_partition(directory, DAY)
                with self.assertRaisesRegex(PublicationError, "busy"):
                    published_partition(directory, DAY)
                self.assertEqual(published_partition(directory, OTHER)[1]["rows"], 1)

    def test_legacy_rejected_without_modifying_it_and_invalid_parameters(self):
        with tempfile.TemporaryDirectory() as directory:
            partition = Path(directory) / f"event_date={DAY}"
            fixture_writer(3)(partition)
            before = file_snapshot(Path(directory))
            for operation in (lambda: published_partition(directory, DAY),
                              lambda: publish(directory, DAY, fixture_writer(9)),
                              lambda: recover_partition(directory, DAY)):
                with self.assertRaises(PublicationError):
                    operation()
            self.assertEqual(file_snapshot(Path(directory)), before)
            with self.assertRaises(ValueError):
                publish(directory, "2025-02-30", fixture_writer(9))
            with self.assertRaises(PublicationError):
                publish("s3://bucket/output", DAY, fixture_writer(9))
