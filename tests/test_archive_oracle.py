"""Exercise the independent raw oracle without downloads or external services."""

from datetime import date
from pathlib import Path
import tempfile
import unittest

from scripts.profile_archive import raw_oracle
from tests.fixture_data import write_fixture


class ArchiveOracleTests(unittest.TestCase):
    def test_fixture_nested_fields_rejections_and_counts(self):
        with tempfile.TemporaryDirectory() as directory:
            write_fixture(directory)
            inputs, lines, accepted, reasons, types, ingested, stamps, rejected = raw_oracle(directory, date(2025, 6, 1))
            self.assertEqual(len(inputs), 2)
            self.assertEqual((lines, len(ingested), len(accepted), sum(reasons.values())), (13, 13, 8, 5))
            self.assertEqual(reasons, {"corrupt_json": 1, "missing_actor_login": 1,
                                      "invalid_timestamp": 1, "unsupported_event_type": 1, "outside_date": 1})
            rows = {r["event_id"]: r for r in accepted}
            self.assertEqual(rows["4"]["pr_number"], 42)
            self.assertEqual(rows["5"]["pr_number"], 43)
            self.assertEqual(rows["6"]["issue_labels"], ["bug", "help wanted"])
            self.assertEqual(rows["3"]["event_timestamp"], "2025-06-01 01:10:00")
            self.assertTrue(rows["1"]["source_file"].startswith(Path(directory).as_uri()))

    def test_missing_input_fails(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(ValueError):
                raw_oracle(directory, date(2025, 6, 1))
