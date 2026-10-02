"""The small-sample oracle stops overflow before parsing or collecting it."""

from collections import Counter
from datetime import date
from pathlib import Path
import tempfile
import unittest
import gzip
import json
from unittest.mock import MagicMock, patch

from scripts import profile_archive as sample
from tests.fixture_data import event, write_archive

DAY = date(2025, 6, 1)


class SampleLimitTests(unittest.TestCase):
    def test_raw_row_overflow_stops_before_normalizing_extra_record(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / f"{DAY}-0.json.gz"
            write_archive(path, [event(i, "PushEvent", "alice", "acme/repo", f"{DAY}T00:00:00Z") for i in range(3)])
            with patch.object(sample, "normalized", wraps=sample.normalized) as normalize, \
                    patch.object(Path, "read_bytes", side_effect=AssertionError("Whole-file hashing is forbidden")):
                with self.assertRaisesRegex(ValueError, "stream_oracle"):
                    sample.raw_oracle(directory, DAY, max_records=2)
                self.assertEqual(normalize.call_count, 2)

    def test_uncompressed_raw_byte_overflow_stops_before_json_parse(self):
        with tempfile.TemporaryDirectory() as directory:
            write_archive(Path(directory) / f"{DAY}-0.json.gz", [event(1, "PushEvent", "alice", "acme/repo", f"{DAY}T00:00:00Z")])
            with patch.object(sample.json, "loads", wraps=sample.json.loads) as parse:
                with self.assertRaisesRegex(ValueError, "stream_oracle"):
                    sample.raw_oracle(directory, DAY, max_bytes=32)
                parse.assert_not_called()

    def test_crlf_and_bare_cr_preserve_original_text_oracle_semantics(self):
        for newline in (b"\r\n", b"\r"):
            with self.subTest(newline=newline), tempfile.TemporaryDirectory() as directory:
                record = json.dumps(event(1, "PushEvent", "alice", "acme/repo", f"{DAY}T00:00:00Z")).encode()
                with gzip.open(Path(directory) / f"{DAY}-0.json.gz", "wb") as stream:
                    stream.write(record + newline + b'{"broken":' + newline)
                result = sample.raw_oracle(directory, DAY)
                self.assertEqual(result[1], 2)
                self.assertEqual(result[3], {"corrupt_json": 1})
                self.assertEqual(result[-1][0]["corrupt_record"], '{"broken":')

    def _oversize_frame(self, rows, serialized_bytes):
        frame = MagicMock()
        frame.columns = ["event_id"]
        frame.limit.return_value.count.return_value = rows
        frame.select.return_value.agg.return_value.first.return_value = (serialized_bytes,)
        empty_oracle = ([], 0, [], Counter(), Counter(), [], [], [])
        # Spark expressions need an active context. Patch only expression building;
        # the test exercises profile's actual guard/collection ordering.
        with patch.object(sample, "raw_oracle", return_value=empty_oracle), \
                patch.object(sample, "read_date", return_value=frame), patch.object(sample, "F"):
            with self.assertRaisesRegex(ValueError, "stream_oracle"):
                sample.profile(None, "/unused", "/unused", DAY, 10, max_records=2, max_bytes=32)
        frame.collect.assert_not_called()
        frame.toJSON.assert_not_called()
        frame.cache.assert_not_called()
        return frame

    def test_frame_row_overflow_stops_before_any_driver_collection(self):
        frame = self._oversize_frame(3, 0)
        frame.select.assert_not_called()

    def test_frame_serialized_byte_overflow_stops_before_any_driver_collection(self):
        frame = self._oversize_frame(1, 33)
        frame.select.return_value.agg.return_value.first.assert_called_once()
