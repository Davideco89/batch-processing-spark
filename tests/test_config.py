"""CLI checks that run without Spark or Docker."""

import contextlib
from datetime import date
import io
import unittest

from github_analytics.config import parse_config


class ConfigTests(unittest.TestCase):
    def test_parses_date_and_output(self):
        config = parse_config(["--date", "2025-06-01", "--output", "/tmp/output"])
        self.assertEqual(config.event_date, date(2025, 6, 1))
        self.assertEqual(config.output, "/tmp/output")

    def test_rejects_invalid_configuration(self):
        for argv in (
            [],
            ["--date", "2025-02-30", "--output", "/tmp/output"],
            ["--date", "2025-06-01", "--output", " "],
        ):
            with self.subTest(argv=argv), contextlib.redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit) as error:
                    parse_config(argv)
                self.assertEqual(error.exception.code, 2)
