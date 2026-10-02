"""Autonomous parameter, launcher and measurable footer-policy checks."""

from datetime import date
from contextlib import redirect_stdout
from pathlib import Path
import io
import json
import tempfile
import unittest
from unittest.mock import patch

from github_analytics.batch import date_bounds, days, run_batch, strict_date
from github_analytics.launcher import submit_command
from github_analytics.scaling import DEFAULT_TARGET_BYTES, shuffle_count
from scripts.profile_spark_events import summarize


class BatchConfigurationTests(unittest.TestCase):
    def test_event_metrics_do_not_mix_stage_ids_across_applications(self):
        with tempfile.TemporaryDirectory() as directory:
            paths = []
            for name, cpu in (("first-app", 7), ("second-app", 11)):
                path = Path(directory) / name
                events = [{"Event": "SparkListenerJobStart", "Stage IDs": [0],
                           "Properties": {"spark.jobGroup.id": name}},
                          {"Event": "SparkListenerTaskEnd", "Stage ID": 0,
                           "Task Info": {"Launch Time": 10, "Finish Time": 30},
                           "Task Metrics": {"Executor CPU Time": cpu,
                                            "Executor Run Time": 20,
                                            "Shuffle Read Metrics": {"Local Bytes Read": cpu},
                                            "Shuffle Write Metrics": {"Shuffle Bytes Written": cpu * 2}}}]
                path.write_text("\n".join(json.dumps(item) for item in events), encoding="utf-8")
                paths.append(path)
            report = summarize(paths)
            self.assertEqual(report["job_groups"]["first-app"]["executor_cpu_ns"], 7)
            self.assertEqual(report["job_groups"]["second-app"]["executor_cpu_ns"], 11)
            self.assertEqual(report["job_groups"]["first-app"]["tasks"], 1)
            self.assertEqual(report["job_groups"]["second-app"]["max_task_shuffle_write_bytes"], 22)

    def test_strict_dates_bounds_and_inclusive_iterator(self):
        for value in ("20250601", "2025-W22-7", "2025-6-1", "2025-02-29", "2025-06-01T00:00:00"):
            with self.assertRaises(ValueError):
                strict_date(value)
        a, b, c = (date(2025, 6, n) for n in (1, 2, 3))
        self.assertEqual(list(days(a, c)), [a, b, c])
        self.assertEqual(list(days(date.max, date.max)), [date.max])
        with self.assertRaises(ValueError):
            list(days(c, a))
        self.assertEqual(date_bounds(start=a, end=c, resume=b), (b, c))
        for kwargs in ({}, {"single": a, "start": a}, {"start": a},
                       {"start": c, "end": a}, {"start": a, "end": b, "resume": c}):
            with self.assertRaises(ValueError):
                date_bounds(**kwargs)

    def test_stop_on_error_and_first_date_stage_resume(self):
        a, b, c = (date(2025, 6, n) for n in (1, 2, 3))
        for job in ("pipeline", "transform", "aggregate"):
            with self.subTest(job=job), patch("github_analytics.runner.run_stage") as stage:
                stage.side_effect = [None, RuntimeError("isolated failure")]
                output = io.StringIO()
                with redirect_stdout(output), self.assertRaisesRegex(RuntimeError, "isolated failure"):
                    run_batch(None, job, a, c, "raw", "out")
                self.assertEqual([call.args[2] for call in stage.call_args_list], [a, b])
                message = output.getvalue().splitlines()[-1]
                self.assertIn(f"Resume the same {job} job explicitly with --resume-date {b}", message)
                self.assertIn("no later dates were started", message)
                if job == "pipeline":
                    self.assertIn("--from-stage <ingest|transform|aggregate>", message)
                else:
                    self.assertNotIn("--from-stage", message)
        with patch("github_analytics.runner.run_stage") as stage:
            run_batch(None, "pipeline", b, c, "absent", "out", from_stage="aggregate")
            self.assertEqual([call.kwargs["from_stage"] for call in stage.call_args_list], ["aggregate", "ingest"])

    def test_shuffle_size_boundary_and_invalid_values(self):
        self.assertEqual([shuffle_count(v) for v in (0, 1, DEFAULT_TARGET_BYTES * 2,
                                                    DEFAULT_TARGET_BYTES * 2 + 1)], [2, 2, 2, 3])
        self.assertEqual(shuffle_count(DEFAULT_TARGET_BYTES * 8), 8)
        for size, target in ((-1, 1), (1, 0)):
            with self.assertRaises(ValueError):
                shuffle_count(size, target)

    def test_launcher_priority_startup_flags_and_forwarded_application(self):
        command = submit_command(["/app/jobs/pipeline.py", "--date", "2025-06-01", "--master", "local[4]",
                                  "--driver-memory", "768m", "--conf", "spark.master=local[3]",
                                  "--conf", "spark.sql.shuffle.partitions=8",
                                  "--conf", "spark.sql.adaptive.enabled=false"],
                                 {"SPARK_MASTER": "local[1]", "SPARK_DRIVER_MEMORY": "bad",
                                  "SPARK_SHUFFLE_PARTITIONS": "2"})
        self.assertEqual(command[:5], ["/opt/spark/bin/spark-submit", "--master", "local[4]", "--driver-memory", "768m"])
        self.assertIn("spark.sql.shuffle.partitions=8", command)
        self.assertIn("spark.sql.adaptive.enabled=false", command)
        self.assertNotIn("spark.master=local[3]", command)
        self.assertEqual(command[-3:], ["/app/jobs/pipeline.py", "--date", "2025-06-01"])
        default = submit_command(["app.py"], {})
        self.assertEqual(default, ["/opt/spark/bin/spark-submit", "--master", "local[2]", "app.py"])
        conf = submit_command(["--conf", "spark.driver.memory=2g", "app.py"], {"SPARK_DRIVER_MEMORY": "1g"})
        self.assertIn("2g", conf)
        for args, env in ((["--master", "local[0]", "app.py"], {}),
                          (["--driver-memory", "0g", "app.py"], {}),
                          (["--executor-cores", "-1", "app.py"], {}),
                          (["app.py"], {"SPARK_SHUFFLE_PARTITIONS": ""}),
                          (["--conf", "broken", "app.py"], {})):
            with self.assertRaises(SystemExit):
                submit_command(args, env)
