"""Validate resource options and construct spark-submit before JVM startup.

Dedicated CLI options > matching --conf > environment > local[2]/Spark defaults.
Application arguments (date/range/paths) are forwarded without shell evaluation.
Cluster masters are accepted configuration; local publication still requires the
same filesystem visible to the driver and workers and is not an object-store API.
"""

import argparse
import os
import re

RESOURCES = (
    ("master", "spark.master", "SPARK_MASTER", "local[2]"),
    ("driver-memory", "spark.driver.memory", "SPARK_DRIVER_MEMORY", None),
    ("executor-memory", "spark.executor.memory", "SPARK_EXECUTOR_MEMORY", None),
    ("executor-cores", "spark.executor.cores", "SPARK_EXECUTOR_CORES", None),
    ("total-executor-cores", "spark.cores.max", "SPARK_TOTAL_EXECUTOR_CORES", None),
)


def validate_resource(option, value):
    if option == "master":
        if not value or any(char.isspace() for char in value):
            raise ValueError("Master must be nonempty and contain no whitespace")
        if value.startswith("local") and not re.fullmatch(r"local(?:\[(?:[1-9]\d*|\*)(?:,[1-9]\d*)?\])?", value):
            raise ValueError("Invalid local master; use local[N] or local[*]")
        if not value.startswith("local") and not (value == "yarn" or value.startswith(("spark://", "k8s://", "mesos://"))):
            raise ValueError("Unsupported master URL")
    elif option.endswith("memory"):
        if not re.fullmatch(r"[1-9]\d*[kKmMgGtT]", value):
            raise ValueError(f"--{option} requires a positive size with k/m/g/t unit")
    elif not re.fullmatch(r"[1-9]\d*", value):
        raise ValueError(f"--{option} must be a positive integer")


def submit_command(argv, environment=None):
    env = os.environ if environment is None else environment
    parser = argparse.ArgumentParser(description="Validated Docker spark-submit launcher", allow_abbrev=False)
    for option, _, _, _ in RESOURCES:
        parser.add_argument("--" + option)
    parser.add_argument("--conf", action="append", default=[])
    parser.add_argument("application")
    args, remaining = parser.parse_known_args(argv)
    conf = {}
    for value in args.conf:
        key, separator, content = value.partition("=")
        if not separator or not key.strip() or not content.strip():
            parser.error("--conf requires a nonempty key=value")
        conf[key] = content
    command = ["/opt/spark/bin/spark-submit"]
    for option, key, variable, default in RESOURCES:
        explicit = getattr(args, option.replace("-", "_"))
        value = explicit if explicit is not None else conf.get(key, env.get(variable, default))
        if value is not None:
            try:
                validate_resource(option, value)
            except ValueError as error:
                parser.error(str(error))
            # Emit the validated selection as a startup flag, remove the lower
            # priority duplicate to avoid depending on Spark argument ordering.
            command.extend(["--" + option, value])
            conf.pop(key, None)
    if "spark.sql.shuffle.partitions" not in conf and "SPARK_SHUFFLE_PARTITIONS" in env:
        conf["spark.sql.shuffle.partitions"] = env["SPARK_SHUFFLE_PARTITIONS"]
    if "spark.sql.shuffle.partitions" in conf:
        try:
            validate_resource("shuffle-partitions", conf["spark.sql.shuffle.partitions"])
        except ValueError as error:
            parser.error(str(error))
    for key, value in conf.items():
        command.extend(["--conf", key + "=" + value])
    return command + [args.application] + remaining
