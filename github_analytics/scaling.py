"""Per-stage volume estimates, explicit shuffle precedence and runtime evidence.

Parquet footer uncompressed column bytes are an input proxy, not in-memory row
size or actual shuffle bytes. The 128 MiB target is a starting hypothesis: keys,
projection, compression and AQE can produce very different task sizes.
"""

import json
from math import ceil
from pathlib import Path
import duckdb
from github_analytics.publication import published_partition

DEFAULT_TARGET_BYTES = 128 * 1024 ** 2


def shuffle_count(estimated_bytes, target_bytes=DEFAULT_TARGET_BYTES):
    if estimated_bytes < 0 or target_bytes < 1:
        raise ValueError("Estimated bytes must be nonnegative and target bytes positive")
    return max(2, ceil(estimated_bytes / target_bytes))


def checkpoint_volume(root, day):
    files, _ = published_partition(root, day)
    with duckdb.connect() as connection:
        # Scalar footer reduction; event rows are never transferred to the driver.
        uncompressed = connection.execute(
            "SELECT coalesce(sum(total_uncompressed_size), 0) FROM parquet_metadata(?)",
            [files]).fetchone()[0]
    return {"files": len(files), "parquet_file_bytes": sum(Path(p).stat().st_size for p in files),
            "parquet_uncompressed_column_bytes": int(uncompressed)}


def configure_stage(spark, stage, day, raw_root, output_root, manual=None,
                    target_bytes=DEFAULT_TARGET_BYTES):
    if target_bytes < 1 or (manual is not None and manual < 1):
        raise ValueError("Shuffle partitions and target bytes must be positive")
    context = spark.sparkContext.getConf()
    launch_override = context.get("spark.sql.shuffle.partitions", None)
    runtime_count = int(spark.conf.get("spark.sql.shuffle.partitions"))
    previous_policy = getattr(spark, "_analytics_shuffle_policy_value", 2)
    if launch_override is not None and int(launch_override) < 1:
        raise ValueError("Launcher spark.sql.shuffle.partitions must be positive")
    datasets = {"ingest": (), "transform": ("ingested",),
                "aggregate": ("clean", "rejected")}[stage]
    volumes = {name: checkpoint_volume(Path(output_root) / name, day) for name in datasets}
    estimated = sum(v["parquet_uncompressed_column_bytes"] for v in volumes.values())
    if manual is not None:
        count, source = manual, "application_override"
    elif launch_override is not None:
        count, source = int(launch_override), "spark_launcher_override"
    elif runtime_count != previous_policy:
        count, source = runtime_count, "session_override"
    else:
        count, source = shuffle_count(estimated, target_bytes), "footer_volume_policy"
        spark._analytics_shuffle_policy_value = count
    spark.conf.set("spark.sql.shuffle.partitions", str(count))
    raw = sorted(Path(raw_root).glob(f"{day}-*.json.gz")) if stage == "ingest" else []
    record = {"date": str(day), "stage": stage, "shuffle_source": source,
              "estimated_input_bytes": estimated, "estimate_kind": "parquet_uncompressed_column_bytes",
              "target_bytes": target_bytes, "inputs": volumes,
              "source_gzip_files": len(raw), "source_gzip_compressed_bytes": sum(p.stat().st_size for p in raw),
              "effective_shuffle_partitions": int(spark.conf.get("spark.sql.shuffle.partitions")),
              "master": spark.sparkContext.master, "default_parallelism": spark.sparkContext.defaultParallelism,
              "driver_memory": context.get("spark.driver.memory", "1g"),
              "executor_memory": context.get("spark.executor.memory", "1g"),
              "executor_cores": context.get("spark.executor.cores", None),
              "total_executor_cores": context.get("spark.cores.max", None),
              "driver_heap_max_bytes": spark._jvm.java.lang.Runtime.getRuntime().maxMemory(),
              "time_zone": spark.conf.get("spark.sql.session.timeZone"),
              "aqe_enabled": spark.conf.get("spark.sql.adaptive.enabled"),
              "aqe_advisory_bytes": spark.conf.get("spark.sql.adaptive.advisoryPartitionSizeInBytes"),
              "aqe_initial_partitions": spark.conf.get("spark.sql.adaptive.coalescePartitions.initialPartitionNum", None),
              "note": "Footer bytes are a proxy; neither compressed gzip nor actual shuffle bytes. "
                      "Ingest has no shuffle estimate. AQE/resource flags are preserved."}
    print("StageSettings " + json.dumps(record, sort_keys=True), flush=True)
    return record
