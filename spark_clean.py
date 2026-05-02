"""
PySpark preprocessing pipeline for NYC yellow taxi data.

Steps: ingest → cleanse → transform (join + UDF) → export
Writes per-step timings and resource snapshots to spark_benchmark.json.

Usage:
    # local smoke-test
    python spark_clean.py --master "local[*]"

    # 2-node cluster
    python spark_clean.py --master spark://MASTER_IP:7077
"""

import argparse
import json
import os
import time

import psutil
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType


# ── UDF ──────────────────────────────────────────────────────────────────────
# Deliberately using @udf (row-by-row, crosses JVM boundary) rather than
# @pandas_udf, to expose the serialization overhead for the UDF deep-dive.
def _avg_speed(distance, duration_sec):
    if distance is None or duration_sec is None or duration_sec <= 0:
        return None
    return float(distance / (duration_sec / 3600.0))


avg_speed_udf = F.udf(_avg_speed, DoubleType())


# ── Helpers ───────────────────────────────────────────────────────────────────
class _Timer:
    def __init__(self):
        self.log = {}

    def time(self, name):
        return self._Ctx(self.log, name)

    class _Ctx:
        def __init__(self, log, name):
            self._log = log
            self._name = name

        def __enter__(self):
            self._start = time.perf_counter()
            return self

        def __exit__(self, *_):
            self._log[self._name] = round(time.perf_counter() - self._start, 3)


def _resources():
    proc = psutil.Process()
    return {
        "cpu_pct": psutil.cpu_percent(interval=1),
        "mem_rss_mb": round(proc.memory_info().rss / 1e6, 1),
    }


# ── Pipeline steps ────────────────────────────────────────────────────────────
def step_ingest(spark, data_dir):
    df = spark.read.parquet(os.path.join(data_dir, "*.parquet"))
    df.cache()
    count = df.count()
    return df, count


def step_cleanse(df):
    required = [
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "trip_distance",
        "PULocationID",
        "DOLocationID",
        "passenger_count",
    ]
    df = df.dropna(subset=required)
    df = df.dropDuplicates()
    df = df.withColumn("tpep_pickup_datetime", F.to_timestamp("tpep_pickup_datetime"))
    df = df.withColumn("tpep_dropoff_datetime", F.to_timestamp("tpep_dropoff_datetime"))
    df = df.filter((F.col("trip_distance") > 0) & (F.col("passenger_count") > 0))
    count = df.count()
    return df, count


def step_transform(spark, df, zone_csv):
    zone = spark.read.option("header", True).csv(zone_csv)
    zone = zone.withColumn("LocationID", F.col("LocationID").cast("long"))

    # Broadcast the small zone lookup (≈200 rows) to avoid a shuffle join.
    pu_zone = F.broadcast(zone).select(
        F.col("LocationID").alias("PULocationID"),
        F.col("Zone").alias("PUZone"),
        F.col("Borough").alias("PUBorough"),
    )
    do_zone = F.broadcast(zone).select(
        F.col("LocationID").alias("DOLocationID"),
        F.col("Zone").alias("DOZone"),
        F.col("Borough").alias("DOBorough"),
    )

    df = df.join(pu_zone, on="PULocationID", how="left")
    df = df.join(do_zone, on="DOLocationID", how="left")

    df = df.withColumn(
        "duration_seconds",
        F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime"),
    )
    df = df.withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))

    # Python UDF — row-by-row, crosses JVM↔Python boundary on every call
    udf_start = time.perf_counter()
    df = df.withColumn("avg_speed_mph", avg_speed_udf(F.col("trip_distance"), F.col("duration_seconds")))
    count = df.count()
    udf_elapsed = round(time.perf_counter() - udf_start, 3)

    return df, count, udf_elapsed


def step_export(df, output_dir):
    df.write.mode("overwrite").parquet(output_dir)


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--master", default=os.environ.get("SPARK_MASTER", "local[*]"))
    parser.add_argument("--data-dir", default=os.environ.get("DATA_DIR", "./data/raw"))
    parser.add_argument("--zone-csv", default=os.environ.get("ZONE_CSV", "./data/raw/taxi_zone_lookup.csv"))
    parser.add_argument("--output", default=os.environ.get("OUTPUT_DIR", "./data/spark_output"))
    parser.add_argument("--benchmark-out", default="spark_benchmark.json")
    args = parser.parse_args()

    spark = (
        SparkSession.builder.appName("NYC_Taxi_Spark_Clean")
        .master(args.master)
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    timer = _Timer()
    res_before = _resources()

    print("=== Spark NYC Taxi Pipeline ===")

    with timer.time("ingest"):
        raw_df, raw_count = step_ingest(spark, args.data_dir)
    print(f"[ingest]    {timer.log['ingest']:.1f}s  rows={raw_count:,}")

    with timer.time("cleanse"):
        clean_df, clean_count = step_cleanse(raw_df)
    print(f"[cleanse]   {timer.log['cleanse']:.1f}s  rows={clean_count:,}")

    with timer.time("transform_join_udf"):
        final_df, final_count, udf_elapsed = step_transform(spark, clean_df, args.zone_csv)
    print(f"[transform] {timer.log['transform_join_udf']:.1f}s  rows={final_count:,}  udf={udf_elapsed:.1f}s")

    with timer.time("export"):
        step_export(final_df, args.output)
    print(f"[export]    {timer.log['export']:.1f}s")

    res_after = _resources()
    total = round(sum(timer.log.values()), 3)
    print(f"[TOTAL]     {total:.1f}s")

    benchmark = {
        "framework": "spark",
        "master": args.master,
        "timings_sec": timer.log,
        "udf_sec": udf_elapsed,
        "total_sec": total,
        "row_counts": {"raw": raw_count, "clean": clean_count, "final": final_count},
        "resources": {"before": res_before, "after": res_after},
    }
    with open(args.benchmark_out, "w") as f:
        json.dump(benchmark, f, indent=2)
    print(f"\nBenchmark written to {args.benchmark_out}")

    spark.stop()


if __name__ == "__main__":
    main()
