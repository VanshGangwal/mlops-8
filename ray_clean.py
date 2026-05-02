"""
Ray Data preprocessing pipeline for NYC yellow taxi data.
Mirrors spark_clean.py step-for-step for a fair benchmark comparison.

Steps: ingest → cleanse → transform (join + UDF) → export
Writes per-step timings and resource snapshots to ray_benchmark.json.

Usage:
    # local smoke-test
    python ray_clean.py

    # 2-node cluster (head node must be started first)
    python ray_clean.py --address ray://HEAD_IP:10001

Performance Tuning Note (AI-assisted):
    ray.put() places the zone lookup DataFrame into the Ray object store once,
    and ray.get() fetches it inside each map_batches call — equivalent to
    Spark's broadcast() hint. This avoids re-serializing the lookup table
    per batch and keeps the join in pure Python/pandas with no JVM boundary.
"""

import argparse
import glob
import json
import os
import time

import numpy as np
import pandas as pd
import psutil
import ray
import ray.data


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


# ── Batch UDFs (pure Python / pandas — no JVM boundary) ──────────────────────
def cleanse_batch(batch: pd.DataFrame) -> pd.DataFrame:
    required = [
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "trip_distance",
        "PULocationID",
        "DOLocationID",
        "passenger_count",
    ]
    batch = batch.dropna(subset=required)
    batch = batch.drop_duplicates()
    batch["tpep_pickup_datetime"] = pd.to_datetime(batch["tpep_pickup_datetime"], utc=True, errors="coerce")
    batch["tpep_dropoff_datetime"] = pd.to_datetime(batch["tpep_dropoff_datetime"], utc=True, errors="coerce")
    batch = batch[(batch["trip_distance"] > 0) & (batch["passenger_count"] > 0)]
    return batch.reset_index(drop=True)


def make_transform_fn(zone_ref):
    """Return a map_batches-compatible function that closes over the zone object ref."""

    def transform_batch(batch: pd.DataFrame) -> pd.DataFrame:
        zone_df = ray.get(zone_ref)
        zone_df = zone_df.rename(columns={"LocationID": "_lid"})

        # Pickup zone join
        pu = zone_df[["_lid", "Zone", "Borough"]].rename(
            columns={"_lid": "PULocationID", "Zone": "PUZone", "Borough": "PUBorough"}
        )
        batch = batch.merge(pu, on="PULocationID", how="left")

        # Dropoff zone join
        do = zone_df[["_lid", "Zone", "Borough"]].rename(
            columns={"_lid": "DOLocationID", "Zone": "DOZone", "Borough": "DOBorough"}
        )
        batch = batch.merge(do, on="DOLocationID", how="left")

        # Duration and derived speed feature (pure pandas/numpy — Python-native)
        dur = (
            pd.to_datetime(batch["tpep_dropoff_datetime"], utc=True, errors="coerce")
            - pd.to_datetime(batch["tpep_pickup_datetime"], utc=True, errors="coerce")
        ).dt.total_seconds()
        batch["duration_seconds"] = dur
        batch["pickup_hour"] = pd.to_datetime(
            batch["tpep_pickup_datetime"], utc=True, errors="coerce"
        ).dt.hour
        batch["avg_speed_mph"] = np.where(
            dur > 0, batch["trip_distance"] / (dur / 3600.0), np.nan
        )
        return batch.reset_index(drop=True)

    return transform_batch


# ── Pipeline steps ────────────────────────────────────────────────────────────
def step_ingest(data_dir: str) -> ray.data.Dataset:
    files = glob.glob(os.path.join(data_dir, "*.parquet"))
    if not files:
        raise FileNotFoundError(f"No Parquet files found in {data_dir}")
    return ray.data.read_parquet(files)


def step_cleanse(ds: ray.data.Dataset) -> ray.data.Dataset:
    return ds.map_batches(cleanse_batch, batch_format="pandas")


def step_transform(ds: ray.data.Dataset, zone_csv: str, zone_ref) -> ray.data.Dataset:
    return ds.map_batches(make_transform_fn(zone_ref), batch_format="pandas")


def step_export(ds: ray.data.Dataset, output_dir: str) -> None:
    os.makedirs(output_dir, exist_ok=True)
    ds.write_parquet(output_dir)


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--address", default=os.environ.get("RAY_ADDRESS", None),
                        help="Ray cluster address, e.g. ray://HEAD_IP:10001 (None = local)")
    parser.add_argument("--data-dir", default=os.environ.get("DATA_DIR", "./data/raw"))
    parser.add_argument("--zone-csv", default=os.environ.get("ZONE_CSV", "./data/raw/taxi_zone_lookup.csv"))
    parser.add_argument("--output", default=os.environ.get("OUTPUT_DIR", "./data/ray_output"))
    parser.add_argument("--benchmark-out", default="ray_benchmark.json")
    args = parser.parse_args()

    ray.init(address=args.address, ignore_reinit_error=True)

    # Put zone lookup into the Ray object store once (broadcast equivalent)
    zone_df = pd.read_csv(args.zone_csv)
    zone_df["LocationID"] = zone_df["LocationID"].astype("int64")
    zone_ref = ray.put(zone_df)

    timer = _Timer()
    res_before = _resources()

    print("=== Ray NYC Taxi Pipeline ===")

    with timer.time("ingest"):
        ds = step_ingest(args.data_dir)
        raw_count = ds.count()
    print(f"[ingest]    {timer.log['ingest']:.1f}s  rows={raw_count:,}")

    with timer.time("cleanse"):
        ds = step_cleanse(ds)
        clean_count = ds.count()
    print(f"[cleanse]   {timer.log['cleanse']:.1f}s  rows={clean_count:,}")

    udf_start = time.perf_counter()
    with timer.time("transform_join_udf"):
        ds = step_transform(ds, args.zone_csv, zone_ref)
        final_count = ds.count()
    udf_elapsed = round(time.perf_counter() - udf_start, 3)
    print(f"[transform] {timer.log['transform_join_udf']:.1f}s  rows={final_count:,}  udf={udf_elapsed:.1f}s")

    with timer.time("export"):
        step_export(ds, args.output)
    print(f"[export]    {timer.log['export']:.1f}s")

    res_after = _resources()
    total = round(sum(timer.log.values()), 3)
    print(f"[TOTAL]     {total:.1f}s")

    benchmark = {
        "framework": "ray",
        "address": args.address or "local",
        "timings_sec": timer.log,
        "udf_sec": udf_elapsed,
        "total_sec": total,
        "row_counts": {"raw": raw_count, "clean": clean_count, "final": final_count},
        "resources": {"before": res_before, "after": res_after},
    }
    with open(args.benchmark_out, "w") as f:
        json.dump(benchmark, f, indent=2)
    print(f"\nBenchmark written to {args.benchmark_out}")

    ray.shutdown()


if __name__ == "__main__":
    main()
