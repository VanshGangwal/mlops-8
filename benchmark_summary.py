"""
Print a side-by-side benchmark comparison of Spark vs Ray pipeline runs.

Usage:
    python benchmark_summary.py spark_benchmark.json ray_benchmark.json
    python benchmark_summary.py                        # uses default filenames
"""

import json
import sys


def load(path):
    with open(path) as f:
        return json.load(f)


def fmt(val):
    return f"{val:>10.2f}" if isinstance(val, (int, float)) else f"{'N/A':>10}"


def speedup(s, r):
    if r and r > 0:
        return f"{s/r:>9.2f}x"
    return f"{'N/A':>10}"


STEPS = ["ingest", "cleanse", "transform_join_udf", "export"]
STEP_LABELS = {
    "ingest": "1. Ingest",
    "cleanse": "2. Cleanse",
    "transform_join_udf": "3. Transform+Join+UDF",
    "export": "4. Export",
}


def print_report(spark, ray):
    width = 65
    line = "─" * width

    print()
    print("╔" + "═" * (width - 2) + "╗")
    print(f"║{'  Spark vs Ray — Benchmark Summary':^{width-2}}║")
    print("╚" + "═" * (width - 2) + "╝")
    print()

    # Header
    print(f"{'Step':<25} {'Spark (s)':>10} {'Ray (s)':>10} {'Speedup':>10}")
    print(line)

    for step in STEPS:
        sv = spark.get("timings_sec", {}).get(step)
        rv = ray.get("timings_sec", {}).get(step)
        label = STEP_LABELS.get(step, step)
        su = speedup(sv, rv) if sv and rv else f"{'N/A':>10}"
        print(f"{label:<25}{fmt(sv)}{fmt(rv)}{su}")

    print(line)

    st = spark.get("total_sec", 0)
    rt = ray.get("total_sec", 0)
    print(f"{'TOTAL':<25}{fmt(st)}{fmt(rt)}{speedup(st, rt)}")

    print()
    print("── UDF Overhead ────────────────────────────────────────────")
    spark_udf = spark.get("udf_sec")
    ray_udf   = ray.get("udf_sec")
    print(f"{'Spark (JVM boundary):':<30}{fmt(spark_udf)} s")
    print(f"{'Ray (Python-native):':<30}{fmt(ray_udf)} s")
    if spark_udf and ray_udf and ray_udf > 0:
        print(f"{'UDF Speedup (Ray/Spark):':<30}{speedup(spark_udf, ray_udf)}")

    print()
    print("── Row Count Parity ────────────────────────────────────────")
    for stage in ("raw", "clean", "final"):
        sc = spark.get("row_counts", {}).get(stage, "N/A")
        rc = ray.get("row_counts", {}).get(stage, "N/A")
        match = "✓" if sc == rc else "✗ MISMATCH"
        print(f"  {stage:<8}  Spark={sc:>10,}  Ray={rc:>10,}  {match}")

    print()
    print("── Resource Usage (driver process) ─────────────────────────")
    for fw, data in [("Spark", spark), ("Ray", ray)]:
        before = data.get("resources", {}).get("before", {})
        after  = data.get("resources", {}).get("after", {})
        print(f"  {fw}:  CPU before={before.get('cpu_pct','?')}%  after={after.get('cpu_pct','?')}%")
        print(f"        MEM before={before.get('mem_rss_mb','?')} MB  after={after.get('mem_rss_mb','?')} MB")

    print()
    print("Note: driver-side psutil is a proxy. See Spark UI (port 8080/4040)")
    print("      and Ray Dashboard (port 8265) for full cluster resource data.")
    print()


def main():
    args = sys.argv[1:]
    spark_path = args[0] if len(args) > 0 else "spark_benchmark.json"
    ray_path   = args[1] if len(args) > 1 else "ray_benchmark.json"

    try:
        spark_data = load(spark_path)
    except FileNotFoundError:
        print(f"Error: {spark_path} not found. Run spark_clean.py first.")
        sys.exit(1)

    try:
        ray_data = load(ray_path)
    except FileNotFoundError:
        print(f"Error: {ray_path} not found. Run ray_clean.py first.")
        sys.exit(1)

    print_report(spark_data, ray_data)


if __name__ == "__main__":
    main()
