[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-22041afd0340ce965d47ae6ef1cefeee28c7c493a6346c4f15d667ab976d596c.svg)](https://classroom.github.com/a/pesOmrUm)

# DA5402 A8 — Spark vs. Ray: The Data Engineering Duel

Identical NYC Taxi data preprocessing pipelines built in **PySpark** and **Ray Data**, deployed on a 2-node cluster, and benchmarked head-to-head.

| | Spark | Ray |
|---|---|---|
| Total time | **62.4 s** | 1414.8 s |
| Cluster | `spark://10.200.166.150:7077` | head at `10.200.166.150:6379` |
| Nodes | 2 × 16 CPUs | 2 × 16 CPUs |
| Rows processed | 19,493,620 | 19,493,620 |

> Ray was slower due to OOM retries on the 7.2 GB worker node. See [report.md](report.md) for the full analysis.

## Video Demo

[Watch the project walkthrough on Google Drive](https://drive.google.com/drive/folders/1v7WoKOyfBd9Y5Ay2ErRgANKorOiW6m_C?usp=sharing)

---

## Repository Structure

```
.
├── spark_clean.py          # PySpark pipeline (ingest → cleanse → transform → export)
├── ray_clean.py            # Ray Data pipeline (identical logic)
├── download_data.py        # Download NYC TLC yellow taxi Parquet files
├── benchmark_summary.py    # Side-by-side comparison of both benchmark JSON outputs
├── setup_spark_cluster.sh  # Start/stop Spark Master and Worker
├── setup_ray_cluster.sh    # Start/stop Ray Head and Worker
├── requirements.txt        # pip dependencies for conda env mlops-8
├── report.md               # Full assignment report (5 sections, framework recommendation)
├── results.md              # Detailed benchmark results and analysis
├── USER_GUIDE.md           # Step-by-step commands for both nodes
└── imgs/                   # Cluster UI screenshots (Spark Master, App UI, Ray Dashboard)
```

---

## Quick Start

### 1. Environment

```bash
conda create -n mlops-8 python=3.13.13 openjdk -c conda-forge -y
conda activate mlops-8
pip install -r requirements.txt
```

### 2. Download Data

```bash
python download_data.py --year 2023 --months 1 2 3 4 5 6
# Downloads ~305 MB to ./data/raw/
```

### 3. Run Locally (single machine)

```bash
# Spark
python spark_clean.py --master "local[*]"

# Ray
python ray_clean.py

# Compare
python benchmark_summary.py spark_benchmark.json ray_benchmark.json
```

### 4. Run on 2-Node Cluster

See [USER_GUIDE.md](USER_GUIDE.md) for the full step-by-step instructions for both machines.

---

## Pipeline Steps

Both `spark_clean.py` and `ray_clean.py` implement the same four steps:

1. **Ingest** — read 6 monthly Parquet files, cast columns to canonical types
2. **Cleanse** — drop nulls, deduplicate on trip key fields, filter zero-distance / zero-passenger trips
3. **Transform** — broadcast-join taxi zone lookup (pickup + dropoff zone names), compute `duration_seconds`, `pickup_hour`, and `avg_speed_mph` via a UDF
4. **Export** — write result to Parquet

### UDF Design (deliberate contrast)

| Framework | UDF | Cost |
|---|---|---|
| Spark | `@udf(DoubleType())` — row-by-row | 18.3 M JVM ↔ Python round-trips → **11.7 s** |
| Ray | `map_batches` + `np.where` — vectorised | Single C-level NumPy kernel, no JVM |

---

## Cluster Screenshots

| Screenshot | Description |
|---|---|
| ![](imgs/spark-master-ui-2.png) | Spark Master — 2 workers ALIVE |
| ![](imgs/ray-dashboard.png) | Ray Dashboard — ALIVE×2, 32 CPUs |

---

## Results Summary

| Step | Spark (s) | Ray (s) |
|---|---|---|
| Ingest | 4.8 | 2.3 |
| Cleanse | 13.5 | 501.8 |
| Transform + UDF | 12.5 | 474.6 |
| Export | 31.5 | 436.1 |
| **Total** | **62.4** | **1414.8** |

Full analysis in [results.md](results.md) · Full report in [report.md](report.md)
