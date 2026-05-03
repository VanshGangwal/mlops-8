# User Guide — Assignment 8: Spark vs Ray Benchmark

Two machines are used throughout this guide:

| Label | IP | User | Role |
|---|---|---|---|
| **Node 1** (yours) | `10.200.166.150` | `vansh-gangwal` | Spark Master / Ray Head / driver |
| **Node 2** (partner) | `10.200.166.9` | `aditya` | Spark Worker / Ray Worker |

Project path on Node 1: `/home/vansh-gangwal/College Folder/MLOps/assignment-8-VanshGangwal`  
Project path on Node 2: `/home/aditya/Desktop/mlops-8`

Conda env name on both machines: `mlops-8` (Python 3.13.13)

---

## 0. One-time Setup

### Node 1 — install dependencies
```bash
conda activate mlops-8
pip install -r requirements.txt
```

### Node 2 — install dependencies
```bash
conda activate mlops-8
pip install pyspark==3.5.5 "ray[data]" "ray[client]" pyarrow pandas psutil
```

---

## 1. Download Data (Node 1 only)

```bash
cd "/home/vansh-gangwal/College Folder/MLOps/assignment-8-VanshGangwal"
conda activate mlops-8
python download_data.py --year 2023 --months 1 2 3 4 5 6
```

Downloads ~305 MB of NYC TLC yellow taxi Parquet files plus `taxi_zone_lookup.csv` into `./data/raw/`.

---

## 2. Spark — 2-Node Cluster

### Node 1 — start master
```bash
cd "/home/vansh-gangwal/College Folder/MLOps/assignment-8-VanshGangwal"
MASTER_IP=10.200.166.150 bash setup_spark_cluster.sh master
```

Master UI is now at **http://10.200.166.150:8080**.

### Node 2 — sync data then start worker
```bash
# Sync data from Node 1 (run this on Node 1)
rsync -az /tmp/ray-taxi-data/ aditya@10.200.166.9:/tmp/ray-taxi-data/

# Then on Node 2:
cd /home/aditya/Desktop/mlops-8
MASTER_IP=10.200.166.150 bash setup_spark_cluster.sh worker
```

Worker UI is now at **http://10.200.166.9:8081**.  
The master UI at port 8080 should show **2 workers alive**.

### Node 1 — run the Spark pipeline
```bash
cd "/home/vansh-gangwal/College Folder/MLOps/assignment-8-VanshGangwal"
conda activate mlops-8
~/miniconda3/envs/mlops-8/bin/python spark_clean.py \
  --master spark://10.200.166.150:7077 \
  --data-dir ./data/raw \
  --zone-csv ./data/raw/taxi_zone_lookup.csv \
  --output ./data/spark_output \
  --benchmark-out spark_benchmark.json
```

App UI is live at **http://10.200.166.150:4040** while the job runs.  
Results are written to `spark_benchmark.json`.

### Stop Spark (either node)
```bash
# Node 1
MASTER_IP=10.200.166.150 bash setup_spark_cluster.sh stop

# Node 2
bash setup_spark_cluster.sh stop
```

---

## 3. Ray — 2-Node Cluster

### Node 1 — start head node
```bash
cd "/home/vansh-gangwal/College Folder/MLOps/assignment-8-VanshGangwal"
bash setup_ray_cluster.sh head
```

Dashboard is now at **http://10.200.166.150:8265**.

### Node 2 — sync data (if not done already) then start worker
```bash
# Sync data from Node 1 (run this on Node 1)
rsync -az /tmp/ray-taxi-data/ aditya@10.200.166.9:/tmp/ray-taxi-data/

# Then on Node 2:
HEAD_IP=10.200.166.150 bash /home/aditya/Desktop/mlops-8/setup_ray_cluster.sh worker
```

The Ray dashboard at port 8265 should show **2 alive nodes** (each with 16 CPUs).

### Node 1 — run the Ray pipeline
```bash
# Copy data to the shared path both nodes can read
mkdir -p /tmp/ray-taxi-data
cp "./data/raw/"*.parquet /tmp/ray-taxi-data/
cp ./data/raw/taxi_zone_lookup.csv /tmp/ray-taxi-data/
rsync -az /tmp/ray-taxi-data/ aditya@10.200.166.9:/tmp/ray-taxi-data/

cd "/home/vansh-gangwal/College Folder/MLOps/assignment-8-VanshGangwal"
~/miniconda3/envs/mlops-8/bin/python ray_clean.py \
  --address auto \
  --data-dir /tmp/ray-taxi-data \
  --zone-csv /tmp/ray-taxi-data/taxi_zone_lookup.csv \
  --output /tmp/ray-taxi-output \
  --benchmark-out ray_benchmark.json
```

> **Why `/tmp/ray-taxi-data/`?** Ray distributes read tasks to any worker node. Both nodes must have the data at the same absolute path. `/tmp/` is always writable on every machine.

Results are written to `ray_benchmark.json`.

### Stop Ray (either node)
```bash
# Node 1
bash setup_ray_cluster.sh stop

# Node 2
~/miniconda3/envs/mlops-8/bin/ray stop
```

---

## 4. Compare Results

```bash
cd "/home/vansh-gangwal/College Folder/MLOps/assignment-8-VanshGangwal"
~/miniconda3/envs/mlops-8/bin/python benchmark_summary.py spark_benchmark.json ray_benchmark.json
```

Prints a side-by-side table of per-step timings, speedup ratios, UDF overhead, row-count parity, and driver-side resource usage.

---

## 5. Pipeline Arguments Reference

### `spark_clean.py`
| Argument | Default | Description |
|---|---|---|
| `--master` | `local[*]` (or `$SPARK_MASTER`) | Spark master URL |
| `--data-dir` | `./data/raw` | Directory containing `.parquet` files |
| `--zone-csv` | `./data/raw/taxi_zone_lookup.csv` | Taxi zone lookup CSV |
| `--output` | `./data/spark_output` | Output directory for Parquet |
| `--benchmark-out` | `spark_benchmark.json` | Benchmark result file |

### `ray_clean.py`
| Argument | Default | Description |
|---|---|---|
| `--address` | `None` (local) | Ray cluster address, e.g. `auto` or `ray://10.200.166.150:10001` |
| `--data-dir` | `./data/raw` | Directory containing `.parquet` files |
| `--zone-csv` | `./data/raw/taxi_zone_lookup.csv` | Taxi zone lookup CSV |
| `--output` | `./data/ray_output` | Output directory for Parquet |
| `--benchmark-out` | `ray_benchmark.json` | Benchmark result file |

### `download_data.py`
| Argument | Default | Description |
|---|---|---|
| `--year` | `2023` | Year to download |
| `--months` | `1 2 … 12` | Months (space-separated) |
| `--dest` | `./data/raw` | Download destination |

---

## 6. UIs to Check / Screenshot

| UI | URL | What to look for |
|---|---|---|
| Spark Master | http://10.200.166.150:8080 | 2 workers registered |
| Spark App | http://10.200.166.150:4040 | Stage progress (while job runs) |
| Spark Worker | http://10.200.166.9:8081 | Executor status |
| Ray Dashboard | http://10.200.166.150:8265 | 2 alive nodes, CPU utilisation |

---

## 7. Local Smoke-Test (no partner machine needed)

```bash
cd "/home/vansh-gangwal/College Folder/MLOps/assignment-8-VanshGangwal"
conda activate mlops-8

# Spark — local mode
~/miniconda3/envs/mlops-8/bin/python spark_clean.py --master "local[*]"

# Ray — local mode (omit --address)
~/miniconda3/envs/mlops-8/bin/python ray_clean.py
```
