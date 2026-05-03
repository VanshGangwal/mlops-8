#!/usr/bin/env bash
# Setup script for a 2-node Spark cluster using PySpark installed in mlops-8.
#
# Usage:
#   NODE 1 (Master):  ./setup_spark_cluster.sh master
#   NODE 2 (Worker):  MASTER_IP=<node1-ip> ./setup_spark_cluster.sh worker
#   Either node:      ./setup_spark_cluster.sh stop
#
# Screenshots to take:
#   Spark Master UI  → http://MASTER_IP:8080   (cluster view, 2 workers)
#   Spark App UI     → http://MASTER_IP:4040   (while spark_clean.py is running)

set -euo pipefail

CONDA_ENV="mlops-8"
SPARK_VERSION="3.5.5"

# Use the conda env's Python directly — avoids pyenv shim interference
CONDA_BASE=$(conda info --base 2>/dev/null || echo "$HOME/miniconda3")
CONDA_PYTHON="$CONDA_BASE/envs/$CONDA_ENV/bin/python"

if [[ ! -x "$CONDA_PYTHON" ]]; then
    echo "Error: conda env '$CONDA_ENV' not found at $CONDA_PYTHON"
    echo "Run: conda create -n mlops-8 python=3.13.2 openjdk -c conda-forge -y"
    exit 1
fi

# SPARK_HOME = pyspark package directory (includes JARs, bin, sbin)
SPARK_HOME=$("$CONDA_PYTHON" -c "import pyspark, os; print(os.path.dirname(pyspark.__file__))")
export SPARK_HOME

# The pip pyspark package omits standalone cluster scripts; fetch them from GitHub if missing.
SBIN_BASE="https://raw.githubusercontent.com/apache/spark/refs/tags/v${SPARK_VERSION}/sbin"
for script in start-master.sh stop-master.sh start-worker.sh stop-worker.sh start-slaves.sh; do
    if [[ ! -f "${SPARK_HOME}/sbin/${script}" ]]; then
        echo "Fetching ${script} from GitHub ..."
        curl -fsSL "${SBIN_BASE}/${script}" -o "${SPARK_HOME}/sbin/${script}"
        chmod +x "${SPARK_HOME}/sbin/${script}"
    fi
done

# Java home from the conda env
JAVA_HOME=$("$CONDA_PYTHON" -c "
import subprocess
r = subprocess.run(['java', '-XshowSettings:property', '-version'],
                   capture_output=True, text=True)
for line in r.stderr.splitlines():
    if 'java.home' in line:
        print(line.split('=')[1].strip())
        break
")
export JAVA_HOME
export PATH="$SPARK_HOME/bin:$SPARK_HOME/sbin:$JAVA_HOME/bin:$PATH"

# Tell Spark to use the conda env's Python for drivers and workers
export PYSPARK_PYTHON="$CONDA_PYTHON"
export PYSPARK_DRIVER_PYTHON="$CONDA_PYTHON"

MODE="${1:-help}"
MASTER_IP="${MASTER_IP:-127.0.0.1}"
MASTER_PORT="${MASTER_PORT:-7077}"
MASTER_URL="spark://${MASTER_IP}:${MASTER_PORT}"

case "$MODE" in
  master)
    echo "Starting Spark Master on this node ..."
    "$SPARK_HOME/sbin/start-master.sh" \
      --host "$MASTER_IP" \
      --port "$MASTER_PORT" \
      --webui-port 8080
    echo ""
    echo "Master URL  : $MASTER_URL"
    echo "Master UI   : http://${MASTER_IP}:8080"
    echo "App UI      : http://${MASTER_IP}:4040  (visible while a job runs)"
    echo ""
    echo "On the worker node run:"
    echo "  MASTER_IP=${MASTER_IP} ./setup_spark_cluster.sh worker"
    echo ""
    echo "To run the pipeline:"
    echo "  conda activate mlops-8"
    echo "  python spark_clean.py --master $MASTER_URL"
    ;;

  worker)
    echo "Starting Spark Worker → $MASTER_URL ..."
    "$SPARK_HOME/sbin/start-worker.sh" "$MASTER_URL" \
      --webui-port 8081
    echo ""
    echo "Worker registered with $MASTER_URL"
    echo "Worker UI   : http://$(hostname -I | awk '{print $1}'):8081"
    ;;

  stop)
    echo "Stopping Spark ..."
    "$SPARK_HOME/sbin/stop-worker.sh" 2>/dev/null || true
    "$SPARK_HOME/sbin/stop-master.sh" 2>/dev/null || true
    echo "Spark stopped."
    ;;

  *)
    echo "Usage: $0 {master|worker|stop}"
    echo "  master  — start Spark Master on this machine"
    echo "  worker  — start Spark Worker (set MASTER_IP env var first)"
    echo "  stop    — stop Spark processes on this machine"
    exit 1
    ;;
esac
