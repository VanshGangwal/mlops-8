#!/usr/bin/env bash
# Setup script for a 2-node Ray cluster using Ray installed in mlops-8.
#
# Usage:
#   NODE 1 (Head):   ./setup_ray_cluster.sh head
#   NODE 2 (Worker): HEAD_IP=<node1-ip> ./setup_ray_cluster.sh worker
#   Either node:     ./setup_ray_cluster.sh stop
#
# Screenshot to take:
#   Ray Dashboard → http://HEAD_IP:8265   (cluster resource view)
#
# To connect the pipeline to the cluster:
#   python ray_clean.py --address ray://HEAD_IP:10001

set -euo pipefail

CONDA_ENV="mlops-8"

# Use the conda env's binaries directly — avoids pyenv shim interference
CONDA_BASE=$(conda info --base 2>/dev/null || echo "$HOME/miniconda3")
CONDA_BIN="$CONDA_BASE/envs/$CONDA_ENV/bin"

if [[ ! -d "$CONDA_BIN" ]]; then
    echo "Error: conda env '$CONDA_ENV' not found at $CONDA_BIN"
    echo "Run: conda create -n mlops-8 python=3.13.2 openjdk -c conda-forge -y"
    exit 1
fi

export PATH="$CONDA_BIN:$PATH"

MODE="${1:-help}"
HEAD_IP="${HEAD_IP:-$(hostname -I | awk '{print $1}')}"
GCS_PORT="${GCS_PORT:-6379}"
RAY_CLIENT_PORT="${RAY_CLIENT_PORT:-10001}"
DASHBOARD_PORT="${DASHBOARD_PORT:-8265}"

case "$MODE" in
  head)
    echo "Starting Ray Head Node on this machine ..."
    ray start \
      --head \
      --port="$GCS_PORT" \
      --ray-client-server-port="$RAY_CLIENT_PORT" \
      --dashboard-host=0.0.0.0 \
      --dashboard-port="$DASHBOARD_PORT"
    echo ""
    echo "Ray Dashboard  : http://${HEAD_IP}:${DASHBOARD_PORT}"
    echo "Ray Client URL : ray://${HEAD_IP}:${RAY_CLIENT_PORT}"
    echo "GCS address    : ${HEAD_IP}:${GCS_PORT}"
    echo ""
    echo "On the worker node run:"
    echo "  HEAD_IP=${HEAD_IP} ./setup_ray_cluster.sh worker"
    echo ""
    echo "To run the pipeline:"
    echo "  conda activate mlops-8"
    echo "  python ray_clean.py --address ray://${HEAD_IP}:${RAY_CLIENT_PORT}"
    ;;

  worker)
    echo "Connecting Ray Worker → ${HEAD_IP}:${GCS_PORT} ..."
    ray start --address="${HEAD_IP}:${GCS_PORT}"
    echo ""
    echo "Worker node joined the cluster at ${HEAD_IP}:${GCS_PORT}"
    echo "Check the dashboard: http://${HEAD_IP}:${DASHBOARD_PORT}"
    ;;

  stop)
    echo "Stopping Ray on this node ..."
    ray stop
    echo "Ray stopped."
    ;;

  *)
    echo "Usage: $0 {head|worker|stop}"
    echo "  head    — start Ray Head Node on this machine"
    echo "  worker  — join as worker (set HEAD_IP env var first)"
    echo "  stop    — stop Ray processes on this machine"
    exit 1
    ;;
esac
