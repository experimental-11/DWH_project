#!/usr/bin/env bash
# One-time install of Airflow WITHOUT Docker (Ubuntu/WSL, Linux or Mac).
# Usage (from anywhere):  bash scripts/install_airflow_local.sh
set -e
REPO="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO"
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
PY=$(python -c 'import sys; print(f"{sys.version_info[0]}.{sys.version_info[1]}")')
pip install "apache-airflow[postgres]==2.10.5" pandas \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-${PY}.txt"
echo "Airflow installed. Now run:  bash scripts/run_airflow_local.sh"
