#!/usr/bin/env bash
# Start Airflow WITHOUT Docker. Keep this terminal open while you work.
# Usage:  bash scripts/run_airflow_local.sh
# If Postgres is not on localhost, set PG_URL first, e.g.
#   export PG_URL=postgres://dwh:dwh@172.25.48.1:5432/pharma_dwh
REPO="$(cd "$(dirname "$0")/.." && pwd)"
source "$REPO/.venv/bin/activate"
export AIRFLOW_HOME="$HOME/airflow"
export AIRFLOW__CORE__DAGS_FOLDER="$REPO/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=True
export AIRFLOW__SCHEDULER__DAG_DIR_LIST_INTERVAL=30
export PYTHONPATH="$REPO/include"
export PHARMA_DATA_DIR="$REPO/data"
export PHARMA_SQL_DIR="$REPO/sql"
export AIRFLOW_CONN_PHARMA_DWH="${PG_URL:-postgres://dwh:dwh@localhost:5432/pharma_dwh}"
echo "Warehouse connection: $AIRFLOW_CONN_PHARMA_DWH"
echo "Airflow UI: http://localhost:8080   user: admin   password: see $AIRFLOW_HOME/standalone_admin_password.txt"
airflow standalone
