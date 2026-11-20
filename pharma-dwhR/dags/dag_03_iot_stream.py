"""DAG 03 - Streaming ingestion of IoT sensor readings (micro-batches every 2 minutes).

Files dropped in data/incoming_stream/ are replayed as a stream into staging.iot_readings;
throughput, latency and malformed counts go to audit.stream_metrics.
(The IoT data mart itself is Phase 3 - here we only ingest and measure.)
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor


def _waiting():
    from config import STREAM_DIR
    from ingestion import files_waiting
    return files_waiting(str(STREAM_DIR))


def _setup(**context):
    from db import run_sql_file
    run_sql_file("01_schemas_and_audit.sql", context["run_id"])


def _stream(**context):
    from streaming import stream_pending
    return stream_pending(**context)


with DAG(
    dag_id="03_iot_stream_ingestion",
    description="Micro-batch streaming of IoT sensor readings into staging",
    start_date=datetime(2026, 11, 1),
    schedule="*/2 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "dwh-team", "retries": 1, "retry_delay": timedelta(seconds=20)},
    tags=["phase1", "streaming"],
) as dag:
    setup = PythonOperator(task_id="setup_schemas", python_callable=_setup)
    wait = PythonSensor(task_id="wait_for_stream_files", python_callable=_waiting,
                        poke_interval=15, timeout=100, mode="reschedule", soft_fail=True)
    stream = PythonOperator(task_id="stream_micro_batches", python_callable=_stream)
    setup >> wait >> stream
