"""DAG 02 - Phase 1 Sales & Demand Intelligence pipeline (up to the query layer).

  setup        : create/upgrade schemas & tables (idempotent)
  ingestion    : file watcher (sensor) -> COPY every landed CSV into staging (ODS)
  transform    : mapping views -> dimensions (SCD1, SCD2, junk) -> facts (with validation)
  query_layer  : indexes / partitions / materialized views -> refresh
  observability: storage metrics + query benchmarks for the DE dashboards

Runs every 5 minutes; if no new file has landed the run is skipped (sensor soft-fails).
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.task_group import TaskGroup

default_args = {
    "owner": "dwh-team",
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
}


def sql_task(filename):
    def _run(**context):
        from db import run_sql_file
        secs = run_sql_file(filename, context["run_id"])
        print(f"{filename} finished in {secs:.2f}s")
    return _run


def _files_waiting():
    from ingestion import files_waiting
    return files_waiting()


def _ingest(**context):
    from ingestion import ingest_pending
    return ingest_pending(**context)


def _storage(**context):
    from metrics import capture_storage_metrics
    return capture_storage_metrics(**context)


def _bench(**context):
    from metrics import benchmark_queries
    return benchmark_queries(**context)


with DAG(
    dag_id="02_sdi_pipeline",
    description="Phase 1: ingest -> staging -> star schema -> query layer (Postgres)",
    start_date=datetime(2026, 11, 1),
    schedule="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["phase1", "sdi"],
) as dag:

    setup = PythonOperator(task_id="setup_schemas", python_callable=sql_task("01_schemas_and_audit.sql"))
    ddl = PythonOperator(task_id="create_star_schema", python_callable=sql_task("02_dwh_ddl.sql"))

    with TaskGroup("ingestion") as ingestion:
        wait = PythonSensor(
            task_id="wait_for_new_files",
            python_callable=_files_waiting,
            poke_interval=20,
            timeout=240,
            mode="reschedule",
            soft_fail=True,           # no files -> rest of the run is skipped, not failed
        )
        load = PythonOperator(task_id="load_files_to_staging", python_callable=_ingest)
        wait >> load

    with TaskGroup("transform") as transform:
        views = PythonOperator(task_id="build_staging_views", python_callable=sql_task("03_staging_views.sql"))
        dim_med = PythonOperator(task_id="load_dim_medicine_scd2", python_callable=sql_task("10_load_dim_medicine.sql"))
        dim_hosp = PythonOperator(task_id="load_dim_hospital", python_callable=sql_task("11_load_dim_hospital.sql"))
        dim_misc = PythonOperator(task_id="load_dim_channel_location_junk", python_callable=sql_task("12_load_dim_sales_misc.sql"))
        f_sales = PythonOperator(task_id="load_fact_sales", python_callable=sql_task("20_load_fact_sales.sql"))
        f_demand = PythonOperator(task_id="load_fact_hospital_demand", python_callable=sql_task("21_load_fact_hospital_demand.sql"))
        views >> dim_med >> [dim_hosp, dim_misc]
        dim_misc >> f_sales
        dim_hosp >> f_demand

    with TaskGroup("query_layer") as query_layer:
        build = PythonOperator(task_id="build_indexes_and_mvs", python_callable=sql_task("30_query_layer.sql"))
        refresh = PythonOperator(task_id="refresh_materialized_views",
                                 python_callable=sql_task("31_refresh_materialized_views.sql"))
        build >> refresh

    with TaskGroup("observability") as observability:
        PythonOperator(task_id="capture_storage_metrics", python_callable=_storage)
        PythonOperator(task_id="benchmark_dashboard_queries", python_callable=_bench)

    setup >> ddl >> ingestion >> transform >> query_layer >> observability
