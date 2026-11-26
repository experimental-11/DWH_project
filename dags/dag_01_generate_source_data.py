"""DAG 01 - Data Oversampling Layer.

Generates one new month of data for each of the 7 source files (one task per file),
drops the delta files into the landing zones and logs the statistics.
Trigger it manually (play button) whenever you want a refresh; the pipeline DAG picks the
files up automatically. Use "Trigger DAG w/ config" to change the scale, e.g. {"scale": 2}.
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from config import SOURCES

default_args = {"owner": "dwh-team", "retries": 1}


def _generate(source_key, **context):
    from generator import generate_source
    scale = float(context["params"].get("scale", 1.0))
    # run_id makes a retry finish the same batch instead of generating another month
    return generate_source(source_key, scale=scale, run_id=context["run_id"])


def _log_stats(**context):
    from db import cursor, run_sql_file
    run_sql_file("01_schemas_and_audit.sql", context["run_id"])   # make sure audit tables exist
    ti = context["ti"]
    rows = []
    for key in SOURCES:
        s = ti.xcom_pull(task_ids=f"generate_{key}")
        if s:
            rows.append((context["run_id"], s["source"], s["rows_before"], s["rows_generated"],
                         s["bad_rows_injected"], s["rows_after"], s["growth_pct"], s["target_period"],
                         s["method"], s["output_file"]))
    with cursor() as cur:
        cur.executemany("""INSERT INTO audit.generation_log
            (run_id, source_key, rows_before, rows_generated, bad_rows_injected, rows_after,
             growth_pct, target_period, method, output_file)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", rows)
    for r in rows:
        print(r)


with DAG(
    dag_id="01_generate_source_data",
    description="Oversample + synthesize a new batch for each of the 7 source files",
    start_date=datetime(2026, 11, 1),
    schedule=None,                 # manual trigger = controlled refreshes for the demo
    catchup=False,
    params={"scale": 1.0},
    default_args=default_args,
    tags=["phase1", "generator"],
) as dag:
    log_stats = PythonOperator(task_id="log_generation_stats", python_callable=_log_stats)

    tasks = {
        key: PythonOperator(
            task_id=f"generate_{key}",
            python_callable=_generate,
            op_kwargs={"source_key": key},
        )
        for key in SOURCES
    }
    # master data first: new SKUs must exist before the sales batch can "launch" them
    master = next(k for k, s in SOURCES.items() if s.get("master"))
    for key, task in tasks.items():
        if key != master:
            tasks[master] >> task
        task >> log_stats
