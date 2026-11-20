"""Database helpers shared by all tasks."""
from __future__ import annotations

import os
import re
import time
from contextlib import contextmanager

from config import POSTGRES_CONN_ID, SQL_DIR


def get_conn():
    """psycopg2 connection. Inside Airflow we use the `pharma_dwh` connection;
    outside Airflow set PHARMA_DWH_DSN, e.g. postgresql://dwh:dwh@localhost:5433/pharma_dwh"""
    dsn = os.environ.get("PHARMA_DWH_DSN")
    if dsn:
        import psycopg2
        return psycopg2.connect(dsn)
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    return PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).get_conn()


@contextmanager
def cursor():
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            yield cur
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def to_snake(name: str) -> str:
    """'MedicineID' -> 'medicine_id', 'Temp (C)' -> 'temp_c'."""
    name = name.strip().lstrip("﻿")
    name = re.sub(r"[^0-9a-zA-Z]+", "_", name)
    name = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
    name = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", name)
    name = re.sub(r"_+", "_", name).strip("_").lower()
    return name or "col"


def run_sql_file(filename: str, run_id: str | None = None) -> float:
    """Execute one file from sql/ in a single transaction and log it to audit.etl_step_log."""
    sql = (SQL_DIR / filename).read_text()
    start = time.perf_counter()
    status, err = "success", None
    try:
        with cursor() as cur:
            cur.execute(sql)
    except Exception as exc:  # log, then re-raise so Airflow marks the task failed
        status, err = "failed", str(exc)[:2000]
        raise
    finally:
        dur = time.perf_counter() - start
        try:
            with cursor() as cur:
                cur.execute(
                    "INSERT INTO audit.etl_step_log(run_id, step_name, duration_s, status, error_message) "
                    "VALUES (%s,%s,%s,%s,%s)", (run_id, filename, dur, status, err))
        except Exception:
            pass  # audit schema may not exist yet on the very first DDL run
    return dur
