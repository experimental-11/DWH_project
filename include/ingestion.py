"""Data Ingestion Layer (batch).

File watcher + loader:
  * data/incoming/ is the landing zone. Any *.csv dropped there is picked up.
  * Each file is COPY'd into staging.<table> (all columns TEXT = raw ODS layer) with lineage
    columns: _ingest_id (sequence), _batch_id, _source_file, _loaded_at.
  * Every file load is logged to audit.batch_load_log (duration, rows, status) -> batch dashboard.
  * Loaded files move to data/archive/, failed ones to data/failed/.
"""
from __future__ import annotations

import csv
import io
import shutil
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from config import ARCHIVE_DIR, FAILED_DIR, INCOMING_DIR, SOURCES, source_for_file
from db import cursor, get_conn, to_snake


def pending_files(directory: Path = INCOMING_DIR) -> list[Path]:
    """The 'file watcher': returns complete CSV files waiting in the landing zone."""
    directory.mkdir(parents=True, exist_ok=True)
    return sorted(p for p in directory.glob("*.csv") if source_for_file(p.name))


def files_waiting(directory: str | None = None) -> bool:
    """Used by the Airflow PythonSensor: True as soon as at least one file has landed."""
    return len(pending_files(Path(directory) if directory else INCOMING_DIR)) > 0


def read_header(path: Path) -> list[str]:
    with open(path, newline="", encoding="utf-8-sig") as fh:
        return next(csv.reader(fh))


def ensure_staging_table(cur, table: str, columns: list[str]):
    """Create staging.<table> (all TEXT) or add any new columns that appeared in the file."""
    cols_sql = ", ".join(f'"{c}" TEXT' for c in columns)
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS staging.{table} (
            _ingest_id   BIGSERIAL PRIMARY KEY,
            _batch_id    TEXT,
            _source_file TEXT,
            _loaded_at   TIMESTAMPTZ DEFAULT now(),
            {cols_sql}
        )""")
    cur.execute("SELECT column_name FROM information_schema.columns "
                "WHERE table_schema='staging' AND table_name=%s", (table,))
    existing = {r[0] for r in cur.fetchall()}
    for c in columns:
        if c not in existing:
            cur.execute(f'ALTER TABLE staging.{table} ADD COLUMN "{c}" TEXT')


def load_file(path: Path, batch_id: str) -> dict:
    key = source_for_file(path.name)
    table = SOURCES[key]["table"]
    started = datetime.now(timezone.utc)
    t0 = time.perf_counter()
    rows, status, err = 0, "success", None

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            columns = [to_snake(c) for c in read_header(path)]
            ensure_staging_table(cur, table, columns)
            col_list = ", ".join(f'"{c}"' for c in columns)
            col_defs = ", ".join(f'"{c}" TEXT' for c in columns)
            cur.execute(f"CREATE TEMP TABLE _landing ({col_defs}) ON COMMIT DROP")
            with open(path, "r", encoding="utf-8-sig", newline="") as fh:
                cur.copy_expert(f"COPY _landing ({col_list}) FROM STDIN WITH (FORMAT csv, HEADER true)", fh)
            cur.execute(f"""
                INSERT INTO staging.{table} (_batch_id, _source_file, {col_list})
                SELECT %s, %s, {col_list} FROM _landing""", (batch_id, path.name))
            rows = cur.rowcount
        conn.commit()
    except Exception as exc:
        conn.rollback()
        status, err = "failed", str(exc)[:2000]
    finally:
        conn.close()

    duration = time.perf_counter() - t0
    with cursor() as cur:
        cur.execute("""
            INSERT INTO audit.batch_load_log
                (batch_id, source_key, source_file, target_table, started_at, finished_at,
                 duration_s, rows_loaded, file_size_mb, status, error_message)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (batch_id, key, path.name, f"staging.{table}", started, datetime.now(timezone.utc),
             duration, rows, path.stat().st_size / 1_048_576, status, err))

    dest = ARCHIVE_DIR if status == "success" else FAILED_DIR
    dest.mkdir(parents=True, exist_ok=True)
    shutil.move(str(path), dest / path.name)
    return {"file": path.name, "table": table, "rows": rows, "seconds": round(duration, 2),
            "status": status, "error": err}


def ingest_pending(**context) -> list[dict]:
    """Airflow task: load every file currently in the landing zone."""
    batch_id = (context.get("run_id") if context else None) or f"manual_{uuid.uuid4().hex[:8]}"
    results = [load_file(p, batch_id) for p in pending_files()]
    for r in results:
        print(r)
    failed = [r for r in results if r["status"] != "success"]
    if failed:
        # the other files are loaded and logged; fail the task so the problem is visible in Airflow
        raise RuntimeError(f"{len(failed)} of {len(results)} file loads failed (moved to data/failed): "
                           + "; ".join(f"{r['file']}: {r['error']}" for r in failed))
    return results
