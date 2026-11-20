"""Data Ingestion Layer (streaming) - IoT sensor readings.

Simulates a stream: files in data/incoming_stream/ are replayed as micro-batches.
  * each micro-batch is stamped with an emit time (= "source row creation" time)
  * rows are validated (non-null reading ID, parsable timestamp); bad rows are counted as
    malformed and written to audit.rejected_rows
  * good rows are inserted into staging.iot_readings; _committed_at is recorded
  * per micro-batch metrics go to audit.stream_metrics -> streaming dashboard
    throughput = events / seconds, latency = commit time - emit time
"""
from __future__ import annotations

import json
import shutil
import time
from datetime import datetime, timezone

import pandas as pd
from psycopg2.extras import execute_values

from config import ARCHIVE_DIR, FAILED_DIR, SOURCES, STREAM, STREAM_DIR
from db import cursor, get_conn, to_snake
from generator import detect_date_columns
from ingestion import ensure_staging_table, pending_files

KEY = "07_iot_streaming_data"


def stream_pending(**context):
    run_id = (context.get("run_id") if context else None) or f"manual_{int(time.time())}"
    spec = SOURCES[KEY]
    table = spec["table"]
    pk = to_snake(spec["pk"] if isinstance(spec["pk"], str) else spec["pk"][0])
    size = STREAM["micro_batch_size"]
    min_seconds_per_batch = size / STREAM["max_events_per_sec"]
    summary = []

    for path in pending_files(STREAM_DIR):
        conn = get_conn()
        total, bad_total, t_file = 0, 0, time.perf_counter()
        try:
            first = pd.read_csv(path, dtype=str, keep_default_na=False, nrows=500)
            first.columns = [to_snake(c) for c in first.columns]
            date_cols = detect_date_columns(first)
            ts_col = spec.get("date_col") and to_snake(spec["date_col"]) or next(iter(date_cols), None)
            with conn.cursor() as cur:
                ensure_staging_table(cur, table, list(first.columns))
                cur.execute(f"ALTER TABLE staging.{table} "
                            "ADD COLUMN IF NOT EXISTS _emitted_at TIMESTAMPTZ, "
                            "ADD COLUMN IF NOT EXISTS _committed_at TIMESTAMPTZ DEFAULT clock_timestamp()")
            conn.commit()

            for chunk_no, chunk in enumerate(pd.read_csv(path, dtype=str, keep_default_na=False,
                                                        chunksize=size)):
                tick = time.perf_counter()
                emitted = datetime.now(timezone.utc)
                chunk.columns = [to_snake(c) for c in chunk.columns]
                ok = chunk[pk].str.strip() != ""
                if ts_col:
                    ok &= pd.to_datetime(chunk[ts_col].where(chunk[ts_col] != ""), errors="coerce",
                                         format=date_cols.get(ts_col)).notna()
                good, bad = chunk[ok], chunk[~ok]
                cols = list(chunk.columns)
                with conn.cursor() as cur:
                    if len(good):
                        execute_values(
                            cur,
                            f"INSERT INTO staging.{table} (_batch_id, _source_file, _emitted_at, "
                            f"{', '.join(chr(34) + c + chr(34) for c in cols)}) VALUES %s",
                            [(run_id, path.name, emitted, *row) for row in good.itertuples(index=False)],
                            page_size=1000)
                    if len(bad):
                        execute_values(
                            cur,
                            "INSERT INTO audit.rejected_rows (source_table, record_id, reject_reason, raw_row) VALUES %s",
                            [(f"staging.{table}", r.get(pk) or None, "null key or bad timestamp",
                              json.dumps(r)) for r in bad.to_dict("records")])
                conn.commit()
                committed = datetime.now(timezone.utc)
                elapsed = time.perf_counter() - tick
                if elapsed < min_seconds_per_batch:          # throttle to the configured rate
                    time.sleep(min_seconds_per_batch - elapsed)
                    elapsed = time.perf_counter() - tick
                with cursor() as cur:
                    cur.execute("""
                        INSERT INTO audit.stream_metrics
                          (run_id, source_file, micro_batch_no, window_start, window_end, events_ingested,
                           malformed_count, duration_s, events_per_sec, avg_latency_s, max_latency_s)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                        (run_id, path.name, chunk_no, emitted, committed, len(good), len(bad), elapsed,
                         len(good) / elapsed if elapsed else None,
                         (committed - emitted).total_seconds(), (committed - emitted).total_seconds()))
                total += len(good)
                bad_total += len(bad)
            dest = ARCHIVE_DIR
        except Exception as exc:
            conn.rollback()
            print(f"stream failed for {path.name}: {exc}")
            dest = FAILED_DIR
        finally:
            conn.close()
        dest.mkdir(parents=True, exist_ok=True)
        shutil.move(str(path), dest / path.name)
        summary.append({"file": path.name, "events": total, "malformed": bad_total,
                        "seconds": round(time.perf_counter() - t_file, 1)})
        print(summary[-1])
    return summary
