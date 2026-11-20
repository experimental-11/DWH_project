"""Run the whole Phase 1 pipeline with ONLY Python + PostgreSQL (no Docker, no Airflow).

    python run_pipeline.py load       # load whatever is in data/incoming (+ stream), build star schema
    python run_pipeline.py generate   # create one new month of data for all 7 files
    python run_pipeline.py refresh    # generate + load  (do this twice for the demo)
    python run_pipeline.py watch      # automatic mode: checks the landing folders every 30 s
    python run_pipeline.py status     # quick row-count check

Connection (default): postgresql://dwh:dwh@localhost:5432/pharma_dwh
Use another one with:  python run_pipeline.py load --dsn postgresql://postgres:MYPASS@localhost:5432/pharma_dwh
"""
import argparse
import os
import sys
import time
import uuid
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "include"))

STEPS = [
    ("Create schemas & audit tables",        "01_schemas_and_audit.sql"),
    ("Create star schema",                   "02_dwh_ddl.sql"),
    # -- ingestion happens here --
    ("Build staging mapping views",          "03_staging_views.sql"),
    ("Load dim_medicine (SCD Type 2)",       "10_load_dim_medicine.sql"),
    ("Load dim_hospital (SCD Type 1)",       "11_load_dim_hospital.sql"),
    ("Load channel / outlet / junk dims",    "12_load_dim_sales_misc.sql"),
    ("Load fact_sales",                      "20_load_fact_sales.sql"),
    ("Load fact_hospital_demand",            "21_load_fact_hospital_demand.sql"),
    ("Indexes, partitions, materialized views", "30_query_layer.sql"),
    ("Refresh materialized views",           "31_refresh_materialized_views.sql"),
]


def banner(text):
    print(f"\n{'=' * 70}\n {text}\n{'=' * 70}")


def run_load(run_id):
    from config import STREAM_DIR
    from db import run_sql_file
    from ingestion import files_waiting, ingest_pending
    from metrics import benchmark_queries, capture_storage_metrics
    from streaming import stream_pending

    banner(f"PIPELINE RUN {run_id}")
    for label, f in STEPS[:2]:
        print(f"  {label:<45} {run_sql_file(f, run_id):6.2f}s")

    print("\n  -- batch ingestion (data/incoming -> staging) --")
    if files_waiting():
        for r in ingest_pending(run_id=run_id):
            print(f"     {r['file']:<55} {r['rows']:>9,} rows {r['seconds']:>7.2f}s  {r['status']}"
                  + (f"  ERROR: {r['error']}" if r["error"] else ""))
    else:
        print("     no new batch files")

    print("\n  -- stream ingestion (data/incoming_stream -> staging.iot_readings) --")
    if files_waiting(str(STREAM_DIR)):
        for r in stream_pending(run_id=run_id):
            print(f"     {r['file']:<55} {r['events']:>9,} events, {r['malformed']} malformed, {r['seconds']}s")
    else:
        print("     no new stream files")

    print("\n  -- transform + query layer --")
    for label, f in STEPS[2:]:
        print(f"  {label:<45} {run_sql_file(f, run_id):6.2f}s")

    print("\n  -- observability --")
    capture_storage_metrics(run_id=run_id)
    benchmark_queries(run_id=run_id)
    show_status()


def run_generate(run_id, scale):
    from config import SOURCES
    from db import cursor, run_sql_file
    from generator import generate_source

    banner(f"GENERATING NEW DATA (scale={scale})")
    run_sql_file("01_schemas_and_audit.sql", run_id)
    rows = []
    for key in SOURCES:
        s = generate_source(key, scale=scale)
        print(f"  {key:<32} +{s['rows_generated']:>7,} rows  period={s['target_period']}  "
              f"bad={s['bad_rows_injected']}  ({s['method']})")
        rows.append((run_id, s["source"], s["rows_before"], s["rows_generated"], s["bad_rows_injected"],
                     s["rows_after"], s["growth_pct"], s["target_period"], s["method"], s["output_file"]))
    with cursor() as cur:
        cur.executemany("""INSERT INTO audit.generation_log
            (run_id, source_key, rows_before, rows_generated, bad_rows_injected, rows_after,
             growth_pct, target_period, method, output_file)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", rows)


def show_status():
    from db import cursor
    with cursor() as cur:
        cur.execute("""
            SELECT 'dwh.fact_sales', count(*), min(sale_date)::text, max(sale_date)::text FROM dwh.fact_sales
            UNION ALL SELECT 'dwh.fact_hospital_demand', count(*), NULL, NULL FROM dwh.fact_hospital_demand
            UNION ALL SELECT 'dwh.dim_medicine (all versions)', count(*), NULL, NULL FROM dwh.dim_medicine
            UNION ALL SELECT 'audit.rejected_rows', count(*), NULL, NULL FROM audit.rejected_rows""")
        print("\n  table                               rows        from         to")
        for t, n, a, b in cur.fetchall():
            print(f"  {t:<34} {n:>9,}   {a or '':<12} {b or ''}")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("command", choices=["load", "generate", "refresh", "watch", "status"])
    ap.add_argument("--dsn", default=os.environ.get("PHARMA_DWH_DSN",
                                                    "postgresql://dwh:dwh@localhost:5432/pharma_dwh"))
    ap.add_argument("--scale", type=float, default=1.0, help="generated rows = scale x one average month")
    ap.add_argument("--interval", type=int, default=30, help="seconds between checks in watch mode")
    args = ap.parse_args()
    os.environ["PHARMA_DWH_DSN"] = args.dsn

    def new_run_id(kind):
        return f"{kind}_{datetime.now():%Y%m%dT%H%M%S}_{uuid.uuid4().hex[:4]}"

    if args.command == "status":
        show_status()
    elif args.command == "load":
        run_load(new_run_id("load"))
    elif args.command == "generate":
        run_generate(new_run_id("gen"), args.scale)
    elif args.command == "refresh":
        run_generate(new_run_id("gen"), args.scale)
        run_load(new_run_id("refresh"))
    elif args.command == "watch":
        from config import STREAM_DIR
        from ingestion import files_waiting
        print(f"Watching data/incoming and data/incoming_stream every {args.interval}s  (Ctrl+C to stop)")
        while True:
            if files_waiting() or files_waiting(str(STREAM_DIR)):
                time.sleep(3)              # let the generator finish writing all files
                run_load(new_run_id("watch"))
                print(f"\nWatching again... (Ctrl+C to stop)")
            time.sleep(args.interval)


if __name__ == "__main__":
    main()
