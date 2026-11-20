"""Storage & query-layer observability.

capture_storage_metrics : row counts + physical size per table, date coverage -> storage dashboard
benchmark_queries       : runs every dashboard widget query twice (raw star join vs
                          materialized view), times it, logs to audit.query_log -> query dashboard
"""
from __future__ import annotations

import time

from config import SOURCES
from db import cursor, get_conn

STAGING_DOMAIN = {spec["table"]: key for key, spec in SOURCES.items()}
DWH_DOMAIN = {
    "fact_sales": "05_pharmacy_sales_data",
    "fact_hospital_demand": "06_hospital_demand_data",
    "dim_medicine": "01_master_data",
}


def capture_storage_metrics(**context):
    run_id = context.get("run_id") if context else "manual"
    with cursor() as cur:
        cur.execute("""
            SELECT n.nspname, c.relname,
                   CASE WHEN c.relkind = 'p'
                        THEN (SELECT COALESCE(sum(pg_total_relation_size(t.relid)), 0)
                              FROM pg_partition_tree(c.oid) t)
                        ELSE pg_total_relation_size(c.oid) END AS size_bytes
            FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname IN ('staging', 'dwh', 'audit')
              AND c.relkind IN ('r', 'p', 'm') AND NOT c.relispartition
            ORDER BY 1, 2""")
        tables = cur.fetchall()
        rows = []
        for schema, table, size in tables:
            cur.execute(f'SELECT count(*) FROM "{schema}"."{table}"')
            count = cur.fetchone()[0]
            if schema == "staging":
                domain = STAGING_DOMAIN.get(table, "other")
            elif schema == "dwh":
                domain = DWH_DOMAIN.get(table, "dimensions / aggregates")
            else:
                domain = "pipeline metadata"
            rows.append((run_id, schema, table, domain, count, size))
        cur.executemany("""INSERT INTO audit.storage_metrics
                           (run_id, schema_name, table_name, source_domain, row_count, size_bytes)
                           VALUES (%s,%s,%s,%s,%s,%s)""", rows)

        cur.execute("""
            INSERT INTO audit.date_coverage (run_id, table_name, min_date, max_date, span_days, distinct_months)
            SELECT %s, 'dwh.fact_sales', min(sale_date), max(sale_date), max(sale_date) - min(sale_date),
                   count(DISTINCT date_trunc('month', sale_date))
            FROM dwh.fact_sales
            UNION ALL
            SELECT %s, 'dwh.fact_hospital_demand', min(d.full_date), max(d.full_date),
                   max(d.full_date) - min(d.full_date), count(DISTINCT d.month_start)
            FROM dwh.fact_hospital_demand f JOIN dwh.dim_date d ON d.date_key = f.month_date_key""",
                    (run_id, run_id))
    print(f"captured {len(rows)} tables")
    return len(rows)


# widget name -> {variant: sql}
WIDGET_QUERIES = {
    "Channel revenue & margin (monthly)": {
        "raw_star_join": """
            SELECT d.year_month, c.channel_name, sum(f.net_revenue), sum(f.gross_margin)
            FROM dwh.fact_sales f JOIN dwh.dim_date d ON d.date_key = f.date_key
            JOIN dwh.dim_channel c ON c.channel_key = f.channel_key
            GROUP BY 1, 2 ORDER BY 1, 2""",
        "materialized_view": """
            SELECT year_month, channel_name, net_revenue, gross_margin
            FROM dwh.mv_channel_revenue_monthly ORDER BY 1, 2""",
    },
    "Top 10 fast / slow movers": {
        "raw_star_join": """
            WITH b AS (SELECT max(sale_date) e FROM dwh.fact_sales)
            SELECT m.medicine_id, m.therapeutic_category, sum(f.quantity) u
            FROM dwh.fact_sales f JOIN dwh.dim_medicine m ON m.medicine_key = f.medicine_key, b
            WHERE f.sale_date > b.e - 90
            GROUP BY 1, 2 ORDER BY u DESC""",
        "materialized_view": """
            SELECT medicine_id, therapeutic_category, units_90d, velocity_class
            FROM dwh.mv_medicine_velocity WHERE velocity_class <> 'Normal' ORDER BY units_90d DESC""",
    },
    "Hospital forecast vs actual": {
        "raw_star_join": """
            SELECT d.year_month, h.hospital_id, sum(f.forecast_qty), sum(f.actual_qty), sum(f.unfulfilled_qty)
            FROM dwh.fact_hospital_demand f JOIN dwh.dim_date d ON d.date_key = f.month_date_key
            JOIN dwh.dim_hospital h ON h.hospital_key = f.hospital_key
            JOIN dwh.dim_medicine m ON m.medicine_key = f.medicine_key
            GROUP BY 1, 2 ORDER BY 1, 2""",
        "materialized_view": """
            SELECT year_month, hospital_id, sum(forecast_qty), sum(actual_qty), sum(unfulfilled_qty)
            FROM dwh.mv_hospital_forecast_accuracy GROUP BY 1, 2 ORDER BY 1, 2""",
    },
    "KPI card: revenue last 30 days": {
        "raw_star_join": """
            SELECT sum(net_revenue) FROM dwh.fact_sales
            WHERE sale_date > (SELECT max(sale_date) - 30 FROM dwh.fact_sales)""",
        "materialized_view": """
            SELECT sum(net_revenue) FROM dwh.mv_sales_daily
            WHERE sale_date > (SELECT max(sale_date) - 30 FROM dwh.mv_sales_daily)""",
    },
    "Daily revenue trend by channel": {
        "raw_star_join": """
            SELECT f.sale_date, c.channel_name, sum(f.net_revenue)
            FROM dwh.fact_sales f JOIN dwh.dim_channel c ON c.channel_key = f.channel_key
            GROUP BY 1, 2 ORDER BY 1""",
        "materialized_view": """
            SELECT sale_date, channel_name, sum(net_revenue)
            FROM dwh.mv_sales_daily GROUP BY 1, 2 ORDER BY 1""",
    },
}


def benchmark_queries(repeats: int = 3, **context):
    run_id = context.get("run_id") if context else "manual"
    log = []
    for widget, variants in WIDGET_QUERIES.items():
        for variant, sql in variants.items():
            for _ in range(repeats):
                conn = get_conn()
                t0 = time.perf_counter()
                status, err, n = "success", None, None
                try:
                    with conn.cursor() as cur:
                        cur.execute("SET statement_timeout = '60s'")
                        cur.execute(sql)
                        n = len(cur.fetchall())
                except Exception as exc:
                    status, err = "failed", str(exc)[:1000]
                finally:
                    conn.close()
                ms = (time.perf_counter() - t0) * 1000
                log.append((run_id, widget, variant, ms, n, status, err))
    with cursor() as cur:
        cur.executemany("""INSERT INTO audit.query_log
                           (run_id, widget_name, variant, duration_ms, rows_returned, status, error_message)
                           VALUES (%s,%s,%s,%s,%s,%s,%s)""", log)
    for row in log[::repeats]:
        print(f"{row[1]:<40} {row[2]:<18} {row[3]:8.1f} ms  {row[5]}")
    return len(log)
