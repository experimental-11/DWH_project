"""Build the Phase 1 data-engineering dashboards for Grafana (provisioned JSON).

    py -3.12 scripts/build_grafana_dashboards.py

Writes one JSON file per dashboard into grafana/provisioning/dashboards/json/ (old JSON files
there are removed first). Grafana loads them automatically (see dashboards.yml next to them);
restart Grafana (`docker compose restart grafana`) after rebuilding. Panels read the audit.*
and dwh.* tables that the pipeline fills, so the dashboards move after every pipeline run.

Dashboards (plan document, Core DE requirements):
  1. Batch ingestion      - load duration per file, rows per batch, success rate
  2. Streaming ingestion  - throughput (events/s), end-to-end latency, malformed rows
  3. Storage & staging    - row counts, storage footprint (MB) per source domain, date coverage
  4. Transformation       - step durations, rows per dim/fact, validation rejects, SCD2 history
  5. Query layer          - queries executed, query error rate, avg execution time per dashboard
                            widget, and the 3 business queries (Q1-Q3) with dropdowns
"""
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "grafana/provisioning/dashboards/json"
DS = {"type": "grafana-postgresql-datasource", "uid": "pharma_dwh"}

# categorical slots (validated reference palette, dark-surface steps - Grafana's default theme)
BLUE, ORANGE, AQUA, YELLOW = "#3987e5", "#d95926", "#199e70", "#c98500"
GOOD, CRITICAL = "#0ca30c", "#e66767"   # status colours: success / failure only


# ----------------------------------------------------------------------------- panel helpers
def _target(sql, fmt="table", ds=DS):
    return {"refId": "A", "datasource": ds, "editorMode": "code", "rawQuery": True,
            "format": fmt, "rawSql": sql.strip()}


def _base(title, desc, kind, x, y, w, h, sql, fmt="table", ds=DS):
    return {"type": kind, "title": title, "description": desc, "datasource": ds,
            "gridPos": {"x": x, "y": y, "w": w, "h": h}, "targets": [_target(sql, fmt, ds)],
            "fieldConfig": {"defaults": {}, "overrides": []}, "options": {}}


def stat(title, desc, x, y, sql, unit="none", color=BLUE, decimals=None, w=6, h=4, thresholds=None):
    p = _base(title, desc, "stat", x, y, w, h, sql)
    d = p["fieldConfig"]["defaults"]
    d["unit"] = unit
    if decimals is not None:
        d["decimals"] = decimals
    if thresholds:
        d["color"] = {"mode": "thresholds"}
        d["thresholds"] = {"mode": "absolute", "steps": thresholds}
    else:
        d["color"] = {"mode": "fixed", "fixedColor": color}
    p["options"] = {"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
                    "colorMode": "value", "graphMode": "none", "textMode": "value_and_name",
                    "justifyMode": "center", "wideLayout": True}
    return p


def bar(title, desc, x, y, w, h, sql, x_field, unit="none", horizontal=True,
        colors=None, stacked=False, value_labels="auto", color_one=BLUE):
    p = _base(title, desc, "barchart", x, y, w, h, sql)
    p["fieldConfig"]["defaults"] = {
        "unit": unit,
        "color": {"mode": "fixed", "fixedColor": color_one},
        "custom": {"fillOpacity": 85, "lineWidth": 0, "gradientMode": "none",
                   "axisGridShow": True, "axisSoftMin": 0},
    }
    p["fieldConfig"]["overrides"] = [
        {"matcher": {"id": "byName", "options": name},
         "properties": [{"id": "color", "value": {"mode": "fixed", "fixedColor": c}}]}
        for name, c in (colors or {}).items()]
    p["options"] = {"orientation": "horizontal" if horizontal else "vertical", "xField": x_field,
                    "showValue": value_labels, "stacking": "normal" if stacked else "none",
                    "groupWidth": 0.7, "barWidth": 0.85, "barRadius": 0.1,
                    "legend": {"showLegend": bool(colors and len(colors) > 1),
                               "displayMode": "list", "placement": "bottom"},
                    "tooltip": {"mode": "multi", "sort": "desc"},
                    "xTickLabelRotation": 0, "xTickLabelSpacing": 0}
    return p


def timeseries(title, desc, x, y, w, h, sql, unit="none", color=BLUE, draw="line", multi=False):
    p = _base(title, desc, "timeseries", x, y, w, h, sql, fmt="time_series")
    p["fieldConfig"]["defaults"] = {
        "unit": unit,
        "color": {"mode": "palette-classic"} if multi else {"mode": "fixed", "fixedColor": color},
        "custom": {"drawStyle": draw, "lineWidth": 2, "fillOpacity": 8 if draw == "line" else 80,
                   "pointSize": 8, "showPoints": "auto", "spanNulls": True,
                   "axisSoftMin": 0, "lineInterpolation": "linear"},
    }
    p["options"] = {"legend": {"showLegend": multi, "displayMode": "list", "placement": "bottom"},
                    "tooltip": {"mode": "multi", "sort": "desc"}}
    return p


def table(title, desc, x, y, w, h, sql, overrides=None, ds=DS):
    p = _base(title, desc, "table", x, y, w, h, sql, ds=ds)
    p["fieldConfig"]["defaults"] = {"custom": {"align": "auto", "cellOptions": {"type": "auto"}}}
    p["fieldConfig"]["overrides"] = overrides or []
    p["options"] = {"showHeader": True, "cellHeight": "sm", "footer": {"show": False}}
    return p


def status_colors(field):
    """Colour the text of a status column: success = green, anything else = red (+ the word)."""
    return [{"matcher": {"id": "byName", "options": field}, "properties": [
        {"id": "custom.cellOptions", "value": {"type": "color-text"}},
        {"id": "mappings", "value": [{"type": "value", "options": {
            "success": {"color": GOOD, "index": 0, "text": "✔ success"},
            "failed": {"color": CRITICAL, "index": 1, "text": "✖ failed"}}}]}]}]


def row(title, y):
    """Section header that groups the panels below it (click to collapse)."""
    return {"type": "row", "title": title, "collapsed": False, "panels": [],
            "gridPos": {"x": 0, "y": y, "w": 24, "h": 1}}


def text(md, x, y, w=24, h=3):
    return {"type": "text", "title": "", "gridPos": {"x": x, "y": y, "w": w, "h": h},
            "options": {"mode": "markdown", "content": md}}


def dashboard(uid, title, panels, time_from="now-24h", tags=(), refresh="10s", variables=()):
    for i, p in enumerate(panels, 1):
        p["id"] = i
    return {"uid": uid, "title": title, "tags": ["phase1", "pharma", *tags], "timezone": "browser",
            "schemaVersion": 39, "version": 1, "editable": True, "graphTooltip": 1,
            "refresh": refresh, "time": {"from": time_from, "to": "now"},
            "templating": {"list": list(variables)},
            "timepicker": {"refresh_intervals": ["5s", "10s", "30s", "1m", "5m"]},
            "panels": panels}


def variable(name, label, sql):
    """Dropdown filled by a SQL query (first value selected by default)."""
    return {"name": name, "label": label, "type": "query", "datasource": DS, "query": sql,
            "definition": sql, "refresh": 1, "sort": 0, "multi": False, "includeAll": False,
            "current": {}, "hide": 0}


# ----------------------------------------------------------------------------- 1. batch ingestion
batch = dashboard("pharma-batch-ingestion", "1 · Batch Ingestion", [
    text("**Batch ingestion.** A Python file watcher (Airflow sensor) picks up CSVs landing in `data/incoming/` and "
         "COPYs each one into `staging.<table>`. Every file load is timed and logged to `audit.batch_load_log`.", 0, 0),
    stat("Batch success rate", "successful file loads / attempted file loads x 100", 0, 3, """
SELECT round(100.0 * count(*) FILTER (WHERE status = 'success') / NULLIF(count(*), 0), 1) AS "success rate"
FROM audit.batch_load_log""", unit="percent", decimals=1,
         thresholds=[{"color": CRITICAL, "value": None}, {"color": YELLOW, "value": 90}, {"color": GOOD, "value": 99.9}]),
    stat("Files loaded", "Number of file loads attempted", 6, 3,
         "SELECT count(*) AS files FROM audit.batch_load_log"),
    stat("Rows loaded to staging", "Total rows read from CSV and inserted into staging (len(df))", 12, 3,
         "SELECT COALESCE(sum(rows_loaded),0) AS rows FROM audit.batch_load_log WHERE status='success'", unit="short"),
    stat("Total load time", "Sum of batch load durations (end - start timestamp per file)", 18, 3,
         "SELECT COALESCE(sum(duration_s),0) AS seconds FROM audit.batch_load_log", unit="s", decimals=1),
    bar("Load duration per source file (s)", "Recommended chart: latest load duration of each source "
        "(Timestamp_end - Timestamp_start). Shows at a glance which large file takes the longest.", 0, 7, 12, 10, """
SELECT DISTINCT ON (source_key) source_key AS source, round(duration_s, 2) AS "load seconds"
FROM audit.batch_load_log ORDER BY source_key, finished_at DESC""", "source", unit="s"),
    bar("Rows processed per batch (latest load per source)", "Exact record count loaded from each file", 12, 7, 12, 10, """
SELECT DISTINCT ON (source_key) source_key AS source, rows_loaded AS "rows"
FROM audit.batch_load_log ORDER BY source_key, finished_at DESC""", "source", unit="short"),
    timeseries("Load throughput over time (rows/s)", "rows loaded / load seconds, one point per file load",
               0, 17, 12, 8, """
SELECT finished_at AS time, source_key AS metric, rows_loaded / NULLIF(duration_s, 0) AS value
FROM audit.batch_load_log WHERE status = 'success' AND $__timeFilter(finished_at) ORDER BY 1""",
               unit="short", draw="points", multi=True),
    table("File load log", "Every file the watcher picked up, newest first", 12, 17, 12, 8, """
SELECT finished_at AS "finished", source_file AS file, target_table AS "table", rows_loaded AS rows,
       round(duration_s, 2) AS seconds, round(file_size_mb, 1) AS "MB", status, error_message AS error
FROM audit.batch_load_log ORDER BY finished_at DESC LIMIT 100""", overrides=status_colors("status")),
], tags=["ingestion"])

# ----------------------------------------------------------------------------- 2. streaming
stream = dashboard("pharma-stream-ingestion", "2 · Streaming Ingestion (IoT)", [
    text("**Streaming ingestion.** IoT readings landing in `data/incoming_stream/` are replayed as micro-batches "
         "(2,000 rows, throttled to 5,000 events/s) into `staging.iot_readings`. Rows with a null reading ID or an "
         "unparsable timestamp are dropped to `audit.rejected_rows`.", 0, 0),
    stat("Overall throughput", "Total records ingested / total stream seconds", 0, 3, """
SELECT round(sum(events_ingested) / NULLIF(sum(duration_s), 0), 0) AS "events/s" FROM audit.stream_metrics""",
         unit="short"),
    stat("Events ingested", "Total good IoT records committed", 6, 3,
         "SELECT COALESCE(sum(events_ingested),0) AS events FROM audit.stream_metrics", unit="short"),
    stat("Avg end-to-end latency", "Mean of (commit timestamp - emit timestamp) per micro-batch", 12, 3,
         "SELECT round(avg(avg_latency_s), 3) AS latency FROM audit.stream_metrics", unit="s", decimals=3),
    stat("Malformed / dropped records", "Rows rejected during stream parsing (null key or bad timestamp)", 18, 3,
         "SELECT COALESCE(sum(malformed_count),0) AS malformed FROM audit.stream_metrics", unit="short",
         thresholds=[{"color": GOOD, "value": None}, {"color": YELLOW, "value": 1}]),
    timeseries("Stream throughput (events/s)", "Recommended chart: events per second for every micro-batch, "
               "to observe pipeline stability over the simulation run", 0, 7, 24, 9, """
SELECT window_end AS time, events_per_sec AS "events/s"
FROM audit.stream_metrics WHERE $__timeFilter(window_end) ORDER BY 1""", unit="short"),
    timeseries("End-to-end latency per micro-batch (s)", "Timestamp_db_commit - Timestamp_emit", 0, 16, 12, 8, """
SELECT window_end AS time, avg_latency_s AS "latency s"
FROM audit.stream_metrics WHERE $__timeFilter(window_end) ORDER BY 1""", unit="s", color=AQUA),
    timeseries("Malformed records per micro-batch", "Dropped rows per micro-batch", 0, 24, 12, 8, """
SELECT window_end AS time, malformed_count AS "malformed"
FROM audit.stream_metrics WHERE $__timeFilter(window_end) ORDER BY 1""", color=ORANGE, draw="bars"),
    table("Stream runs", "One row per stream file replayed", 12, 16, 12, 16, """
SELECT max(window_end) AS "finished", source_file AS file, count(*) AS "micro-batches",
       sum(events_ingested) AS events, sum(malformed_count) AS malformed,
       round(sum(events_ingested) / NULLIF(sum(duration_s), 0), 0) AS "events/s",
       round(avg(avg_latency_s), 3) AS "avg latency s"
FROM audit.stream_metrics GROUP BY run_id, source_file ORDER BY 1 DESC"""),
], time_from="now-24h", tags=["ingestion", "streaming"])

# ----------------------------------------------------------------------------- 3. storage
LATEST_STORAGE = "(SELECT run_id FROM audit.storage_metrics ORDER BY captured_at DESC LIMIT 1)"
storage = dashboard("pharma-storage", "3 · Storage & Staging", [
    text("**Storage & staging layer (PostgreSQL).** Raw files land in schema `staging` (all TEXT, append-only, with "
         "lineage columns); the star schema lives in `dwh`; pipeline metrics in `audit`. Snapshot captured after "
         "every pipeline run.", 0, 0),
    stat("Database size", "Physical size of the pharma_dwh database on disk (pg_database_size)", 0, 3,
         "SELECT round(pg_database_size('pharma_dwh') / 1048576.0, 1) AS \"MB\"", unit="decmbytes", decimals=1),
    stat("Rows in dimension + fact tables", "Total cumulative records stored across dwh.dim_* and dwh.fact_* (COUNT(*))",
         6, 3, f"""
SELECT COALESCE(sum(row_count),0) AS rows FROM audit.storage_metrics
WHERE run_id = {LATEST_STORAGE} AND schema_name = 'dwh' AND (table_name LIKE 'dim\\_%' OR table_name LIKE 'fact\\_%')""",
         unit="short"),
    stat("Rows in staging (ODS)", "Total raw rows in the staging schema", 12, 3, f"""
SELECT COALESCE(sum(row_count),0) AS rows FROM audit.storage_metrics
WHERE run_id = {LATEST_STORAGE} AND schema_name = 'staging'""", unit="short"),
    stat("Sales date coverage", "MAX(sale_date) - MIN(sale_date) in fact_sales; should span 2023-2025 (+ refreshes)",
         18, 3, """
SELECT DISTINCT ON (table_name) to_char(min_date,'YYYY-MM-DD') || ' → ' || to_char(max_date,'YYYY-MM-DD') AS span
FROM audit.date_coverage WHERE table_name = 'dwh.fact_sales' ORDER BY table_name, captured_at DESC"""),
    bar("Storage footprint by source domain (MB)", "Recommended chart: storage contributed by each of the 7 source "
        "domains (staging + warehouse tables built from it); pipeline metadata shown separately", 0, 7, 12, 10, f"""
SELECT source_domain AS domain, round(sum(size_bytes) / 1048576.0, 2) AS "MB"
FROM audit.storage_metrics WHERE run_id = {LATEST_STORAGE}
GROUP BY source_domain ORDER BY 2 DESC""", "domain", unit="decmbytes"),
    bar("Share of stored rows by source domain (%)", "% of all stored rows contributed by each source domain",
        12, 7, 12, 10, f"""
SELECT source_domain AS domain,
       round(100.0 * sum(row_count) / NULLIF(sum(sum(row_count)) OVER (), 0), 2) AS "% of rows"
FROM audit.storage_metrics WHERE run_id = {LATEST_STORAGE} AND schema_name <> 'audit'
GROUP BY source_domain ORDER BY 2 DESC""", "domain", unit="percent"),
    bar("Row count per table", "SELECT COUNT(*) FROM each staging and warehouse table (latest snapshot)",
        0, 17, 12, 12, f"""
SELECT schema_name || '.' || table_name AS "table", row_count AS rows
FROM audit.storage_metrics WHERE run_id = {LATEST_STORAGE} AND schema_name <> 'audit'
ORDER BY row_count DESC""", "table", unit="short"),
    table("Date coverage span", "Validation that the 2023-2025 range is fully represented "
          "(MAX - MIN, distinct year-month partitions)", 12, 17, 12, 5, """
SELECT DISTINCT ON (table_name) table_name AS "table", min_date AS "from", max_date AS "to",
       span_days AS "span (days)", distinct_months AS "distinct months"
FROM audit.date_coverage ORDER BY table_name, captured_at DESC"""),
    timeseries("Fact table growth across pipeline runs", "Row counts captured after every run: the warehouse keeps "
               "its history and grows with each refresh", 12, 22, 12, 7, """
SELECT captured_at AS time, table_name AS metric, row_count AS value
FROM audit.storage_metrics
WHERE schema_name = 'dwh' AND table_name IN ('fact_sales', 'fact_hospital_demand') AND $__timeFilter(captured_at)
ORDER BY 1""", unit="short", multi=True),
], time_from="now-7d", tags=["storage"])

# ----------------------------------------------------------------------------- 5. query layer
perf_panels = [
    stat("Queries executed", "Total analytical queries executed (benchmarks after each pipeline run + business "
         "queries run on this dashboard)", 0, 0, "SELECT count(*) AS queries FROM audit.query_log",
         w=12),
    stat("Query error rate", "failed queries / executed queries x 100", 12, 0, """
SELECT round(100.0 * count(*) FILTER (WHERE status <> 'success') / NULLIF(count(*), 0), 2) AS "error rate"
FROM audit.query_log""", unit="percent", decimals=2, w=12,
         thresholds=[{"color": GOOD, "value": None}, {"color": CRITICAL, "value": 0.01}]),
    bar("Avg execution time per dashboard widget (ms)", "Average query time per widget/KPI card, raw star join vs "
        "materialized view - spots unoptimised joins", 0, 4, 24, 10, """
SELECT widget_name AS widget,
       round(avg(duration_ms) FILTER (WHERE variant = 'raw_star_join'), 1)     AS "raw star join",
       round(avg(duration_ms) FILTER (WHERE variant = 'materialized_view'), 1) AS "materialized view"
FROM audit.query_log WHERE status = 'success' AND variant IN ('raw_star_join', 'materialized_view')
GROUP BY widget_name ORDER BY 2 DESC NULLS LAST""",
        "widget", unit="ms", horizontal=False,
        colors={"raw star join": ORANGE, "materialized view": BLUE}),
]


# ----------------------------------------------------------------------------- 4. transformation
STEPS = ("('03_staging_views.sql','10_load_dim_medicine.sql','11_load_dim_hospital.sql',"
         "'12_load_dim_sales_misc.sql','20_load_fact_sales.sql','21_load_fact_hospital_demand.sql')")
LATEST_TRANSFORM = ("(SELECT run_id FROM audit.etl_step_log WHERE step_name = '20_load_fact_sales.sql' "
                    "ORDER BY executed_at DESC LIMIT 1)")
transform = dashboard("pharma-transformation", "4 · Transformation & Dimensional Modeling", [
    text("**Transformation & dimensional modeling.** Staging views rename + type-cast the raw TEXT columns; then "
         "`dim_medicine` (**SCD Type 2**, price history), `dim_hospital` / `dim_location` / `dim_channel` (**SCD Type 1**), "
         "`dim_sales_profile` (**junk dimension**: payment x discount x order size x customer segment x payer x promotion) and `dim_date` (**role-playing**: "
         "sale date / demand month) are loaded, followed by `fact_sales` (**transaction fact**, partitioned by month, "
         "degenerate dimension `mfg_batch_id`) and `fact_hospital_demand` (**periodic snapshot**, hospital x medicine "
         "x month). Loads are incremental (watermarks); rows failing validation go to `audit.rejected_rows`.",
         0, 0, h=4),
    stat("Last transform duration", "Sum of the transformation steps (views -> dims -> facts) in the latest load",
         0, 4, f"""
SELECT round(sum(duration_s), 1) AS seconds FROM audit.etl_step_log
WHERE run_id = {LATEST_TRANSFORM} AND step_name IN {STEPS}""", unit="s", decimals=1),
    stat("Fact rows in the mart", "fact_sales + fact_hospital_demand", 6, 4, """
SELECT (SELECT count(*) FROM dwh.fact_sales) + (SELECT count(*) FROM dwh.fact_hospital_demand) AS rows""",
         unit="short"),
    stat("Validation pass rate", "fact rows / (fact rows + rejected rows) x 100 for the two SDI facts", 12, 4, """
WITH r AS (SELECT count(*) n FROM audit.rejected_rows
           WHERE source_table IN ('staging.pharmacy_sales', 'staging.hospital_demand')),
     f AS (SELECT (SELECT count(*) FROM dwh.fact_sales) + (SELECT count(*) FROM dwh.fact_hospital_demand) n)
SELECT round(100.0 * f.n / NULLIF(f.n + r.n, 0), 2) AS "pass rate" FROM f, r""", unit="percent", decimals=2,
         thresholds=[{"color": CRITICAL, "value": None}, {"color": YELLOW, "value": 95},
                     {"color": GOOD, "value": 99}]),
    stat("SCD2 history versions", "dim_medicine rows that are closed historical versions; grows when prices change",
         18, 4, """
SELECT count(*) FILTER (WHERE NOT is_current) AS "closed versions" FROM dwh.dim_medicine""", unit="short"),
    bar("Duration per transformation step (latest load, s)", "Time of each SQL step of the latest load",
        0, 8, 12, 9, f"""
SELECT step_name AS step, round(duration_s, 2) AS seconds FROM audit.etl_step_log
WHERE run_id = {LATEST_TRANSFORM} AND step_name IN {STEPS} ORDER BY executed_at""", "step", unit="s"),
    bar("Rows per dimension & fact table", "Current row count of every star-schema table", 12, 8, 12, 9, """
SELECT t AS "table", n AS rows FROM (
  SELECT 'fact_sales' t, count(*) n FROM dwh.fact_sales UNION ALL
  SELECT 'fact_hospital_demand', count(*) FROM dwh.fact_hospital_demand UNION ALL
  SELECT 'dim_medicine', count(*) FROM dwh.dim_medicine UNION ALL
  SELECT 'dim_location', count(*) FROM dwh.dim_location UNION ALL
  SELECT 'dim_sales_profile (junk)', count(*) FROM dwh.dim_sales_profile UNION ALL
  SELECT 'dim_hospital', count(*) FROM dwh.dim_hospital UNION ALL
  SELECT 'dim_channel', count(*) FROM dwh.dim_channel) x ORDER BY n DESC""", "table", unit="short"),
    table("Rejected rows by validation rule", "price > 0, valid dates, non-null SKU IDs, known medicine ... "
          "(empty = no rejects)", 0, 17, 12, 8, """
SELECT source_table AS source, reject_reason AS rule, count(*) AS rows, max(rejected_at) AS "last seen"
FROM audit.rejected_rows GROUP BY 1, 2 ORDER BY 3 DESC"""),
    table("Incremental load watermarks", "Last staging _ingest_id processed per target (next run starts after it)",
          12, 17, 12, 8, """
SELECT target, last_ingest_id AS "last ingest id", updated_at AS updated FROM audit.etl_watermark ORDER BY target"""),
    table("Load reconciliation (staged = loaded + rejected + duplicates)", "Every fact load counts its rows; "
          "'unexplained' must be 0, otherwise the load fails and nothing is saved", 0, 34, 24, 7, """
SELECT checked_at AS "checked", target, staged_rows AS staged, loaded_rows AS loaded, rejected_rows AS rejected,
       duplicate_rows AS duplicates, unexplained_rows AS unexplained
FROM audit.load_reconciliation ORDER BY checked_at DESC, target LIMIT 50"""),
    table("SCD Type 2 history: dim_medicine versions", "Medicines with more than one version (price/cost changes "
          "from the generator). Empty until the first refresh with price changes.", 0, 25, 12, 9, """
SELECT medicine_id, version_no AS version, medicine_name, unit_cost, unit_price, valid_from, valid_to, is_current
FROM dwh.dim_medicine
WHERE medicine_id IN (SELECT medicine_id FROM dwh.dim_medicine GROUP BY 1 HAVING count(*) > 1)
ORDER BY medicine_id, version_no"""),
    timeseries("Transformation step duration across runs (s)", "One point per step per load", 12, 25, 12, 9, f"""
SELECT executed_at AS time, step_name AS metric, duration_s AS value FROM audit.etl_step_log
WHERE step_name IN {STEPS} AND $__timeFilter(executed_at) ORDER BY 1""", unit="s", draw="points", multi=True),
], time_from="now-7d", tags=["transformation"])

# ----------------------------------------------------------------------------- 5b. business queries (part of dashboard 5)
YEARS = "SELECT DISTINCT extract(year FROM sale_date)::int FROM dwh.mv_sales_daily ORDER BY 1 DESC"
DISEASES = "SELECT DISTINCT disease_category FROM dwh.dim_medicine WHERE is_current ORDER BY 1"
QUARTERS = ("SELECT DISTINCT d.year_no || '-Q' || d.quarter_no FROM dwh.fact_hospital_demand f "
            "JOIN dwh.dim_date d ON d.date_key = f.month_date_key ORDER BY 1 DESC")
business_panels = [
    text("**Stakeholder queries on the SDI data mart.** Pick the parameters at the top; every panel calls a SQL "
         "function (`dwh.q1_…`, `dwh.q2_…`, `dwh.q3_…`) that runs the dimensional query and logs its execution time "
         "to `audit.query_log` (see dashboard 5). For free-form SQL use **Explore** (left menu) → data source "
         "*PharmaDWH* → *Code*.", 0, 0),
    table("Q1 · Most common $disease medicine per city ($year)", "fact_sales → dim_date (year), dim_medicine "
          "(disease category), dim_location (city); ranked by packs sold", 0, 3, 12, 12, """
SELECT city, medicine_name AS medicine, packs_sold AS "packs sold", share_of_city_pct AS "% of city's packs"
FROM dwh.q1_top_medicine_per_city($year, '$disease')"""),
    bar("Q1 · Packs sold of the top medicine per city", "Same query as the table", 12, 3, 12, 12, """
SELECT city || ' · ' || medicine_name AS "city · medicine", packs_sold AS "packs sold"
FROM dwh.q1_top_medicine_per_city($year, '$disease') ORDER BY packs_sold DESC""", "city · medicine",
        unit="short"),
    bar("Q2 · Net sales by therapeutic class: Physical vs E-commerce ($q2_year, top 15)", "fact_sales → dim_date, "
        "dim_channel, dim_medicine (therapeutic class)", 0, 15, 12, 13, """
SELECT therapeutic_class AS class,
       sum(net_sales_usd) FILTER (WHERE channel = 'Physical Pharmacy') AS "Physical Pharmacy",
       sum(net_sales_usd) FILTER (WHERE channel = 'E-commerce')        AS "E-commerce"
FROM dwh.q2_channel_by_therapeutic_class($q2_year)
GROUP BY therapeutic_class ORDER BY 2 DESC NULLS LAST LIMIT 15""", "class", unit="currencyUSD",
        colors={"Physical Pharmacy": BLUE, "E-commerce": ORANGE}, value_labels="never"),
    table("Q2 · Net sales & gross margin by channel and therapeutic class ($q2_year)", "All classes",
          12, 15, 12, 13, """
SELECT channel, therapeutic_class AS "therapeutic class", net_sales_usd AS "net sales $",
       gross_margin_usd AS "gross margin $", margin_pct AS "margin %"
FROM dwh.q2_channel_by_therapeutic_class($q2_year)"""),
    bar("Q3 · Controlled-medicine packs requested per state ($quarter)", "fact_hospital_demand → dim_date (role: "
        "demand month), dim_medicine (controlled flag), dim_hospital (state)", 0, 28, 12, 10, """
SELECT state, sum(requested_packs) AS "requested packs"
FROM dwh.q3_controlled_requests_by_state('$quarter') GROUP BY state ORDER BY 2 DESC""", "state", unit="short",
        color_one=AQUA),
    table("Q3 · Requested packs per state and controlled medicine ($quarter)", "requested / forecast > 1 means "
          "hospitals asked for more than they forecast", 12, 28, 12, 10, """
SELECT state, medicine_name AS medicine, requested_packs AS "requested packs",
       requested_vs_forecast AS "requested / forecast"
FROM dwh.q3_controlled_requests_by_state('$quarter')"""),
]

def _shift(panels, dy):
    for p in panels:
        p["gridPos"]["y"] += dy
    return panels


# one dashboard: query performance (y 0-14) | business queries Q1-Q3
_biz = [p for p in business_panels if p["type"] != "text"]          # Q1-Q3 panels, y 3..38
query = dashboard("pharma-query-layer", "5 · Analytical Query Layer", [
    row("Query performance", 0),
    *_shift(perf_panels, 1),
    row("Business queries (Q1-Q3): pick the parameters in the dropdowns at the top", 15),
    *_shift(_biz, 13),
], time_from="now-7d", tags=["query", "business"], refresh="",
   variables=[variable("year", "Q1 year", YEARS), variable("disease", "Q1 disease category", DISEASES),
              variable("q2_year", "Q2 year", YEARS), variable("quarter", "Q3 quarter", QUARTERS)])

PROVIDER = """apiVersion: 1
providers:
  - name: pharma-phase1
    folder: "Pharma DWH - Phase 1"
    type: file
    disableDeletion: false
    allowUiUpdates: true
    updateIntervalSeconds: 30
    options:
      path: /etc/grafana/provisioning/dashboards/json
"""

if __name__ == "__main__":
    boards = {"1_batch_ingestion": batch, "2_stream_ingestion": stream, "3_storage": storage,
              "4_transformation": transform, "5_query_layer": query}
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    for old in OUT_DIR.glob("*.json"):          # removed / renamed dashboards disappear from Grafana
        old.unlink()
    (OUT_DIR.parent / "dashboards.yml").write_text(PROVIDER)
    for name, board in boards.items():
        (OUT_DIR / f"{name}.json").write_text(json.dumps(board, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"wrote {len(boards)} dashboards to {OUT_DIR}")
