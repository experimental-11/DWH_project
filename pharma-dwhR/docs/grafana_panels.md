# DE observability dashboards (Grafana)

Grafana: http://localhost:3000 (admin / admin). The `PharmaDWH` Postgres data source is already set up.
For each panel: **Dashboards → New → Add visualization → PharmaDWH → Code** (switch the query editor from Builder to Code), paste the SQL, and pick the visualization type.
Set the dashboard auto-refresh (top right) to **10s** so the panels move during the demo.

---

## 1. Batch ingestion dashboard

**Load duration per source file**: Bar chart, horizontal (Bar chart → Orientation: Horizontal)
```sql
SELECT source_key AS "source", round(avg(duration_s), 2) AS "avg load seconds"
FROM audit.batch_load_log
GROUP BY source_key ORDER BY 2 DESC;
```

**Rows processed per batch**: Table or Bar chart
```sql
SELECT finished_at AS "time", source_file, rows_loaded, round(duration_s, 2) AS seconds, status
FROM audit.batch_load_log ORDER BY finished_at DESC;
```

**Batch success rate %**: Stat or Gauge
```sql
SELECT round(100.0 * count(*) FILTER (WHERE status = 'success') / NULLIF(count(*), 0), 1) AS "success %"
FROM audit.batch_load_log;
```

**Throughput (rows/sec) per load**: Time series
```sql
SELECT finished_at AS "time", source_key, rows_loaded / NULLIF(duration_s, 0) AS rows_per_sec
FROM audit.batch_load_log WHERE status = 'success' ORDER BY 1;
```

---

## 2. Streaming ingestion dashboard

**Stream throughput (events/sec)**: Time series
```sql
SELECT window_end AS "time", events_per_sec FROM audit.stream_metrics ORDER BY 1;
```

**End-to-end latency (s)**: Time series
```sql
SELECT window_end AS "time", avg_latency_s AS "latency s" FROM audit.stream_metrics ORDER BY 1;
```

**Malformed / dropped records**: Stat
```sql
SELECT COALESCE(sum(malformed_count), 0) AS malformed FROM audit.stream_metrics;
```

**Overall throughput**: Stat (total events ÷ total stream seconds)
```sql
SELECT round(sum(events_ingested) / NULLIF(sum(duration_s), 0), 0) AS "events/sec" FROM audit.stream_metrics;
```

---

## 3. Storage dashboard (always shows the latest snapshot)

**Storage breakdown by source domain (MB)**: Pie chart (Donut)
```sql
WITH latest AS (SELECT run_id FROM audit.storage_metrics ORDER BY captured_at DESC LIMIT 1)
SELECT source_domain, round(sum(size_bytes) / 1048576.0, 2) AS mb
FROM audit.storage_metrics WHERE run_id = (SELECT run_id FROM latest)
GROUP BY source_domain ORDER BY 2 DESC;
```

**Row count per table**: Bar chart
```sql
WITH latest AS (SELECT run_id FROM audit.storage_metrics ORDER BY captured_at DESC LIMIT 1)
SELECT schema_name || '.' || table_name AS "table", row_count
FROM audit.storage_metrics WHERE run_id = (SELECT run_id FROM latest) AND schema_name <> 'audit'
ORDER BY row_count DESC;
```

**Row growth over refreshes**: Time series (this shows the warehouse growing with each refresh)
```sql
SELECT captured_at AS "time", table_name, row_count
FROM audit.storage_metrics WHERE table_name IN ('fact_sales', 'fact_hospital_demand') ORDER BY 1;
```

**Total database size (MB)**: Stat
```sql
SELECT round(pg_database_size('pharma_dwh') / 1048576.0, 1) AS "DB size MB";
```

**Date coverage span**: Table
```sql
SELECT DISTINCT ON (table_name) table_name, min_date, max_date, span_days, distinct_months
FROM audit.date_coverage ORDER BY table_name, captured_at DESC;
```

---

## 4. Query-layer dashboard

**Avg execution time per widget: raw star join vs materialized view**: Bar chart (column)
```sql
SELECT widget_name, variant, round(avg(duration_ms), 1) AS avg_ms
FROM audit.query_log WHERE status = 'success'
GROUP BY 1, 2 ORDER BY 1, 2;
```
(In the Bar chart options, set *X axis* = widget_name. Use the "Grouping to matrix" transform with columns = variant to get side-by-side bars.)

**Daily active query count**: Stat or Time series
```sql
SELECT date_trunc('day', started_at) AS "time", count(*) AS queries
FROM audit.query_log GROUP BY 1 ORDER BY 1;
```

**Query error rate %**: Stat
```sql
SELECT round(100.0 * count(*) FILTER (WHERE status <> 'success') / NULLIF(count(*), 0), 2) AS "error %"
FROM audit.query_log;
```

**Query time trend**: Time series
```sql
SELECT started_at AS "time", widget_name || ' / ' || variant AS metric, duration_ms
FROM audit.query_log ORDER BY 1;
```

---

## Validation rejects (useful for the report)
```sql
SELECT source_table, reject_reason, count(*) FROM audit.rejected_rows GROUP BY 1, 2 ORDER BY 1, 3 DESC;
```
