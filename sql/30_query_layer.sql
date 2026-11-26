-- =====================================================================================
-- 30  Analytical query layer & optimisation (idempotent)
--   * B-tree indexes on fact foreign keys (created on the partitioned parent, so every
--     monthly partition - including future ones - gets them automatically)
--   * monthly RANGE partitions on fact_sales.sale_date  -> partition pruning on date filters
--   * materialized views = pre-aggregated summary tables behind each dashboard widget
--   Each MV has a UNIQUE index so it can be refreshed CONCURRENTLY (readers never blocked).
-- =====================================================================================

CREATE INDEX IF NOT EXISTS ix_fs_date      ON dwh.fact_sales (date_key);
CREATE INDEX IF NOT EXISTS ix_fs_medicine  ON dwh.fact_sales (medicine_key);
CREATE INDEX IF NOT EXISTS ix_fs_channel   ON dwh.fact_sales (channel_key);
CREATE INDEX IF NOT EXISTS ix_fs_location  ON dwh.fact_sales (location_key);
CREATE INDEX IF NOT EXISTS ix_fs_profile   ON dwh.fact_sales (profile_key);
CREATE INDEX IF NOT EXISTS ix_fs_hospital  ON dwh.fact_sales (hospital_key);

CREATE INDEX IF NOT EXISTS ix_fhd_month    ON dwh.fact_hospital_demand (month_date_key);
CREATE INDEX IF NOT EXISTS ix_fhd_hosp_med ON dwh.fact_hospital_demand (hospital_key, medicine_key);

-- ---------- Widget 1: Channel revenue split & gross margin (monthly) -----------------
CREATE MATERIALIZED VIEW IF NOT EXISTS dwh.mv_channel_revenue_monthly AS
SELECT d.year_month,
       d.month_start,
       c.channel_name,
       count(*)                                   AS txn_count,
       sum(f.quantity)                            AS units_sold,
       round(sum(f.net_revenue), 2)               AS net_revenue,
       round(sum(f.cost_amount), 2)               AS cost_amount,
       round(sum(f.gross_margin), 2)              AS gross_margin,
       round(100 * sum(f.gross_margin) / NULLIF(sum(f.net_revenue), 0), 2) AS margin_pct
FROM dwh.fact_sales f
JOIN dwh.dim_date d    ON d.date_key = f.date_key
JOIN dwh.dim_channel c ON c.channel_key = f.channel_key
GROUP BY d.year_month, d.month_start, c.channel_name;
CREATE UNIQUE INDEX IF NOT EXISTS ux_mv_channel ON dwh.mv_channel_revenue_monthly (year_month, channel_name);

-- ---------- Widget 2: Fast- vs slow-moving medicines (last 90 days of business) ------
CREATE MATERIALIZED VIEW IF NOT EXISTS dwh.mv_medicine_velocity AS
WITH bounds AS (SELECT max(sale_date) AS end_date FROM dwh.fact_sales),
sold AS (
    SELECT m.medicine_id, sum(f.quantity) AS units, sum(f.net_revenue) AS revenue, count(*) AS txns
    FROM dwh.fact_sales f
    JOIN dwh.dim_medicine m ON m.medicine_key = f.medicine_key
    CROSS JOIN bounds b
    WHERE f.sale_date > b.end_date - 90
    GROUP BY m.medicine_id
),
ranked AS (
    SELECT cur.medicine_id, cur.medicine_name,
           COALESCE(cur.therapeutic_category, 'Unknown') AS therapeutic_category,
           cur.is_cold_chain,
           COALESCE(s.units, 0)                 AS units_90d,
           round(COALESCE(s.units, 0) / 90.0, 2) AS avg_daily_units,
           round(COALESCE(s.revenue, 0), 2)     AS revenue_90d,
           COALESCE(s.txns, 0)                  AS txns_90d,
           rank() OVER (ORDER BY COALESCE(s.units, 0) DESC) AS rank_fast,
           rank() OVER (ORDER BY COALESCE(s.units, 0) ASC)  AS rank_slow,
           rank() OVER (PARTITION BY cur.therapeutic_category ORDER BY COALESCE(s.units, 0) DESC) AS rank_in_category
    FROM dwh.dim_medicine cur
    LEFT JOIN sold s ON s.medicine_id = cur.medicine_id
    WHERE cur.is_current
)
SELECT *,
       CASE WHEN rank_fast <= 10 THEN 'Fast-moving'
            WHEN rank_slow <= 10 THEN 'Slow-moving'
            ELSE 'Normal' END AS velocity_class
FROM ranked;
CREATE UNIQUE INDEX IF NOT EXISTS ux_mv_velocity ON dwh.mv_medicine_velocity (medicine_id);

-- ---------- Widget 3: Hospital forecast vs actual consumption & unfulfilled demand ---
CREATE MATERIALIZED VIEW IF NOT EXISTS dwh.mv_hospital_forecast_accuracy AS
SELECT d.year_month,
       d.month_start,
       h.hospital_id,
       COALESCE(h.hospital_name, h.hospital_id)       AS hospital_name,
       COALESCE(m.therapeutic_category, 'Unknown')    AS therapeutic_category,
       sum(f.forecast_qty)                            AS forecast_qty,
       sum(f.actual_qty)                              AS actual_qty,
       sum(f.unfulfilled_qty)                         AS unfulfilled_qty,
       round(sum(f.actual_qty) / NULLIF(sum(f.forecast_qty), 0), 3)  AS actual_to_forecast_ratio,
       round(100 * sum(f.unfulfilled_qty)
             / NULLIF(sum(f.actual_qty) + sum(f.unfulfilled_qty), 0), 2) AS unfulfilled_pct
FROM dwh.fact_hospital_demand f
JOIN dwh.dim_date d     ON d.date_key = f.month_date_key
JOIN dwh.dim_hospital h ON h.hospital_key = f.hospital_key
JOIN dwh.dim_medicine m ON m.medicine_key = f.medicine_key
GROUP BY d.year_month, d.month_start, h.hospital_id, h.hospital_name, m.therapeutic_category;
CREATE UNIQUE INDEX IF NOT EXISTS ux_mv_hospital
    ON dwh.mv_hospital_forecast_accuracy (year_month, hospital_id, therapeutic_category);

-- ---------- Widget 4: Daily sales trend by channel & category (KPI cards, trends) ----
CREATE MATERIALIZED VIEW IF NOT EXISTS dwh.mv_sales_daily AS
SELECT f.sale_date,
       c.channel_name,
       COALESCE(m.therapeutic_category, 'Unknown') AS therapeutic_category,
       count(*)                       AS txn_count,
       sum(f.quantity)                AS units_sold,
       round(sum(f.net_revenue), 2)   AS net_revenue,
       round(sum(f.gross_margin), 2)  AS gross_margin
FROM dwh.fact_sales f
JOIN dwh.dim_channel c  ON c.channel_key = f.channel_key
JOIN dwh.dim_medicine m ON m.medicine_key = f.medicine_key
GROUP BY f.sale_date, c.channel_name, COALESCE(m.therapeutic_category, 'Unknown');
CREATE UNIQUE INDEX IF NOT EXISTS ux_mv_daily ON dwh.mv_sales_daily (sale_date, channel_name, therapeutic_category);

-- =====================================================================================
-- Parameterised dimensional queries for stakeholders (used by Grafana dashboard
-- "7 · Run Dimensional Queries"). Every call times itself and is logged to
-- audit.query_log (variant = 'live_dashboard'), so each BI fetch increments the
-- Daily Active Query Count and feeds Query Execution Time.
-- =====================================================================================

-- Q1: most common medicine per city for a disease category in a year (by packs sold)
DROP FUNCTION IF EXISTS dwh.q1_top_medicine_per_city(INT, TEXT);
CREATE OR REPLACE FUNCTION dwh.q1_top_medicine_per_city(p_year INT, p_disease TEXT)
RETURNS TABLE (city TEXT, medicine_name TEXT, packs_sold BIGINT, share_of_city_pct NUMERIC)
LANGUAGE plpgsql AS $$
#variable_conflict use_column
DECLARE t0 TIMESTAMPTZ := clock_timestamp(); n INT;
BEGIN
    RETURN QUERY
    WITH s AS (
        SELECT l.city, m.medicine_name, sum(f.quantity)::bigint AS packs
        FROM dwh.fact_sales f
        JOIN dwh.dim_date d     ON d.date_key = f.date_key
        JOIN dwh.dim_medicine m ON m.medicine_key = f.medicine_key
        JOIN dwh.dim_location l ON l.location_key = f.location_key
        WHERE d.year_no = p_year AND m.disease_category = p_disease
        GROUP BY l.city, m.medicine_name),
    r AS (SELECT s.*, rank() OVER (PARTITION BY s.city ORDER BY s.packs DESC) AS rnk,
                 round(100.0 * s.packs / sum(s.packs) OVER (PARTITION BY s.city), 1) AS pct
          FROM s)
    SELECT r.city, r.medicine_name, r.packs, r.pct FROM r WHERE r.rnk = 1 ORDER BY r.city;
    GET DIAGNOSTICS n = ROW_COUNT;
    INSERT INTO audit.query_log (run_id, widget_name, variant, started_at, duration_ms, rows_returned, status)
    VALUES ('grafana', 'Q1 top medicine per city', 'live_dashboard', t0,
            extract(epoch FROM clock_timestamp() - t0) * 1000, n, 'success');
END $$;

-- Q2: net sales & gross margin, E-commerce vs Physical pharmacy, by therapeutic class
DROP FUNCTION IF EXISTS dwh.q2_channel_by_therapeutic_class(INT);
CREATE OR REPLACE FUNCTION dwh.q2_channel_by_therapeutic_class(p_year INT)
RETURNS TABLE (channel TEXT, therapeutic_class TEXT, net_sales_usd NUMERIC, gross_margin_usd NUMERIC,
               margin_pct NUMERIC)
LANGUAGE plpgsql AS $$
#variable_conflict use_column
DECLARE t0 TIMESTAMPTZ := clock_timestamp(); n INT;
BEGIN
    RETURN QUERY
    SELECT CASE WHEN c.channel_name = 'E-commerce' THEN 'E-commerce' ELSE 'Physical Pharmacy' END,
           m.therapeutic_category,
           round(sum(f.net_revenue), 2), round(sum(f.gross_margin), 2),
           round(100 * sum(f.gross_margin) / NULLIF(sum(f.net_revenue), 0), 1)
    FROM dwh.fact_sales f
    JOIN dwh.dim_date d     ON d.date_key = f.date_key
    JOIN dwh.dim_channel c  ON c.channel_key = f.channel_key
    JOIN dwh.dim_medicine m ON m.medicine_key = f.medicine_key
    WHERE d.year_no = p_year
    GROUP BY 1, 2 ORDER BY 3 DESC;
    GET DIAGNOSTICS n = ROW_COUNT;
    INSERT INTO audit.query_log (run_id, widget_name, variant, started_at, duration_ms, rows_returned, status)
    VALUES ('grafana', 'Q2 channel x therapeutic class', 'live_dashboard', t0,
            extract(epoch FROM clock_timestamp() - t0) * 1000, n, 'success');
END $$;

-- Q3: requested quantity of controlled medicines per state in a quarter ('2025-Q4')
DROP FUNCTION IF EXISTS dwh.q3_controlled_requests_by_state(TEXT);
CREATE OR REPLACE FUNCTION dwh.q3_controlled_requests_by_state(p_quarter TEXT)
RETURNS TABLE (state TEXT, medicine_name TEXT, requested_packs NUMERIC, requested_vs_forecast NUMERIC)
LANGUAGE plpgsql AS $$
#variable_conflict use_column
DECLARE t0 TIMESTAMPTZ := clock_timestamp(); n INT;
BEGIN
    RETURN QUERY
    SELECT h.region, m.medicine_name, sum(f.requested_qty),
           round(sum(f.requested_qty) / NULLIF(sum(f.forecast_qty), 0), 3)
    FROM dwh.fact_hospital_demand f
    JOIN dwh.dim_date d     ON d.date_key = f.month_date_key
    JOIN dwh.dim_hospital h ON h.hospital_key = f.hospital_key
    JOIN dwh.dim_medicine m ON m.medicine_key = f.medicine_key
    WHERE m.is_controlled AND d.year_no || '-Q' || d.quarter_no = p_quarter
    GROUP BY h.region, m.medicine_name ORDER BY 1, 2;
    GET DIAGNOSTICS n = ROW_COUNT;
    INSERT INTO audit.query_log (run_id, widget_name, variant, started_at, duration_ms, rows_returned, status)
    VALUES ('grafana', 'Q3 controlled requests by state', 'live_dashboard', t0,
            extract(epoch FROM clock_timestamp() - t0) * 1000, n, 'success');
END $$;
