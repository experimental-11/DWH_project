-- =====================================================================================
-- 01  Schemas, audit (observability) tables and safe-cast helper functions
--     Idempotent: safe to run on every pipeline run.
-- =====================================================================================
CREATE SCHEMA IF NOT EXISTS staging;   -- ODS: raw text copies of the source files
CREATE SCHEMA IF NOT EXISTS dwh;       -- star schema (dimensions + facts) + query layer
CREATE SCHEMA IF NOT EXISTS audit;     -- pipeline metrics feeding the DE dashboards

-- ---------- data generation log (oversampling report statistics) ----------------------
CREATE TABLE IF NOT EXISTS audit.generation_log (
    id                BIGSERIAL PRIMARY KEY,
    run_id            TEXT,
    source_key        TEXT,
    rows_before       BIGINT,
    rows_generated    BIGINT,
    bad_rows_injected BIGINT,
    rows_after        BIGINT,
    growth_pct        NUMERIC(8,2),
    target_period     TEXT,
    method            TEXT,
    output_file       TEXT,
    generated_at      TIMESTAMPTZ DEFAULT now()
);

-- ---------- batch ingestion dashboard -------------------------------------------------
CREATE TABLE IF NOT EXISTS audit.batch_load_log (
    id            BIGSERIAL PRIMARY KEY,
    batch_id      TEXT,
    source_key    TEXT,
    source_file   TEXT,
    target_table  TEXT,
    started_at    TIMESTAMPTZ,
    finished_at   TIMESTAMPTZ,
    duration_s    NUMERIC(12,3),
    rows_loaded   BIGINT,
    file_size_mb  NUMERIC(12,3),
    status        TEXT,              -- success | failed
    error_message TEXT
);

-- ---------- streaming ingestion dashboard ---------------------------------------------
CREATE TABLE IF NOT EXISTS audit.stream_metrics (
    id              BIGSERIAL PRIMARY KEY,
    run_id          TEXT,
    source_file     TEXT,
    micro_batch_no  INT,
    window_start    TIMESTAMPTZ,
    window_end      TIMESTAMPTZ,
    events_ingested INT,
    malformed_count INT,
    duration_s      NUMERIC(12,4),
    events_per_sec  NUMERIC(14,2),
    avg_latency_s   NUMERIC(12,4),
    max_latency_s   NUMERIC(12,4)
);

-- ---------- validation rejects (batch + stream) ---------------------------------------
CREATE TABLE IF NOT EXISTS audit.rejected_rows (
    id            BIGSERIAL PRIMARY KEY,
    source_table  TEXT,
    record_id     TEXT,
    reject_reason TEXT,
    raw_row       JSONB,
    ingest_id     BIGINT,
    rejected_at   TIMESTAMPTZ DEFAULT now()
);

-- ---------- storage dashboard ---------------------------------------------------------
CREATE TABLE IF NOT EXISTS audit.storage_metrics (
    id           BIGSERIAL PRIMARY KEY,
    run_id       TEXT,
    captured_at  TIMESTAMPTZ DEFAULT now(),
    schema_name  TEXT,
    table_name   TEXT,
    source_domain TEXT,
    row_count    BIGINT,
    size_bytes   BIGINT
);
CREATE TABLE IF NOT EXISTS audit.date_coverage (
    id              BIGSERIAL PRIMARY KEY,
    run_id          TEXT,
    captured_at     TIMESTAMPTZ DEFAULT now(),
    table_name      TEXT,
    min_date        DATE,
    max_date        DATE,
    span_days       INT,
    distinct_months INT
);

-- ---------- query dashboard -----------------------------------------------------------
CREATE TABLE IF NOT EXISTS audit.query_log (
    id            BIGSERIAL PRIMARY KEY,
    run_id        TEXT,
    widget_name   TEXT,              -- which dashboard visual / KPI the query feeds
    variant       TEXT,              -- raw_star_join | materialized_view
    started_at    TIMESTAMPTZ DEFAULT now(),
    duration_ms   NUMERIC(12,3),
    rows_returned INT,
    status        TEXT,
    error_message TEXT
);

-- ---------- ETL step timings + incremental watermarks ---------------------------------
CREATE TABLE IF NOT EXISTS audit.etl_step_log (
    id            BIGSERIAL PRIMARY KEY,
    run_id        TEXT,
    step_name     TEXT,
    executed_at   TIMESTAMPTZ DEFAULT now(),
    duration_s    NUMERIC(12,3),
    status        TEXT,
    error_message TEXT
);
CREATE TABLE IF NOT EXISTS audit.etl_watermark (
    target         TEXT PRIMARY KEY,   -- which dwh table this watermark belongs to
    last_ingest_id BIGINT NOT NULL DEFAULT 0,
    updated_at     TIMESTAMPTZ DEFAULT now()
);
INSERT INTO audit.etl_watermark(target) VALUES
    ('dim_medicine'), ('dim_hospital'), ('dim_sales_misc'), ('fact_sales'), ('fact_hospital_demand')
ON CONFLICT DO NOTHING;

-- ---------- safe casts: bad values become NULL instead of crashing the load -----------
CREATE OR REPLACE FUNCTION audit.try_numeric(v TEXT) RETURNS NUMERIC
LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE WHEN btrim(v) ~ '^[-+]?[0-9]*\.?[0-9]+([eE][-+]?[0-9]+)?$' THEN btrim(v)::numeric END
$$;

CREATE OR REPLACE FUNCTION audit.try_int(v TEXT) RETURNS BIGINT
LANGUAGE sql IMMUTABLE AS $$
    SELECT round(audit.try_numeric(v))::bigint
$$;

-- fmt follows Postgres to_date patterns: 'YYYY-MM-DD', 'DD/MM/YYYY', 'MM/DD/YYYY', 'YYYY-MM'
CREATE OR REPLACE FUNCTION audit.try_date(v TEXT, fmt TEXT DEFAULT 'YYYY-MM-DD') RETURNS DATE
LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE d DATE;
BEGIN
    IF v IS NULL OR btrim(v) = '' THEN RETURN NULL; END IF;
    d := to_date(left(btrim(v), length(fmt)), fmt);
    IF d < DATE '1990-01-01' OR d > DATE '2100-01-01' THEN RETURN NULL; END IF;
    RETURN d;
EXCEPTION WHEN others THEN
    RETURN NULL;
END $$;

CREATE OR REPLACE FUNCTION audit.try_timestamp(v TEXT) RETURNS TIMESTAMP
LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN
    IF v IS NULL OR btrim(v) = '' THEN RETURN NULL; END IF;
    RETURN btrim(v)::timestamp;
EXCEPTION WHEN others THEN
    RETURN NULL;
END $$;

CREATE OR REPLACE FUNCTION audit.try_bool(v TEXT) RETURNS BOOLEAN
LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE lower(btrim(v)) WHEN 'true' THEN true WHEN 't' THEN true WHEN 'yes' THEN true
                                WHEN 'y' THEN true WHEN '1' THEN true
                                WHEN 'false' THEN false WHEN 'f' THEN false WHEN 'no' THEN false
                                WHEN 'n' THEN false WHEN '0' THEN false END
$$;
