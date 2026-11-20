-- =====================================================================================
-- 02  Phase 1 star schema: Sales & Demand Intelligence (SDI) data mart
--
--   fact_sales            transaction fact      grain: 1 row = 1 sales transaction
--   fact_hospital_demand  periodic snapshot     grain: 1 row = 1 hospital x medicine x month
--
--   dim_date      conformed, role-playing (sale date / demand month / validity dates)
--   dim_medicine  conformed, SCD Type 2 (price & cost history)
--   dim_hospital  SCD Type 1
--   dim_channel   SCD Type 1 (Retail / Hospital / E-commerce ...)
--   dim_outlet    SCD Type 1 (pharmacy / customer point of sale)
--   dim_sales_profile  JUNK dimension (payment method x discount flag x order size band)
--   mfg_batch_id on fact_sales = degenerate dimension (lineage back to manufacturing)
-- =====================================================================================

-- ---------- dim_date -----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dwh.dim_date (
    date_key      INT PRIMARY KEY,          -- yyyymmdd
    full_date     DATE NOT NULL UNIQUE,
    day_of_month  SMALLINT,
    day_name      TEXT,
    day_of_week   SMALLINT,                 -- 1 = Monday
    is_weekend    BOOLEAN,
    week_of_year  SMALLINT,
    month_no      SMALLINT,
    month_name    TEXT,
    quarter_no    SMALLINT,
    year_no       SMALLINT,
    year_month    CHAR(7),                  -- 2024-03
    month_start   DATE
);
INSERT INTO dwh.dim_date
SELECT to_char(d, 'YYYYMMDD')::int, d, extract(day FROM d), trim(to_char(d, 'Day')),
       extract(isodow FROM d), extract(isodow FROM d) IN (6, 7), extract(week FROM d),
       extract(month FROM d), trim(to_char(d, 'Month')), extract(quarter FROM d),
       extract(year FROM d), to_char(d, 'YYYY-MM'), date_trunc('month', d)::date
FROM generate_series(DATE '2020-01-01', DATE '2030-12-31', INTERVAL '1 day') AS g(d)
ON CONFLICT (date_key) DO NOTHING;

-- ---------- dim_medicine (SCD Type 2) ------------------------------------------------
CREATE TABLE IF NOT EXISTS dwh.dim_medicine (
    medicine_key         SERIAL PRIMARY KEY,     -- surrogate key
    medicine_id          TEXT NOT NULL,          -- natural / business key
    medicine_name        TEXT,
    therapeutic_category TEXT,
    dosage_form          TEXT,
    manufacturer         TEXT,
    unit_cost            NUMERIC(14,4),          -- tracked: Type 2
    unit_price           NUMERIC(14,4),          -- tracked: Type 2
    storage_temp_min_c   NUMERIC(6,2),
    storage_temp_max_c   NUMERIC(6,2),
    shelf_life_days      INT,
    is_cold_chain        BOOLEAN,
    row_hash             TEXT NOT NULL,
    valid_from           DATE NOT NULL,
    valid_to             DATE NOT NULL DEFAULT DATE '9999-12-31',
    is_current           BOOLEAN NOT NULL DEFAULT true,
    version_no           INT NOT NULL DEFAULT 1,
    loaded_at            TIMESTAMPTZ DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_medicine_current ON dwh.dim_medicine(medicine_id) WHERE is_current;
CREATE INDEX IF NOT EXISTS ix_dim_medicine_lookup ON dwh.dim_medicine(medicine_id, valid_from, valid_to);

-- ---------- dim_hospital (SCD Type 1) ------------------------------------------------
CREATE TABLE IF NOT EXISTS dwh.dim_hospital (
    hospital_key  SERIAL PRIMARY KEY,
    hospital_id   TEXT NOT NULL UNIQUE,
    hospital_name TEXT,
    city          TEXT,
    region        TEXT,
    updated_at    TIMESTAMPTZ DEFAULT now()
);

-- ---------- dim_channel (SCD Type 1) -------------------------------------------------
CREATE TABLE IF NOT EXISTS dwh.dim_channel (
    channel_key  SERIAL PRIMARY KEY,
    channel_name TEXT NOT NULL UNIQUE
);

-- ---------- dim_outlet (SCD Type 1) --------------------------------------------------
CREATE TABLE IF NOT EXISTS dwh.dim_outlet (
    outlet_key  SERIAL PRIMARY KEY,
    outlet_id   TEXT NOT NULL UNIQUE,
    outlet_type TEXT,
    city        TEXT,
    region      TEXT,
    updated_at  TIMESTAMPTZ DEFAULT now()
);

-- ---------- dim_sales_profile (JUNK dimension) ---------------------------------------
CREATE TABLE IF NOT EXISTS dwh.dim_sales_profile (
    profile_key     SERIAL PRIMARY KEY,
    payment_method  TEXT NOT NULL,
    has_discount    BOOLEAN NOT NULL,
    order_size_band TEXT NOT NULL,          -- Single / Small / Bulk
    UNIQUE (payment_method, has_discount, order_size_band)
);

-- ---------- fact_sales (transaction fact, partitioned by month on sale_date) ---------
CREATE TABLE IF NOT EXISTS dwh.fact_sales (
    sales_transaction_id TEXT NOT NULL,
    sale_date            DATE NOT NULL,
    date_key             INT  NOT NULL REFERENCES dwh.dim_date(date_key),
    medicine_key         INT  NOT NULL REFERENCES dwh.dim_medicine(medicine_key),
    channel_key          INT  NOT NULL REFERENCES dwh.dim_channel(channel_key),
    outlet_key           INT  REFERENCES dwh.dim_outlet(outlet_key),
    profile_key          INT  NOT NULL REFERENCES dwh.dim_sales_profile(profile_key),
    mfg_batch_id         TEXT,                          -- degenerate dimension
    quantity             INT  NOT NULL,
    unit_price           NUMERIC(14,4) NOT NULL,
    gross_amount         NUMERIC(16,4) NOT NULL,        -- qty x price
    discount_amount      NUMERIC(16,4) NOT NULL DEFAULT 0,
    net_revenue          NUMERIC(16,4) NOT NULL,        -- additive
    cost_amount          NUMERIC(16,4),                 -- qty x unit cost (version valid on sale date)
    gross_margin         NUMERIC(16,4),                 -- net_revenue - cost_amount
    ingest_id            BIGINT,                        -- lineage back to staging row
    loaded_at            TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (sales_transaction_id, sale_date)
) PARTITION BY RANGE (sale_date);

-- creates monthly partitions fact_sales_YYYYMM covering [from_date, to_date]
CREATE OR REPLACE FUNCTION dwh.ensure_sales_partitions(from_date DATE, to_date DATE)
RETURNS INT LANGUAGE plpgsql AS $$
DECLARE m DATE; n INT := 0; part TEXT;
BEGIN
    IF from_date IS NULL OR to_date IS NULL THEN RETURN 0; END IF;
    m := date_trunc('month', from_date)::date;
    WHILE m <= to_date LOOP
        part := 'fact_sales_' || to_char(m, 'YYYYMM');
        IF to_regclass('dwh.' || part) IS NULL THEN
            EXECUTE format('CREATE TABLE dwh.%I PARTITION OF dwh.fact_sales FOR VALUES FROM (%L) TO (%L)',
                           part, m, (m + INTERVAL '1 month')::date);
            n := n + 1;
        END IF;
        m := (m + INTERVAL '1 month')::date;
    END LOOP;
    RETURN n;
END $$;

-- ---------- fact_hospital_demand (periodic snapshot, monthly) ------------------------
CREATE TABLE IF NOT EXISTS dwh.fact_hospital_demand (
    demand_record_id TEXT PRIMARY KEY,
    month_date_key   INT NOT NULL REFERENCES dwh.dim_date(date_key),   -- role: demand month
    hospital_key     INT NOT NULL REFERENCES dwh.dim_hospital(hospital_key),
    medicine_key     INT NOT NULL REFERENCES dwh.dim_medicine(medicine_key),
    forecast_qty     NUMERIC(14,2),
    actual_qty       NUMERIC(14,2),
    unfulfilled_qty  NUMERIC(14,2),
    ingest_id        BIGINT,
    loaded_at        TIMESTAMPTZ DEFAULT now()
);
