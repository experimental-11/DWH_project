-- =====================================================================================
-- 03  STAGING MAPPING VIEWS  <<<  THE ONLY FILE YOU NEED TO EDIT FOR YOUR COLUMN NAMES  >>>
--
-- The loader stores every CSV column as TEXT in staging.<table>, with the header converted
-- to snake_case (MedicineID -> medicine_id, UnitPrice -> unit_price, "Temp (C)" -> temp_c).
-- Run   python scripts/inspect_sources.py   to print the exact staging column names.
--
-- Each view renames + type-casts the raw columns into the standard names the rest of the
-- warehouse uses. Edit ONLY the LEFT side of each "AS" if your column is called differently.
-- If you don't have a column at all, replace it with NULL (e.g.  NULL::text AS city ).
-- Dates: change the format argument of try_date if needed ('DD/MM/YYYY', 'MM/DD/YYYY' ...).
-- =====================================================================================

DROP VIEW IF EXISTS staging.v_medicine CASCADE;
CREATE VIEW staging.v_medicine AS
SELECT
    _ingest_id,
    NULLIF(btrim(medicine_id), '')               AS medicine_id,
    medicine_name                                AS medicine_name,
    therapeutic_category                         AS therapeutic_category,
    dosage_form                                  AS dosage_form,
    manufacturer_name                            AS manufacturer,
    audit.try_numeric(unit_cost)                 AS unit_cost,
    audit.try_numeric(unit_price)                AS unit_price,
    audit.try_numeric(min_storage_temp_c)        AS storage_temp_min_c,
    audit.try_numeric(max_storage_temp_c)        AS storage_temp_max_c,
    audit.try_int(shelf_life_days)::int          AS shelf_life_days
FROM staging.master_data;

DROP VIEW IF EXISTS staging.v_sales CASCADE;
CREATE VIEW staging.v_sales AS
SELECT
    _ingest_id,
    NULLIF(btrim(sales_transaction_id), '')      AS sales_transaction_id,
    audit.try_date(transaction_date, 'YYYY-MM-DD') AS sale_date,
    NULLIF(btrim(medicine_id), '')               AS medicine_id,
    NULLIF(btrim(manufacturing_order_id), '')    AS mfg_batch_id,
    NULLIF(btrim(pharmacy_id), '')               AS outlet_id,
    pharmacy_type                                AS outlet_type,
    city                                         AS city,
    region                                       AS region,
    NULLIF(btrim(sales_channel), '')             AS channel_name,
    audit.try_int(quantity_sold)::int            AS quantity,
    audit.try_numeric(unit_selling_price)        AS unit_price,
    -- if your file has a discount PERCENT instead of an amount, use e.g.
    --   audit.try_numeric(quantity_sold) * audit.try_numeric(unit_selling_price) * audit.try_numeric(discount_pct) / 100
    COALESCE(audit.try_numeric(discount_amount), 0) AS discount_amount,
    payment_method                               AS payment_method
FROM staging.pharmacy_sales;

DROP VIEW IF EXISTS staging.v_hospital_demand CASCADE;
CREATE VIEW staging.v_hospital_demand AS
SELECT
    _ingest_id,
    NULLIF(btrim(hospital_demand_record_id), '') AS demand_record_id,
    NULLIF(btrim(hospital_id), '')               AS hospital_id,
    hospital_name                                AS hospital_name,
    city                                         AS city,
    region                                       AS region,
    NULLIF(btrim(medicine_id), '')               AS medicine_id,
    -- demand month: works for '2024-03' and '2024-03-01'
    date_trunc('month', audit.try_date(demand_month, 'YYYY-MM'))::date AS demand_month,
    audit.try_numeric(forecasted_demand)         AS forecast_qty,
    audit.try_numeric(actual_consumption)        AS actual_qty,
    audit.try_numeric(unfulfilled_demand)        AS unfulfilled_qty
FROM staging.hospital_demand;

-- =====================================================================================
-- DO NOT EDIT BELOW: derived columns shared by the dimension and fact loads
-- =====================================================================================
CREATE VIEW staging.v_sales_clean AS
SELECT v.*,
       COALESCE(NULLIF(btrim(v.payment_method), ''), 'Unknown')            AS payment_method_clean,
       COALESCE(v.discount_amount, 0) > 0                                    AS has_discount,
       CASE WHEN v.quantity <= 1 THEN 'Single'
            WHEN v.quantity <= 10 THEN 'Small'
            ELSE 'Bulk' END                                                  AS order_size_band
FROM staging.v_sales v;
