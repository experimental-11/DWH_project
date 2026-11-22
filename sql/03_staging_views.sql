-- =====================================================================================
-- 03  STAGING MAPPING VIEWS  <<<  THE ONLY FILE YOU NEED TO EDIT FOR YOUR COLUMN NAMES  >>>
--
-- The loader stores every CSV column as TEXT in staging.<table>, with the header converted
-- to snake_case (MedicineID -> medicine_id, UnitPrice -> unit_price, "Temp (C)" -> temp_c).
-- (Already mapped to the provided dataset headers.)
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
    generic_name                                 AS medicine_name,
    therapeutic_class                            AS therapeutic_category,   -- e.g. Biguanide (spec: "by Therapeutic Class")
    disease_category                             AS disease_category,       -- e.g. Diabetes Mellitus
    audit.try_bool(controlled_medicine_flag)     AS is_controlled,          -- Y/N -> boolean
    dosage_form                                  AS dosage_form,
    primary_plant_id                             AS manufacturer,           -- no manufacturer name in the file; plant ID
    audit.try_numeric(unit_manufacturing_cost_usd)  AS unit_cost,
    audit.try_numeric(recommended_retail_price_usd) AS unit_price,
    audit.try_numeric(storage_min_temp_c)        AS storage_temp_min_c,
    audit.try_numeric(storage_max_temp_c)        AS storage_temp_max_c,
    (audit.try_int(shelf_life_months) * 30)::int AS shelf_life_days,         -- file holds months
    icd10_category                               AS icd10_category,
    strength                                     AS strength,
    audit.try_int(pack_size)::int                AS pack_size,
    prescription_class                           AS prescription_class
FROM staging.master_data;

DROP VIEW IF EXISTS staging.v_sales CASCADE;
CREATE VIEW staging.v_sales AS
SELECT
    _ingest_id,
    NULLIF(btrim(sales_transaction_id), '')      AS sales_transaction_id,
    audit.try_date(sales_date, 'YYYY-MM-DD')     AS sale_date,
    CASE WHEN sales_timestamp_utc ~ '^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}'
         THEN sales_timestamp_utc::timestamptz END AS sale_timestamp_utc,
    NULLIF(btrim(medicine_id), '')               AS medicine_id,
    NULLIF(btrim(production_batch_id), '')       AS mfg_batch_id,
    NULLIF(btrim(pharmacy_id), '')               AS pharmacy_id,
    pharmacy_name                                AS pharmacy_name,
    pharmacy_gln                                 AS pharmacy_gln,
    pharmacy_type                                AS pharmacy_type,
    pharmacy_city                                AS city,
    pharmacy_state                               AS region,
    -- The file has only 2 SalesChannel values (Physical Pharmacy / Ecommerce); hospital
    -- pharmacy sales are identified by PharmacyType. Derive the 3 business channels:
    CASE WHEN NULLIF(btrim(sales_channel), '') IS NULL THEN NULL
         WHEN sales_channel = 'Ecommerce'           THEN 'E-commerce'
         WHEN pharmacy_type = 'Hospital Pharmacy'   THEN 'Hospital Pharmacy'
         ELSE 'Retail Pharmacy' END              AS channel_name,
    audit.try_int(quantity_sold_packs)::int      AS quantity,
    audit.try_numeric(unit_retail_price_usd)     AS unit_price,
    COALESCE(audit.try_numeric(discount_amount_usd), 0) AS discount_amount,
    audit.try_numeric(cost_of_goods_sold_usd)    AS cogs_amount,             -- source COGS (used for margin)
    audit.try_numeric(sales_tax_usd)             AS sales_tax_amount,
    payment_method                               AS payment_method,
    customer_segment                             AS customer_segment,
    payer_type                                   AS payer_type,
    promotion_code                               AS promotion_code
FROM staging.pharmacy_sales;

DROP VIEW IF EXISTS staging.v_hospital_demand CASCADE;
CREATE VIEW staging.v_hospital_demand AS
SELECT
    _ingest_id,
    NULLIF(btrim(hospital_demand_record_id), '') AS demand_record_id,
    NULLIF(btrim(hospital_id), '')               AS hospital_id,
    hospital_name                                AS hospital_name,
    hospital_type                                AS hospital_type,
    city                                         AS city,
    state                                        AS region,
    audit.try_int(catchment_population)::int     AS catchment_population,
    audit.try_int(licensed_beds)::int            AS licensed_beds,
    audit.try_int(icu_beds)::int                 AS icu_beds,
    NULLIF(btrim(medicine_id), '')               AS medicine_id,
    -- demand month: works for '2024-03' and '2024-03-01'
    date_trunc('month', audit.try_date(demand_month, 'YYYY-MM'))::date AS demand_month,
    audit.try_numeric(forecast_demand_packs)     AS forecast_qty,
    audit.try_numeric(requested_quantity_packs)  AS requested_qty,
    audit.try_numeric(approved_quantity_packs)   AS approved_qty,
    audit.try_numeric(received_quantity_packs)   AS received_qty,
    audit.try_numeric(emergency_request_quantity_packs) AS emergency_qty,
    audit.try_numeric(dispensed_quantity_packs)  AS dispensed_qty,
    audit.try_numeric(consumed_quantity_packs)   AS actual_qty,
    audit.try_numeric(unfulfilled_demand_packs)  AS unfulfilled_qty,
    audit.try_numeric(estimated_disease_cases)   AS estimated_disease_cases
FROM staging.hospital_demand;

-- =====================================================================================
-- DO NOT EDIT BELOW: derived columns shared by the dimension and fact loads
-- =====================================================================================
CREATE VIEW staging.v_sales_clean AS
SELECT v.*,
       COALESCE(NULLIF(btrim(v.payment_method), ''), 'Unknown')            AS payment_method_clean,
       COALESCE(NULLIF(btrim(v.customer_segment), ''), 'Unknown')          AS customer_segment_clean,
       COALESCE(NULLIF(btrim(v.payer_type), ''), 'Unknown')                AS payer_type_clean,
       COALESCE(NULLIF(btrim(v.promotion_code), ''), 'None')               AS promotion_code_clean,
       COALESCE(v.discount_amount, 0) > 0                                    AS has_discount,
       CASE WHEN v.quantity <= 1 THEN 'Single'
            WHEN v.quantity <= 10 THEN 'Small'
            ELSE 'Bulk' END                                                  AS order_size_band
FROM staging.v_sales v;
