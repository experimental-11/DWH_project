-- =====================================================================================
-- 12  dim_channel, dim_location (SCD Type 1) and dim_sales_profile (JUNK dimension)
-- =====================================================================================
CREATE TEMP TABLE _s ON COMMIT DROP AS
SELECT *
FROM staging.v_sales_clean
WHERE _ingest_id > (SELECT last_ingest_id FROM audit.etl_watermark WHERE target = 'dim_sales_misc');

INSERT INTO dwh.dim_channel (channel_name)
SELECT DISTINCT channel_name FROM _s WHERE channel_name IS NOT NULL
ON CONFLICT (channel_name) DO NOTHING;

INSERT INTO dwh.dim_location (pharmacy_id, pharmacy_name, pharmacy_gln, pharmacy_type, city, region)
SELECT DISTINCT ON (pharmacy_id) pharmacy_id, pharmacy_name, pharmacy_gln, pharmacy_type, city, region
FROM _s WHERE pharmacy_id IS NOT NULL
ORDER BY pharmacy_id, _ingest_id DESC
ON CONFLICT (pharmacy_id) DO UPDATE
SET pharmacy_name = EXCLUDED.pharmacy_name, pharmacy_gln = EXCLUDED.pharmacy_gln,
    pharmacy_type = EXCLUDED.pharmacy_type, city = EXCLUDED.city, region = EXCLUDED.region,
    updated_at = now();

-- junk dimension: one row per combination of low-cardinality flags
INSERT INTO dwh.dim_sales_profile (payment_method, has_discount, order_size_band,
       customer_segment, payer_type, promotion_code)
SELECT DISTINCT payment_method_clean, has_discount, order_size_band,
       customer_segment_clean, payer_type_clean, promotion_code_clean
FROM _s WHERE quantity IS NOT NULL
ON CONFLICT DO NOTHING;

UPDATE audit.etl_watermark
SET last_ingest_id = COALESCE((SELECT max(_ingest_id) FROM _s), last_ingest_id), updated_at = now()
WHERE target = 'dim_sales_misc';
