-- =====================================================================================
-- 12  dim_channel, dim_outlet (SCD Type 1) and dim_sales_profile (JUNK dimension)
-- =====================================================================================
CREATE TEMP TABLE _s ON COMMIT DROP AS
SELECT *
FROM staging.v_sales_clean
WHERE _ingest_id > (SELECT last_ingest_id FROM audit.etl_watermark WHERE target = 'dim_sales_misc');

INSERT INTO dwh.dim_channel (channel_name)
SELECT DISTINCT channel_name FROM _s WHERE channel_name IS NOT NULL
ON CONFLICT (channel_name) DO NOTHING;

INSERT INTO dwh.dim_outlet (outlet_id, outlet_type, city, region)
SELECT DISTINCT ON (outlet_id) outlet_id, outlet_type, city, region
FROM _s WHERE outlet_id IS NOT NULL
ORDER BY outlet_id, _ingest_id DESC
ON CONFLICT (outlet_id) DO UPDATE
SET outlet_type = EXCLUDED.outlet_type, city = EXCLUDED.city, region = EXCLUDED.region,
    updated_at = now();

-- junk dimension: one row per combination of low-cardinality flags
INSERT INTO dwh.dim_sales_profile (payment_method, has_discount, order_size_band)
SELECT DISTINCT payment_method_clean, has_discount, order_size_band
FROM _s WHERE quantity IS NOT NULL
ON CONFLICT DO NOTHING;

UPDATE audit.etl_watermark
SET last_ingest_id = COALESCE((SELECT max(_ingest_id) FROM _s), last_ingest_id), updated_at = now()
WHERE target = 'dim_sales_misc';
