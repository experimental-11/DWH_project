-- =====================================================================================
-- 11  dim_hospital  -  SCD Type 1 (latest values overwrite)
-- =====================================================================================
INSERT INTO dwh.dim_hospital (hospital_id, hospital_name, city, region)
SELECT DISTINCT ON (hospital_id) hospital_id, hospital_name, city, region
FROM staging.v_hospital_demand
WHERE _ingest_id > (SELECT last_ingest_id FROM audit.etl_watermark WHERE target = 'dim_hospital')
  AND hospital_id IS NOT NULL
ORDER BY hospital_id, _ingest_id DESC
ON CONFLICT (hospital_id) DO UPDATE
SET hospital_name = EXCLUDED.hospital_name, city = EXCLUDED.city, region = EXCLUDED.region,
    updated_at = now();

UPDATE audit.etl_watermark
SET last_ingest_id = COALESCE((SELECT max(_ingest_id) FROM staging.hospital_demand), last_ingest_id),
    updated_at = now()
WHERE target = 'dim_hospital';
