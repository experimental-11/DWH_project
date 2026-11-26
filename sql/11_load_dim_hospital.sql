-- =====================================================================================
-- 11  dim_hospital  -  SCD Type 1 (latest values overwrite)
-- =====================================================================================
INSERT INTO dwh.dim_hospital (hospital_id, hospital_name, hospital_type, city, region,
       catchment_population, licensed_beds, icu_beds)
SELECT DISTINCT ON (hospital_id) hospital_id, hospital_name, hospital_type, city, region,
       catchment_population, licensed_beds, icu_beds
FROM staging.v_hospital_demand
WHERE _ingest_id > (SELECT last_ingest_id FROM audit.etl_watermark WHERE target = 'dim_hospital')
  AND hospital_id IS NOT NULL
ORDER BY hospital_id, _ingest_id DESC
ON CONFLICT (hospital_id) DO UPDATE
SET hospital_name = EXCLUDED.hospital_name, hospital_type = EXCLUDED.hospital_type,
    city = EXCLUDED.city, region = EXCLUDED.region,
    catchment_population = EXCLUDED.catchment_population,
    licensed_beds = EXCLUDED.licensed_beds, icu_beds = EXCLUDED.icu_beds,
    updated_at = now();

-- FormularyStatus: comes with the demand rows but is the same for every hospital and month,
-- so it is a medicine attribute (Type 1, written to every version of the medicine)
UPDATE dwh.dim_medicine d
SET formulary_status = s.formulary_status
FROM (SELECT DISTINCT ON (medicine_id) medicine_id, formulary_status
      FROM staging.v_hospital_demand
      WHERE _ingest_id > (SELECT last_ingest_id FROM audit.etl_watermark WHERE target = 'dim_hospital')
        AND medicine_id IS NOT NULL AND formulary_status IS NOT NULL
      ORDER BY medicine_id, _ingest_id DESC) s
WHERE d.medicine_id = s.medicine_id
  AND d.formulary_status IS DISTINCT FROM s.formulary_status;

UPDATE audit.etl_watermark
SET last_ingest_id = COALESCE((SELECT max(_ingest_id) FROM staging.hospital_demand), last_ingest_id),
    updated_at = now()
WHERE target = 'dim_hospital';
