-- =====================================================================================
-- 10  dim_medicine  -  SCD Type 2
--   * new medicine_id                 -> insert version 1 (valid from 1900-01-01)
--   * attributes changed (row_hash)   -> close current version, insert version n+1
--   * change effective date           = day after the latest sale already in the warehouse
--                                       (the simulated "business clock")
--   * only staging rows newer than the watermark are processed (incremental load)
--   * icd10_category, strength, pack_size, prescription_class are descriptive Type 1
--     attributes: not in row_hash, overwritten on every version (no new version)
-- =====================================================================================

CREATE TEMP TABLE _src ON COMMIT DROP AS
SELECT DISTINCT ON (v.medicine_id)
       v.*,
       md5(concat_ws('|', v.medicine_name, v.therapeutic_category, v.disease_category, v.is_controlled,
                     v.dosage_form, v.manufacturer,
                     v.unit_cost, v.unit_price, v.storage_temp_min_c, v.storage_temp_max_c,
                     v.shelf_life_days)) AS row_hash
FROM staging.v_medicine v
WHERE v._ingest_id > (SELECT last_ingest_id FROM audit.etl_watermark WHERE target = 'dim_medicine')
  AND v.medicine_id IS NOT NULL
ORDER BY v.medicine_id, v._ingest_id DESC;

CREATE TEMP TABLE _eff ON COMMIT DROP AS
SELECT COALESCE((SELECT max(sale_date) FROM dwh.fact_sales) + 1, CURRENT_DATE) AS eff_date;

-- (a) changed rows whose current version started on/after the effective date: overwrite in place
UPDATE dwh.dim_medicine d
SET medicine_name = s.medicine_name, therapeutic_category = s.therapeutic_category,
    disease_category = s.disease_category, is_controlled = s.is_controlled,
    dosage_form = s.dosage_form, manufacturer = s.manufacturer, unit_cost = s.unit_cost,
    unit_price = s.unit_price, storage_temp_min_c = s.storage_temp_min_c,
    storage_temp_max_c = s.storage_temp_max_c, shelf_life_days = s.shelf_life_days,
    is_cold_chain = s.storage_temp_max_c <= 8, row_hash = s.row_hash, loaded_at = now()
FROM _src s, _eff e
WHERE d.medicine_id = s.medicine_id AND d.is_current
  AND d.row_hash <> s.row_hash AND d.valid_from >= e.eff_date;

-- (b) other changed rows: expire the current version ...
CREATE TEMP TABLE _changed ON COMMIT DROP AS
SELECT d.medicine_id, d.version_no
FROM dwh.dim_medicine d JOIN _src s USING (medicine_id)
WHERE d.is_current AND d.row_hash <> s.row_hash;

UPDATE dwh.dim_medicine d
SET valid_to = e.eff_date - 1, is_current = false
FROM _changed c, _eff e
WHERE d.medicine_id = c.medicine_id AND d.is_current;

-- ... and insert the new version
INSERT INTO dwh.dim_medicine (medicine_id, medicine_name, therapeutic_category, disease_category,
       is_controlled, dosage_form,
       manufacturer, unit_cost, unit_price, storage_temp_min_c, storage_temp_max_c,
       shelf_life_days, is_cold_chain, icd10_category, strength, pack_size, prescription_class,
       formulary_status, row_hash, valid_from, version_no)
SELECT s.medicine_id, s.medicine_name, s.therapeutic_category, s.disease_category,
       s.is_controlled, s.dosage_form, s.manufacturer,
       s.unit_cost, s.unit_price, s.storage_temp_min_c, s.storage_temp_max_c, s.shelf_life_days,
       s.storage_temp_max_c <= 8, s.icd10_category, s.strength, s.pack_size, s.prescription_class,
       (SELECT o.formulary_status FROM dwh.dim_medicine o                -- carried over from the old version
         WHERE o.medicine_id = s.medicine_id AND o.version_no = c.version_no),
       s.row_hash, e.eff_date, c.version_no + 1
FROM _src s JOIN _changed c USING (medicine_id), _eff e;

-- (b2) Type 1 attributes: overwrite on all versions of the medicine
UPDATE dwh.dim_medicine d
SET icd10_category = s.icd10_category, strength = s.strength, pack_size = s.pack_size,
    prescription_class = s.prescription_class
FROM _src s
WHERE d.medicine_id = s.medicine_id
  AND (d.icd10_category, d.strength, d.pack_size, d.prescription_class)
      IS DISTINCT FROM (s.icd10_category, s.strength, s.pack_size, s.prescription_class);

-- (c) brand-new medicines
INSERT INTO dwh.dim_medicine (medicine_id, medicine_name, therapeutic_category, disease_category,
       is_controlled, dosage_form,
       manufacturer, unit_cost, unit_price, storage_temp_min_c, storage_temp_max_c,
       shelf_life_days, is_cold_chain, icd10_category, strength, pack_size, prescription_class,
       row_hash, valid_from, version_no)
SELECT s.medicine_id, s.medicine_name, s.therapeutic_category, s.disease_category,
       s.is_controlled, s.dosage_form, s.manufacturer,
       s.unit_cost, s.unit_price, s.storage_temp_min_c, s.storage_temp_max_c, s.shelf_life_days,
       s.storage_temp_max_c <= 8, s.icd10_category, s.strength, s.pack_size, s.prescription_class,
       s.row_hash, DATE '1900-01-01', 1
FROM _src s
WHERE NOT EXISTS (SELECT 1 FROM dwh.dim_medicine d WHERE d.medicine_id = s.medicine_id);

UPDATE audit.etl_watermark
SET last_ingest_id = COALESCE((SELECT max(_ingest_id) FROM staging.master_data), last_ingest_id),
    updated_at = now()
WHERE target = 'dim_medicine';
