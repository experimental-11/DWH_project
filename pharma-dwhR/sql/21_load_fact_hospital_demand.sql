-- =====================================================================================
-- 21  fact_hospital_demand  -  periodic snapshot (hospital x medicine x month)
-- =====================================================================================
CREATE TEMP TABLE _new ON COMMIT DROP AS
SELECT v.*,
       CASE
         WHEN v.demand_record_id IS NULL                    THEN 'null demand record id'
         WHEN v.hospital_id IS NULL                         THEN 'null hospital id'
         WHEN v.medicine_id IS NULL                         THEN 'null medicine (SKU) id'
         WHEN v.demand_month IS NULL                        THEN 'invalid or missing month'
         WHEN v.demand_month NOT BETWEEN DATE '2020-01-01' AND DATE '2030-12-31'
                                                            THEN 'month outside calendar'
         WHEN v.forecast_qty < 0 OR v.actual_qty < 0        THEN 'negative quantity'
         WHEN NOT EXISTS (SELECT 1 FROM dwh.dim_medicine m WHERE m.medicine_id = v.medicine_id)
                                                            THEN 'unknown medicine id'
       END AS reject_reason
FROM staging.v_hospital_demand v
WHERE v._ingest_id > (SELECT last_ingest_id FROM audit.etl_watermark WHERE target = 'fact_hospital_demand');

INSERT INTO audit.rejected_rows (source_table, record_id, reject_reason, raw_row, ingest_id)
SELECT 'staging.hospital_demand', n.demand_record_id, n.reject_reason, to_jsonb(n), n._ingest_id
FROM _new n
WHERE n.reject_reason IS NOT NULL;

INSERT INTO dwh.fact_hospital_demand (demand_record_id, month_date_key, hospital_key, medicine_key,
       forecast_qty, actual_qty, unfulfilled_qty, ingest_id)
SELECT n.demand_record_id,
       to_char(n.demand_month, 'YYYYMMDD')::int,
       h.hospital_key,
       m.medicine_key,
       n.forecast_qty,
       n.actual_qty,
       n.unfulfilled_qty,
       n._ingest_id
FROM _new n
JOIN dwh.dim_hospital h ON h.hospital_id = n.hospital_id
JOIN dwh.dim_medicine m
  ON m.medicine_id = n.medicine_id AND n.demand_month BETWEEN m.valid_from AND m.valid_to
WHERE n.reject_reason IS NULL
ON CONFLICT (demand_record_id) DO NOTHING;

UPDATE audit.etl_watermark
SET last_ingest_id = COALESCE((SELECT max(_ingest_id) FROM _new), last_ingest_id), updated_at = now()
WHERE target = 'fact_hospital_demand';

ANALYZE dwh.fact_hospital_demand;
