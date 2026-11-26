-- =====================================================================================
-- 21  fact_hospital_demand  -  periodic snapshot (hospital x medicine x month)
--   Same pattern as 20: validation -> dimension lookups (no silent drops) ->
--   insert -> reconciliation (staged = loaded + rejected + duplicates, else the load fails).
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
         WHEN v.forecast_qty < 0 OR v.actual_qty < 0 OR v.requested_qty < 0
                                                            THEN 'negative quantity'
         WHEN NOT EXISTS (SELECT 1 FROM dwh.dim_medicine m WHERE m.medicine_id = v.medicine_id)
                                                            THEN 'unknown medicine id'
       END AS reject_reason
FROM staging.v_hospital_demand v
WHERE v._ingest_id > (SELECT last_ingest_id FROM audit.etl_watermark WHERE target = 'fact_hospital_demand');

CREATE TEMP TABLE _keyed ON COMMIT DROP AS
SELECT n.*, h.hospital_key, m.medicine_key
FROM _new n
LEFT JOIN dwh.dim_hospital h ON h.hospital_id = n.hospital_id
LEFT JOIN dwh.dim_medicine m
  ON m.medicine_id = n.medicine_id AND n.demand_month BETWEEN m.valid_from AND m.valid_to;

UPDATE _keyed
SET reject_reason = CASE WHEN hospital_key IS NULL THEN 'no dim_hospital row'
                         ELSE 'no dim_medicine version valid in the demand month' END
WHERE reject_reason IS NULL AND (hospital_key IS NULL OR medicine_key IS NULL);

INSERT INTO audit.rejected_rows (source_table, record_id, reject_reason, raw_row, ingest_id)
SELECT 'staging.hospital_demand', k.demand_record_id, k.reject_reason, to_jsonb(k), k._ingest_id
FROM _keyed k
WHERE k.reject_reason IS NOT NULL;

CREATE TEMP TABLE _recon ON COMMIT DROP AS
SELECT count(*)                                                         AS staged,
       count(*) FILTER (WHERE reject_reason IS NOT NULL)                AS rejected,
       count(*) FILTER (WHERE reject_reason IS NULL)
         - count(DISTINCT demand_record_id) FILTER (WHERE reject_reason IS NULL) AS dup_in_batch,
       (SELECT count(DISTINCT k.demand_record_id) FROM _keyed k
         WHERE k.reject_reason IS NULL
           AND EXISTS (SELECT 1 FROM dwh.fact_hospital_demand f WHERE f.demand_record_id = k.demand_record_id))
                                                                        AS already_loaded
FROM _keyed;

WITH ins AS (
    INSERT INTO dwh.fact_hospital_demand (demand_record_id, month_date_key, hospital_key, medicine_key,
           forecast_qty, requested_qty, approved_qty, received_qty, emergency_qty, dispensed_qty,
           actual_qty, unfulfilled_qty, stockout_days, estimated_disease_cases, ingest_id)
    SELECT k.demand_record_id,
           to_char(k.demand_month, 'YYYYMMDD')::int,
           k.hospital_key,
           k.medicine_key,
           k.forecast_qty,
           k.requested_qty,
           k.approved_qty,
           k.received_qty,
           k.emergency_qty,
           k.dispensed_qty,
           k.actual_qty,
           k.unfulfilled_qty,
           k.stockout_days,
           k.estimated_disease_cases,
           k._ingest_id
    FROM _keyed k
    WHERE k.reject_reason IS NULL
    ON CONFLICT (demand_record_id) DO NOTHING
    RETURNING 1
)
INSERT INTO audit.load_reconciliation (target, staged_rows, loaded_rows, rejected_rows, duplicate_rows, unexplained_rows)
SELECT 'fact_hospital_demand', r.staged, (SELECT count(*) FROM ins), r.rejected, r.dup_in_batch + r.already_loaded,
       r.staged - (SELECT count(*) FROM ins) - r.rejected - r.dup_in_batch - r.already_loaded
FROM _recon r
WHERE r.staged > 0;

DO $$
DECLARE u BIGINT;
BEGIN
    SELECT unexplained_rows INTO u FROM audit.load_reconciliation
    WHERE target = 'fact_hospital_demand' AND checked_at = now();
    IF u <> 0 THEN
        RAISE EXCEPTION 'fact_hospital_demand reconciliation failed: % staged rows were neither loaded, rejected nor duplicates', u;
    END IF;
END $$;

UPDATE audit.etl_watermark
SET last_ingest_id = COALESCE((SELECT max(_ingest_id) FROM _new), last_ingest_id), updated_at = now()
WHERE target = 'fact_hospital_demand';

ANALYZE dwh.fact_hospital_demand;
