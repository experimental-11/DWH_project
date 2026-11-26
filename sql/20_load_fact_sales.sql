-- =====================================================================================
-- 20  fact_sales  -  incremental load with validation and reconciliation
--   Validation rules (Phase 1 spec): price > 0, valid dates, non-null SKU IDs
--   (+ non-null transaction id, quantity > 0, channel present, SKU exists in dim_medicine)
--   Dimension lookups: a row that passes validation but finds no dimension row is also
--   rejected (with the reason), so no row can disappear silently.
--   medicine_key = the SCD2 version that was valid ON THE SALE DATE.
--   Reconciliation: staged rows = loaded + rejected + duplicates, logged to
--   audit.load_reconciliation; if it does not add up, the load fails and nothing is saved.
-- =====================================================================================
CREATE TEMP TABLE _new ON COMMIT DROP AS
SELECT v.*,
       CASE
         WHEN v.sales_transaction_id IS NULL                 THEN 'null transaction id'
         WHEN v.medicine_id IS NULL                          THEN 'null medicine (SKU) id'
         WHEN v.sale_date IS NULL                            THEN 'invalid or missing date'
         WHEN v.sale_date NOT BETWEEN DATE '2020-01-01' AND DATE '2030-12-31'
                                                             THEN 'date outside calendar'
         WHEN v.unit_price IS NULL OR v.unit_price <= 0      THEN 'price missing or <= 0'
         WHEN v.quantity IS NULL OR v.quantity <= 0          THEN 'quantity missing or <= 0'
         WHEN v.channel_name IS NULL                         THEN 'missing channel'
         WHEN NOT EXISTS (SELECT 1 FROM dwh.dim_medicine m WHERE m.medicine_id = v.medicine_id)
                                                             THEN 'unknown medicine id'
       END AS reject_reason
FROM staging.v_sales_clean v
WHERE v._ingest_id > (SELECT last_ingest_id FROM audit.etl_watermark WHERE target = 'fact_sales');

-- dimension lookups (LEFT JOIN, so every staged row stays in _keyed)
CREATE TEMP TABLE _keyed ON COMMIT DROP AS
SELECT n.*,
       m.medicine_key, m.unit_cost AS dim_unit_cost, c.channel_key, l.location_key, h.hospital_key, p.profile_key
FROM _new n
LEFT JOIN dwh.dim_medicine m
  ON m.medicine_id = n.medicine_id AND n.sale_date BETWEEN m.valid_from AND m.valid_to
LEFT JOIN dwh.dim_channel c  ON c.channel_name = n.channel_name
LEFT JOIN dwh.dim_location l ON l.pharmacy_id = n.pharmacy_id
LEFT JOIN dwh.dim_hospital h ON h.hospital_id = n.hospital_id        -- only hospital-pharmacy sales have one
LEFT JOIN dwh.dim_sales_profile p
  ON p.payment_method = n.payment_method_clean
 AND p.has_discount = n.has_discount
 AND p.order_size_band = n.order_size_band
 AND p.customer_segment = n.customer_segment_clean
 AND p.payer_type = n.payer_type_clean
 AND p.promotion_code = n.promotion_code_clean;

UPDATE _keyed
SET reject_reason = CASE WHEN medicine_key IS NULL THEN 'no dim_medicine version valid on the sale date'
                         WHEN channel_key IS NULL  THEN 'no dim_channel row'
                         ELSE 'no dim_sales_profile row' END
WHERE reject_reason IS NULL
  AND (medicine_key IS NULL OR channel_key IS NULL OR profile_key IS NULL);

INSERT INTO audit.rejected_rows (source_table, record_id, reject_reason, raw_row, ingest_id)
SELECT 'staging.pharmacy_sales', k.sales_transaction_id, k.reject_reason, to_jsonb(k), k._ingest_id
FROM _keyed k
WHERE k.reject_reason IS NOT NULL;

SELECT dwh.ensure_sales_partitions(min(sale_date), max(sale_date))
FROM _keyed WHERE reject_reason IS NULL;

-- duplicates: repeated inside this batch, or already in the warehouse (e.g. a file loaded twice)
CREATE TEMP TABLE _recon ON COMMIT DROP AS
SELECT count(*)                                                        AS staged,
       count(*) FILTER (WHERE reject_reason IS NOT NULL)               AS rejected,
       count(*) FILTER (WHERE reject_reason IS NULL)
         - count(DISTINCT (sales_transaction_id, sale_date)) FILTER (WHERE reject_reason IS NULL) AS dup_in_batch,
       (SELECT count(DISTINCT (k.sales_transaction_id, k.sale_date)) FROM _keyed k
         WHERE k.reject_reason IS NULL
           AND EXISTS (SELECT 1 FROM dwh.fact_sales f
                       WHERE f.sales_transaction_id = k.sales_transaction_id AND f.sale_date = k.sale_date))
                                                                       AS already_loaded
FROM _keyed;

WITH ins AS (
    INSERT INTO dwh.fact_sales (sales_transaction_id, sale_date, sale_timestamp_utc, date_key, medicine_key,
           channel_key, location_key, hospital_key, profile_key, mfg_batch_id, quantity, returned_qty,
           unit_price, gross_amount, discount_amount, net_revenue, sales_tax_amount, cost_amount,
           gross_margin, ingest_id)
    SELECT k.sales_transaction_id,
           k.sale_date,
           k.sale_timestamp_utc,
           to_char(k.sale_date, 'YYYYMMDD')::int,
           k.medicine_key,
           k.channel_key,
           k.location_key,
           k.hospital_key,
           k.profile_key,
           k.mfg_batch_id,
           k.quantity,
           COALESCE(k.returned_qty, 0),
           k.unit_price,
           k.quantity * k.unit_price,
           k.discount_amount,
           k.quantity * k.unit_price - k.discount_amount,
           k.sales_tax_amount,
           -- cost = source COGS; falls back to qty x unit manufacturing cost if COGS is missing
           COALESCE(k.cogs_amount, k.quantity * k.dim_unit_cost),
           (k.quantity * k.unit_price - k.discount_amount) - COALESCE(k.cogs_amount, k.quantity * k.dim_unit_cost),
           k._ingest_id
    FROM _keyed k
    WHERE k.reject_reason IS NULL
    ON CONFLICT DO NOTHING
    RETURNING 1
)
INSERT INTO audit.load_reconciliation (target, staged_rows, loaded_rows, rejected_rows, duplicate_rows, unexplained_rows)
SELECT 'fact_sales', r.staged, (SELECT count(*) FROM ins), r.rejected, r.dup_in_batch + r.already_loaded,
       r.staged - (SELECT count(*) FROM ins) - r.rejected - r.dup_in_batch - r.already_loaded
FROM _recon r
WHERE r.staged > 0;

DO $$
DECLARE u BIGINT;
BEGIN
    SELECT unexplained_rows INTO u FROM audit.load_reconciliation
    WHERE target = 'fact_sales' AND checked_at = now();          -- now() = start of this transaction
    IF u <> 0 THEN
        RAISE EXCEPTION 'fact_sales reconciliation failed: % staged rows were neither loaded, rejected nor duplicates', u;
    END IF;
END $$;

UPDATE audit.etl_watermark
SET last_ingest_id = COALESCE((SELECT max(_ingest_id) FROM _new), last_ingest_id), updated_at = now()
WHERE target = 'fact_sales';

ANALYZE dwh.fact_sales;
