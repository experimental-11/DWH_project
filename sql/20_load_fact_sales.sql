-- =====================================================================================
-- 20  fact_sales  -  incremental load with validation
--   Validation rules (Phase 1 spec): price > 0, valid dates, non-null SKU IDs
--   (+ non-null transaction id, quantity > 0, channel present, SKU exists in dim_medicine)
--   Failing rows go to audit.rejected_rows with the reason; good rows go to the fact.
--   medicine_key = the SCD2 version that was valid ON THE SALE DATE.
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

INSERT INTO audit.rejected_rows (source_table, record_id, reject_reason, raw_row, ingest_id)
SELECT 'staging.pharmacy_sales', n.sales_transaction_id, n.reject_reason, to_jsonb(n), n._ingest_id
FROM _new n
WHERE n.reject_reason IS NOT NULL;

SELECT dwh.ensure_sales_partitions(min(sale_date), max(sale_date))
FROM _new WHERE reject_reason IS NULL;

INSERT INTO dwh.fact_sales (sales_transaction_id, sale_date, sale_timestamp_utc, date_key, medicine_key,
       channel_key, location_key, profile_key, mfg_batch_id, quantity, unit_price, gross_amount,
       discount_amount, net_revenue, sales_tax_amount, cost_amount, gross_margin, ingest_id)
SELECT n.sales_transaction_id,
       n.sale_date,
       n.sale_timestamp_utc,
       to_char(n.sale_date, 'YYYYMMDD')::int,
       m.medicine_key,
       c.channel_key,
       l.location_key,
       p.profile_key,
       n.mfg_batch_id,
       n.quantity,
       n.unit_price,
       n.quantity * n.unit_price,
       n.discount_amount,
       n.quantity * n.unit_price - n.discount_amount,
       n.sales_tax_amount,
       -- cost = source COGS; falls back to qty x unit manufacturing cost if COGS is missing
       COALESCE(n.cogs_amount, n.quantity * m.unit_cost),
       (n.quantity * n.unit_price - n.discount_amount) - COALESCE(n.cogs_amount, n.quantity * m.unit_cost),
       n._ingest_id
FROM _new n
JOIN dwh.dim_medicine m
  ON m.medicine_id = n.medicine_id AND n.sale_date BETWEEN m.valid_from AND m.valid_to
JOIN dwh.dim_channel c ON c.channel_name = n.channel_name
LEFT JOIN dwh.dim_location l ON l.pharmacy_id = n.pharmacy_id
JOIN dwh.dim_sales_profile p
  ON p.payment_method = n.payment_method_clean
 AND p.has_discount = n.has_discount
 AND p.order_size_band = n.order_size_band
 AND p.customer_segment = n.customer_segment_clean
 AND p.payer_type = n.payer_type_clean
 AND p.promotion_code = n.promotion_code_clean
WHERE n.reject_reason IS NULL
ON CONFLICT DO NOTHING;

UPDATE audit.etl_watermark
SET last_ingest_id = COALESCE((SELECT max(_ingest_id) FROM _new), last_ingest_id), updated_at = now()
WHERE target = 'fact_sales';

ANALYZE dwh.fact_sales;
