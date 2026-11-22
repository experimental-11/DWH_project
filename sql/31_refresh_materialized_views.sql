-- =====================================================================================
-- 31  Refresh the pre-aggregated layer after every load (CONCURRENTLY = no read locks)
-- =====================================================================================
REFRESH MATERIALIZED VIEW CONCURRENTLY dwh.mv_channel_revenue_monthly;
REFRESH MATERIALIZED VIEW CONCURRENTLY dwh.mv_medicine_velocity;
REFRESH MATERIALIZED VIEW CONCURRENTLY dwh.mv_hospital_forecast_accuracy;
REFRESH MATERIALIZED VIEW CONCURRENTLY dwh.mv_sales_daily;
ANALYZE dwh.mv_channel_revenue_monthly;
ANALYZE dwh.mv_medicine_velocity;
ANALYZE dwh.mv_hospital_forecast_accuracy;
ANALYZE dwh.mv_sales_daily;
