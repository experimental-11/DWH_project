-- =====================================================================================
-- 40  Business queries for the SDI star schema (the 3 dimensional queries in SDI.Excel)
--     Not part of the automated pipeline: run them by hand (pgAdmin / psql / Grafana).
-- =====================================================================================

-- Q1: Which medication is the most common in each city to treat diabetes in 2025?
--     fact_sales -> dim_date (year), dim_medicine (disease category), dim_location (city)
WITH ranked AS (
    SELECT l.city, m.medicine_name, sum(f.quantity) AS packs_sold,
           rank() OVER (PARTITION BY l.city ORDER BY sum(f.quantity) DESC) AS rnk
    FROM dwh.fact_sales f
    JOIN dwh.dim_date d     ON d.date_key = f.date_key
    JOIN dwh.dim_medicine m ON m.medicine_key = f.medicine_key
    JOIN dwh.dim_location l ON l.location_key = f.location_key
    WHERE d.year_no = 2025
      AND m.disease_category = 'Diabetes Mellitus'
    GROUP BY l.city, m.medicine_name
)
SELECT city, medicine_name, packs_sold FROM ranked WHERE rnk = 1 ORDER BY city;

-- Q2: For the current year, Net Sales and Gross Margin of E-commerce vs Physical
--     pharmacies, broken down by therapeutic class.
--     fact_sales -> dim_date (year), dim_channel (channel), dim_medicine (therapeutic class)
--     "Current year" = latest year in the warehouse.
SELECT CASE WHEN c.channel_name = 'E-commerce' THEN 'E-commerce' ELSE 'Physical Pharmacy' END AS channel,
       m.therapeutic_category              AS therapeutic_class,
       round(sum(f.net_revenue), 2)         AS net_sales_usd,
       round(sum(f.gross_margin), 2)        AS gross_margin_usd
FROM dwh.fact_sales f
JOIN dwh.dim_date d     ON d.date_key = f.date_key
JOIN dwh.dim_channel c  ON c.channel_key = f.channel_key
JOIN dwh.dim_medicine m ON m.medicine_key = f.medicine_key
WHERE d.year_no = (SELECT max(year_no) FROM dwh.fact_sales fs JOIN dwh.dim_date dd ON dd.date_key = fs.date_key)
GROUP BY 1, 2
ORDER BY 2, 1;

-- Q3: What quantity of controlled medications was requested by each state in the last quarter?
--     fact_hospital_demand -> dim_date (role: demand month), dim_medicine (controlled flag),
--     dim_hospital (state). "Last quarter" = latest quarter in the warehouse.
WITH last_q AS (
    SELECT date_trunc('quarter', max(d.full_date))::date AS q_start
    FROM dwh.fact_hospital_demand f JOIN dwh.dim_date d ON d.date_key = f.month_date_key
)
SELECT h.region AS state, m.medicine_name, sum(f.requested_qty) AS requested_packs
FROM dwh.fact_hospital_demand f
JOIN dwh.dim_date d     ON d.date_key = f.month_date_key
JOIN dwh.dim_hospital h ON h.hospital_key = f.hospital_key
JOIN dwh.dim_medicine m ON m.medicine_key = f.medicine_key
CROSS JOIN last_q
WHERE m.is_controlled
  AND d.full_date >= last_q.q_start AND d.full_date < last_q.q_start + INTERVAL '3 months'
GROUP BY h.region, m.medicine_name
ORDER BY h.region, m.medicine_name;
