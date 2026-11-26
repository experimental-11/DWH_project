# Pharma Cold Chain DWH: Phase 1 (Sales & Demand Intelligence)

An automated data engineering pipeline built with **Apache Airflow**, **PostgreSQL** and **Grafana**, all running in Docker:

```
 generator (DAG 01)          file watcher + loader (DAG 02 / 03)        transform (DAG 02)              query layer
 ───────────────────         ───────────────────────────────────        ──────────────────              ───────────
 data/source/*.csv  ──►  data/incoming/        ──►  staging.*   ──►  dwh.dim_* (SCD1/SCD2/junk)  ──►  indexes, monthly partitions,
   (+1 month / run)      data/incoming_stream/       (raw TEXT,        dwh.fact_sales                    materialized views
                                                      ODS)             dwh.fact_hospital_demand
                                   │                    │                    │                               │
                                   └──── audit.* metrics (batch, stream, rejects, storage, query) ◄──────────┘ ──► Grafana
```

| Layer | Where | What it does |
|---|---|---|
| Oversampling | `include/generator.py`, DAG `01_generate_source_data` | Adds one new month of data to every source file and drops a delta file into the landing zone. Retry-safe: a retried task finishes the same batch (plan files in `data/source/.pending/`) instead of adding the month twice |
| Ingestion (batch) | `include/ingestion.py`, DAG `02_sdi_pipeline` | A sensor watches `data/incoming/`; every file is copied into `staging.<table>` and logged in `audit.batch_load_log` |
| Ingestion (stream) | `include/streaming.py`, DAG `03_iot_stream_ingestion` | IoT readings are replayed in micro-batches; throughput, latency and malformed counts go to `audit.stream_metrics` |
| Storage / staging | schema `staging` | Raw, append-only copy of every file, with lineage columns (`_ingest_id`, `_batch_id`, `_source_file`) |
| Transformation | `sql/03`–`sql/21` | Validation → dimensions → facts, loaded incrementally using watermarks. Each fact load is reconciled (staged = loaded + rejected + duplicates) in `audit.load_reconciliation`; if it does not add up, the load fails |
| Query layer | `sql/30`, `sql/31`, `include/metrics.py` | Indexes, partitions, materialized views and the Q1–Q3 functions; every query is timed into `audit.query_log` |

### Star schema (Phase 1 SDI mart)

```
                 dim_date (role-playing: sale date / demand month)
                     │                                  │
dim_channel  ─┐      │                                  │      ┌─ dim_hospital (SCD1)
dim_location ─┼── fact_sales ──── dim_medicine (SCD2) ──── fact_hospital_demand
dim_sales_profile ┘  (transaction grain,         (conformed)    (periodic snapshot:
  (JUNK dim)          partitioned by month,                       hospital x medicine x month)
                      degenerate dim: mfg_batch_id)
```

| Table | Grain / type | Attributes & measures |
|---|---|---|
| `fact_sales` | 1 row = 1 sales transaction | quantity, returned packs, unit price, gross, discount, net revenue, sales tax, cost, gross margin, sale timestamp (UTC), `mfg_batch_id`; `hospital_key` on hospital-pharmacy sales |
| `fact_hospital_demand` | 1 row = hospital x medicine x month | forecast, requested, approved, received, emergency, dispensed, consumed (actual), unfulfilled packs, stockout days, estimated disease cases (semi-additive: repeated on each medicine of a disease) |
| `dim_medicine` | SCD2 | name, therapeutic class, disease category, controlled, dosage form, plant, cost, price, storage temps, shelf life, cold chain; ICD-10, strength, pack size, prescription class, formulary status (Type 1 inside the SCD2 dim) |
| `dim_hospital` | SCD1 | name, type, city, state, catchment population, licensed beds, ICU beds |
| `dim_location` | SCD1 | the pharmacy (point of sale): id, name, GLN, type, city, state |
| `dim_channel` | SCD1 | Retail Pharmacy / Hospital Pharmacy / E-commerce |
| `dim_sales_profile` | junk | payment method x discount flag x order size band x customer segment x payer type x promotion code |
| `dim_date` | role-playing | sale date / demand month |

---

# Step-by-step

## Step 0: Install the tools (one time)

1. **Docker Desktop**: https://www.docker.com/products/docker-desktop/. On Windows, accept the WSL 2 option during install, restart, and open Docker Desktop once so the whale icon turns green.
2. **Python 3.12** with the packages in `requirements.txt` (`py -3.12 -m pip install -r requirements.txt`). Only needed to rebuild the Grafana dashboards or run the generator outside Airflow.
3. Optional: **DBeaver** or **pgAdmin**, to browse the database (host `localhost`, port `5433`, db `pharma_dwh`, user and password from your `.env`).

## Step 1: Copy the 7 dataset files in

The file names must start with the prefixes `01_master_data` … `07_iot_streaming_data`.

1. Copy all 7 original CSVs into **`data/source/`**. This is the "system of record", and the generator appends to it.
2. Copy them again into the landing zones. This triggers the first (historical) load:
   - files `01` … `06` → **`data/incoming/`**
   - file `07` → **`data/incoming_stream/`**

Windows PowerShell (from the repo folder):
```powershell
Copy-Item data\source\0[1-6]_*.csv data\incoming\
Copy-Item data\source\07_*.csv data\incoming_stream\
```

Use the original UTF-8 CSVs. Do not open and re-save them in Excel: Excel changes the date format and turns GTIN codes into scientific notation.

`sql/03_staging_views.sql` is already mapped to the provided dataset headers.

## Step 2: Set your passwords, then start the stack

All passwords live in a `.env` file next to `docker-compose.yml`. It is listed in `.gitignore`,
so it never goes to GitHub. Create it from the template and change every `change_me` value
(letters and digits only):

```bash
cp .env.example .env        # Windows PowerShell: Copy-Item .env.example .env
```

| Variable | Used for |
|---|---|
| `POSTGRES_USER` / `POSTGRES_PASSWORD` / `POSTGRES_DB` | the warehouse database (Airflow tasks and Grafana connect with it) |
| `AIRFLOW_DB_PASSWORD` | Airflow's own metadata database |
| `AIRFLOW_ADMIN_PASSWORD` | Airflow web UI login (user `admin`) |
| `GF_ADMIN_USER` / `GF_ADMIN_PASSWORD` | Grafana login (first start only; later change it in the Grafana UI) |

The Postgres passwords are applied only when the database volume is first created. To change them
later, run `docker compose down -v` (this deletes the warehouse) and start again.

```bash
docker compose up -d
docker compose ps          # all 3 should be "running" / "healthy" after ~1 minute
```

| Service | URL | Login |
|---|---|---|
| Airflow | http://localhost:8081 (host port 8081, in case 8080 is taken) | `admin` / `AIRFLOW_ADMIN_PASSWORD` |
| Grafana | http://localhost:3000 | `GF_ADMIN_USER` / `GF_ADMIN_PASSWORD` |
| Postgres | `localhost:5433` | `POSTGRES_USER` / `POSTGRES_PASSWORD`, database `POSTGRES_DB` |

The Airflow web UI can take 2–3 minutes to come up after `docker compose up -d`.
The `psql -U dwh` commands below use the default user name; replace `dwh` if you changed `POSTGRES_USER`.

## Step 3: First run = historical load

In Airflow, **un-pause** (toggle on):
- `02_sdi_pipeline`: runs every 5 min
- `03_iot_stream_ingestion`: runs every 2 min

Or press ▶ on `02_sdi_pipeline` to start right away. Click the run → **Graph** view and watch the tasks go green:

```
setup_schemas → create_star_schema → ingestion(wait_for_new_files → load_files_to_staging)
 → transform(build_staging_views → load_dim_medicine_scd2 → load_dim_hospital / load_dim_channel_location_junk
             → load_fact_hospital_demand / load_fact_sales)
 → query_layer(build_indexes_and_mvs → refresh_materialized_views)
 → observability(capture_storage_metrics, benchmark_dashboard_queries)
```

If a task goes red, click it → **Logs**, fix the cause, then click the task → **Clear** to re-run it.

Check the result:
```bash
docker compose exec postgres psql -U dwh -d pharma_dwh -c "
SELECT 'fact_sales' t, count(*), min(sale_date)::text, max(sale_date)::text FROM dwh.fact_sales
UNION ALL SELECT 'fact_hospital_demand', count(*), NULL, NULL FROM dwh.fact_hospital_demand
UNION ALL SELECT 'dim_medicine', count(*), NULL, NULL FROM dwh.dim_medicine
UNION ALL SELECT 'rejected rows', count(*), NULL, NULL FROM audit.rejected_rows;"
```
Expected: 500,000 sales rows covering 2023–2025, 27,216 demand rows and 63 medicines.

When the load is finished you can pause the DAGs again. Without new files they only run their file checks.

## Step 4: Refresh #1

1. Make sure `02_sdi_pipeline` and `03_iot_stream_ingestion` are un-paused.
2. Airflow → `01_generate_source_data` → ▶ **Trigger DAG**. It creates one new month (e.g. 2026-01) of data for all 7 files.
3. Within 5 minutes `02_sdi_pipeline` detects the new files and runs again (or press ▶ on it yourself).
4. Re-run the check query from Step 3. Row counts go up and `max(sale_date)` moves into the new month.

Show that the warehouse kept its history and that SCD Type 2 kicked in:
```bash
docker compose exec postgres psql -U dwh -d pharma_dwh -c "
SELECT medicine_id, version_no, unit_price, valid_from, valid_to, is_current
FROM dwh.dim_medicine WHERE medicine_id IN
 (SELECT medicine_id FROM dwh.dim_medicine GROUP BY 1 HAVING count(*) > 1) ORDER BY 1, 2;"
```

## Step 5: Refresh #2

Trigger `01_generate_source_data` again and wait for the pipeline. The data now reaches the next month (e.g. 2026-02).
For a bigger refresh, use **Trigger DAG w/ config** and pass `{"scale": 2}` (two months' worth of rows).

## Step 6: Query layer: show the optimisation

```bash
docker compose exec postgres psql -U dwh -d pharma_dwh -c "
SELECT widget_name, variant, round(avg(duration_ms),1) avg_ms, count(*) runs
FROM audit.query_log GROUP BY 1,2 ORDER BY 1,2;"
```
The materialized-view variant should be many times faster than the raw star join. To show partition pruning:
```bash
docker compose exec postgres psql -U dwh -d pharma_dwh -c "
EXPLAIN SELECT sum(net_revenue) FROM dwh.fact_sales WHERE sale_date >= '2025-12-01' AND sale_date < '2026-01-01';"
```
Only `fact_sales_202512` appears in the plan. The three business queries are in `sql/40_business_queries.sql`.

## Step 7: Dashboards in Grafana

Open http://localhost:3000 → Dashboards → folder **Pharma DWH - Phase 1**. The 5 dashboards are provisioned automatically from `grafana/provisioning/dashboards/json/`:

| Dashboard | Shows |
|---|---|
| 1 · Batch Ingestion | load duration per file, rows per batch, success rate |
| 2 · Streaming Ingestion (IoT) | throughput (events/s), end-to-end latency, malformed records |
| 3 · Storage & Staging | row counts, storage per source domain, date coverage |
| 4 · Transformation & Dimensional Modeling | step durations, rows per dim/fact, validation rejects, SCD2 history |
| 5 · Analytical Query Layer | queries executed, query error rate, average execution time per dashboard widget (raw star join vs materialized view), and the Q1–Q3 business queries with dropdowns |

Dashboards 1–4 auto-refresh every 10 s. Dashboard 5 has no auto-refresh, so each business query is counted only when someone actually runs it.

For free-form SQL use Grafana **Explore** (left menu) → data source *PharmaDWH* → *Code*.

To change the dashboards, edit `scripts/build_grafana_dashboards.py`, run `py -3.12 scripts/build_grafana_dashboards.py`, then `docker compose restart grafana`.

## Step 8: Data generation report (PDF, 1–2 pages)

The statistics come from:
```bash
docker compose exec postgres psql -U dwh -d pharma_dwh -c "
SELECT source_key, target_period, rows_before, rows_generated, bad_rows_injected, rows_after, growth_pct, method
FROM audit.generation_log ORDER BY generated_at, source_key;"
```
The method is described at the top of `include/generator.py`.

The dataset CSVs are **not** in the repository: `.gitignore` excludes `data/**/*.csv`, because the sales
file (218 MB) is over GitHub's 100 MB limit. Only the empty folders are kept (`.gitkeep`). Share the
dataset separately (for example a zip on Google Drive, linked here) and copy it in as described in Step 1.

---

## Useful commands

```bash
docker compose logs -f airflow          # Airflow logs
docker compose restart airflow          # after changing include/*.py
docker compose down                     # stop (keeps the data)
docker compose down -v                  # stop AND delete the warehouse (start over from Step 1)
```

Run the generator without Airflow (from the repo root):
```bash
py -3.12 include/generator.py --only 05_pharmacy_sales_data --scale 1
```

## Troubleshooting

| Problem | Fix |
|---|---|
| `port is already allocated` | Change the left side of the port in `docker-compose.yml` (e.g. `"5434:5432"`) |
| DAG not showing | Wait 30 s. Check **Browse → DAG import errors** |
| File ended up in `data/failed/` | `SELECT source_file, error_message FROM audit.batch_load_log WHERE status='failed';` |
| Sensor task shows *skipped* | Normal: no new files had landed, so the run was skipped |
| A load task is red | Check its log. A failed file fails `load_files_to_staging` (the other files still load). A reconciliation error means rows were lost: see `audit.load_reconciliation` and `audit.rejected_rows` |
| Want to reload everything | `docker compose down -v`, empty `data/archive`, restore the original CSVs in `data/source`, repeat Step 1 |

## Repo layout

```
dags/        Airflow DAGs (thin: they only wire tasks together)
include/     Python logic: config, generator, ingestion, streaming, metrics, db helpers
sql/         All SQL, numbered in execution order
scripts/     build_grafana_dashboards.py (dashboards as code)
grafana/     provisioned data sources + dashboard JSON
docs/        project reports
data/        source / incoming / incoming_stream / archive / failed
```
