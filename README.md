[README.md](https://github.com/user-attachments/files/35135489/README.md)
# Pharma Cold Chain DWH: Phase 1 (Sales & Demand Intelligence)

## QUICK START: only Python + PostgreSQL (no Docker, no Airflow)

**1. Create the database (once).** In pgAdmin, connect as `postgres`, open the Query Tool, and run:
```sql
CREATE ROLE dwh WITH LOGIN PASSWORD 'dwh';
CREATE DATABASE pharma_dwh OWNER dwh;
```

**2. Install the Python packages (once).** Open a terminal (Command Prompt) inside this folder:
```
pip install -r requirements.txt
```

**3. Put the data in place.**
- Copy all 7 CSVs into `data\source\`
- Copy files `01`–`06` again into `data\incoming\`
- Copy file `07` again into `data\incoming_stream\`

**4. Column names: already done.** `sql\03_staging_views.sql` is mapped to the provided dataset headers.
(`python scripts\inspect_sources.py` prints them if you ever need to check.)

**5. First load: all the history goes into Postgres.**
```
python run_pipeline.py load
```

**6. Refresh #1 and refresh #2: new data is generated and loaded.**
```
python run_pipeline.py refresh
python run_pipeline.py refresh
```
After each one, the row counts go up and the latest date moves one month forward. Check any time with:
```
python run_pipeline.py status
```

**Automatic mode (file watcher):** `python run_pipeline.py watch` checks the landing folders every 30 s. In a second terminal, run `python run_pipeline.py generate`, and the watcher picks the new files up by itself.

If your Postgres password, user or port is different:
```
python run_pipeline.py load --dsn postgresql://postgres:YOURPASSWORD@localhost:5432/pharma_dwh
```

Look at the results in pgAdmin under `pharma_dwh`. The schemas are `staging` (raw files), `dwh` (star schema + materialized views) and `audit` (load times, rejects, storage, query timings). Everything below this point is the Airflow version of the same pipeline.

---

An automated data engineering pipeline built with **Apache Airflow** and **PostgreSQL**:

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
| Oversampling | `include/generator.py`, DAG `01_generate_source_data` | Adds one new month of data to every source file and drops a delta file into the landing zone |
| Ingestion (batch) | `include/ingestion.py`, DAG `02_sdi_pipeline` | A sensor watches `data/incoming/`; every file is copied into `staging.<table>` and logged in `audit.batch_load_log` |
| Ingestion (stream) | `include/streaming.py`, DAG `03_iot_stream_ingestion` | IoT readings are replayed in micro-batches; throughput, latency and malformed counts go to `audit.stream_metrics` |
| Storage / staging | schema `staging` | Raw, append-only copy of every file, with lineage columns (`_ingest_id`, `_batch_id`, `_source_file`) |
| Transformation | `sql/03`–`sql/21` | Validation → dimensions → facts, loaded incrementally using watermarks |
| Query layer | `sql/30`, `sql/31`, `include/metrics.py` | Indexes, partitions and materialized views; every widget query is timed into `audit.query_log` |

### Star schema (Phase 1 SDI mart)

```
                 dim_date (role-playing: sale date / demand month)
                     │                                  │
dim_channel ─┐       │                                  │      ┌─ dim_hospital (SCD1)
dim_outlet  ─┼── fact_sales ──── dim_medicine (SCD2) ──── fact_hospital_demand
dim_sales_profile ┘  (transaction grain,         (conformed)    (periodic snapshot:
  (JUNK dim)          partitioned by month,                       hospital x medicine x month)
                      degenerate dim: mfg_batch_id)
```

---

# Step-by-step

## Step 0: Install the tools (one time)

1. **Docker Desktop**: https://www.docker.com/products/docker-desktop/. On Windows, accept the WSL 2 option during install, restart, and open Docker Desktop once so the whale icon turns green.
2. **Git**: https://git-scm.com/downloads
3. **Python 3.10+** with pandas. It's only needed for the helper script in Step 3: `pip install pandas`
4. Optional: **DBeaver** or **pgAdmin**, to browse the database (host `localhost`, port `5433`, db `pharma_dwh`, user/pass `dwh`/`dwh`).

Check:
```bash
docker --version
docker compose version
```

## Step 1: Put the repo on GitHub

Unzip this folder, then from inside it:
```bash
git init
git add .
git commit -m "Phase 1 skeleton"
git branch -M main
git remote add origin https://github.com/<your-group>/pharma-cold-chain-dwh.git
git push -u origin main
```

## Step 2: Copy the 7 dataset files in

The file names must start with the prefixes `01_master_data` … `07_iot_streaming_data`, as in the spec.

1. Copy all 7 original CSVs into **`data/source/`**. This is the "system of record", and the generator appends to it.
2. Copy them again into the landing zones. This triggers the first (historical) load:
   - files `01` … `06` → **`data/incoming/`**
   - file `07` → **`data/incoming_stream/`**

Windows PowerShell (from the repo folder):
```powershell
Copy-Item data\source\0[1-6]_*.csv data\incoming\
Copy-Item data\source\07_*.csv data\incoming_stream\
```
Mac / Linux:
```bash
cp data/source/0[1-6]_*.csv data/incoming/
cp data/source/07_*.csv data/incoming_stream/
```

## Step 3: Match the column names (important, ~10 minutes)

The SQL was written against *assumed* column names. Check them against your real files:

```bash
python scripts/inspect_sources.py
```

This prints every CSV header and the name it gets in Postgres (snake_case: `UnitPrice` → `unit_price`), plus which columns hold dates.

Now open **`sql/03_staging_views.sql`**. It's the only SQL file you edit. In each view, change the **left side** of each `AS` to your real staging column name:

```sql
audit.try_numeric(unit_selling_price)  AS unit_price,   -- your file might call it "selling_price"
```
- If a column doesn't exist in your data, replace it with `NULL::text AS city` (or `NULL::numeric`).
- If dates aren't `YYYY-MM-DD`, change the format: `audit.try_date(transaction_date, 'DD/MM/YYYY')`.
- If sales have a discount **percentage** instead of an amount, use the commented formula in the file.

Also check `include/config.py` → `SOURCES`: the `pk` values must match the CSV headers. `inspect_sources.py` warns you if they don't.

> Stuck? Paste the output of `inspect_sources.py` to your assistant and ask for the mapping file.

## Step 4: Start the stack

```bash
docker compose up -d
docker compose ps          # all 3 should be "running" / "healthy" after ~1 minute
```

Linux only: first run `echo "AIRFLOW_UID=$(id -u)" > .env` so Airflow can move files in `data/`.

| Service | URL | Login |
|---|---|---|
| Airflow | http://localhost:8080 | user `admin`. Get the password with the command below |
| Grafana | http://localhost:3000 | `admin` / `admin` |
| Postgres | `localhost:5433` | `dwh` / `dwh`, database `pharma_dwh` |

Airflow admin password:
```bash
docker compose exec airflow cat /opt/airflow/standalone_admin_password.txt
```

In the Airflow UI you should see 3 DAGs. If one shows an import error, click it to read the error. It's usually a typo in a `.py` file.

## Step 5: First run = historical load

In Airflow, **un-pause** (toggle on):
- `02_sdi_pipeline`: runs every 5 min
- `03_iot_stream_ingestion`: runs every 2 min

Or press the play button ▶ on `02_sdi_pipeline` to start it right away. Click the run → **Graph** view and watch the tasks go green:

```
setup_schemas → create_star_schema → ingestion(wait_for_new_files → load_files_to_staging)
 → transform(build_staging_views → load_dim_medicine_scd2 → dim_hospital / dim_channel_outlet_junk
             → load_fact_sales / load_fact_hospital_demand)
 → query_layer(build_indexes_and_mvs → refresh_materialized_views)
 → observability(capture_storage_metrics, benchmark_dashboard_queries)
```

If a task goes red, click it → **Logs**. A `column "xyz" does not exist` error means Step 3 needs fixing. Fix the view, then click the failed task → **Clear** to re-run it.

Check the result:
```bash
docker compose exec postgres psql -U dwh -d pharma_dwh -c "
SELECT 'fact_sales' t, count(*), min(sale_date)::text, max(sale_date)::text FROM dwh.fact_sales
UNION ALL SELECT 'fact_hospital_demand', count(*), NULL, NULL FROM dwh.fact_hospital_demand
UNION ALL SELECT 'dim_medicine', count(*), NULL, NULL FROM dwh.dim_medicine
UNION ALL SELECT 'rejected rows', count(*), NULL, NULL FROM audit.rejected_rows;"
```
Expected: about 500,000 sales rows (minus rejects) covering 2023–2025, 27,216 demand rows and 63 medicines.

## Step 6: Refresh #1

1. Airflow → `01_generate_source_data` → ▶ **Trigger DAG**. It creates one new month (e.g. 2026-01) of data for all 7 files.
2. Within 5 minutes `02_sdi_pipeline` detects the new files and runs again (or press ▶ on it yourself).
3. Re-run the check query from Step 5. Row counts go up and `max(sale_date)` moves into the new month.

Show that the warehouse kept its history (old rows still there) and that SCD Type 2 kicked in:
```bash
docker compose exec postgres psql -U dwh -d pharma_dwh -c "
SELECT medicine_id, version_no, unit_price, valid_from, valid_to, is_current
FROM dwh.dim_medicine WHERE medicine_id IN
 (SELECT medicine_id FROM dwh.dim_medicine GROUP BY 1 HAVING count(*) > 1) ORDER BY 1, 2;"
```

## Step 7: Refresh #2

Trigger `01_generate_source_data` again and wait for the pipeline. The data now reaches the next month (e.g. 2026-02).
For a bigger refresh, use **Trigger DAG w/ config** and pass `{"scale": 2}` (two months' worth of rows).

Tip: wait until all 7 generator tasks are green before the pipeline run starts. Otherwise one file can be picked up in the next run (that's harmless, just slower).

## Step 8: Query layer: show the optimisation

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
Only `fact_sales_202512` appears in the plan.

## Step 9: DE dashboards in Grafana (batch, stream, storage, query)

Open http://localhost:3000. Every panel's SQL is in **`docs/grafana_panels.md`**. Build 4 dashboards (≈4 panels each), set auto-refresh to 10s, then trigger a refresh (Step 6) and watch them move.
Export each dashboard (Share → Export → Save to file) into `grafana/dashboards/` and commit it.

## Step 10: Data generation report (PDF, 1–2 pages)

The statistics come from:
```bash
docker compose exec postgres psql -U dwh -d pharma_dwh -c "
SELECT source_key, target_period, rows_before, rows_generated, bad_rows_injected, rows_after, growth_pct, method
FROM audit.generation_log ORDER BY generated_at, source_key;"
```
The method (bootstrap resampling, one month per batch, why that size, injected bad rows) is described at the top of `include/generator.py`. Rewrite it in your own words, add the table, and export to PDF.

Then commit and push. The original CSVs (500k sales rows) may be too large for GitHub's 100 MB limit. If so, use Git LFS (`git lfs track "data/source/*.csv"`) or upload a zip to Google Drive and put the link in this README.

---

## Useful commands

```bash
docker compose logs -f airflow          # Airflow logs
docker compose restart airflow          # after changing include/*.py
docker compose down                     # stop (keeps the data)
docker compose down -v                  # stop AND delete the warehouse (start over from Step 2)
```

Run the generator without Airflow (from the repo root):
```bash
python include/generator.py --only 05_pharmacy_sales_data --scale 1
```

## Troubleshooting

| Problem | Fix |
|---|---|
| `port is already allocated` | Change the left side of the port in `docker-compose.yml` (e.g. `"5434:5432"`) |
| DAG not showing | Wait 30 s. Check **Browse → DAG import errors** |
| File ended up in `data/failed/` | `SELECT source_file, error_message FROM audit.batch_load_log WHERE status='failed';` |
| Sensor task shows *skipped* | Normal: no new files had landed, so the run was skipped |
| Want to reload everything | `docker compose down -v`, empty `data/archive`, restore the original CSVs in `data/source`, repeat Step 2 |

## Repo layout

```
dags/        Airflow DAGs (thin: they only wire tasks together)
include/     Python logic: config, generator, ingestion, streaming, metrics, db helpers
sql/         All SQL, numbered in execution order (03 = the column-mapping file you edit)
scripts/     inspect_sources.py
docs/        grafana_panels.md
data/        source / incoming / incoming_stream / archive / failed
```
