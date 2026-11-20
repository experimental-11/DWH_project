"""Central configuration for the pipeline.

Edit SOURCES if your CSV column names differ from the defaults below.
  pk        : primary key column as it appears in the CSV header (list = composite key;
              new IDs are generated for the FIRST column, the rest are kept).
  date_col  : the main business date of a row (used to move generated rows into a new period).
              None = auto-detect (first column whose name contains date/time/month and parses).
  table     : name of the staging table in Postgres (schema `staging`).
  stream    : True = file goes to the streaming path (data/incoming_stream) instead of batch.
  scale     : multiplier on the generated batch size (1.0 = one average month of history).
"""
import os
from pathlib import Path

DATA_DIR = Path(os.environ.get("PHARMA_DATA_DIR", Path(__file__).resolve().parents[1] / "data"))
SQL_DIR = Path(os.environ.get("PHARMA_SQL_DIR", Path(__file__).resolve().parents[1] / "sql"))

SOURCE_DIR = DATA_DIR / "source"            # system-of-record CSVs (grow with every generator run)
INCOMING_DIR = DATA_DIR / "incoming"        # landing zone watched by the batch ingestion DAG
STREAM_DIR = DATA_DIR / "incoming_stream"   # landing zone watched by the IoT streaming DAG
ARCHIVE_DIR = DATA_DIR / "archive"          # successfully loaded files are moved here
FAILED_DIR = DATA_DIR / "failed"            # files that raised an error are moved here

POSTGRES_CONN_ID = "pharma_dwh"

SOURCES = {
    "01_master_data":                {"table": "master_data",           "pk": "MedicineID",             "date_col": None, "master": True},
    "02_manufacturing_data":         {"table": "manufacturing",         "pk": "ManufacturingOrderID",   "date_col": None},
    "03_manufacturer_delivery_data": {"table": "manufacturer_delivery", "pk": ["ShipmentID", "LineNo"], "date_col": None},
    "04_warehouse_data":             {"table": "warehouse_inventory",   "pk": "InventorySnapshotID",    "date_col": None},
    "05_pharmacy_sales_data":        {"table": "pharmacy_sales",        "pk": "SalesTransactionID",     "date_col": None},
    "06_hospital_demand_data":       {"table": "hospital_demand",       "pk": "HospitalDemandRecordID", "date_col": None},
    "07_iot_streaming_data":         {"table": "iot_readings",          "pk": "SensorReadingID",        "date_col": None, "stream": True},
}

# ---- generator settings -------------------------------------------------------------
GENERATOR = {
    "default_scale": 1.0,       # 1.0 => each batch = one average month of the file's history
    "bad_row_ratio": 0.005,     # 0.5% of generated rows are deliberately broken (tests validation)
    "master_new_skus": 2,       # new medicines added per master-data batch
    "master_price_changes": 3,  # existing medicines whose price changes (drives SCD Type 2)
    "price_change_pct": 0.05,   # +5% price change
    "random_seed": None,        # set an int for reproducible batches
}

# ---- streaming settings -------------------------------------------------------------
STREAM = {
    "micro_batch_size": 2000,   # rows per micro-batch insert
    "max_events_per_sec": 5000, # throttle so the throughput chart is meaningful
}


def source_for_file(filename: str):
    """Map a file name such as '05_pharmacy_sales_data__batch_20261120T101500.csv' to its source key."""
    for key in SOURCES:
        if filename.startswith(key):
            return key
    return None
