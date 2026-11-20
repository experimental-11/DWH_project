"""Step 3 helper: print the columns of every source CSV as they will appear in staging.

    python scripts/inspect_sources.py            (run from the repo root)

Use the output to edit sql/03_staging_views.sql (left side of each AS) and, if needed,
the pk/date_col entries in include/config.py.
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "include"))

import pandas as pd                      # noqa: E402
from config import SOURCE_DIR, SOURCES   # noqa: E402
from db import to_snake                  # noqa: E402
from generator import detect_date_columns  # noqa: E402

for key, spec in SOURCES.items():
    path = SOURCE_DIR / f"{key}.csv"
    print("=" * 90)
    if not path.exists():
        print(f"{key}.csv  -> NOT FOUND in data/source/")
        continue
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    df.columns = [c.lstrip("﻿") for c in df.columns]
    print(f"{key}.csv   rows={len(df):,}   staging table = staging.{spec['table']}")
    pk = spec["pk"] if isinstance(spec["pk"], list) else [spec["pk"]]
    missing = [c for c in pk if c not in df.columns]
    if missing:
        print(f"  !! configured pk {missing} not in header -> fix include/config.py")
    dates = detect_date_columns(df)
    print(f"  {'CSV header':<32} {'staging column':<32} sample")
    for c in df.columns:
        sample = next((v for v in df[c].head(50) if v != ""), "")
        flag = f"   [date {dates[c]}]" if c in dates else ""
        print(f"  {c:<32} {to_snake(c):<32} {sample[:30]}{flag}")
