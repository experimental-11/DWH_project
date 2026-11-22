"""Data Oversampling Layer.

Method (write this up in the 1-2 page PDF):
  * Bootstrap resampling: a batch is built by sampling existing rows WITH replacement, so the
    distributions (channel mix, medicine mix, quantities, prices) match the historical data and
    all foreign keys (MedicineID, BatchID, WarehouseID ...) stay valid.
  * Batch size = seasonal naive forecast x scale: rows in the SAME calendar month last year
    x year-over-year growth (last 12 months / previous 12 months, capped to 0.8-1.25).
    The data is strongly seasonal (Dec/Jan peaks), so a flat average would look like a drop.
    Files with < 24 months of history fall back to the average month.
    One batch = one new month of business activity, so every refresh pushes the warehouse
    into a NEW time period.
  * New primary keys continue the existing ID sequence (e.g. TXN0500001 -> TXN0500002 ...).
  * The main business date of every sampled row is moved into the next month after the
    latest date in the file. Days are drawn with the historical WEEKDAY profile (rows per
    Monday, per Tuesday ...), and the original time of day is kept. Every other date column
    in the row is shifted by the same amount (so expiry - manufacture gaps stay realistic).
  * Monthly-grain files (all dates on day 1, e.g. hospital demand) copy the latest month as a
    template (each hospital x medicine combination appears once). Quantity columns (*Packs)
    get a per-row demand-level factor ~N(1, 0.07), and the forecast gets an extra independent
    error ~N(1, 0.05), so forecast vs actual moves from month to month.
  * Master data: a few new SKUs + price changes on existing SKUs (exercises SCD Type 2).
    New SKUs "launch" in sales: in the next sales batch, a share of the parent medicine's
    rows (the SKU they were cloned from, same GTIN14) is relabelled to the new SKU.
  * A small share of rows is deliberately broken (blank key / bad date / negative price) so the
    validation and reject counters have something to catch.
  * New rows are APPENDED to data/source/<file>.csv (the system of record keeps growing) and
    the same rows are written as a delta file into the landing zone for the file watcher.

Run from Airflow (DAG 01_generate_source_data) or by hand:
    python include/generator.py --scale 1.0
"""
from __future__ import annotations

import argparse
import re
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

from config import GENERATOR, INCOMING_DIR, SOURCE_DIR, SOURCES, STREAM_DIR

DATE_FORMATS = [
    "%Y-%m-%dT%H:%M:%SZ",          # ISO-8601 UTC, e.g. 2023-11-12T13:43:13Z (sales, IoT, mfg timestamps)
    "%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M",
    "%Y-%m-%d", "%Y/%m/%d", "%d/%m/%Y %H:%M:%S", "%d/%m/%Y %H:%M", "%d/%m/%Y",
    "%m/%d/%Y %H:%M:%S", "%m/%d/%Y", "%d-%m-%Y", "%Y-%m",
]
DATE_HINT = re.compile(r"date|time|month|expiry|period|timestamp", re.I)


# ----------------------------------------------------------------------------- helpers
def _sniff_format(series: pd.Series) -> str | None:
    sample = series[series != ""].head(300)
    if sample.empty:
        return None
    for fmt in DATE_FORMATS:
        try:
            pd.to_datetime(sample, format=fmt)
            return fmt
        except (ValueError, TypeError):
            continue
    return None


def detect_date_columns(df: pd.DataFrame) -> dict[str, str]:
    """Return {column: strftime format} for every column that holds dates."""
    found = {}
    for col in df.columns:
        if col.lower().endswith("id"):
            continue
        fmt = _sniff_format(df[col])
        if fmt:
            found[col] = fmt
    return found


def _parse(series: pd.Series, fmt: str) -> pd.Series:
    return pd.to_datetime(series.where(series != ""), format=fmt, errors="coerce")


def _fmt(ts: pd.Series, fmt: str) -> pd.Series:
    out = ts.dt.strftime(fmt)
    if fmt.endswith(".%f"):
        out = out.str[:-3]
    return out.fillna("")


def _id_factory(existing: pd.Series):
    """Return f(k) -> k new unique IDs that continue the existing sequence."""
    vals = existing[existing != ""].astype(str)
    nums = pd.to_numeric(vals, errors="coerce")
    if len(vals) and nums.notna().all():
        start = int(nums.max()) + 1
        return lambda k: [str(start + i) for i in range(k)]
    parts = vals.str.extract(r"^(.*?)(\d+)$").dropna()
    if parts.empty:
        stamp = datetime.now().strftime("%Y%m%d%H%M%S")
        return lambda k: [f"GEN{stamp}{i:06d}" for i in range(k)]
    prefix = parts[0].mode().iloc[0]
    digits = parts.loc[parts[0] == prefix, 1]
    width = int(digits.str.len().max())
    start = int(pd.to_numeric(digits).max()) + 1
    return lambda k: [f"{prefix}{str(start + i).zfill(width)}" for i in range(k)]


def _decimals(series: pd.Series) -> int:
    s = series[series.str.contains(r"\.", na=False)].head(200)
    return int(s.str.split(".").str[1].str.len().max()) if len(s) else 0


def _append_csv(df: pd.DataFrame, path: Path):
    """Append rows to an existing CSV, making sure we start on a new line."""
    with open(path, "rb+") as fh:
        fh.seek(0, 2)
        if fh.tell() > 0:
            fh.seek(-1, 2)
            if fh.read(1) != b"\n":
                fh.write(b"\n")
    df.to_csv(path, mode="a", header=False, index=False)


def _price_cols(df):
    return [c for c in df.columns if re.search(r"price|cost|amount|revenue", c, re.I)]


def _seasonal_size(months: pd.Series, target, scale: float):
    """Rows for the target month = same month last year x YoY growth (seasonal naive)."""
    counts = months.value_counts().sort_index()
    same = target - 12
    if len(counts) >= 24 and same in counts.index:
        last12 = counts[(counts.index >= target - 12) & (counts.index <= target - 1)].sum()
        prev12 = counts[(counts.index >= target - 24) & (counts.index <= target - 13)].sum()
        growth = float(np.clip(last12 / prev12, 0.8, 1.25)) if prev12 else 1.0
        n = max(1, int(round(counts[same] * growth * scale)))
        return n, f"seasonal: {same} x YoY growth {growth:.3f}"
    return max(1, int(round(counts.mean() * scale))), "average month"


def _day_weights(main_valid: pd.Series, target):
    """Days of the target month, weighted by the historical rows-per-weekday rate."""
    days = pd.date_range(target.start_time, periods=target.days_in_month, freq="D")
    d = main_valid.dt.normalize()
    span = pd.date_range(d.min(), d.max(), freq="D")
    rows_per_dow = d.dt.dayofweek.value_counts()
    days_per_dow = pd.Series(span.dayofweek).value_counts()
    rate = (rows_per_dow / days_per_dow).reindex(range(7)).fillna(0)
    w = rate.reindex(days.dayofweek).to_numpy(dtype=float)
    if w.sum() <= 0:
        w = np.ones(len(days))
    return days, w / w.sum()


def _perturb_monthly(batch: pd.DataFrame, rng) -> pd.DataFrame:
    """Add month-to-month variation to quantity (*Packs) columns of a monthly template."""
    qty_cols = [c for c in batch.columns if re.search(r"packs$", c, re.I)]
    if not qty_cols:
        return batch
    level = rng.normal(1.0, 0.07, len(batch)).clip(0.75, 1.25)      # demand level per row
    for c in qty_cols:
        v = pd.to_numeric(batch[c], errors="coerce")
        f = level
        if "forecast" in c.lower():                                   # independent forecast error
            f = level * rng.normal(1.0, 0.05, len(batch)).clip(0.85, 1.15)
        new = (v * f).round().clip(lower=0)
        batch[c] = new.map(lambda x: str(int(x)) if pd.notna(x) else "").where(v.notna(), batch[c])
    return batch


def _launch_new_skus(batch: pd.DataFrame, df: pd.DataFrame, rng) -> int:
    """Relabel a share of the parent medicine's rows to SKUs that have no history yet."""
    share = GENERATOR.get("new_sku_launch_share", 0)
    master_path = SOURCE_DIR / "01_master_data.csv"
    needed = {"MedicineID", "GTIN14"}
    if share <= 0 or not master_path.exists() or not needed <= set(batch.columns):
        return 0
    master = pd.read_csv(master_path, dtype=str, keep_default_na=False)
    master.columns = [c.lstrip("﻿") for c in master.columns]
    known = set(df["MedicineID"])
    moved = 0
    for _, sku in master[~master["MedicineID"].isin(known)].iterrows():
        parents = master[(master["GTIN14"] == sku["GTIN14"]) & master["MedicineID"].isin(known)]
        if parents.empty:
            continue
        idx = batch.index[batch["MedicineID"] == parents["MedicineID"].iloc[0]]
        k = int(round(len(idx) * share))
        if k == 0:
            continue
        take = rng.choice(idx, size=k, replace=False)
        batch.loc[take, "MedicineID"] = sku["MedicineID"]
        if "GenericName" in batch.columns and "GenericName" in master.columns:
            batch.loc[take, "GenericName"] = sku["GenericName"]
        moved += k
    return moved


def _inject_bad_rows(df, pk0, main_col, rng, ratio):
    n_bad = int(round(len(df) * ratio))
    if n_bad == 0:
        return df, 0
    idx = rng.choice(df.index, size=n_bad, replace=False)
    thirds = np.array_split(idx, 3)
    df.loc[thirds[0], pk0] = ""                                   # null key
    if main_col:
        df.loc[thirds[1], main_col] = "not-a-date"                # unparsable date
    price = [c for c in _price_cols(df) if "price" in c.lower()]
    if price:
        df.loc[thirds[2], price[0]] = "-1"                        # price <= 0
    return df, n_bad


# ----------------------------------------------------------------------------- generators
def _generate_master(key, spec, df, rng):
    pk = spec["pk"] if isinstance(spec["pk"], str) else spec["pk"][0]
    n_new, n_chg = GENERATOR["master_new_skus"], GENERATOR["master_price_changes"]

    new_rows = df.sample(n=n_new, replace=True, random_state=rng.integers(1e9)).copy()
    new_rows[pk] = _id_factory(df[pk])(n_new)
    name_cols = [c for c in df.columns if "name" in c.lower()]
    if name_cols:
        new_rows[name_cols[0]] = new_rows[name_cols[0]] + " (New SKU)"

    changed_idx = rng.choice(df.index, size=min(n_chg, len(df)), replace=False)
    for col in _price_cols(df):
        dec = _decimals(df[col])
        vals = pd.to_numeric(df.loc[changed_idx, col], errors="coerce") * (1 + GENERATOR["price_change_pct"])
        df.loc[changed_idx, col] = vals.round(dec).map(lambda v: f"{v:.{dec}f}" if pd.notna(v) else "")
    changed = df.loc[changed_idx].copy()

    batch = pd.concat([changed, new_rows], ignore_index=True)
    full = pd.concat([df, new_rows], ignore_index=True)        # master is rewritten, not appended
    return batch, full, {"method": "new SKUs + price changes (SCD2)",
                         "new_skus": n_new, "price_changes": len(changed)}


def _generate_transactions(key, spec, df, rng, scale):
    pk_list = [spec["pk"]] if isinstance(spec["pk"], str) else list(spec["pk"])
    pk0 = pk_list[0]
    date_cols = detect_date_columns(df)
    main_col = spec.get("date_col") or next((c for c in date_cols if DATE_HINT.search(c)), None) \
        or next(iter(date_cols), None)
    if main_col and main_col not in date_cols:
        date_cols[main_col] = _sniff_format(df[main_col])

    info = {"date_col": main_col}
    if not main_col:
        # no usable date: plain bootstrap with new IDs
        n = max(1, int(round(len(df) / 36 * scale)))
        batch = df.sample(n=n, replace=True, random_state=rng.integers(1e9)).reset_index(drop=True)
        batch[pk0] = _id_factory(df[pk0])(len(batch))
        info.update(method="bootstrap (no date column found)", target_period=None)
        return batch, info

    main_fmt = date_cols[main_col]
    main = _parse(df[main_col], main_fmt)
    valid = main.notna()
    months = main[valid].dt.to_period("M")
    latest = months.max()
    target = latest + 1
    t_start = target.start_time
    days_in_month = target.days_in_month
    has_time = "%H" in main_fmt
    monthly_grain = (main_fmt == "%Y-%m") or (not has_time and (main[valid].dt.day == 1).all())

    if monthly_grain:
        # periodic snapshot: copy the latest month (every hospital x medicine combo once)
        template = df[valid & (months == latest).reindex(df.index, fill_value=False)]
        n = int(round(len(template) * scale))
        batch = template.sample(n=n, replace=scale > 1, random_state=rng.integers(1e9)) if scale != 1 \
            else template.copy()
        batch = batch.reset_index(drop=True)
        batch = _perturb_monthly(batch, rng)
        unit = np.arange(len(batch))
        new_main = pd.Series([t_start] * len(batch))
        info["method"] = "latest-month template + demand noise (monthly grain)"
    else:
        n_rows, size_method = _seasonal_size(months, target, scale)
        src = df[valid]
        if len(pk_list) > 1:                                   # composite key: sample whole groups
            groups = src.groupby(pk0).indices
            gids = np.array(list(groups.keys()))
            rows_per_group = len(src) / len(gids)
            k = max(1, int(round(n_rows / rows_per_group)))
            chosen = rng.choice(gids, size=k, replace=True)
            pieces, units = [], []
            for u, g in enumerate(chosen):
                pieces.append(src.iloc[groups[g]])
                units.extend([u] * len(groups[g]))
            batch = pd.concat(pieces, ignore_index=True)
            unit = np.array(units)
            n_units = k
        else:
            batch = src.sample(n=n_rows, replace=True, random_state=rng.integers(1e9)).reset_index(drop=True)
            unit = np.arange(len(batch))
            n_units = len(batch)
        # day of month follows the historical weekday profile
        days, weights = _day_weights(main[valid], target)
        unit_days = days[rng.choice(len(days), size=n_units, p=weights)]
        if has_time:
            # keep each unit's original time of day (preserves the intraday pattern)
            old = _parse(batch[main_col], main_fmt)
            tod = (old - old.dt.normalize()).groupby(unit).first().to_numpy()
            unit_dates = unit_days + pd.to_timedelta(tod)
        else:
            unit_dates = unit_days
        new_main = pd.Series(np.asarray(unit_dates)[unit])
        info["method"] = ("bootstrap resampling" + (" of whole groups" if len(pk_list) > 1 else "")
                          + f", weekday-weighted days, size = {size_method}")
        if key in GENERATOR.get("new_sku_files", []):
            info["new_sku_rows"] = _launch_new_skus(batch, df, rng)

    # shift every date column by the same delta as the main date
    old_main = _parse(batch[main_col], main_fmt)
    delta = new_main.values - old_main.values
    for col, fmt in date_cols.items():
        if not fmt:
            continue
        parsed = _parse(batch[col], fmt)
        shifted = parsed + pd.to_timedelta(delta)
        batch[col] = _fmt(shifted, fmt).where(parsed.notna(), batch[col])

    # new primary keys (one per unit for composite keys)
    new_ids = np.array(_id_factory(df[pk0])(int(unit.max()) + 1))
    batch[pk0] = new_ids[unit]

    info["target_period"] = str(target)
    return batch, info


def generate_source(key: str, scale: float | None = None, seed: int | None = None) -> dict:
    """Generate one new batch for one source file. Returns statistics for logging."""
    spec = SOURCES[key]
    scale = GENERATOR["default_scale"] if scale is None else scale
    seed = GENERATOR["random_seed"] if seed is None else seed
    rng = np.random.default_rng(seed)

    src_path = SOURCE_DIR / f"{key}.csv"
    if not src_path.exists():
        raise FileNotFoundError(f"{src_path} not found - copy the original CSVs into data/source first")
    df = pd.read_csv(src_path, dtype=str, keep_default_na=False)
    df.columns = [c.lstrip("﻿") for c in df.columns]
    rows_before = len(df)

    if spec.get("master"):
        batch, full, info = _generate_master(key, spec, df, rng)
        full.to_csv(src_path, index=False)
        n_bad = 0
    else:
        batch, info = _generate_transactions(key, spec, df, rng, scale)
        pk0 = spec["pk"] if isinstance(spec["pk"], str) else spec["pk"][0]
        batch, n_bad = _inject_bad_rows(batch, pk0, info.get("date_col"), rng, GENERATOR["bad_row_ratio"])
        _append_csv(batch, src_path)

    stamp = datetime.now().strftime("%Y%m%dT%H%M%S")
    out_dir = STREAM_DIR if spec.get("stream") else INCOMING_DIR
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{key}__batch_{stamp}.csv"
    tmp = out_path.with_suffix(".tmp")                 # write then rename => watcher never sees half a file
    batch.to_csv(tmp, index=False)
    tmp.rename(out_path)

    return {
        "source": key,
        "rows_before": rows_before,
        "rows_generated": int(len(batch)),
        "bad_rows_injected": int(n_bad),
        "rows_after": rows_before + (len(batch) if not spec.get("master") else info.get("new_skus", 0)),
        "growth_pct": round(100 * len(batch) / max(rows_before, 1), 2),
        "target_period": info.get("target_period"),
        "method": info.get("method"),
        "output_file": out_path.name,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
    }


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--scale", type=float, default=None)
    ap.add_argument("--only", nargs="*", help="source keys, e.g. 05_pharmacy_sales_data")
    ap.add_argument("--seed", type=int, default=None)
    args = ap.parse_args()
    for k in (args.only or SOURCES):
        print(generate_source(k, args.scale, args.seed))
