from __future__ import annotations

import json
import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

# lbi_app/etl/load.py -> repo root
REPO_ROOT = Path(__file__).resolve().parents[2]
APP_DATA_DIR = REPO_ROOT / "data"
# Use Parquet to preserve pandas dtypes (datetime, categorical, etc.).
CLEAN_PATH = APP_DATA_DIR / "companies_clean.parquet"
DETAIL_LOOKUPS_PATH = APP_DATA_DIR / "detail_lookups.json"


def load_companies_snapshot(
    df: pd.DataFrame,
    *,
    out_path: Path = CLEAN_PATH,
) -> None:
    """
    Write the cleaned companies snapshot to the path that the Dash app will ship/read.

    This is the "Load" step for the snapshot-based deployment model.
    """
    logger.info("=== LOAD STEP ===")

    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Atomic-ish write: write temp then replace.
    # Always produce both parquet and csv snapshots.  The dashboard reads
    # parquet, but having a csv alongside makes manual inspection easier.
    # We write via temporary files then move to avoid partial writes.

    # primary output (parquet)
    pq_out = out_path.with_suffix(".parquet")
    pq_tmp = pq_out.with_suffix(pq_out.suffix + ".tmp")
    df.to_parquet(pq_tmp, index=False)
    pq_tmp.replace(pq_out)

    # also produce CSV copy
    csv_out = out_path.with_suffix(".csv")
    csv_tmp = csv_out.with_suffix(csv_out.suffix + ".tmp")
    df.to_csv(csv_tmp, index=False)
    csv_tmp.replace(csv_out)

    logger.info("Wrote companies snapshot (parquet & csv):\n   %s\n   %s", pq_out, csv_out)


def _format_year_founded(value: object) -> str:
    """Format a year/datetime value for display."""
    if pd.isna(value):
        return "N/A"
    timestamp = pd.to_datetime(value, errors="coerce")
    if pd.notna(timestamp):
        return str(timestamp.year)
    return str(value)


def _format_list_cell(value: object) -> str:
    """Format a list/array column value for display."""
    if isinstance(value, (list, tuple, set)):
        items = [str(item).strip() for item in value if pd.notna(item) and str(item).strip()]
        return ", ".join(items) if items else "N/A"

    if hasattr(value, "tolist") and not isinstance(value, (str, bytes)):
        converted = value.tolist()
        if isinstance(converted, list):
            items = [str(item).strip() for item in converted if pd.notna(item) and str(item).strip()]
            return ", ".join(items) if items else "N/A"

    if pd.isna(value):
        return "N/A"

    text = str(value).strip()
    return text or "N/A"


def _format_rows(df: pd.DataFrame) -> list[dict[str, str]]:
    """Format a dataframe into list of detail rows."""
    year_col = "year founded" if "year founded" in df.columns else None
    cat_col = "categories" if "categories" in df.columns else None
    stage_col = "latest clinical stage" if "latest clinical stage" in df.columns else None
    geo_source = "geo_country" if "geo_country" in df.columns else "geo_clean" if "geo_clean" in df.columns else "geo"

    records = []
    for _, row in df.iterrows():
        year = _format_year_founded(row[year_col]) if year_col else "N/A"
        cat = _format_list_cell(row[cat_col]) if cat_col else "N/A"
        stage_val = row[stage_col]
        stage = "N/A" if stage_col is None or pd.isna(stage_val) else str(stage_val).strip()
        geo = _format_list_cell(row[geo_source]) if geo_source in df.columns else "N/A"
        records.append({
            "Company": str(row["Company"]),
            "Year Founded": year,
            "Category": cat,
            "Clinical Stage": stage,
            "Geo": geo,
        })
    return records


def _build_lookup(df: pd.DataFrame, key_col: str) -> dict[str, list[dict[str, str]]]:
    """Build a lookup dict: key -> list of formatted rows."""
    df = df.dropna(subset=["Company", key_col]).copy()
    if "full overall score" in df.columns:
        df = df.sort_values("full overall score", ascending=False)
    result: dict[str, list[dict[str, str]]] = {}
    for key, group in df.groupby(key_col, observed=True, sort=False):
        result[str(key)] = _format_rows(group)
    return result


def _precompute_stage_details(df: pd.DataFrame) -> dict[str, list[dict[str, str]]]:
    return _build_lookup(df, "latest clinical stage")


def _precompute_year_details(df: pd.DataFrame) -> dict[str, list[dict[str, str]]]:
    df = df.copy()
    df["_year_key"] = pd.to_datetime(df["year founded"], errors="coerce").dt.year.astype("Int64").astype(str)
    df = df[df["_year_key"] != "<NA>"]
    return _build_lookup(df, "_year_key")


def _precompute_category_details(df: pd.DataFrame) -> dict[str, list[dict[str, str]]]:
    df = df.copy()
    df = df.dropna(subset=["categories"])
    df = df.explode("categories")
    df["_cat_key"] = df["categories"]
    return _build_lookup(df, "_cat_key")


def _precompute_country_details(df: pd.DataFrame) -> dict[str, list[dict[str, str]]]:
    col = "geo_country" if "geo_country" in df.columns else "geo_clean"
    df = df.copy()
    df = df.dropna(subset=[col])
    df = df.explode(col)
    df = df[df[col] != "Unknown"]
    df["_country_key"] = df[col]
    return _build_lookup(df, "_country_key")


def save_detail_lookups(
    df: pd.DataFrame,
    *,
    out_path: Path = DETAIL_LOOKUPS_PATH,
) -> None:
    """
    Precompute and save detail lookups (stage, year, category, country) as JSON.
    
    Called by the ETL pipeline after loading the cleaned snapshot.
    The Dash app loads this at startup instead of computing on-the-fly.
    """
    logger.info("=== PRECOMPUTING DETAIL LOOKUPS ===")

    lookups = {
        "stage_details": _precompute_stage_details(df),
        "year_details": _precompute_year_details(df),
        "category_details": _precompute_category_details(df),
        "country_details": _precompute_country_details(df),
    }

    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Atomic-ish write: write temp then replace.
    tmp = out_path.with_suffix(out_path.suffix + ".tmp")
    with open(tmp, "w") as f:
        json.dump(lookups, f, indent=2)
    tmp.replace(out_path)

    logger.info("Wrote detail lookups: %s", out_path)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

if __name__ == "__main__":
    main()