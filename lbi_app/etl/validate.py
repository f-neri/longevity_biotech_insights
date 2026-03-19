from __future__ import annotations

import json
import logging
from pathlib import Path

import pandas as pd

from lbi_app.etl.load import DETAIL_LOOKUPS_PATH
from lbi_app.viz.plots import (
    category_polar_bar_figure,
    clinical_stage_bar_figure,
    companies_founded_over_time_figure,
    geo_map_figure,
)

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parents[2]
CLEAN_PATH = REPO_ROOT / "data" / "companies_clean.parquet"

REQUIRED_LOOKUP_KEYS = (
    "stage_details",
    "year_details",
    "category_details",
    "country_details",
)

REQUIRED_DASHBOARD_COLUMNS = (
    "Company",
    "categories",
    "geo_country",
    "year founded",
    "latest clinical stage",
    "full overall score",
)


def _validate_required_columns(df: pd.DataFrame) -> None:
    missing = [column for column in REQUIRED_DASHBOARD_COLUMNS if column not in df.columns]
    if missing:
        raise ValueError(f"Snapshot is missing required dashboard columns: {missing}")


def _validate_lookup_payload(lookups: object) -> None:
    if not isinstance(lookups, dict):
        raise ValueError("Detail lookups payload must be a dictionary.")

    missing = [key for key in REQUIRED_LOOKUP_KEYS if key not in lookups]
    if missing:
        raise ValueError(f"Detail lookups are missing required keys: {missing}")


def validate_dashboard_artifacts(
    *,
    snapshot_path: Path = CLEAN_PATH,
    detail_lookups_path: Path = DETAIL_LOOKUPS_PATH,
) -> pd.DataFrame:
    """
    Validate the generated dashboard artifacts by loading them and rebuilding
    the figures the app depends on.
    """
    logger.info("=== VALIDATE STEP ===")

    if not snapshot_path.exists():
        raise FileNotFoundError(f"Snapshot not found at {snapshot_path}")

    if not detail_lookups_path.exists():
        raise FileNotFoundError(f"Detail lookups not found at {detail_lookups_path}")

    df = pd.read_parquet(snapshot_path)
    _validate_required_columns(df)

    with open(detail_lookups_path) as f:
        lookups = json.load(f)

    _validate_lookup_payload(lookups)

    category_polar_bar_figure(df, top_n=10)
    companies_founded_over_time_figure(df)
    clinical_stage_bar_figure(df)
    geo_map_figure(df)

    logger.info(
        "Validated dashboard artifacts successfully (%s rows, %s columns).",
        df.shape[0],
        df.shape[1],
    )
    return df


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    validate_dashboard_artifacts()


if __name__ == "__main__":
    main()