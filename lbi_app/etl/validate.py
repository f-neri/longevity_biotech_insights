from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from lbi_app.viz.plots import (
    category_polar_bar_figure,
    clinical_stage_bar_figure,
    companies_founded_over_time_figure,
    geo_map_figure,
)

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parents[2]
CLEAN_PATH = REPO_ROOT / "data" / "companies_clean.parquet"

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


def validate_dashboard_artifacts(
    *,
    snapshot_path: Path = CLEAN_PATH,
) -> pd.DataFrame:
    """
    Validate the generated dashboard artifacts by loading them and rebuilding
    the figures the app depends on.
    """
    logger.info("=== VALIDATE STEP ===")

    if not snapshot_path.exists():
        raise FileNotFoundError(f"Snapshot not found at {snapshot_path}")

    df = pd.read_parquet(snapshot_path)
    _validate_required_columns(df)

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