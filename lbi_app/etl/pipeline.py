from __future__ import annotations

import logging

from lbi_app.etl.extract import extract_companies_raw
from lbi_app.etl.transform import transform_companies_from_csv
from lbi_app.etl.load import load_companies_snapshot
from lbi_app.etl.validate import validate_dashboard_artifacts

logger = logging.getLogger(__name__)


def run_pipeline() -> None:
    """
    Run full ETL pipeline:
    1) Extract raw data
    2) Transform into cleaned dataframe
    3) Load (write) snapshot used by the Dash app
    4) Validate the dashboard artifacts the app depends on
    """
    extract_companies_raw()

    df_clean = transform_companies_from_csv()

    load_companies_snapshot(df_clean)
    validate_dashboard_artifacts()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    run_pipeline()


if __name__ == "__main__":
    main()