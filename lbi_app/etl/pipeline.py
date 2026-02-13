from __future__ import annotations

import logging
from lbi_app.etl.extract import extract_companies_raw
from lbi_app.etl.transform import transform_companies_from_csv

logger = logging.getLogger(__name__)

def run_pipeline() -> None:
    """
    Run full ETL pipeline:
    1. Extract raw data
    2. Transform into cleaned dataset
    """
    extract_companies_raw()

    transform_companies_from_csv()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    run_pipeline()


if __name__ == "__main__":
    main()
