from __future__ import annotations

from lbi_app.etl.extract import extract_companies_raw
from lbi_app.etl.transform import transform_companies_from_csv


def run_pipeline() -> None:
    """
    Run full ETL pipeline:
    1. Extract raw data
    2. Transform into cleaned dataset
    """
    print("Step 1/2: Extracting raw companies data...")
    extract_companies_raw()

    print("Step 2/2: Transforming into cleaned dataset...")
    transform_companies_from_csv()

    print("Pipeline complete. Data updated.")


def main() -> None:
    run_pipeline()


if __name__ == "__main__":
    main()
