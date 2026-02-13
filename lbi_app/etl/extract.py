from __future__ import annotations

import logging
from pathlib import Path
import pandas as pd


logger = logging.getLogger(__name__)

SHEET_ID = "1DEjg-1EsbT_ZKxhdcZJrLinyqttbxxNJopP_PoCdodw"
GID = 0

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
RAW_PATH = DATA_DIR / "companies_raw.csv"


def extract_companies_raw(
    *,
    sheet_id: str = SHEET_ID,
    gid: int = GID,
    out_path: Path = RAW_PATH,
) -> pd.DataFrame:

    logger.info("=== EXTRACT STEP ===")

    out_path.parent.mkdir(parents=True, exist_ok=True)

    csv_url = (
        "https://docs.google.com/spreadsheets/d/"
        f"{sheet_id}/export?format=csv&gid={gid}"
    )

    logger.info(
        "Pulling data from URL:\n"
        "   %s",
        csv_url
    )

    try:
        df_raw = pd.read_csv(csv_url)
    except Exception as e:
        logger.exception("Failed to download sheet")
        raise RuntimeError(f"Failed to download sheet: {e}") from e

    df_raw.to_csv(out_path, index=False)

    logger.info(
        "Saved raw CSV (%s rows, %s cols) at:\n"
        "   %s",
        df_raw.shape[0],
        df_raw.shape[1],
        out_path,
    )

    return df_raw


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    extract_companies_raw()


if __name__ == "__main__":
    main()
