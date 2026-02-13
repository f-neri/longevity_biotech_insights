from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

SHEET_ID = "1DEjg-1EsbT_ZKxhdcZJrLinyqttbxxNJopP_PoCdodw"
GID = 0

# lbi_app/etl/extract.py -> repo root
REPO_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = REPO_ROOT / "data"
RAW_PATH = DATA_DIR / "companies_raw.csv"


def extract_companies_raw(
    *,
    sheet_id: str = SHEET_ID,
    gid: int = GID,
    out_path: Path | None = RAW_PATH,
) -> pd.DataFrame:
    """
    Download the ABI companies Google Sheet as a raw CSV.

    Parameters
    ----------
    sheet_id:
        Google Sheet ID.
    gid:
        Worksheet GID.
    out_path:
        Where to write the raw CSV. Use None to skip writing.

    Returns
    -------
    pd.DataFrame
        Raw companies table.
    """
    logger.info("=== EXTRACT STEP ===")

    csv_url = (
        "https://docs.google.com/spreadsheets/d/"
        f"{sheet_id}/export?format=csv&gid={gid}"
    )

    logger.info("Pulling data from URL:\n   %s", csv_url)

    try:
        df_raw = pd.read_csv(csv_url)
    except Exception as e:
        logger.exception("Failed to download sheet CSV")
        raise RuntimeError(
            "Failed to download the Google Sheet CSV. "
            "If this persists, try opening the URL in a browser to confirm access."
        ) from e

    if out_path is not None:
        out_path = Path(out_path)
        out_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            df_raw.to_csv(out_path, index=False)
        except PermissionError as e:
            logger.exception("Failed to write CSV (file may be open elsewhere)")
            raise PermissionError(
                f"Permission denied writing to {out_path}. "
                "If the file is open in Excel, close it and try again."
            ) from e

        logger.info(
            "Saved raw CSV (%s rows, %s cols) at:\n   %s",
            df_raw.shape[0],
            df_raw.shape[1],
            out_path,
        )

    return df_raw


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    extract_companies_raw()


if __name__ == "__main__":
    main()
