from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

# lbi_app/etl/load.py -> repo root
REPO_ROOT = Path(__file__).resolve().parents[2]
APP_DATA_DIR = REPO_ROOT / "data"
CLEAN_PATH = APP_DATA_DIR / "companies_clean.csv"


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

    # Atomic-ish write: write temp then replace
    tmp_path = out_path.with_suffix(out_path.suffix + ".tmp")
    df.to_csv(tmp_path, index=False)
    tmp_path.replace(out_path)

    logger.info("Wrote companies snapshot:\n   %s", out_path)

def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

if __name__ == "__main__":
    main()