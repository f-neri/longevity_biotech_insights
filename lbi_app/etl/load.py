from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

# lbi_app/etl/load.py -> repo root
REPO_ROOT = Path(__file__).resolve().parents[2]
APP_DATA_DIR = REPO_ROOT / "data"
# Use Parquet to preserve pandas dtypes (datetime, categorical, etc.).
CLEAN_PATH = APP_DATA_DIR / "companies_clean.parquet"


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

def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

if __name__ == "__main__":
    main()