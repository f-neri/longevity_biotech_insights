from __future__ import annotations

from pathlib import Path
import pandas as pd


SHEET_ID = "1DEjg-1EsbT_ZKxhdcZJrLinyqttbxxNJopP_PoCdodw"
GID = 0  # first worksheet/tab

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
RAW_PATH = DATA_DIR / "companies_raw.csv"


def extract_companies_raw(
    *,
    sheet_id: str = SHEET_ID,
    gid: int = GID,
    out_path: Path = RAW_PATH,
) -> pd.DataFrame:
    """
    Extract the companies table from the public Google Sheet
    and save it as a raw CSV.
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)

    csv_url = (
        f"https://docs.google.com/spreadsheets/d/"
        f"{sheet_id}/export?format=csv&gid={gid}"
    )

    df_raw = pd.read_csv(csv_url)
    df_raw.to_csv(out_path, index=False)

    return df_raw


def main() -> None:
    df = extract_companies_raw()
    print(f"Saved raw companies CSV to: {RAW_PATH}")
    print(f"Rows={len(df):,} Cols={df.shape[1]:,}")


if __name__ == "__main__":
    main()
