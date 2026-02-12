from __future__ import annotations

from pathlib import Path
import pandas as pd


DATA_DIR = Path(__file__).resolve().parent.parent / "data"
RAW_PATH = DATA_DIR / "companies_raw.csv"
CLEAN_PATH = DATA_DIR / "companies_clean.csv"


def clean_column_names(cols: pd.Index) -> pd.Index:
    """
    Clean column names:
    - remove text in (...) and [...]
    - replace newlines with spaces
    - collapse repeated whitespace
    - strip leading/trailing whitespace
    """
    return (
        cols.astype(str)
        .str.replace(r"\([^)]*\)", "", regex=True)
        .str.replace(r"\[[^]]*\]", "", regex=True)
        .str.replace("\n", " ", regex=False)
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
    )


def normalize_object_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Replace embedded newlines in string columns with spaces and strip whitespace.
    """
    out = df.copy()

    obj_cols = out.select_dtypes(include=["object"]).columns
    out[obj_cols] = (
        out[obj_cols]
        .astype(str)
        .apply(lambda s: s.str.replace("\n", " ", regex=False).str.strip())
        .replace({"nan": pd.NA})
    )

    return out


def transform_companies(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Apply cleaning and normalization steps to raw companies data.
    """
    df = df_raw.copy()

    # 1) Clean column names
    df.columns = clean_column_names(df.columns)

    # 2) Drop columns that became empty
    df = df.loc[:, df.columns != ""]

    # 3) Normalize string columns
    df = normalize_object_columns(df)

    return df


def transform_companies_from_csv(
    raw_path: Path = RAW_PATH,
    out_path: Path = CLEAN_PATH,
) -> pd.DataFrame:
    """
    Load raw CSV, transform it, and save cleaned CSV.
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)

    df_raw = pd.read_csv(raw_path)
    df_clean = transform_companies(df_raw)

    df_clean.to_csv(out_path, index=False)

    return df_clean


def main() -> None:
    df = transform_companies_from_csv()
    print(f"Saved cleaned companies CSV to: {CLEAN_PATH}")
    print(f"Rows={len(df):,} Cols={df.shape[1]:,}")


if __name__ == "__main__":
    main()
