from __future__ import annotations

import logging
from pathlib import Path
import pandas as pd
import json

DATA_DIR = Path(__file__).resolve().parents[2] / "data"
RAW_PATH = DATA_DIR / "companies_raw.csv"

logger = logging.getLogger(__name__)

def clean_string_series(s: pd.Series) -> pd.Series:
    """
    Standard string normalization used across the pipeline.
    Applied to both column names and string values.
    """
    return (
        s.astype("string")
        .str.replace(r"\([^)]*\)", "", regex=True)
        .str.replace(r"\[[^]]*\]", "", regex=True)
        .str.replace("\n", " ", regex=False)
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
    )

def clean_column_names(cols: pd.Index) -> pd.Index:
    cleaned = clean_string_series(pd.Series(cols))
    return pd.Index(cleaned)


def make_unique_columns(cols: pd.Index) -> pd.Index:
    """
    Ensure column names are unique by appending __{n} to duplicates.
    Example: ["A", "A", "B", "A"] -> ["A", "A__2", "B", "A__3"]
    """
    seen: dict[str, int] = {}
    new_cols: list[str] = []

    for c in cols.astype(str).tolist():
        if c not in seen:
            seen[c] = 1
            new_cols.append(c)
        else:
            seen[c] += 1
            new_cols.append(f"{c}__{seen[c]}")

    return pd.Index(new_cols)


def normalize_object_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Replace embedded newlines in string columns with spaces and strip whitespace.
    Converts the literal string "nan" back to missing values (pd.NA).
    """
    out = df.copy()

    obj_cols = out.select_dtypes(include=["object", "string"]).columns
    if len(obj_cols) == 0:
        logger.info("No object (string) columns detected; skipping string normalization.")
        return out

    out[obj_cols] = out[obj_cols].apply(clean_string_series)

    logger.info("Normalized %d object columns.", len(obj_cols))

    return out


def drop_fully_empty_columns(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    """
    Drop columns where every value is missing / blank.
    For object columns, treat "" as missing too.
    """
    out = df.copy()

    obj_cols = out.select_dtypes(include=["object", "string"]).columns
    if len(obj_cols) > 0:
        out[obj_cols] = out[obj_cols].replace({"": pd.NA})

    empty_cols = out.columns[out.isna().all()].astype(str).tolist()
    if empty_cols:
        out = out.drop(columns=empty_cols)

    return out, empty_cols


def transform_companies(
    df_raw: pd.DataFrame,
    drop_all_empty_columns: bool = True,
) -> pd.DataFrame:
    """
    Apply cleaning and normalization steps to raw companies data.
    """
    df = df_raw.copy()
    n_rows_in, n_cols_in = df.shape
    
    # 1) Clean column names (but keep original names for audit/logging)
    original_cols = df.columns.astype(str).tolist()
    cleaned_cols = clean_column_names(df.columns).astype(str).tolist()

    for before, after in zip(original_cols, cleaned_cols, strict=False):
        if before != after:
            logger.debug("Column rename (clean): %r -> %r", before, after)

    # 2) Rename columns that became empty -> unnamed_col_{i} (log original)
    empty_idxs = [i for i, c in enumerate(cleaned_cols) if c == ""]
    for i in empty_idxs:
        logger.warning(
            "Empty column name after cleaning at index=%d. Original name=%r -> Renamed to %s",
            i,
            original_cols[i],
            f"unnamed_col_{i}",
        )
        cleaned_cols[i] = f"unnamed_col_{i}"

    df.columns = pd.Index(cleaned_cols)

    # 3) Detect + resolve duplicates after cleaning/renaming
    col_series = pd.Series(df.columns.astype(str))
    dup_mask = col_series.duplicated(keep=False)
    had_dupes = bool(dup_mask.any())

    dup_names = sorted(col_series[dup_mask].unique().tolist()) if had_dupes else []
    if had_dupes:
        logger.warning(
            "Detected %d duplicated column name(s) after cleaning: %s",
            len(dup_names),
            dup_names,
        )
        new_cols = make_unique_columns(df.columns)
        for before, after in zip(df.columns.astype(str), new_cols.astype(str), strict=False):
            if before != after:
                logger.debug("Column rename (dedupe): %r -> %r", before, after)
        df.columns = new_cols

    # 4) Normalize string columns
    df = normalize_object_columns(df)

    # 5) Optionally drop fully empty columns (safe)
    dropped_empty_cols: list[str] = []
    if drop_all_empty_columns:
        df, dropped_empty_cols = drop_fully_empty_columns(df)
        if dropped_empty_cols:
            logger.info("Dropped %d fully empty column(s): %s", len(dropped_empty_cols), dropped_empty_cols)

    # 6) Summary
    n_rows_out, n_cols_out = df.shape
    
    summary = {
        "input_rows": n_rows_in,
        "output_rows": n_rows_out,
        "input_cols": n_cols_in,
        "output_cols": n_cols_out,
        "empty_name_columns_renamed": len(empty_idxs),
        "duplicate_names_detected": len(dup_names),
        "fully_empty_columns_dropped": len(dropped_empty_cols),
    }

    logger.info(
        "Transform summary:\n%s",
        json.dumps(summary, indent=4),
    )

    return df


def transform_companies_from_csv(
    raw_path: Path = RAW_PATH,
    drop_all_empty_columns: bool = True,
) -> pd.DataFrame:
    logger.info("=== TRANSFORM STEP ===")
    
    logger.info(
        "Reading raw companies CSV from:\n"
        "   %s",
        raw_path
    )
    
    df_raw = pd.read_csv(raw_path)
    logger.info("Raw companies loaded. rows=%d cols=%d", df_raw.shape[0], df_raw.shape[1])

    df_clean = transform_companies(df_raw, drop_all_empty_columns=drop_all_empty_columns)

    return df_clean


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    try:
        _ = transform_companies_from_csv()
    except FileNotFoundError:
        logger.exception("File not found. Check RAW_PATH / CLEAN_PATH configuration.")
        raise
    except Exception:
        logger.exception("Unexpected error during transform.")
        raise


if __name__ == "__main__":
    main()
