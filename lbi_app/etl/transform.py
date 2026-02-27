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
    Applies `clean_string_series()` to each column with dtype
    "object" or "string" to standardize textual formatting
    (whitespace normalization, removal of bracketed text, etc.).

    This step performs formatting normalization only.
    It does not perform semantic cleaning such as missing-value coercion.
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


def clean_categories(s: pd.Series) -> pd.Series:
    """
    Clean category strings by:
    1. Splitting comma-separated categories (multiple choices)
    2. Removing subcategory suffixes (e.g., " / mTOR" from "metabolism / mTOR")
    3. Stripping trailing annotation characters (*, ?, etc)
    4. Stripping trailing words like "and", "or", "and/or"
    5. Normalizing simple plurals when the singular also appears in the same list
    6. Returning as a list for each row
    
    Example:
        "metabolism / mTOR, mitochondria / NAD+" 
        -> ["metabolism", "mitochondria"]
        
        "stem cells*, epigenetic?"
        -> ["stem cells", "epigenetic"]
        
        "metabolism and/or cell communication?"
        -> ["metabolism", "cell communication"]
        
        "clock" and "clocks" -> ["clock"]
    """
    def process_category_string(cat_str):
        if pd.isna(cat_str):
            return None
        
        # Split by comma to get individual categories
        categories = str(cat_str).split(",")
        
        # For each category, keep only the main part (before " / ") and strip annotations
        cleaned: list[str] = []
        for cat in categories:
            # Extract main category (before " / " if present)
            main_cat = cat.split("/")[0].strip()
            
            # Strip trailing annotation characters (*, ?, comma, etc)
            main_cat = main_cat.rstrip("*?,")
            
            # Strip trailing logical connectives and preserve single words
            main_cat = main_cat.rstrip()
            words = main_cat.split()
            while words and words[-1].lower() in ("and", "or", "and/or"):
                words.pop()
            main_cat = " ".join(words)
            
            if main_cat:  # only add non-empty
                cleaned.append(main_cat)
        
        # Return as list (or None if empty)
        return cleaned if cleaned else None
    
    return s.apply(process_category_string)


def standardize_date_columns(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    """
    Detect and convert columns with date-related names to datetime dtype.
    
    Only columns whose names contain specific keywords are considered for conversion.
    Supported keywords: "date", "time", "founded", "acquisition", "launch", "started", 
    "created", "established".
    
    Conversion is only applied if at least 50% of non-null values successfully parse 
    as datetime, to avoid unnecessary NaT introduction in ambiguous columns.
    
    Args:
        df: Input DataFrame.
    
    Returns:
        Tuple of (modified DataFrame with converted date columns, list of column names 
        that were successfully converted).
    """
    out = df.copy()
    converted_cols: list[str] = []
    
    # Common date-related column name patterns
    date_keywords = ["date", "time", "founded", "acquisition", "launch", "started", "created", "established"]
    
    # Object/string columns that might contain dates
    obj_cols = out.select_dtypes(include=["object", "string"]).columns
    
    for col in obj_cols:
        col_lower = col.lower()
        
        # Check if column name suggests it contains dates
        is_date_col = any(keyword in col_lower for keyword in date_keywords)
        
        if not is_date_col:
            continue
        
        # Skip if column is mostly empty
        non_null_count = out[col].notna().sum()
        if non_null_count < 1:
            continue
        
        try:
            # Replace empty strings with NaN first
            col_data = out[col].replace({"": pd.NA})
            
            # Try to infer and convert to datetime
            converted = pd.to_datetime(
                col_data,
                errors="coerce",
            )
            
            # Check if conversion was successful (at least 50% of non-null values converted)
            successful_conversions = converted.notna().sum()
            conversion_rate = successful_conversions / non_null_count
            
            if conversion_rate >= 0.5:
                out[col] = converted
                converted_cols.append(col)
                logger.info(
                    "Converted column %r to datetime. Success rate: %.1f%% (%d/%d values)",
                    col,
                    conversion_rate * 100,
                    successful_conversions,
                    non_null_count,
                )
            else:
                logger.debug(
                    "Column %r has insufficient datetime conversions (%.1f%%). Skipping.",
                    col,
                    conversion_rate * 100,
                )
        except Exception as e:
            logger.debug("Failed to convert column %r to datetime: %s", col, str(e))
            continue
    
    return out, converted_cols


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
    
    df.rename(columns={df.columns[0]: "Company"}, inplace=True)
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

    # 4b) Clean categories column
    if "categories" in df.columns:
        df["categories"] = clean_categories(df["categories"])
        logger.info("Cleaned categories column (removed subcategories, standardized multiple choices)")

        # Global plural normalization: if both "X" and "Xs" appear anywhere in the dataset,
        # replace all "Xs" with "X"
        all_cats = df["categories"].explode().dropna().astype(str).unique()
        global_plural_map: dict[str, str] = {}
        for cat in all_cats:
            if cat.endswith("s"):
                base = cat[:-1]
                if base in all_cats:
                    global_plural_map[cat] = base
        
        if global_plural_map:
            logger.info("Normalizing plural categories globally: %s", global_plural_map)
            def remap_row(lst):
                if not lst:
                    return lst
                return [global_plural_map.get(x, x) for x in lst]
            df["categories"] = df["categories"].apply(remap_row)
        
        # Final deduplication: remove duplicate categories within each row
        def deduplicate_row(lst):
            if not lst:
                return lst
            seen = set()
            result = []
            for v in lst:
                if v not in seen:
                    seen.add(v)
                    result.append(v)
            return result
        
        df["categories"] = df["categories"].apply(deduplicate_row)


    # 5) Optionally drop fully empty columns
    dropped_empty_cols: list[str] = []
    if drop_all_empty_columns:
        df, dropped_empty_cols = drop_fully_empty_columns(df)
        if dropped_empty_cols:
            logger.info("Dropped %d fully empty column(s): %s", len(dropped_empty_cols), dropped_empty_cols)

    # 6) Standardize date columns (detect common date patterns and convert to datetime)
    df, converted_date_cols = standardize_date_columns(df)


    # 7) Summary
    n_rows_out, n_cols_out = df.shape
    
    summary = {
        "input_rows": n_rows_in,
        "output_rows": n_rows_out,
        "input_cols": n_cols_in,
        "output_cols": n_cols_out,
        "empty_name_columns_renamed": len(empty_idxs),
        "duplicate_names_detected": len(dup_names),
        "fully_empty_columns_dropped": len(dropped_empty_cols),
        "date_columns_converted": len(converted_date_cols),
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
