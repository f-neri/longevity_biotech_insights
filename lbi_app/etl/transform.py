from __future__ import annotations

import logging
from pathlib import Path
import pandas as pd
import json
import re

DATA_DIR = Path(__file__).resolve().parents[2] / "data"
RAW_PATH = DATA_DIR / "companies_raw.csv"

logger = logging.getLogger(__name__)

CLINICAL_STAGE_ORDER = [
    "Pre-Clinical",
    "Phase 1",
    "Phase 2",
    "Phase 3",
    "Pre-Commercial",
    "Commercial",
]

# ---------------------------------------------------------------------------
# Geo parsing lookup tables
# ---------------------------------------------------------------------------

_US_STATE_ABBREVS: dict[str, str] = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas",
    "CA": "California", "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware",
    "FL": "Florida", "GA": "Georgia", "HI": "Hawaii", "ID": "Idaho",
    "IL": "Illinois", "IN": "Indiana", "IA": "Iowa", "KS": "Kansas",
    "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland",
    "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi",
    "MO": "Missouri", "MT": "Montana", "NE": "Nebraska", "NV": "Nevada",
    "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico", "NY": "New York",
    "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio", "OK": "Oklahoma",
    "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina",
    "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas", "UT": "Utah",
    "VT": "Vermont", "VA": "Virginia", "WA": "Washington", "WV": "West Virginia",
    "WI": "Wisconsin", "WY": "Wyoming", "DC": "District of Columbia",
}

# City abbreviations / well-known US cities → US state (checked before state abbrevs,
# so e.g. "SD" resolves to San Diego / California rather than South Dakota)
_US_CITIES: dict[str, str] = {
    "SF": "California", "SD": "California", "LA": "California",
    "SAN FRANCISCO": "California", "SAN DIEGO": "California",
    "LOS ANGELES": "California", "PALO ALTO": "California",
    "SOUTH SAN FRANCISCO": "California", "REDWOOD CITY": "California",
    "MENLO PARK": "California", "SANTA BARBARA": "California",
    "SAN JOSE": "California",
    "NYC": "New York", "NEW YORK": "New York", "NY": "New York",
    "BOSTON": "Massachusetts", "CAMBRIDGE": "Massachusetts",
    "DC": "District of Columbia", "WASHINGTON": "District of Columbia",
    "WASHINGTON DC": "District of Columbia",
    "CHICAGO": "Illinois",
    "HOUSTON": "Texas", "DALLAS": "Texas", "AUSTIN": "Texas",
    "DETROIT": "Michigan",
    "MIAMI": "Florida", "TAMPA": "Florida", "TAMPA BAY": "Florida",
    "JACKSONVILLE": "Florida", "ORLANDO": "Florida",
    "SEATTLE": "Washington",
    "DENVER": "Colorado",
    "ATLANTA": "Georgia",
    "RENO": "Nevada", "LAS VEGAS": "Nevada",
    "BIRMINGHAM": "Alabama",
    "GREENVILLE": "South Carolina", "CHARLESTON": "South Carolina",
    "CINCINNATI": "Ohio", "CINCINATTI": "Ohio",  # common misspelling
    "COLUMBUS": "Ohio", "CLEVELAND": "Ohio",
    "SYRACUSE": "New York", "BUFFALO": "New York",
    "INDIANAPOLIS": "Indiana",
    "MINNEAPOLIS": "Minnesota",
    "KANSAS CITY": "Missouri", "ST. LOUIS": "Missouri",
    "NASHVILLE": "Tennessee", "MEMPHIS": "Tennessee",
    "CHARLOTTE": "North Carolina", "RALEIGH": "North Carolina",
    "PITTSBURGH": "Pennsylvania", "PHILADELPHIA": "Pennsylvania",
    "BALTIMORE": "Maryland",
    "RICHMOND": "Virginia",
    "PORTLAND": "Oregon",
    "SALT LAKE CITY": "Utah",
    "PHOENIX": "Arizona", "TUCSON": "Arizona",
    "OMAHA": "Nebraska",
}

_COUNTRY_ALIASES: dict[str, str] = {
    "US": "United States", "U.S.": "United States", "USA": "United States",
    "UNITED STATES": "United States",
    "UK": "United Kingdom", "UNITED KINGDOM": "United Kingdom",
    "ENGLAND": "United Kingdom", "SCOTLAND": "United Kingdom",
    "WALES": "United Kingdom", "GREAT BRITAIN": "United Kingdom",
    "NETHERLANDS": "Netherlands", "HOLLAND": "Netherlands",
    "SWITZERLAND": "Switzerland",
    "ISRAEL": "Israel",
    "JAPAN": "Japan",
    "AUSTRIA": "Austria",
    "SPAIN": "Spain",
    "DENMARK": "Denmark",
    "CROATIA": "Croatia",
    "SOUTH KOREA": "South Korea", "KOREA": "South Korea",
    "INDIA": "India",
    "CANADA": "Canada",
    "GERMANY": "Germany",
    "FRANCE": "France",
    "AUSTRALIA": "Australia",
    "CHINA": "China",
    "SWEDEN": "Sweden",
    "NORWAY": "Norway",
    "BELGIUM": "Belgium",
    "ITALY": "Italy",
    "SINGAPORE": "Singapore",
    "IRELAND": "Ireland",
    "BRAZIL": "Brazil",
    "PORTUGAL": "Portugal",
    "FINLAND": "Finland",
    "CZECH REPUBLIC": "Czech Republic", "CZECHIA": "Czech Republic",
    "POLAND": "Poland",
    "TAIWAN": "Taiwan",
    "HONG KONG": "Hong Kong",
    "NEW ZEALAND": "New Zealand",
    "SOUTH AFRICA": "South Africa",
    "ARGENTINA": "Argentina",
    "MEXICO": "Mexico",
    "RUSSIA": "Russia",
}

_CITY_TO_COUNTRY: dict[str, str] = {
    "ZURICH": "Switzerland", "GENEVA": "Switzerland", "BASEL": "Switzerland",
    "TOKYO": "Japan", "OSAKA": "Japan",
    "BARCELONA": "Spain", "MADRID": "Spain",
    "COPENHAGEN": "Denmark",
    "LONDON": "United Kingdom", "OXFORD": "United Kingdom",
    "AMSTERDAM": "Netherlands", "LEIDEN": "Netherlands",
    "VIENNA": "Austria",
    "NEWCASTLE": "United Kingdom",
    "TORONTO": "Canada", "VANCOUVER": "Canada", "MONTREAL": "Canada",
    "PARIS": "France", "LYON": "France",
    "BERLIN": "Germany", "MUNICH": "Germany", "HAMBURG": "Germany",
    "STOCKHOLM": "Sweden",
    "OSLO": "Norway",
    "BANGALORE": "India", "MUMBAI": "India",
    "SEOUL": "South Korea", "BUSAN": "South Korea",
    "TEL AVIV": "Israel", "JERUSALEM": "Israel",
    "BRUSSELS": "Belgium",
    "ROME": "Italy", "MILAN": "Italy",
    "SINGAPORE": "Singapore",
    "SYDNEY": "Australia", "MELBOURNE": "Australia",
    "BEIJING": "China", "SHANGHAI": "China",
}


def clean_geo(s: pd.Series) -> pd.Series:
    """
    Parse raw geo strings into canonical location lists.

    Returns:
    - ["United States - {State}", ...] for US locations
    - ["Country", ...] for international locations
    - ["Unknown"] for unrecognised / blank values

    All recognised locations are retained and deduplicated in encounter order.
    """

    def _single_token(token: str) -> str | None:
        upper = token.strip().upper()
        if not upper:
            return None
        if upper in _US_CITIES:
            return f"United States - {_US_CITIES[upper]}"
        if upper in _US_STATE_ABBREVS:
            return f"United States - {_US_STATE_ABBREVS[upper]}"
        if upper in _COUNTRY_ALIASES:
            return _COUNTRY_ALIASES[upper]
        if upper in _CITY_TO_COUNTRY:
            return _CITY_TO_COUNTRY[upper]
        return None

    def _parse_chunk(chunk: str) -> list[str]:
        chunk = chunk.strip()
        if not chunk:
            return []

        # Try the whole chunk as a single token first
        result = _single_token(chunk)
        if result:
            return [result]

        # Split by comma and try contextual interpretations
        parts = [p.strip() for p in chunk.split(",") if p.strip()]
        if len(parts) < 2:
            return []

        last = parts[-1].upper()
        first = parts[0].upper()

        # "City, ST" pattern → US state abbreviation
        if len(parts) == 2 and last in _US_STATE_ABBREVS:
            return [f"United States - {_US_STATE_ABBREVS[last]}"]

        # Collect all recognised tokens among comma-separated parts.
        # This preserves multi-country entries such as "South Korea, Boston"
        # (South Korea + United States) and "China, UAE, US, Canada".
        found: list[str] = []
        for part in parts:
            result = _single_token(part)
            if result and result not in found:
                found.append(result)

        return found

    def _parse(raw: object) -> list[str]:
        if pd.isna(raw) or str(raw).strip() == "":
            return ["Unknown"]

        text = str(raw).strip()
        found: list[str] = []

        # Split on ";" to separate multi-location entries and collect all recognised.
        for chunk in [p.strip() for p in text.split(";")]:
            for result in _parse_chunk(chunk):
                if result not in found:
                    found.append(result)

        return found or ["Unknown"]

    return s.apply(_parse)


def derive_geo_country(s: pd.Series) -> pd.Series:
    """
    Collapse US state entries in geo_clean lists to their top-level country.

    "United States - {State}" -> "United States", all other entries kept as-is.
    Deduplicates within each list so a company with multiple US states
    appears as a single "United States" entry.
    """
    def _collapse(locations: object) -> list[str]:
        if not isinstance(locations, list):
            return ["Unknown"]
        seen: list[str] = []
        for loc in locations:
            country = "United States" if str(loc).startswith("United States - ") else str(loc)
            if country not in seen:
                seen.append(country)
        return sorted(seen) if seen else ["Unknown"]

    return s.apply(_collapse)


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
            
            # Capitalize each word
            main_cat = main_cat.title()
            
            if main_cat:  # only add non-empty
                cleaned.append(main_cat)
        
        # Return as list (or None if empty)
        return cleaned if cleaned else None
    
    return s.apply(process_category_string)


def clean_clinical_stage(s: pd.Series) -> pd.Series:
    """
    Normalize messy clinical stage strings into ordered stage lists per company.

    Output categories are exactly:
    - Pre-Clinical
    - Phase 1
    - Phase 2
    - Phase 3
    - Pre-Commercial
    - Commercial

    If multiple stage hints appear in the same row, retain all matched stages
    in canonical order and remove duplicates.
    """
    stage_patterns = {
        "Pre-Clinical": r"\b(pre-?clinical|vet pre-?clinical)\b",
        "Phase 1": r"\b(ph\.? ?1|phase ?1|clinical trials|early clinical|vet clinical)\b",
        "Phase 2": r"\b(ph\.? ?2|phase ?2)\b",
        "Phase 3": r"\b(ph\.? ?3|phase ?3|pivotal)\b",
        "Pre-Commercial": r"\b(pre-?commercial)\b",
        "Commercial": r"\b(commercial|approved|fda approved|fda accelerated approval|ph\.? ?4|phase ?4)\b",
    }

    def _normalize_value(value: object) -> list[str] | None:
        if pd.isna(value):
            return None

        text = str(value).strip().lower()
        if not text:
            return None

        # Remove annotation noise while preserving separators and tokens.
        text = text.replace("*", "").replace("?", "")
        text = text.replace(";", ",")
        text = re.sub(r"\s+", " ", text).strip()

        matches = [
            stage
            for stage in CLINICAL_STAGE_ORDER
            if re.search(stage_patterns[stage], text)
        ]

        return matches or None

    return s.apply(_normalize_value)


def derive_latest_clinical_stage(s: pd.Series) -> pd.Series:
    """
    Derive one stage per row from a list-valued clinical stage column.
    Uses the last value in each list and returns an ordered categorical series.
    """
    def _latest(value: object) -> str | None:
        if isinstance(value, list):
            cleaned = [
                str(stage).strip()
                for stage in value
                if pd.notna(stage) and str(stage).strip()
            ]
            return cleaned[-1] if cleaned else None
        return None

    latest = s.apply(_latest)
    return pd.Categorical(latest, categories=CLINICAL_STAGE_ORDER, ordered=True)


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

    # 4a) Clean clinical stage column to a canonical, ordered stage taxonomy.
    if "clinical stage" in df.columns:
        df["clinical stage"] = clean_clinical_stage(df["clinical stage"])
        df["latest clinical stage"] = derive_latest_clinical_stage(df["clinical stage"])
        logger.info(
            "Cleaned clinical stage column and derived latest clinical stage"
        )

    # 4b) Clean categories column
    if "categories" in df.columns:
        df["categories"] = clean_categories(df["categories"])
        logger.info("Cleaned categories column (removed subcategories, standardized multiple choices)")

    # 4c) Parse geo column into canonical country / US-state locations
    if "geo" in df.columns:
        df["geo_clean"] = clean_geo(df["geo"])
        df["geo_country"] = derive_geo_country(df["geo_clean"])
        logger.info("Parsed geo column into canonical locations and derived geo_country.")

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
