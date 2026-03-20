import pandas as pd

from lbi_app.etl.transform import (
    clean_categories,
    clean_clinical_stage,
    clean_geo,
    derive_geo_country,
    make_unique_columns,
)


def test_make_unique_columns_appends_suffixes_for_duplicates() -> None:
    cols = pd.Index(["A", "A", "B", "A"])

    result = make_unique_columns(cols)

    assert result.tolist() == ["A", "A__2", "B", "A__3"]


def test_clean_categories_splits_removes_suffixes_and_normalizes_case() -> None:
    series = pd.Series(["metabolism / mTOR, stem cells*, epigenetic? and/or"])

    result = clean_categories(series)

    assert result.iloc[0] == ["Metabolism", "Stem Cells", "Epigenetic"]


def test_clean_clinical_stage_extracts_ordered_matches() -> None:
    series = pd.Series(["pre-clinical; ph 2, approved"])

    result = clean_clinical_stage(series)

    assert result.iloc[0] == ["Pre-Clinical", "Phase 2", "Commercial"]


def test_clean_geo_and_derive_geo_country_handle_mixed_locations() -> None:
    series = pd.Series(["Boston, MA; South Korea; US"])

    geo_clean = clean_geo(series)
    geo_country = derive_geo_country(geo_clean)

    assert geo_clean.iloc[0] == [
        "United States - Massachusetts",
        "South Korea",
        "United States",
    ]
    assert geo_country.iloc[0] == ["South Korea", "United States"]
