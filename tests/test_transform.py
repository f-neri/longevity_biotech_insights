import pandas as pd

from lbi_app.etl.transform import (
    clean_categories,
    clean_clinical_stage,
    clean_geo,
    clean_operating_status,
    clean_total_raised_usd_m,
    derive_geo_country,
    make_unique_columns,
    transform_companies,
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


def test_clean_total_raised_usd_m_parses_currency_and_text_values() -> None:
    series = pd.Series(["$2,500", "2.5B", "87", "unknown", "undisclosed but funded***"])

    result = clean_total_raised_usd_m(series)

    assert result.iloc[0] == 2500.0
    assert result.iloc[1] == 2500.0
    assert result.iloc[2] == 87.0
    assert pd.isna(result.iloc[3])
    assert pd.isna(result.iloc[4])


def test_clean_operating_status_maps_free_text_to_canonical_values() -> None:
    series = pd.Series([
        "operating*",
        "acquired*, operating independly",
        "closed / defunct",
        None,
    ])

    result = clean_operating_status(series)

    assert result.astype(str).tolist() == ["Operating", "Acquired", "Closed", "Unknown"]


def test_transform_companies_creates_total_raised_usd_m_column() -> None:
    raw = pd.DataFrame(
        {
            "name": ["A", "B", "C"],
            "tot. raised": ["$120", "unknown", "2.5B"],
            "year founded": ["2015", "2018", "2020"],
            "categories": ["metabolism", "stem cells", "epigenetic"],
            "clinical stage": ["pre-clinical", "phase 1", "approved"],
            "operating status": ["operating", "acquired (see notes)", None],
            "geo": ["SF", "Boston, MA", "London"],
            "full overall score": [1.0, 2.0, 3.0],
        }
    )

    result = transform_companies(raw)

    assert "total_raised_usd_m" in result.columns
    assert "operating status" in result.columns
    assert result.loc[0, "total_raised_usd_m"] == 120.0
    assert pd.isna(result.loc[1, "total_raised_usd_m"])
    assert result.loc[2, "total_raised_usd_m"] == 2500.0
    assert result["operating status"].astype(str).tolist() == ["Operating", "Acquired", "Unknown"]
