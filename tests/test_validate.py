from pathlib import Path

import pandas as pd
import pytest

import lbi_app.etl.validate as validate_module
from lbi_app.etl.validate import (
    REQUIRED_DASHBOARD_COLUMNS,
    _validate_required_columns,
    validate_dashboard_artifacts,
)


def _sample_dashboard_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "Company": ["A", "B"],
            "categories": [["Metabolism"], ["Stem Cells"]],
            "geo_country": [["United States"], ["United Kingdom"]],
            "year founded": pd.to_datetime(["2015-01-01", "2020-01-01"]),
            "total_raised_usd_m": [120.0, 250.0],
            "latest clinical stage": ["Pre-Clinical", "Phase 1"],
            "full overall score": [1.0, 2.0],
        }
    )


def test_validate_required_columns_raises_for_missing_columns() -> None:
    df = pd.DataFrame({"Company": ["A"]})

    with pytest.raises(ValueError, match="missing required dashboard columns"):
        _validate_required_columns(df)


def test_validate_dashboard_artifacts_raises_for_missing_snapshot(tmp_path: Path) -> None:
    missing_path = tmp_path / "missing.parquet"

    with pytest.raises(FileNotFoundError, match="Snapshot not found"):
        validate_dashboard_artifacts(snapshot_path=missing_path)


def test_validate_dashboard_artifacts_rebuilds_figures(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    snapshot_path = tmp_path / "companies_clean.parquet"
    snapshot_path.touch()
    df = _sample_dashboard_df()
    calls: list[str] = []

    def fake_read_parquet(path: Path) -> pd.DataFrame:
        assert path == snapshot_path
        return df

    def recorder(name: str):
        def _record(frame: pd.DataFrame, *args: object, **kwargs: object) -> object:
            assert frame.equals(df)
            calls.append(name)
            return object()

        return _record

    monkeypatch.setattr(validate_module.pd, "read_parquet", fake_read_parquet)
    monkeypatch.setattr(validate_module, "category_polar_bar_figure", recorder("category"))
    monkeypatch.setattr(validate_module, "companies_founded_over_time_figure", recorder("founded"))
    monkeypatch.setattr(validate_module, "total_raised_lollipop_figure", recorder("raised"))
    monkeypatch.setattr(validate_module, "clinical_stage_bar_figure", recorder("clinical"))
    monkeypatch.setattr(validate_module, "geo_map_figure", recorder("geo"))

    result = validate_dashboard_artifacts(snapshot_path=snapshot_path)

    assert result.equals(df)
    assert calls == ["category", "founded", "raised", "clinical", "geo"]


def test_validate_dashboard_artifacts_raises_for_missing_dashboard_columns(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    snapshot_path = tmp_path / "companies_clean.parquet"
    snapshot_path.touch()
    incomplete_df = _sample_dashboard_df().drop(columns=[REQUIRED_DASHBOARD_COLUMNS[-1]])

    monkeypatch.setattr(validate_module.pd, "read_parquet", lambda path: incomplete_df)

    with pytest.raises(ValueError, match="missing required dashboard columns"):
        validate_dashboard_artifacts(snapshot_path=snapshot_path)
