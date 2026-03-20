import pandas as pd
import pytest

from lbi_app.etl.validate import (
    _validate_lookup_payload,
    _validate_required_columns,
    REQUIRED_LOOKUP_KEYS,
)


def test_validate_required_columns_raises_for_missing_columns() -> None:
    df = pd.DataFrame({"Company": ["A"]})

    with pytest.raises(ValueError, match="missing required dashboard columns"):
        _validate_required_columns(df)


def test_validate_lookup_payload_raises_for_missing_required_keys() -> None:
    payload = {"stage_details": {}}

    with pytest.raises(ValueError, match="missing required keys"):
        _validate_lookup_payload(payload)


def test_validate_lookup_payload_accepts_complete_payload() -> None:
    payload = {key: {} for key in REQUIRED_LOOKUP_KEYS}

    _validate_lookup_payload(payload)
