from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from level1_ofi_qr.schema import (
    TRADE_CORRECTION,
    TRADE_COLUMNS,
    TRADE_EXCHANGE,
    TRADE_ID,
    TRADE_PRICE,
    TRADE_SOURCE,
    TRADE_SEQUENCE_NUMBER,
    SchemaValidationError,
    validate_trade_frame,
)

FIXTURE_PATH = Path(__file__).resolve().parents[1] / "fixtures" / "sample_trades.csv"


def load_fixture_frame() -> pd.DataFrame:
    return pd.read_csv(FIXTURE_PATH, parse_dates=["event_time"])


def test_validate_trade_frame_accepts_normalized_fixture() -> None:
    trades = load_fixture_frame()

    validated = validate_trade_frame(trades)

    assert validated is trades


def test_validate_trade_frame_rejects_missing_required_columns() -> None:
    trades = load_fixture_frame().drop(columns=[TRADE_COLUMNS[0]])

    with pytest.raises(SchemaValidationError, match="Missing required columns"):
        validate_trade_frame(trades)


def test_validate_trade_frame_requires_trade_fields() -> None:
    trades = load_fixture_frame()
    trades.loc[0, TRADE_PRICE] = pd.NA

    with pytest.raises(SchemaValidationError, match="Trade rows must populate"):
        validate_trade_frame(trades)


def test_validate_trade_frame_requires_trade_exchange() -> None:
    trades = load_fixture_frame()
    trades.loc[0, TRADE_EXCHANGE] = pd.NA

    with pytest.raises(SchemaValidationError, match="trade_exchange"):
        validate_trade_frame(trades)


def test_validate_trade_frame_requires_audit_fields() -> None:
    trades = load_fixture_frame()
    trades.loc[0, TRADE_CORRECTION] = pd.NA
    trades.loc[0, TRADE_ID] = pd.NA
    trades.loc[0, TRADE_SOURCE] = pd.NA
    trades.loc[1, TRADE_SEQUENCE_NUMBER] = pd.NA

    with pytest.raises(SchemaValidationError, match="Trade rows must populate"):
        validate_trade_frame(trades)
