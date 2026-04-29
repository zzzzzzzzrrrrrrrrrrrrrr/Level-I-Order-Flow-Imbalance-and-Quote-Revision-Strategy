from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from level1_ofi_qr.schema import (
    ASK_EXCHANGE,
    BID,
    BID_EXCHANGE,
    QUOTE_COLUMNS,
    QUOTE_EXCHANGE,
    QUOTE_SEQUENCE_NUMBER,
    SchemaValidationError,
    validate_quote_frame,
)

FIXTURE_PATH = Path(__file__).resolve().parents[1] / "fixtures" / "sample_quotes.csv"


def load_fixture_frame() -> pd.DataFrame:
    return pd.read_csv(FIXTURE_PATH, parse_dates=["event_time"])


def test_validate_quote_frame_accepts_normalized_fixture() -> None:
    quotes = load_fixture_frame()

    validated = validate_quote_frame(quotes)

    assert validated is quotes


def test_validate_quote_frame_rejects_missing_required_columns() -> None:
    quotes = load_fixture_frame().drop(columns=[QUOTE_COLUMNS[0]])

    with pytest.raises(SchemaValidationError, match="Missing required columns"):
        validate_quote_frame(quotes)


def test_validate_quote_frame_requires_quote_fields() -> None:
    quotes = load_fixture_frame()
    quotes.loc[0, BID] = pd.NA

    with pytest.raises(SchemaValidationError, match="Quote rows must populate"):
        validate_quote_frame(quotes)


def test_validate_quote_frame_requires_bid_and_ask_exchange() -> None:
    quotes = load_fixture_frame()
    quotes.loc[0, BID_EXCHANGE] = pd.NA
    quotes.loc[1, ASK_EXCHANGE] = pd.NA

    with pytest.raises(SchemaValidationError, match="bid_exchange|ask_exchange"):
        validate_quote_frame(quotes)


def test_validate_quote_frame_requires_quote_exchange_and_sequence_number() -> None:
    quotes = load_fixture_frame()
    quotes.loc[0, QUOTE_EXCHANGE] = pd.NA
    quotes.loc[1, QUOTE_SEQUENCE_NUMBER] = pd.NA

    with pytest.raises(SchemaValidationError, match="quote_exchange|quote_sequence_number"):
        validate_quote_frame(quotes)
