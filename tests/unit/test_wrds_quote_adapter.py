from __future__ import annotations

from pathlib import Path

import pandas as pd

from level1_ofi_qr.adapters import normalize_wrds_quotes
from level1_ofi_qr.schema import (
    ASK_EXCHANGE,
    BID,
    BID_EXCHANGE,
    NBBO_QUOTE_CONDITION,
    QUOTE_COLUMNS,
    SOURCE,
    SYMBOL,
)
from level1_ofi_qr.utils import load_data_slice_config

CONFIG_PATH = (
    Path(__file__).resolve().parents[2]
    / "configs"
    / "data"
    / "aapl_wrds_20260408_20260410.yaml"
)
FIXTURE_PATH = Path(__file__).resolve().parents[1] / "fixtures" / "wrds_raw_quotes.csv"


def load_fixture_frame() -> pd.DataFrame:
    return pd.read_csv(FIXTURE_PATH)


def test_normalize_wrds_quotes_maps_fixture_into_quote_schema() -> None:
    config = load_data_slice_config(CONFIG_PATH)
    raw_quotes = load_fixture_frame()

    normalized = normalize_wrds_quotes(raw_quotes, config=config)

    assert tuple(normalized.columns) == QUOTE_COLUMNS
    assert normalized.loc[0, SOURCE] == "wrds_taq_nbbom"
    assert normalized.loc[0, SYMBOL] == "AAPL"
    assert normalized.loc[0, BID_EXCHANGE] == "Q"
    assert normalized.loc[1, ASK_EXCHANGE] == "B"
    assert normalized.loc[0, NBBO_QUOTE_CONDITION] == "R"
    assert normalized.loc[0, BID] == 190.00
    assert str(normalized["event_time"].dt.tz) == "America/New_York"


def test_normalize_wrds_quotes_combines_symbol_suffix_with_separator() -> None:
    config = load_data_slice_config(CONFIG_PATH)
    raw_quotes = load_fixture_frame()
    raw_quotes["sym_suffix"] = raw_quotes["sym_suffix"].astype("string")
    raw_quotes.loc[0, "sym_root"] = "BRK"
    raw_quotes.loc[0, "sym_suffix"] = "B"

    normalized = normalize_wrds_quotes(raw_quotes, config=config)

    assert normalized.loc[0, SYMBOL] == "BRK.B"


def test_normalize_wrds_quotes_accepts_mixed_time_precision() -> None:
    config = load_data_slice_config(CONFIG_PATH)
    raw_quotes = load_fixture_frame()
    raw_quotes.loc[1, "time_m"] = "09:31:01"

    normalized = normalize_wrds_quotes(raw_quotes, config=config)

    assert str(normalized.loc[1, "event_time"]) == "2026-04-10 09:31:01-04:00"
