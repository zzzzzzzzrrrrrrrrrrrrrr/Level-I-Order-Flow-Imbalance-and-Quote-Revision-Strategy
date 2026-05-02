from __future__ import annotations

from pathlib import Path

import pandas as pd

from level1_ofi_qr.adapters import normalize_wrds_trades
from level1_ofi_qr.schema import (
    SALE_CONDITION,
    SOURCE,
    SYMBOL,
    TRADE_COLUMNS,
    TRADE_EXCHANGE,
)
from level1_ofi_qr.utils import load_data_slice_config

CONFIG_PATH = (
    Path(__file__).resolve().parents[2]
    / "configs"
    / "data"
    / "aapl_wrds_20260313_20260410.yaml"
)
FIXTURE_PATH = Path(__file__).resolve().parents[1] / "fixtures" / "wrds_raw_trades.csv"


def load_fixture_frame() -> pd.DataFrame:
    return pd.read_csv(FIXTURE_PATH)


def test_normalize_wrds_trades_maps_fixture_into_trade_schema() -> None:
    config = load_data_slice_config(CONFIG_PATH)
    raw_trades = load_fixture_frame()

    normalized = normalize_wrds_trades(raw_trades, config=config)

    assert tuple(normalized.columns) == TRADE_COLUMNS
    assert normalized.loc[0, SOURCE] == "wrds_taq_ctm"
    assert normalized.loc[0, SYMBOL] == "AAPL"
    assert normalized.loc[1, TRADE_EXCHANGE] == "Q"
    assert normalized.loc[1, SALE_CONDITION] == "@"
    assert str(normalized["event_time"].dt.tz) == "America/New_York"
