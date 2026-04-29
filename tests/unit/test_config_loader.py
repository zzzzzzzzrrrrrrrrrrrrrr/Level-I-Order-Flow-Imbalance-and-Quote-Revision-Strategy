from __future__ import annotations

from datetime import date, time
from pathlib import Path

from level1_ofi_qr.utils import load_data_slice_config

CONFIG_PATH = (
    Path(__file__).resolve().parents[2]
    / "configs"
    / "data"
    / "aapl_wrds_20260424_20260428.yaml"
)


def test_load_data_slice_config_parses_wrds_slice() -> None:
    config = load_data_slice_config(CONFIG_PATH)

    assert config.slice_name == "aapl_wrds_latest_3_trading_days"
    assert config.symbols == ("AAPL",)
    assert config.time_range.trading_dates == (
        date(2026, 4, 24),
        date(2026, 4, 27),
        date(2026, 4, 28),
    )
    assert config.time_range.market_open == time(9, 30)
    assert config.time_range.market_close == time(16, 0)
    assert config.data_contract.quote_mapping["quote_exchange"] == "ex"
    assert config.data_contract.quote_mapping["bid_exchange"] == "bidex"
    assert config.data_contract.trade_mapping["sale_condition"] == "tr_scond"
    assert config.data_contract.trade_mapping["trade_sequence_number"] == "tr_seqnum"
