from __future__ import annotations

from datetime import date, time
from pathlib import Path

from level1_ofi_qr.utils import load_data_slice_config

CONFIG_PATH = (
    Path(__file__).resolve().parents[2]
    / "configs"
    / "data"
    / "aapl_wrds_20260313_20260410.yaml"
)


def test_load_data_slice_config_parses_wrds_slice() -> None:
    config = load_data_slice_config(CONFIG_PATH)

    assert config.slice_name == "aapl_wrds_20260313_20260410"
    assert config.symbols == ("AAPL",)
    assert config.time_range.trading_dates == (
        date(2026, 3, 13),
        date(2026, 3, 16),
        date(2026, 3, 17),
        date(2026, 3, 18),
        date(2026, 3, 19),
        date(2026, 3, 20),
        date(2026, 3, 23),
        date(2026, 3, 24),
        date(2026, 3, 25),
        date(2026, 3, 26),
        date(2026, 3, 27),
        date(2026, 3, 30),
        date(2026, 3, 31),
        date(2026, 4, 1),
        date(2026, 4, 2),
        date(2026, 4, 6),
        date(2026, 4, 7),
        date(2026, 4, 8),
        date(2026, 4, 9),
        date(2026, 4, 10),
    )
    assert config.time_range.market_open == time(9, 30)
    assert config.time_range.market_close == time(16, 0)
    assert config.source["quote_dataset"] == "nbbom"
    assert config.data_contract.quote_scope == "national_bbo"
    assert config.data_contract.quote_source == "wrds_taq_nbbom"
    assert config.data_contract.trade_source == "wrds_taq_ctm"
    assert config.data_contract.quote_mapping["event_time"]["expr"] == "combine_date_time"
    assert config.data_contract.quote_mapping["source"]["literal"] == "wrds_taq_nbbom"
    assert config.data_contract.quote_mapping["bid_exchange"] == "best_bidex"
    assert config.data_contract.quote_mapping["nbbo_quote_condition"] == "nbbo_qu_cond"
    assert config.data_contract.trade_mapping["sale_condition"] == "tr_scond"
    assert config.data_contract.trade_mapping["source"]["literal"] == "wrds_taq_ctm"
    assert config.data_contract.trade_mapping["trade_sequence_number"] == "tr_seqnum"
