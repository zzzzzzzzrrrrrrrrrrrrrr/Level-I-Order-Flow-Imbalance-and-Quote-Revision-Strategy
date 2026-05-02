from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from level1_ofi_qr.trade_signing import (
    MATCHED_MIDQUOTE,
    QUOTE_RULE_SIGN,
    SIGNED_TRADE_SIZE,
    TICK_RULE_SIGN,
    TRADE_SIGN,
    TRADE_SIGNING_SCOPE_NOTE,
    TRADE_SIGN_SOURCE,
    TradeSigningError,
    build_trade_signing_dataset,
    build_trade_signs_v1,
)
from level1_ofi_qr.utils import load_data_slice_config


ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = ROOT / "configs" / "data" / "aapl_wrds_20260313_20260410.yaml"


def aligned_trade_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "raw_row_index": [1, 2, 3, 4, 5, 6, 7],
            "event_time": pd.to_datetime(
                [
                    "2026-04-10T09:31:00-04:00",
                    "2026-04-10T09:31:01-04:00",
                    "2026-04-10T09:31:02-04:00",
                    "2026-04-10T09:31:03-04:00",
                    "2026-04-11T09:31:00-04:00",
                    "2026-04-11T09:31:01-04:00",
                    "2026-04-10T09:31:01-04:00",
                ],
                format="mixed",
            ),
            "symbol": ["AAPL", "AAPL", "AAPL", "AAPL", "AAPL", "AAPL", "MSFT"],
            "source": ["wrds_taq_ctm"] * 7,
            "trade_exchange": ["Q"] * 7,
            "sale_condition": ["@"] * 7,
            "trade_correction": ["00"] * 7,
            "trade_sequence_number": [100, 101, 102, 103, 200, 201, 300],
            "trade_price": [100.00, 100.03, 100.01, 100.01, 101.00, 101.05, 200.00],
            "trade_size": [100, 200, 300, 400, 500, 600, 700],
            "trading_date": [
                "2026-04-10",
                "2026-04-10",
                "2026-04-10",
                "2026-04-10",
                "2026-04-11",
                "2026-04-11",
                "2026-04-10",
            ],
            "is_quote_matched": [False, True, True, True, False, False, False],
            "matched_quote_event_time": pd.to_datetime(
                [
                    pd.NaT,
                    "2026-04-10T09:31:00.500000-04:00",
                    "2026-04-10T09:31:01.500000-04:00",
                    "2026-04-10T09:31:02.500000-04:00",
                    pd.NaT,
                    pd.NaT,
                    pd.NaT,
                ],
                format="mixed",
            ),
            "matched_quote_trading_date": [
                pd.NA,
                "2026-04-10",
                "2026-04-10",
                "2026-04-10",
                pd.NA,
                pd.NA,
                pd.NA,
            ],
            "quote_lag_ms": [pd.NA, 500.0, 500.0, 500.0, pd.NA, pd.NA, pd.NA],
            "quote_source": [pd.NA, "wrds_taq_nbbom", "wrds_taq_nbbom", "wrds_taq_nbbom", pd.NA, pd.NA, pd.NA],
            "quote_raw_row_index": [pd.NA, 10, 11, 12, pd.NA, pd.NA, pd.NA],
            "quote_bid_exchange": [pd.NA, "Q", "Q", "Q", pd.NA, pd.NA, pd.NA],
            "quote_ask_exchange": [pd.NA, "Q", "Q", "Q", pd.NA, pd.NA, pd.NA],
            "quote_nbbo_quote_condition": [pd.NA, "R", "R", "R", pd.NA, pd.NA, pd.NA],
            "quote_bid": [pd.NA, 99.98, 100.00, 100.00, pd.NA, pd.NA, pd.NA],
            "quote_ask": [pd.NA, 100.02, 100.10, 100.02, pd.NA, pd.NA, pd.NA],
            "quote_bid_size": [pd.NA, 500, 500, 500, pd.NA, pd.NA, pd.NA],
            "quote_ask_size": [pd.NA, 600, 600, 600, pd.NA, pd.NA, pd.NA],
        }
    )


def test_build_trade_signs_v1_uses_quote_rule_then_tick_fallback() -> None:
    result = build_trade_signs_v1(aligned_trade_frame())
    signed = result.signed_trades

    assert len(signed) == len(aligned_trade_frame())
    assert signed[MATCHED_MIDQUOTE].iloc[1] == pytest.approx(100.00)
    assert pd.isna(signed[QUOTE_RULE_SIGN].iloc[0])
    assert signed[QUOTE_RULE_SIGN].tolist()[1:3] == [1, -1]
    assert pd.isna(signed[QUOTE_RULE_SIGN].iloc[3])
    assert pd.isna(signed[TICK_RULE_SIGN].iloc[0])
    assert signed[TICK_RULE_SIGN].tolist()[1:4] == [1, -1, -1]
    assert signed[TRADE_SIGN].tolist()[:6] == [0, 1, -1, -1, 0, 1]
    assert signed[TRADE_SIGN_SOURCE].tolist()[:6] == [
        "unknown",
        "quote_rule",
        "quote_rule",
        "tick_rule",
        "unknown",
        "tick_rule",
    ]
    assert signed[SIGNED_TRADE_SIZE].tolist()[:6] == [0, 200, -300, -400, 0, 600]

    diagnostics = result.diagnostics
    assert diagnostics.row_preserving is True
    assert diagnostics.input_aligned_trade_rows == 7
    assert diagnostics.output_signed_trade_rows == 7
    assert diagnostics.quote_matched_rows == 3
    assert diagnostics.quote_unmatched_rows == 4
    assert diagnostics.quote_rule_signed_rows == 2
    assert diagnostics.tick_rule_signed_rows == 2
    assert diagnostics.unknown_sign_rows == 3
    assert diagnostics.buy_sign_rows == 2
    assert diagnostics.sell_sign_rows == 2
    assert diagnostics.quote_midpoint_tie_rows == 1
    assert diagnostics.condition_filters_applied is False
    assert diagnostics.ofi_features_implemented is False
    assert diagnostics.labels_implemented is False
    assert diagnostics.backtest_implemented is False


def test_tick_rule_does_not_cross_symbol_or_trading_date() -> None:
    result = build_trade_signs_v1(aligned_trade_frame())
    signed = result.signed_trades

    day_two_first = signed.loc[
        (signed["symbol"] == "AAPL") & (signed["trading_date"] == "2026-04-11")
    ].iloc[0]
    msft_first = signed.loc[signed["symbol"] == "MSFT"].iloc[0]

    assert pd.isna(day_two_first[TICK_RULE_SIGN])
    assert day_two_first[TRADE_SIGN] == 0
    assert pd.isna(msft_first[TICK_RULE_SIGN])
    assert msft_first[TRADE_SIGN] == 0


def test_build_trade_signs_v1_requires_alignment_columns() -> None:
    missing_alignment = aligned_trade_frame().drop(columns=["quote_bid"])

    with pytest.raises(TradeSigningError, match="alignment columns"):
        build_trade_signs_v1(missing_alignment)


def test_build_trade_signing_dataset_writes_outputs(tmp_path: Path) -> None:
    config = load_data_slice_config(CONFIG_PATH)
    processed_root = tmp_path / "processed"
    slice_root = processed_root / config.slice_name
    slice_root.mkdir(parents=True)
    aligned_trade_frame().to_csv(
        slice_root / f"{config.slice_name}_trades_aligned_quotes.csv",
        index=False,
    )

    result = build_trade_signing_dataset(config, processed_dir=processed_root)

    assert result.paths.signed_trade_path.exists()
    assert result.paths.manifest_path.exists()
    manifest = json.loads(result.paths.manifest_path.read_text())
    assert manifest["trade_signing_scope_note"] == TRADE_SIGNING_SCOPE_NOTE
    assert manifest["trade_signing_status"] == {
        "trade_signing_implemented": "v1",
        "condition_filters_applied": False,
        "sale_condition_filters_applied": False,
        "nbbo_quote_condition_filters_applied": False,
        "ofi_features_implemented": False,
        "labels_implemented": False,
        "backtest_implemented": False,
        "research_grade_signed_sample": False,
    }
    diagnostics = manifest["diagnostics"]
    assert diagnostics["row_preserving"] is True
    assert diagnostics["trade_signing_method"] == "quote_rule_with_tick_rule_fallback_v1"
    assert diagnostics["trade_sign_group_keys"] == ["symbol", "trading_date"]
    assert diagnostics["quote_rule_signed_rows"] == 2
    assert diagnostics["tick_rule_signed_rows"] == 2
    assert diagnostics["unknown_sign_rows"] == 3
