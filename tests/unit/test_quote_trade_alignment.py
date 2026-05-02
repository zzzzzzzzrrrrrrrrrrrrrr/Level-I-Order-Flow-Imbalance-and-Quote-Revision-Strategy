from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from level1_ofi_qr.alignment import (
    ALIGNMENT_SCOPE_NOTE,
    IS_QUOTE_MATCHED,
    MATCHED_QUOTE_EVENT_TIME,
    MATCHED_QUOTE_TRADING_DATE,
    QUOTE_LAG_MS,
    TRADING_DATE,
    AlignmentError,
    align_trades_to_prior_quotes,
    build_quote_trade_alignment,
    build_quote_trade_alignment_tolerance_sensitivity,
)
from level1_ofi_qr.utils import load_data_slice_config


ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = ROOT / "configs" / "data" / "aapl_wrds_20260313_20260410.yaml"


def quote_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "raw_row_index": [10, 11, 12],
            "event_time": pd.to_datetime(
                [
                    "2026-04-10T09:31:00-04:00",
                    "2026-04-10T09:31:01-04:00",
                    "2026-04-10T09:31:00-04:00",
                ],
                format="mixed",
            ),
            "symbol": ["AAPL", "AAPL", "MSFT"],
            "source": ["wrds_taq_nbbom", "wrds_taq_nbbom", "wrds_taq_nbbom"],
            "bid_exchange": ["Q", "Q", "N"],
            "ask_exchange": ["Q", "B", "N"],
            "nbbo_quote_condition": ["R", "R", "R"],
            "bid": [190.00, 190.01, 410.00],
            "ask": [190.02, 190.01, 410.05],
            "bid_size": [500, 450, 200],
            "ask_size": [600, 550, 250],
        }
    )


def trade_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "raw_row_index": [20, 21, 22, 23],
            "event_time": pd.to_datetime(
                [
                    "2026-04-10T09:31:00-04:00",
                    "2026-04-10T09:31:00.500000-04:00",
                    "2026-04-10T09:31:02-04:00",
                    "2026-04-10T09:31:00.500000-04:00",
                ],
                format="mixed",
            ),
            "symbol": ["AAPL", "AAPL", "AAPL", "MSFT"],
            "source": ["wrds_taq_ctm", "wrds_taq_ctm", "wrds_taq_ctm", "wrds_taq_ctm"],
            "trade_exchange": ["Q", "Q", "Q", "N"],
            "sale_condition": ["@", "@", "@", "@"],
            "trade_correction": ["00", "00", "00", "00"],
            "trade_sequence_number": [1000, 1001, 1002, 2000],
            "trade_price": [190.01, 190.02, 190.04, 410.02],
            "trade_size": [100, 200, 100, 50],
        }
    )


def test_align_trades_to_prior_quotes_uses_strictly_prior_quote() -> None:
    result = align_trades_to_prior_quotes(trade_frame(), quote_frame())
    aligned = result.aligned_trades

    assert pd.isna(aligned.loc[0, MATCHED_QUOTE_EVENT_TIME])
    assert not bool(aligned.loc[0, IS_QUOTE_MATCHED])
    assert aligned.loc[1, "quote_raw_row_index"] == 10
    assert aligned.loc[1, QUOTE_LAG_MS] == 500.0
    assert aligned.loc[2, "quote_raw_row_index"] == 11
    assert aligned.loc[2, QUOTE_LAG_MS] == 1000.0
    assert aligned.loc[3, "quote_raw_row_index"] == 12
    assert aligned.loc[3, "quote_bid"] == 410.00

    matched = aligned.loc[aligned[IS_QUOTE_MATCHED]]
    assert (matched[MATCHED_QUOTE_EVENT_TIME] < matched["event_time"]).all()
    assert (matched[QUOTE_LAG_MS] > 0).all()
    assert len(aligned) == len(trade_frame())
    assert result.diagnostics.aligned_trade_rows == 4
    assert result.diagnostics.matched_trade_rows == 3
    assert result.diagnostics.unmatched_trade_rows == 1
    assert result.diagnostics.allow_exact_matches is False
    assert result.diagnostics.session_boundary_policy == "same_symbol_same_trading_date_only"
    assert result.diagnostics.alignment_group_keys == ("symbol", "trading_date")
    assert result.diagnostics.cross_session_match_count == 0
    assert result.diagnostics.condition_filters_applied is False
    assert result.diagnostics.trade_signing_applied is False
    assert result.diagnostics.matched_locked_quote_count == 1
    assert result.diagnostics.matched_locked_quote_ratio == 1 / 3


def test_align_trades_to_prior_quotes_respects_trading_session_boundary() -> None:
    quotes = pd.DataFrame(
        {
            "event_time": pd.to_datetime(
                [
                    "2026-04-09T15:59:59-04:00",
                    "2026-04-10T09:31:00-04:00",
                ],
                format="mixed",
            ),
            "symbol": ["AAPL", "AAPL"],
            "source": ["wrds_taq_nbbom", "wrds_taq_nbbom"],
            "bid_exchange": ["Q", "Q"],
            "ask_exchange": ["Q", "Q"],
            "nbbo_quote_condition": ["R", "R"],
            "bid": [189.00, 190.00],
            "ask": [189.02, 190.02],
            "bid_size": [100, 500],
            "ask_size": [100, 600],
        }
    )
    trades = pd.DataFrame(
        {
            "event_time": pd.to_datetime(
                [
                    "2026-04-10T09:30:00-04:00",
                    "2026-04-10T09:31:01-04:00",
                ],
                format="mixed",
            ),
            "symbol": ["AAPL", "AAPL"],
            "source": ["wrds_taq_ctm", "wrds_taq_ctm"],
            "trade_exchange": ["Q", "Q"],
            "sale_condition": ["@", "@"],
            "trade_correction": ["00", "00"],
            "trade_sequence_number": [2001, 2002],
            "trade_price": [190.01, 190.02],
            "trade_size": [100, 200],
        }
    )

    result = align_trades_to_prior_quotes(trades, quotes)
    aligned = result.aligned_trades

    assert aligned[TRADING_DATE].tolist() == ["2026-04-10", "2026-04-10"]
    assert not bool(aligned.loc[0, IS_QUOTE_MATCHED])
    assert pd.isna(aligned.loc[0, MATCHED_QUOTE_EVENT_TIME])
    assert bool(aligned.loc[1, IS_QUOTE_MATCHED])
    assert aligned.loc[1, MATCHED_QUOTE_TRADING_DATE] == "2026-04-10"
    assert aligned.loc[1, "quote_bid"] == 190.00
    assert result.diagnostics.matched_trade_rows == 1
    assert result.diagnostics.unmatched_trade_rows == 1
    assert result.diagnostics.cross_session_match_count == 0


def test_align_trades_to_prior_quotes_applies_optional_tolerance() -> None:
    result = align_trades_to_prior_quotes(
        trade_frame(),
        quote_frame(),
        tolerance=pd.Timedelta(milliseconds=750),
    )
    aligned = result.aligned_trades

    assert pd.isna(aligned.loc[0, MATCHED_QUOTE_EVENT_TIME])
    assert aligned.loc[1, QUOTE_LAG_MS] == 500.0
    assert pd.isna(aligned.loc[2, MATCHED_QUOTE_EVENT_TIME])
    assert aligned.loc[3, QUOTE_LAG_MS] == 500.0
    assert result.diagnostics.matched_trade_rows == 2
    assert result.diagnostics.unmatched_trade_rows == 2
    assert result.diagnostics.tolerance == "750ms"
    assert (
        result.diagnostics.tolerance_policy
        == "retain_trade_unmatched_if_quote_lag_exceeds_tolerance"
    )


def test_align_trades_to_prior_quotes_preserves_trade_order() -> None:
    trades = trade_frame().iloc[[2, 0, 1]].reset_index(drop=True)

    result = align_trades_to_prior_quotes(trades, quote_frame())

    assert result.aligned_trades["trade_sequence_number"].tolist() == [1002, 1000, 1001]


def test_align_trades_to_prior_quotes_does_not_cross_match_symbols() -> None:
    quotes = pd.DataFrame(
        {
            "event_time": pd.to_datetime(
                [
                    "2026-04-10T09:31:00-04:00",
                    "2026-04-10T09:31:01-04:00",
                ],
                format="mixed",
            ),
            "symbol": ["AAPL", "MSFT"],
            "source": ["wrds_taq_nbbom", "wrds_taq_nbbom"],
            "bid_exchange": ["Q", "N"],
            "ask_exchange": ["Q", "N"],
            "nbbo_quote_condition": ["R", "R"],
            "bid": [190.00, 410.00],
            "ask": [190.02, 410.05],
            "bid_size": [500, 200],
            "ask_size": [600, 250],
        }
    )
    trades = trade_frame().iloc[[1, 3]].copy()
    trades.loc[:, "symbol"] = ["AAPL", "MSFT"]
    trades.loc[:, "event_time"] = pd.to_datetime(
        [
            "2026-04-10T09:31:01.500000-04:00",
            "2026-04-10T09:31:00.500000-04:00",
        ],
        format="mixed",
    )

    result = align_trades_to_prior_quotes(trades.reset_index(drop=True), quotes)
    aligned = result.aligned_trades

    assert bool(aligned.loc[0, IS_QUOTE_MATCHED])
    assert aligned.loc[0, "quote_bid"] == 190.00
    assert not bool(aligned.loc[1, IS_QUOTE_MATCHED])
    assert pd.isna(aligned.loc[1, MATCHED_QUOTE_EVENT_TIME])


def test_align_trades_to_prior_quotes_rejects_negative_tolerance() -> None:
    with pytest.raises(AlignmentError, match="non-negative"):
        align_trades_to_prior_quotes(
            trade_frame(),
            quote_frame(),
            tolerance=pd.Timedelta(milliseconds=-1),
        )


def test_build_quote_trade_alignment_writes_outputs(tmp_path: Path) -> None:
    config = load_data_slice_config(CONFIG_PATH)
    processed_root = tmp_path / "processed"
    slice_root = processed_root / config.slice_name
    slice_root.mkdir(parents=True)
    quote_frame().to_csv(slice_root / f"{config.slice_name}_quotes_clean.csv", index=False)
    trade_frame().to_csv(slice_root / f"{config.slice_name}_trades_clean.csv", index=False)

    result = build_quote_trade_alignment(config, processed_dir=processed_root)

    assert result.paths.aligned_trade_path.exists()
    assert result.paths.manifest_path.exists()
    assert result.diagnostics.matched_trade_rows == 3
    manifest = json.loads(result.paths.manifest_path.read_text())
    assert manifest["alignment_status"]["alignment_implemented"] is True
    assert manifest["alignment_status"]["allow_exact_matches"] is False
    assert manifest["alignment_status"]["trade_signing_applied"] is False
    assert manifest["alignment_scope_note"] == ALIGNMENT_SCOPE_NOTE
    diagnostics = manifest["diagnostics"]
    assert diagnostics["input_trade_rows"] == 4
    assert diagnostics["input_quote_rows"] == 3
    assert diagnostics["aligned_trade_rows"] == 4
    assert diagnostics["matched_trade_rows"] == 3
    assert diagnostics["unmatched_trade_rows"] == 1
    assert diagnostics["matched_locked_quote_count"] == 1
    assert diagnostics["matched_locked_quote_ratio"] == 1 / 3
    assert diagnostics["session_boundary_policy"] == "same_symbol_same_trading_date_only"
    assert diagnostics["alignment_group_keys"] == ["symbol", "trading_date"]
    assert diagnostics["cross_session_match_count"] == 0
    assert diagnostics["min_quote_lag_ms"] > 0


def test_build_quote_trade_alignment_tolerance_sensitivity_writes_summary(
    tmp_path: Path,
) -> None:
    config = load_data_slice_config(CONFIG_PATH)
    processed_root = tmp_path / "processed"
    slice_root = processed_root / config.slice_name
    slice_root.mkdir(parents=True)
    quote_frame().to_csv(slice_root / f"{config.slice_name}_quotes_clean.csv", index=False)
    trade_frame().to_csv(slice_root / f"{config.slice_name}_trades_clean.csv", index=False)

    result = build_quote_trade_alignment_tolerance_sensitivity(
        config,
        processed_dir=processed_root,
    )

    assert result.paths.summary_json_path.exists()
    assert result.paths.summary_csv_path.exists()
    summary = json.loads(result.paths.summary_json_path.read_text())
    assert summary["alignment_scope_note"] == ALIGNMENT_SCOPE_NOTE
    assert summary["tolerance_decision"] == "not_selected"
    assert [row["candidate_tolerance"] for row in result.summary] == [
        "None",
        "5s",
        "1s",
        "500ms",
        "100ms",
    ]
    assert all(row["cross_session_match_count"] == 0 for row in result.summary)
