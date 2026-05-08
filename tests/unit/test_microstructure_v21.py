from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from level1_ofi_qr.diagnostics.microstructure_v21.candidate_pool import microprice
from level1_ofi_qr.diagnostics.microstructure_v21.cancellation import first_cancellation
from level1_ofi_qr.diagnostics.microstructure_v21.config import MicrostructureV21Config
from level1_ofi_qr.diagnostics.microstructure_v21.execution_selector import select_execution_mode
from level1_ofi_qr.diagnostics.microstructure_v21.metrics import summarize_orders
from level1_ofi_qr.diagnostics.microstructure_v21.passive_fill import (
    MarketArrays,
    find_passive_fill,
)
from level1_ofi_qr.diagnostics.microstructure_v21 import workflow as v21_workflow
from level1_ofi_qr.diagnostics.microstructure_v21.workflow import (
    _candidate_pool_masks,
    _read_prediction_candidate_events,
    _select_variants_chronologically,
)


def make_market() -> MarketArrays:
    times = pd.to_datetime(
        [
            "2026-04-10T09:30:00.000-04:00",
            "2026-04-10T09:30:00.100-04:00",
            "2026-04-10T09:30:00.200-04:00",
        ]
    )
    trades = pd.to_datetime(
        [
            "2026-04-10T09:29:59.900-04:00",
            "2026-04-10T09:30:00.000-04:00",
            "2026-04-10T09:30:00.100-04:00",
        ]
    )
    return MarketArrays(
        quote_time_ns=times.astype("int64").to_numpy(),
        bid=np.array([100.00, 99.99, 99.98]),
        ask=np.array([100.02, 100.00, 99.99]),
        midquote=np.array([100.01, 99.995, 99.985]),
        microprice_gap_bps=np.array([0.4, 0.2, -0.2]),
        quote_revision_bps=np.array([0.0, -0.3, -0.2]),
        quoted_spread=np.array([0.02, 0.01, 0.01]),
        trade_time_ns=trades.astype("int64").to_numpy(),
        trade_price=np.array([100.00, 100.00, 99.99]),
    )


def test_microprice_formula_uses_displayed_depth_weights() -> None:
    result = microprice(
        pd.Series([100.00]),
        pd.Series([100.02]),
        pd.Series([300.0]),
        pd.Series([100.0]),
    )

    assert result.iloc[0] == 100.015


def test_passive_fill_does_not_use_same_timestamp_evidence() -> None:
    market = make_market()
    submission_ns = int(pd.Timestamp("2026-04-10T09:30:00.000-04:00").value)
    cancel_ns = int(pd.Timestamp("2026-04-10T09:30:00.050-04:00").value)

    fill = find_passive_fill(
        side=1,
        submission_time_ns=submission_ns,
        limit_price=100.00,
        cancel_time_ns=cancel_ns,
        market=market,
        queue_haircut="optimistic",
        tick_size=0.01,
    )

    assert not fill.filled
    assert fill.fill_evidence == "no_cross"


def test_passive_fill_cannot_occur_before_order_submission() -> None:
    market = make_market()
    submission_ns = int(pd.Timestamp("2026-04-10T09:30:00.050-04:00").value)
    cancel_ns = int(pd.Timestamp("2026-04-10T09:30:00.150-04:00").value)

    fill = find_passive_fill(
        side=1,
        submission_time_ns=submission_ns,
        limit_price=100.00,
        cancel_time_ns=cancel_ns,
        market=market,
        queue_haircut="conservative",
        tick_size=0.01,
    )

    assert fill.filled
    assert fill.fill_time_ns == int(pd.Timestamp("2026-04-10T09:30:00.100-04:00").value)


def test_unfilled_passive_order_has_no_realized_pnl_in_metrics() -> None:
    orders = pd.DataFrame(
        [
            {
                "variant_id": "v",
                "trading_date": "2026-04-10",
                "candidate_signal": True,
                "filled": False,
                "gross_pnl": 0.0,
                "cost": 0.0,
                "net_pnl": 0.0,
                "unfilled_opportunity_cost": 0.25,
            }
        ]
    )

    summary = summarize_orders(orders, group_columns=("variant_id",))

    assert summary.loc[0, "submitted_orders"] == 1
    assert summary.loc[0, "filled_orders"] == 0
    assert summary.loc[0, "net_pnl"] == 0.0
    assert summary.loc[0, "unfilled_opportunity_cost"] == 0.25


def test_submitted_and_filled_order_metrics_diverge_with_unfilled_orders() -> None:
    orders = pd.DataFrame(
        [
            {
                "variant_id": "v",
                "trading_date": "2026-04-10",
                "candidate_signal": True,
                "filled": True,
                "gross_pnl": 0.04,
                "cost": 0.01,
                "net_pnl": 0.03,
                "unfilled_opportunity_cost": 0.0,
            },
            {
                "variant_id": "v",
                "trading_date": "2026-04-10",
                "candidate_signal": True,
                "filled": False,
                "gross_pnl": 0.0,
                "cost": 0.0,
                "net_pnl": 0.0,
                "unfilled_opportunity_cost": 0.02,
            },
        ]
    )

    summary = summarize_orders(orders, group_columns=("variant_id",))

    assert summary.loc[0, "fill_rate"] == 0.5
    assert summary.loc[0, "net_pnl_per_filled_order"] == 0.03
    assert summary.loc[0, "net_pnl_per_submitted_order"] == 0.015


def test_market_entry_requires_edge_above_full_market_cost_plus_buffer() -> None:
    weak = pd.Series(
        {
            "predicted_edge_bps": 1.20,
            "expected_cost_bps": 1.00,
            "tradable_edge_bps": 0.20,
            "microprice_aligned": True,
        }
    )
    strong = weak.copy()
    strong["predicted_edge_bps"] = 1.60
    strong["tradable_edge_bps"] = 0.60

    assert (
        select_execution_mode(
            weak,
            edge_threshold_passed=True,
            microprice_usage="entry_gate",
            adverse_selection_buffer_bps=0.50,
            safety_margin_bps=0.0,
        )
        != "market_entry"
    )
    assert (
        select_execution_mode(
            strong,
            edge_threshold_passed=True,
            microprice_usage="entry_gate",
            adverse_selection_buffer_bps=0.50,
            safety_margin_bps=0.0,
        )
        == "market_entry"
    )


def test_validation_fold_selection_uses_only_prior_dates() -> None:
    daily_metrics = pd.DataFrame(
        [
            {"variant_id": "A", "trading_date": "2026-04-01", "net_pnl_per_submitted_order": 1.0},
            {"variant_id": "B", "trading_date": "2026-04-01", "net_pnl_per_submitted_order": -1.0},
            {"variant_id": "A", "trading_date": "2026-04-02", "net_pnl_per_submitted_order": 1.0},
            {"variant_id": "B", "trading_date": "2026-04-02", "net_pnl_per_submitted_order": -1.0},
            {"variant_id": "A", "trading_date": "2026-04-03", "net_pnl_per_submitted_order": -10.0},
            {"variant_id": "B", "trading_date": "2026-04-03", "net_pnl_per_submitted_order": 10.0},
        ]
    )

    selection = _select_variants_chronologically(
        daily_metrics,
        diagnostics_config=MicrostructureV21Config(validation_min_dates=2),
    )

    assert len(selection) == 1
    assert selection.loc[0, "test_date"] == "2026-04-03"
    assert selection.loc[0, "selected_variant_id"] == "A"
    assert not bool(selection.loc[0, "test_used_for_selection"])


def test_prediction_candidate_reader_treats_nan_score_as_no_side(tmp_path) -> None:
    path = tmp_path / "predictions.csv"
    pd.DataFrame(
        [
            {
                "event_time": "2026-04-10T09:30:00-04:00",
                "symbol": "AAPL",
                "trading_date": "2026-04-10",
                "fold_id": "fold_001",
                "test_date": "2026-04-10",
                "selected_threshold": 1.5,
                "model_score": np.nan,
                "cost_aware_estimated_cost_bps": 0.1,
                "signal_midquote": 100.0,
                "signal_quoted_spread": 0.01,
            },
            {
                "event_time": "2026-04-10T09:30:01-04:00",
                "symbol": "AAPL",
                "trading_date": "2026-04-10",
                "fold_id": "fold_001",
                "test_date": "2026-04-10",
                "selected_threshold": 1.5,
                "model_score": 2.0,
                "cost_aware_estimated_cost_bps": 0.1,
                "signal_midquote": 100.0,
                "signal_quoted_spread": 0.01,
            },
        ]
    ).to_csv(path, index=False)

    candidates = _read_prediction_candidate_events(path)

    assert len(candidates) == 1
    assert candidates.loc[0, "side"] == 1
    assert candidates.loc[0, "predicted_edge_bps"] == 2.0


def test_prediction_candidate_reader_sorts_across_chunks_before_side_changes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    chunk_late = pd.DataFrame(
        [
            _prediction_row("2026-04-10T09:30:01-04:00", score=2.0),
        ]
    )
    chunk_early = pd.DataFrame(
        [
            _prediction_row("2026-04-10T09:30:00-04:00", score=2.0),
        ]
    )

    def fake_read_csv(*_args: object, **_kwargs: object) -> list[pd.DataFrame]:
        return [chunk_late.copy(), chunk_early.copy()]

    monkeypatch.setattr(v21_workflow.pd, "read_csv", fake_read_csv)

    candidates = _read_prediction_candidate_events(Path("unused.csv"))

    assert len(candidates) == 1
    assert candidates.loc[0, "event_time"] == pd.Timestamp("2026-04-10T09:30:00-04:00")


def test_spread_quantile_candidate_pool_uses_only_prior_symbol_dates() -> None:
    candidates = pd.DataFrame(
        [
            _candidate_pool_row("AAA", "2026-04-01", quoted_spread=0.01),
            _candidate_pool_row("AAA", "2026-04-02", quoted_spread=0.02),
            _candidate_pool_row("AAA", "2026-04-03", quoted_spread=0.02),
        ]
    )

    masks = _candidate_pool_masks(
        candidates,
        diagnostics_config=MicrostructureV21Config(candidate_pools=("spread_q1_or_q2",)),
    )

    assert masks["spread_q1_or_q2"].tolist() == [False, False, False]


def test_cancellation_volatility_spike_uses_config_threshold_not_future_window_median() -> None:
    market = make_market()
    market = MarketArrays(
        quote_time_ns=market.quote_time_ns,
        bid=market.bid,
        ask=market.ask,
        midquote=market.midquote,
        microprice_gap_bps=np.array([1.0, 1.0, 1.0]),
        quote_revision_bps=np.array([0.0, 1.5, 10.0]),
        quoted_spread=np.array([0.01, 0.01, 0.01]),
        trade_time_ns=market.trade_time_ns,
        trade_price=market.trade_price,
    )

    cancel = first_cancellation(
        side=1,
        submission_time_ns=int(pd.Timestamp("2026-04-10T09:30:00.000-04:00").value),
        ttl_ns=int(pd.Timedelta("1s").value),
        entry_spread=0.01,
        market=market,
        use_microprice_cancel=False,
        tick_size=0.01,
        volatility_spike_threshold_bps=1.0,
    )

    assert cancel.cancel_reason == "volatility_spike"
    assert cancel.cancel_time_ns == int(pd.Timestamp("2026-04-10T09:30:00.100-04:00").value)


def _prediction_row(event_time: str, *, score: float) -> dict[str, object]:
    return {
        "event_time": event_time,
        "symbol": "AAPL",
        "trading_date": "2026-04-10",
        "fold_id": "fold_001",
        "test_date": "2026-04-10",
        "selected_threshold": 1.5,
        "model_score": score,
        "cost_aware_estimated_cost_bps": 0.1,
        "signal_midquote": 100.0,
        "signal_quoted_spread": 0.01,
    }


def _candidate_pool_row(
    symbol: str,
    trading_date: str,
    *,
    quoted_spread: float,
) -> dict[str, object]:
    return {
        "symbol": symbol,
        "trading_date": trading_date,
        "quoted_spread": quoted_spread,
        "displayed_depth": 1000.0,
        "tradable_edge_bps": 1.0,
        "predicted_edge_bps": 1.0,
        "selected_threshold_numeric": 0.5,
    }
