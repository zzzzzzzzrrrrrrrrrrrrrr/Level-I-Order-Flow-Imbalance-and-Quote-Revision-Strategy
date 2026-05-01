from __future__ import annotations

import pandas as pd
import pytest

from level1_ofi_qr.diagnostics import (
    MicrostructureDiagnosticsConfig,
    build_cost_aware_microstructure_diagnostics,
    write_microstructure_figures,
)


def make_ledger() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "model_backtest_id": "fold_001_candidate_001",
                "fold_id": "fold_001",
                "candidate_id": "candidate_001",
                "feature_set": "test_features",
                "score_threshold": 1.5,
                "selected_threshold": 1.5,
                "cost_multiplier": 1.0,
                "selected_cost_multiplier": 1.0,
                "cooldown_seconds": 0,
                "min_holding_seconds": 0,
                "test_date": "2026-04-10",
                "event_time": pd.Timestamp("2026-04-10T09:30:00-04:00"),
                "symbol": "AAPL",
                "trading_date": "2026-04-10",
                "signal_row_index": 1,
                "previous_position": 0.0,
                "target_position": 1.0,
                "fill_midquote": 100.0,
                "quoted_spread": 0.02,
                "event_cost": 0.01,
            },
            {
                "model_backtest_id": "fold_001_candidate_001",
                "fold_id": "fold_001",
                "candidate_id": "candidate_001",
                "feature_set": "test_features",
                "score_threshold": 1.5,
                "selected_threshold": 1.5,
                "cost_multiplier": 1.0,
                "selected_cost_multiplier": 1.0,
                "cooldown_seconds": 0,
                "min_holding_seconds": 0,
                "test_date": "2026-04-10",
                "event_time": pd.Timestamp("2026-04-10T09:30:02-04:00"),
                "symbol": "AAPL",
                "trading_date": "2026-04-10",
                "signal_row_index": 2,
                "previous_position": 1.0,
                "target_position": 0.0,
                "fill_midquote": 100.05,
                "quoted_spread": 0.02,
                "event_cost": 0.01,
            },
        ]
    )


def make_quotes() -> pd.DataFrame:
    times = pd.to_datetime(
        [
            "2026-04-10T09:29:59.900-04:00",
            "2026-04-10T09:30:00.000-04:00",
            "2026-04-10T09:30:00.250-04:00",
            "2026-04-10T09:30:01.000-04:00",
            "2026-04-10T09:30:02.000-04:00",
        ]
    )
    mids = [99.99, 100.00, 100.03, 100.04, 100.05]
    spreads = [0.02] * len(times)
    return pd.DataFrame(
        {
            "event_time": times,
            "symbol": ["AAPL"] * len(times),
            "trading_date": ["2026-04-10"] * len(times),
            "bid": [mid - spread / 2 for mid, spread in zip(mids, spreads, strict=True)],
            "ask": [mid + spread / 2 for mid, spread in zip(mids, spreads, strict=True)],
            "bid_size": [100, 110, 120, 100, 100],
            "ask_size": [90, 80, 70, 100, 100],
            "midquote": mids,
            "quoted_spread": spreads,
            "relative_spread": [spread / mid for mid, spread in zip(mids, spreads, strict=True)],
            "quote_revision_bps": [0.0, 1.0, 3.0, 1.0, 1.0],
            "quote_event_interval_ms": [100.0, 100.0, 250.0, 750.0, 1000.0],
        }
    )


def make_signals() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "event_time": [pd.Timestamp("2026-04-10T09:30:00-04:00")],
            "symbol": ["AAPL"],
            "trading_date": ["2026-04-10"],
            "signal_quote_imbalance": [0.2],
            "signal_quote_revision_bps": [1.0],
            "signal_quoted_spread": [0.02],
            "signal_relative_spread": [0.0002],
            "signed_flow_imbalance_10_trades": [1.0],
            "signed_flow_imbalance_50_trades": [1.0],
            "signed_flow_imbalance_100_trades": [-1.0],
            "signed_flow_imbalance_100ms": [0.8],
            "signed_flow_imbalance_500ms": [0.7],
            "signed_flow_imbalance_1s": [0.2],
        }
    )


def make_predictions() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "event_time": [pd.Timestamp("2026-04-10T09:30:00-04:00")],
            "symbol": ["AAPL"],
            "trading_date": ["2026-04-10"],
            "model_score": [2.0],
            "cost_aware_estimated_cost_bps": [1.0],
            "cost_aware_predicted_edge_bps": [2.0],
        }
    )


def make_trades() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "event_time": pd.to_datetime(
                [
                    "2026-04-10T09:30:00.100-04:00",
                    "2026-04-10T09:30:00.300-04:00",
                ]
            ),
            "symbol": ["AAPL", "AAPL"],
            "trading_date": ["2026-04-10", "2026-04-10"],
            "trade_price": [99.99, 100.04],
            "trade_size": [10, 20],
            "trade_sign": [-1, 1],
            "signed_trade_size": [-10, 20],
        }
    )


def test_microstructure_diagnostics_adds_edge_and_state_features() -> None:
    result = build_cost_aware_microstructure_diagnostics(
        make_ledger(),
        make_quotes(),
        trade_rows=make_trades(),
        signal_rows=make_signals(),
        prediction_rows=make_predictions(),
        config=MicrostructureDiagnosticsConfig(horizons=("250ms",), cost_stress_multipliers=(0, 1)),
    )

    trade = result.trades.iloc[0]
    assert trade["gross_pnl"] == pytest.approx(0.05)
    assert trade["cost"] == pytest.approx(0.02)
    assert trade["net_pnl"] == pytest.approx(0.03)
    assert trade["expected_net_edge_bps"] == pytest.approx(1.0)
    assert trade["edge_ratio"] == pytest.approx(2.0)
    assert trade["entry_depth_imbalance"] > 0
    assert trade["entry_microprice"] > trade["entry_midquote"]
    assert trade["entry_microprice_gap_bps"] > 0
    assert trade["ofi_persistence_count"] == 5
    assert set(result.strategy_variants["variant"]) == {
        "v1_market_entry_market_exit",
        "v2_spread_q1_edge_gate_market_exit",
        "v2_spread_q1_edge_microprice_gate_market_exit",
        "v2_spread_q1_edge_microprice_gate_5s_exit",
        "v2_spread_q1_microprice_passive_entry_market_exit",
        "v2_spread_q1_microprice_passive_entry_limit_timeout_exit",
    }


def test_horizon_and_cost_stress_tables_are_reported() -> None:
    result = build_cost_aware_microstructure_diagnostics(
        make_ledger(),
        make_quotes(),
        trade_rows=make_trades(),
        signal_rows=make_signals(),
        prediction_rows=make_predictions(),
        config=MicrostructureDiagnosticsConfig(
            horizons=("250ms", "2s"),
            cost_stress_multipliers=(0.0, 1.0, 2.0),
        ),
    )

    assert set(result.horizon["horizon"]) == {"250ms", "2s"}
    first_horizon = result.horizon.loc[result.horizon["horizon"] == "250ms"].iloc[0]
    assert first_horizon["mfe"] == pytest.approx(0.03)
    assert first_horizon["mae"] == pytest.approx(0.0)
    stress = result.cost_stress.loc[result.cost_stress["fold_id"] == "ALL"]
    assert stress.loc[stress["cost_multiplier"] == 0.0, "net_pnl"].iloc[0] == pytest.approx(0.05)
    assert stress.loc[stress["cost_multiplier"] == 2.0, "net_pnl"].iloc[0] == pytest.approx(0.01)


def test_conservative_limit_entry_requires_crossing() -> None:
    no_cross_quotes = make_quotes()
    no_cross_quotes["ask"] = [100.01, 100.01, 100.03, 100.05, 100.06]
    no_cross_quotes["bid"] = [99.99, 99.99, 100.01, 100.03, 100.04]
    no_cross_trades = make_trades()
    no_cross_trades["trade_price"] = [100.02, 100.04]

    result = build_cost_aware_microstructure_diagnostics(
        make_ledger(),
        no_cross_quotes,
        trade_rows=no_cross_trades,
        signal_rows=make_signals(),
        prediction_rows=make_predictions(),
        config=MicrostructureDiagnosticsConfig(horizons=("250ms",)),
    )

    execution = result.execution.set_index("execution_scenario")
    assert execution.loc["market_entry_market_exit", "filled_round_trips"] == 1
    assert execution.loc["limit_entry_market_exit", "filled_round_trips"] == 0
    assert execution.loc["limit_entry_limit_or_timeout_exit", "filled_round_trips"] == 0


def test_reversal_order_cost_is_split_between_close_and_open() -> None:
    ledger = pd.DataFrame(
        [
            {
                "model_backtest_id": "fold_001_candidate_001",
                "fold_id": "fold_001",
                "event_time": pd.Timestamp("2026-04-10T09:30:00-04:00"),
                "symbol": "AAPL",
                "trading_date": "2026-04-10",
                "previous_position": 0.0,
                "target_position": 1.0,
                "fill_midquote": 100.0,
                "quoted_spread": 0.02,
                "event_cost": 0.01,
            },
            {
                "model_backtest_id": "fold_001_candidate_001",
                "fold_id": "fold_001",
                "event_time": pd.Timestamp("2026-04-10T09:30:01-04:00"),
                "symbol": "AAPL",
                "trading_date": "2026-04-10",
                "previous_position": 1.0,
                "target_position": -1.0,
                "fill_midquote": 100.05,
                "quoted_spread": 0.02,
                "event_cost": 0.02,
            },
            {
                "model_backtest_id": "fold_001_candidate_001",
                "fold_id": "fold_001",
                "event_time": pd.Timestamp("2026-04-10T09:30:02-04:00"),
                "symbol": "AAPL",
                "trading_date": "2026-04-10",
                "previous_position": -1.0,
                "target_position": 0.0,
                "fill_midquote": 100.03,
                "quoted_spread": 0.02,
                "event_cost": 0.01,
            },
        ]
    )

    result = build_cost_aware_microstructure_diagnostics(
        ledger,
        make_quotes(),
        config=MicrostructureDiagnosticsConfig(horizons=("250ms",)),
    )

    assert len(result.trades) == 2
    assert result.trades["cost"].sum() == pytest.approx(0.04)


def test_microstructure_figures_are_written(tmp_path) -> None:
    result = build_cost_aware_microstructure_diagnostics(
        make_ledger(),
        make_quotes(),
        trade_rows=make_trades(),
        signal_rows=make_signals(),
        prediction_rows=make_predictions(),
        config=MicrostructureDiagnosticsConfig(horizons=("250ms", "5s")),
    )

    paths = write_microstructure_figures(
        result,
        slice_name="unit_slice",
        figures_dir=tmp_path,
    )

    assert paths.strategy_variants_svg_path.exists()
    assert paths.horizon_svg_path.exists()
    assert paths.execution_svg_path.exists()
    assert paths.spread_breakdown_svg_path.exists()
    assert "Microstructure Strategy V2" in paths.strategy_variants_svg_path.read_text()
