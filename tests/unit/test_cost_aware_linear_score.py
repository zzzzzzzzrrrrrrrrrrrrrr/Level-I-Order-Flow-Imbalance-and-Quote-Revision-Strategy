from __future__ import annotations

import pandas as pd
import pytest

from level1_ofi_qr.models import (
    COST_AWARE_COST_BLOCKED_COLUMN,
    COST_AWARE_LINEAR_SCORE_SIGNAL_COLUMN,
    COST_AWARE_LINEAR_SCORE_STRATEGY,
    MODEL_SCORE_COLUMN,
    MODEL_SIGNAL_COLUMN,
    CostAwareLinearScoreConfig,
    ModelFeatureSet,
    ModelTrainingV1Config,
    build_cost_aware_linear_score_signals,
    run_cost_aware_linear_score_v1,
    run_model_training_v1,
)
from level1_ofi_qr.signals import build_sequential_gate_signals_v1


ONE_FEATURE = ModelFeatureSet(name="one_feature", columns=("signal_quote_imbalance",))


def scored_rows(scores: list[float], *, spread: float = 0.0) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "event_time": pd.date_range(
                "2026-04-10T09:31:00-04:00",
                periods=len(scores),
                freq="1s",
            ),
            "symbol": ["AAPL"] * len(scores),
            "trading_date": ["2026-04-10"] * len(scores),
            MODEL_SCORE_COLUMN: scores,
            "signal_midquote": [100.0] * len(scores),
            "signal_quoted_spread": [spread] * len(scores),
        }
    )


def model_rows(dates: tuple[str, ...] = ("2026-04-08", "2026-04-09", "2026-04-10")) -> pd.DataFrame:
    rows = []
    for date_index, trading_date in enumerate(dates):
        if date_index == 0:
            features = [-1.0, 1.0, 0.0]
            directions = [-1, 1, 0]
            midquotes = [100.0, 100.5, 100.5]
        else:
            features = [1.6, 2.6, 0.0]
            directions = [1, 1, 0]
            midquotes = [101.0 + date_index, 100.0 + date_index, 102.0 + date_index]
        for row_index, (feature, direction, midquote) in enumerate(
            zip(features, directions, midquotes, strict=True)
        ):
            rows.append(
                {
                    "event_time": pd.Timestamp(
                        f"{trading_date}T09:31:0{row_index}-04:00"
                    ),
                    "symbol": "AAPL",
                    "trading_date": trading_date,
                    "signal_midquote": midquote,
                    "signal_quoted_spread": 0.0,
                    "signal_quote_imbalance": feature,
                    "future_midquote_direction_500ms": direction,
                    "future_midquote_return_bps_500ms": float(direction),
                    "label_available_500ms": True,
                }
            )
    return pd.DataFrame(rows)


def cost_aware_config(**overrides: object) -> CostAwareLinearScoreConfig:
    values = {
        "min_train_dates": 1,
        "feature_sets": (ONE_FEATURE,),
        "score_threshold_grid": (1.5, 2.0, 2.5, 3.0),
        "include_quantile_thresholds": False,
        "cost_multiplier_grid": (1.0,),
        "cooldown_seconds_grid": (0,),
        "min_holding_seconds_grid": (0,),
        "min_train_observations": 2,
        "min_validation_trades": 1,
        "fixed_bps": 0.0,
        "slippage_ticks": 0.0,
    }
    values.update(overrides)
    return CostAwareLinearScoreConfig(**values)


def test_existing_baselines_still_run_without_cost_aware_signal() -> None:
    linear_result = run_model_training_v1(
        model_rows(),
        config=ModelTrainingV1Config(
            min_train_dates=1,
            feature_sets=(ONE_FEATURE,),
            score_threshold_grid=(0.0, 0.5),
            min_train_observations=2,
            min_validation_orders=1,
        ),
    )

    assert MODEL_SIGNAL_COLUMN in linear_result.predictions.columns
    assert COST_AWARE_LINEAR_SCORE_SIGNAL_COLUMN not in linear_result.predictions.columns

    sequential_result = build_sequential_gate_signals_v1(
        pd.DataFrame(
            {
                "event_time": pd.to_datetime(["2026-04-10T09:31:00-04:00"]),
                "symbol": ["AAPL"],
                "trading_date": ["2026-04-10"],
                "signed_flow_imbalance_500ms": [1.0],
            }
        ),
        pd.DataFrame(
            {
                "event_time": pd.to_datetime(["2026-04-10T09:31:00-04:00"]),
                "symbol": ["AAPL"],
                "trading_date": ["2026-04-10"],
                "midquote": [100.0],
                "quote_imbalance": [1.0],
                "quote_revision_bps": [1.0],
                "quoted_spread": [0.0],
                "relative_spread": [0.0],
            }
        ),
    )
    assert sequential_result.signals["sequential_gate_signal"].tolist() == [1]


def test_cost_aware_strategy_can_choose_thresholds_above_one_point_five() -> None:
    result = run_cost_aware_linear_score_v1(model_rows(), config=cost_aware_config())

    selected = result.candidates.loc[result.candidates["selected_for_test"] == True].iloc[0]
    assert selected["selection_objective"] == "maximize_validation_net_pnl"
    assert selected["score_threshold"] > 1.5
    assert result.diagnostics.strategy_variant == COST_AWARE_LINEAR_SCORE_STRATEGY
    assert result.diagnostics.reuses_linear_score_output is True


def test_cost_aware_rule_blocks_trade_when_estimated_cost_exceeds_edge() -> None:
    result = build_cost_aware_linear_score_signals(
        scored_rows([1.6], spread=0.20),
        threshold=1.5,
        cost_multiplier=1.0,
        cooldown_seconds=0,
        min_holding_seconds=0,
    )

    assert result[COST_AWARE_LINEAR_SCORE_SIGNAL_COLUMN].tolist() == [0]
    assert result[COST_AWARE_COST_BLOCKED_COLUMN].tolist() == [True]


def test_cooldown_reduces_position_changes() -> None:
    no_cooldown = build_cost_aware_linear_score_signals(
        scored_rows([2.0, -2.0, 2.0, -2.0, 0.0]),
        threshold=1.5,
        cost_multiplier=1.0,
        cooldown_seconds=0,
        min_holding_seconds=0,
    )
    with_cooldown = build_cost_aware_linear_score_signals(
        scored_rows([2.0, -2.0, 2.0, -2.0, 0.0]),
        threshold=1.5,
        cost_multiplier=1.0,
        cooldown_seconds=3,
        min_holding_seconds=0,
    )

    def changes(values: pd.Series) -> int:
        return int(values.ne(values.shift(fill_value=0)).sum())

    assert changes(with_cooldown[COST_AWARE_LINEAR_SCORE_SIGNAL_COLUMN]) < changes(
        no_cooldown[COST_AWARE_LINEAR_SCORE_SIGNAL_COLUMN]
    )


def test_multi_fold_reporting_aggregates_correctly() -> None:
    result = run_cost_aware_linear_score_v1(
        model_rows(("2026-04-08", "2026-04-09", "2026-04-10", "2026-04-11")),
        config=cost_aware_config(),
    )

    report = result.report.iloc[0]
    assert result.diagnostics.fold_count == 2
    assert report["gross_pnl"] == pytest.approx(result.summary["gross_pnl"].sum())
    assert report["cost"] == pytest.approx(result.summary["cost"].sum())
    assert report["net_pnl"] == pytest.approx(result.summary["net_pnl"].sum())
    assert report["num_trades"] == int(result.summary["num_trades"].sum())
    assert "fold_001=" in report["selected_threshold_by_fold"]
    assert "fold_002=" in report["selected_threshold_by_fold"]
    assert "fold_001=1.0" in report["selected_cost_multiplier_by_fold"]
