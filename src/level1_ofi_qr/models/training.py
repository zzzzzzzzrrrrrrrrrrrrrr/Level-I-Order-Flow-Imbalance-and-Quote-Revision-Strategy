"""AAPL prototype model training and held-out accounting backtest."""

from __future__ import annotations

from dataclasses import dataclass
from itertools import product
from typing import Final

import pandas as pd
import numpy as np

from ..alignment import TRADING_DATE
from ..execution import TargetPositionAccountingConfig, run_target_position_accounting_v1
from ..schema import EVENT_TIME, SYMBOL
from ..signals import SIGNAL_QUOTE_IMBALANCE, SIGNAL_QUOTE_REVISION_BPS

MODEL_TRAINING_POLICY_NOTE: Final[str] = (
    "Model training v1 learns a standardized linear directional score on train "
    "dates, selects feature-set / score-threshold candidates on validation "
    "dates, and evaluates the frozen selected candidate on held-out test dates. "
    "The current scope is an AAPL single-slice prototype, not a generalized "
    "research-grade model claim."
)
MODEL_SPLIT_POLICY: Final[str] = "chronological_train_validation_test"
MODEL_TRAINING_POLICY: Final[str] = "train_linear_feature_score_on_train_dates"
MODEL_SELECTION_POLICY: Final[str] = "select_feature_set_and_threshold_on_validation"
MODEL_TEST_POLICY: Final[str] = "evaluate_selected_model_once_on_test"
MODEL_OBJECTIVE: Final[str] = "maximize_validation_final_equity"
MODEL_SIGNAL_COLUMN: Final[str] = "model_signal"
MODEL_SCORE_COLUMN: Final[str] = "model_score"
LINEAR_SCORE_STRATEGY: Final[str] = "linear_score"
COST_AWARE_LINEAR_SCORE_STRATEGY: Final[str] = "cost_aware_linear_score"
COST_AWARE_LINEAR_SCORE_SIGNAL_COLUMN: Final[str] = "cost_aware_linear_score_signal"
COST_AWARE_ESTIMATED_COST_BPS_COLUMN: Final[str] = "cost_aware_estimated_cost_bps"
COST_AWARE_PREDICTED_EDGE_BPS_COLUMN: Final[str] = "cost_aware_predicted_edge_bps"
COST_AWARE_COST_BLOCKED_COLUMN: Final[str] = "cost_aware_cost_blocked"
COST_AWARE_OBJECTIVE: Final[str] = "maximize_validation_net_pnl"

DEFAULT_LABEL_HORIZON: Final[str] = "500ms"
DEFAULT_SCORE_THRESHOLDS: Final[tuple[float, ...]] = (
    0.0,
    0.10,
    0.25,
    0.50,
    0.75,
    1.0,
    1.25,
    1.50,
    2.0,
)
DEFAULT_MAX_POSITION: Final[float] = 1.0
DEFAULT_COOLDOWN: Final[str] = "0ms"
DEFAULT_FIXED_BPS: Final[float] = 0.0
DEFAULT_SLIPPAGE_TICKS: Final[float] = 0.0
DEFAULT_TICK_SIZE: Final[float] = 0.01
DEFAULT_COST_AWARE_SCORE_THRESHOLDS: Final[tuple[float, ...]] = (
    1.5,
    2.0,
    2.5,
    3.0,
    3.5,
    4.0,
)
DEFAULT_COST_AWARE_QUANTILE_TOP_FRACTIONS: Final[tuple[float, ...]] = (
    0.10,
    0.05,
    0.02,
    0.01,
)
DEFAULT_COST_MULTIPLIER_GRID: Final[tuple[float, ...]] = (1.0, 1.5, 2.0, 2.5)
DEFAULT_COST_AWARE_COOLDOWN_SECONDS_GRID: Final[tuple[int, ...]] = (0, 1, 3, 5)
DEFAULT_MIN_HOLDING_SECONDS_GRID: Final[tuple[int, ...]] = (0, 1, 3, 5)


class ModelTrainingV1Error(ValueError):
    """Raised when model training v1 cannot be completed."""


@dataclass(frozen=True)
class ModelFeatureSet:
    """Named feature set candidate."""

    name: str
    columns: tuple[str, ...]


CORE_FEATURE_SET: Final[ModelFeatureSet] = ModelFeatureSet(
    name="qi_qr_flow_500ms",
    columns=(
        SIGNAL_QUOTE_IMBALANCE,
        SIGNAL_QUOTE_REVISION_BPS,
        "signed_flow_imbalance_500ms",
        "signed_flow_imbalance_50_trades",
    ),
)
MULTIWINDOW_FEATURE_SET: Final[ModelFeatureSet] = ModelFeatureSet(
    name="qi_qr_flow_multiwindow",
    columns=(
        SIGNAL_QUOTE_IMBALANCE,
        SIGNAL_QUOTE_REVISION_BPS,
        "signed_flow_imbalance_10_trades",
        "signed_flow_imbalance_50_trades",
        "signed_flow_imbalance_100_trades",
        "signed_flow_imbalance_100ms",
        "signed_flow_imbalance_500ms",
        "signed_flow_imbalance_1s",
    ),
)
DEFAULT_FEATURE_SETS: Final[tuple[ModelFeatureSet, ...]] = (
    CORE_FEATURE_SET,
    MULTIWINDOW_FEATURE_SET,
)


@dataclass(frozen=True)
class ModelTrainingV1Config:
    """Configuration for model training v1."""

    min_train_dates: int = 1
    label_horizon: str = DEFAULT_LABEL_HORIZON
    feature_sets: tuple[ModelFeatureSet, ...] = DEFAULT_FEATURE_SETS
    score_threshold_grid: tuple[float, ...] = DEFAULT_SCORE_THRESHOLDS
    max_position: float = DEFAULT_MAX_POSITION
    cooldown: str = DEFAULT_COOLDOWN
    max_trades_per_day: int | None = None
    fixed_bps: float = DEFAULT_FIXED_BPS
    slippage_ticks: float = DEFAULT_SLIPPAGE_TICKS
    tick_size: float = DEFAULT_TICK_SIZE
    flat_on_no_signal: bool = True
    eod_flat: bool = True
    min_train_observations: int = 100
    min_validation_orders: int = 1000


@dataclass(frozen=True)
class CostAwareLinearScoreConfig:
    """Configuration for the cost-aware linear-score strategy variant."""

    min_train_dates: int = 1
    label_horizon: str = DEFAULT_LABEL_HORIZON
    feature_sets: tuple[ModelFeatureSet, ...] = DEFAULT_FEATURE_SETS
    score_threshold_grid: tuple[float, ...] = DEFAULT_COST_AWARE_SCORE_THRESHOLDS
    include_quantile_thresholds: bool = True
    quantile_top_fractions: tuple[float, ...] = DEFAULT_COST_AWARE_QUANTILE_TOP_FRACTIONS
    cost_multiplier_grid: tuple[float, ...] = DEFAULT_COST_MULTIPLIER_GRID
    cooldown_seconds_grid: tuple[int, ...] = DEFAULT_COST_AWARE_COOLDOWN_SECONDS_GRID
    min_holding_seconds_grid: tuple[int, ...] = DEFAULT_MIN_HOLDING_SECONDS_GRID
    max_position: float = DEFAULT_MAX_POSITION
    max_trades_per_day: int | None = None
    fixed_bps: float = DEFAULT_FIXED_BPS
    slippage_ticks: float = DEFAULT_SLIPPAGE_TICKS
    tick_size: float = DEFAULT_TICK_SIZE
    flat_on_no_signal: bool = True
    eod_flat: bool = True
    min_train_observations: int = 100
    min_validation_trades: int = 1


@dataclass(frozen=True)
class TrainedLinearFeatureModel:
    """Train-date feature statistics and learned linear coefficients."""

    feature_set_name: str
    feature_columns: tuple[str, ...]
    label_column: str
    train_rows: int
    train_fit_rows: int
    feature_means: dict[str, float]
    feature_stds: dict[str, float]
    coefficients: dict[str, float]


@dataclass(frozen=True)
class ModelTrainingV1Diagnostics:
    """Diagnostics for model training v1."""

    input_signal_rows: int
    output_prediction_rows: int
    output_candidate_rows: int
    output_order_rows: int
    output_ledger_rows: int
    output_summary_rows: int
    trading_dates: tuple[str, ...]
    fold_count: int
    candidate_count_per_fold: int
    label_horizon: str
    label_column: str
    feature_sets: tuple[dict[str, object], ...]
    score_threshold_grid: tuple[float, ...]
    min_validation_orders: int
    split_policy: str
    model_training_policy: str
    model_selection_policy: str
    test_policy: str
    objective: str
    selected_candidates: tuple[dict[str, object], ...]
    model_training_implemented: bool = True
    validation_used_for_selection: bool = True
    test_used_for_selection: bool = False
    parameter_reselection_on_test: bool = False
    rule_based_signal_used_for_backtest: bool = False
    research_grade_model_claim: bool = False
    research_grade_backtest: bool = False


@dataclass(frozen=True)
class ModelTrainingV1Result:
    """Model training v1 outputs."""

    predictions: pd.DataFrame
    candidates: pd.DataFrame
    orders: pd.DataFrame
    ledger: pd.DataFrame
    summary: pd.DataFrame
    diagnostics: ModelTrainingV1Diagnostics


@dataclass(frozen=True)
class CostAwareLinearScoreDiagnostics:
    """Diagnostics for the cost-aware linear-score variant."""

    input_signal_rows: int
    output_prediction_rows: int
    output_candidate_rows: int
    output_order_rows: int
    output_ledger_rows: int
    output_summary_rows: int
    output_report_rows: int
    trading_dates: tuple[str, ...]
    fold_count: int
    candidate_count_per_fold: int
    label_horizon: str
    label_column: str
    feature_sets: tuple[dict[str, object], ...]
    score_threshold_grid: tuple[float, ...]
    include_quantile_thresholds: bool
    quantile_top_fractions: tuple[float, ...]
    cost_multiplier_grid: tuple[float, ...]
    cooldown_seconds_grid: tuple[int, ...]
    min_holding_seconds_grid: tuple[int, ...]
    min_validation_trades: int
    strategy_variant: str
    base_score_column: str
    signal_column: str
    objective: str
    selected_candidates: tuple[dict[str, object], ...]
    validation_used_for_selection: bool = True
    test_used_for_selection: bool = False
    parameter_reselection_on_test: bool = False
    reuses_linear_score_output: bool = True
    selection_uses_net_pnl: bool = True
    research_grade_model_claim: bool = False
    research_grade_backtest: bool = False


@dataclass(frozen=True)
class CostAwareLinearScoreResult:
    """Cost-aware linear-score variant outputs."""

    predictions: pd.DataFrame
    candidates: pd.DataFrame
    orders: pd.DataFrame
    ledger: pd.DataFrame
    summary: pd.DataFrame
    report: pd.DataFrame
    diagnostics: CostAwareLinearScoreDiagnostics


@dataclass(frozen=True)
class _PreparedCostAwareGroup:
    """Numeric arrays reused across cost-aware candidate evaluations."""

    time_ns: np.ndarray
    score: np.ndarray
    midquote: np.ndarray
    spread: np.ndarray
    valid_price: np.ndarray
    raw_cost_bps: np.ndarray


@dataclass(frozen=True)
class _DesiredCostAwareGroup:
    """Desired target signal arrays for one threshold and cost multiplier."""

    time_ns: np.ndarray
    midquote: np.ndarray
    spread: np.ndarray
    valid_price: np.ndarray
    desired: np.ndarray
    active_signal_rows: int


def run_model_training_v1(
    signal_rows: pd.DataFrame,
    *,
    config: ModelTrainingV1Config = ModelTrainingV1Config(),
) -> ModelTrainingV1Result:
    """Train, validate, and held-out-test model candidates."""

    _validate_config(config)
    _validate_inputs(signal_rows, config=config)

    rows = signal_rows.copy()
    rows[TRADING_DATE] = rows[TRADING_DATE].astype(str)
    trading_dates = tuple(sorted(rows[TRADING_DATE].dropna().unique()))
    if len(trading_dates) < config.min_train_dates + 2:
        raise ModelTrainingV1Error(
            "Model training v1 needs at least min_train_dates + validation + test dates."
        )

    label_column = _label_direction_column(config.label_horizon)
    candidate_rows: list[dict[str, object]] = []
    prediction_frames: list[pd.DataFrame] = []
    order_frames: list[pd.DataFrame] = []
    ledger_frames: list[pd.DataFrame] = []
    summary_frames: list[pd.DataFrame] = []
    selected_candidates: list[dict[str, object]] = []
    fold_number = 0

    for validation_index in range(config.min_train_dates, len(trading_dates) - 1):
        fold_number += 1
        fold_id = f"fold_{fold_number:03d}"
        train_dates = trading_dates[:validation_index]
        validation_date = trading_dates[validation_index]
        test_date = trading_dates[validation_index + 1]
        train_rows = rows.loc[rows[TRADING_DATE].isin(train_dates)]
        validation_rows = rows.loc[rows[TRADING_DATE] == validation_date]
        test_rows = rows.loc[rows[TRADING_DATE] == test_date]

        fold_candidates = []
        model_number = 0
        for feature_set in config.feature_sets:
            model_number += 1
            model = _train_linear_feature_model(
                train_rows,
                feature_set=feature_set,
                label_column=label_column,
                config=config,
            )
            for threshold in config.score_threshold_grid:
                candidate_id = f"candidate_{len(candidate_rows) + len(fold_candidates) + 1:04d}"
                validation_scored = _score_rows(validation_rows, model, threshold=threshold)
                validation_summary = _evaluate_scored_rows(validation_scored, config=config)
                fold_candidates.append(
                    {
                        "fold_id": fold_id,
                        "candidate_id": candidate_id,
                        "model_id": f"model_{model_number:04d}",
                        "feature_set": model.feature_set_name,
                        "feature_columns": ",".join(model.feature_columns),
                        "score_threshold": threshold,
                        "label_horizon": config.label_horizon,
                        "label_column": label_column,
                        "train_start_date": train_dates[0],
                        "train_end_date": train_dates[-1],
                        "validation_date": validation_date,
                        "test_date": test_date,
                        "train_rows": len(train_rows),
                        "train_fit_rows": model.train_fit_rows,
                        "validation_rows": len(validation_rows),
                        "test_rows": len(test_rows),
                        "max_position": config.max_position,
                        "cooldown": config.cooldown,
                        "max_trades_per_day": config.max_trades_per_day,
                        "fixed_bps": config.fixed_bps,
                        "slippage_ticks": config.slippage_ticks,
                        "tick_size": config.tick_size,
                        "coefficients": _format_float_mapping(model.coefficients),
                        **_prefix_metrics(validation_summary, prefix="validation"),
                        "validation_selection_eligible": (
                            int(validation_summary["order_rows"])
                            >= config.min_validation_orders
                        ),
                        "_model": model,
                    }
                )

        selected = _select_validation_candidate(fold_candidates)
        selected_model = selected["_model"]
        selected_threshold = float(selected["score_threshold"])
        test_scored = _score_rows(test_rows, selected_model, threshold=selected_threshold)
        test_summary = _evaluate_scored_rows(test_scored, config=config)
        test_accounting = run_target_position_accounting_v1(
            test_scored,
            config=_accounting_config(config),
        )
        metadata = _metadata_from_candidate(selected)

        selected_candidates.append(_selected_candidate_record(selected))
        prediction_frames.append(_prediction_frame(test_scored, metadata, config=config))
        order_frames.append(_annotate_frame(test_accounting.orders, metadata))
        ledger_frames.append(_annotate_frame(test_accounting.ledger, metadata))
        summary_frames.append(_annotate_frame(test_accounting.summary, metadata))

        for candidate_row in fold_candidates:
            is_selected = candidate_row["candidate_id"] == selected["candidate_id"]
            output_row = {
                key: value
                for key, value in candidate_row.items()
                if not key.startswith("_")
            }
            output_row.update(
                {
                    "selection_status": (
                        "selected_on_validation" if is_selected else "not_selected"
                    ),
                    "selected_for_test": is_selected,
                    "selection_objective": MODEL_OBJECTIVE,
                    "test_used_for_selection": False,
                }
            )
            if is_selected:
                output_row.update(_prefix_metrics(test_summary, prefix="test"))
            candidate_rows.append(output_row)

    predictions = _concat_or_empty(prediction_frames)
    candidates = pd.DataFrame(candidate_rows)
    orders = _concat_or_empty(order_frames)
    ledger = _concat_or_empty(ledger_frames)
    summary = _concat_or_empty(summary_frames)
    diagnostics = ModelTrainingV1Diagnostics(
        input_signal_rows=len(rows),
        output_prediction_rows=len(predictions),
        output_candidate_rows=len(candidates),
        output_order_rows=len(orders),
        output_ledger_rows=len(ledger),
        output_summary_rows=len(summary),
        trading_dates=trading_dates,
        fold_count=fold_number,
        candidate_count_per_fold=len(config.feature_sets) * len(config.score_threshold_grid),
        label_horizon=config.label_horizon,
        label_column=label_column,
        feature_sets=tuple(
            {"name": feature_set.name, "columns": feature_set.columns}
            for feature_set in config.feature_sets
        ),
        score_threshold_grid=config.score_threshold_grid,
        min_validation_orders=config.min_validation_orders,
        split_policy=MODEL_SPLIT_POLICY,
        model_training_policy=MODEL_TRAINING_POLICY,
        model_selection_policy=MODEL_SELECTION_POLICY,
        test_policy=MODEL_TEST_POLICY,
        objective=MODEL_OBJECTIVE,
        selected_candidates=tuple(selected_candidates),
    )
    return ModelTrainingV1Result(
        predictions=predictions,
        candidates=candidates,
        orders=orders,
        ledger=ledger,
        summary=summary,
        diagnostics=diagnostics,
    )


def run_cost_aware_linear_score_v1(
    signal_rows: pd.DataFrame,
    *,
    config: CostAwareLinearScoreConfig = CostAwareLinearScoreConfig(),
) -> CostAwareLinearScoreResult:
    """Train the linear score and evaluate the cost-aware strategy variant."""

    _validate_cost_aware_config(config)
    _validate_inputs(signal_rows, config=config)

    rows = signal_rows.copy()
    rows[TRADING_DATE] = rows[TRADING_DATE].astype(str)
    trading_dates = tuple(sorted(rows[TRADING_DATE].dropna().unique()))
    if len(trading_dates) < config.min_train_dates + 2:
        raise ModelTrainingV1Error(
            "Cost-aware linear score needs at least min_train_dates + validation + test dates."
        )

    label_column = _label_direction_column(config.label_horizon)
    candidate_rows: list[dict[str, object]] = []
    prediction_frames: list[pd.DataFrame] = []
    order_frames: list[pd.DataFrame] = []
    ledger_frames: list[pd.DataFrame] = []
    summary_frames: list[pd.DataFrame] = []
    selected_candidates: list[dict[str, object]] = []
    fold_number = 0

    for validation_index in range(config.min_train_dates, len(trading_dates) - 1):
        fold_number += 1
        fold_id = f"fold_{fold_number:03d}"
        train_dates = trading_dates[:validation_index]
        validation_date = trading_dates[validation_index]
        test_date = trading_dates[validation_index + 1]
        train_rows = rows.loc[rows[TRADING_DATE].isin(train_dates)]
        validation_rows = rows.loc[rows[TRADING_DATE] == validation_date]
        test_rows = rows.loc[rows[TRADING_DATE] == test_date]

        fold_candidates = []
        model_number = 0
        for feature_set in config.feature_sets:
            model_number += 1
            model = _train_linear_feature_model(
                train_rows,
                feature_set=feature_set,
                label_column=label_column,
                config=config,
            )
            validation_base = _score_rows(validation_rows, model, threshold=0.0)
            threshold_candidates = _cost_aware_threshold_candidates(
                validation_base[MODEL_SCORE_COLUMN],
                config=config,
            )
            prepared_validation_groups = _prepare_cost_aware_groups(
                validation_base,
                config=config,
            )
            for threshold_candidate in threshold_candidates:
                threshold_value = float(threshold_candidate["threshold_value"])
                for cost_multiplier in config.cost_multiplier_grid:
                    desired_validation_groups = _cost_aware_desired_groups(
                        prepared_validation_groups,
                        threshold=threshold_value,
                        cost_multiplier=float(cost_multiplier),
                    )
                    for cooldown_seconds, min_holding_seconds in product(
                        config.cooldown_seconds_grid,
                        config.min_holding_seconds_grid,
                    ):
                        candidate_id = (
                            f"candidate_{len(candidate_rows) + len(fold_candidates) + 1:04d}"
                        )
                        validation_summary = _evaluate_cost_aware_desired_groups(
                            desired_validation_groups,
                            input_rows=len(validation_base),
                            threshold=threshold_value,
                            cost_multiplier=float(cost_multiplier),
                            cooldown_seconds=int(cooldown_seconds),
                            min_holding_seconds=int(min_holding_seconds),
                            config=config,
                        )
                        fold_candidates.append(
                            {
                                "strategy_variant": COST_AWARE_LINEAR_SCORE_STRATEGY,
                                "fold_id": fold_id,
                                "candidate_id": candidate_id,
                                "model_id": f"model_{model_number:04d}",
                                "feature_set": model.feature_set_name,
                                "feature_columns": ",".join(model.feature_columns),
                                "score_threshold": float(
                                    threshold_candidate["threshold_value"]
                                ),
                                "threshold_type": threshold_candidate["threshold_type"],
                                "threshold_label": threshold_candidate["threshold_label"],
                                "quantile_top_fraction": threshold_candidate[
                                    "quantile_top_fraction"
                                ],
                                "cost_multiplier": float(cost_multiplier),
                                "cooldown_seconds": int(cooldown_seconds),
                                "min_holding_seconds": int(min_holding_seconds),
                                "label_horizon": config.label_horizon,
                                "label_column": label_column,
                                "train_start_date": train_dates[0],
                                "train_end_date": train_dates[-1],
                                "validation_date": validation_date,
                                "test_date": test_date,
                                "train_rows": len(train_rows),
                                "train_fit_rows": model.train_fit_rows,
                                "validation_rows": len(validation_rows),
                                "test_rows": len(test_rows),
                                "max_position": config.max_position,
                                "max_trades_per_day": config.max_trades_per_day,
                                "fixed_bps": config.fixed_bps,
                                "slippage_ticks": config.slippage_ticks,
                                "tick_size": config.tick_size,
                                "coefficients": _format_float_mapping(model.coefficients),
                                **_prefix_metrics(validation_summary, prefix="validation"),
                                "validation_selection_eligible": (
                                    int(validation_summary["num_trades"])
                                    >= config.min_validation_trades
                                ),
                                "_model": model,
                            }
                        )

        selected = _select_cost_aware_validation_candidate(fold_candidates)
        selected_model = selected["_model"]
        selected_threshold = float(selected["score_threshold"])
        test_base = _score_rows(test_rows, selected_model, threshold=0.0)
        test_scored = build_cost_aware_linear_score_signals(
            test_base,
            threshold=selected_threshold,
            cost_multiplier=float(selected["cost_multiplier"]),
            cooldown_seconds=int(selected["cooldown_seconds"]),
            min_holding_seconds=int(selected["min_holding_seconds"]),
            fixed_bps=config.fixed_bps,
            slippage_ticks=config.slippage_ticks,
            tick_size=config.tick_size,
        )
        test_summary = _evaluate_cost_aware_scored_rows(test_scored, config=config)
        test_accounting = run_target_position_accounting_v1(
            test_scored,
            config=_cost_aware_accounting_config(config),
        )
        metadata = _cost_aware_metadata_from_candidate(selected)

        selected_candidates.append(_cost_aware_selected_candidate_record(selected))
        prediction_frames.append(_cost_aware_prediction_frame(test_scored, metadata, config=config))
        order_frames.append(_annotate_frame(test_accounting.orders, metadata))
        ledger_frames.append(_annotate_frame(test_accounting.ledger, metadata))
        summary_frames.append(_annotate_frame(test_accounting.summary, metadata))

        for candidate_row in fold_candidates:
            is_selected = candidate_row["candidate_id"] == selected["candidate_id"]
            output_row = {
                key: value
                for key, value in candidate_row.items()
                if not key.startswith("_")
            }
            output_row.update(
                {
                    "selection_status": (
                        "selected_on_validation" if is_selected else "not_selected"
                    ),
                    "selected_for_test": is_selected,
                    "selection_objective": COST_AWARE_OBJECTIVE,
                    "test_used_for_selection": False,
                }
            )
            if is_selected:
                output_row.update(_prefix_metrics(test_summary, prefix="test"))
            candidate_rows.append(output_row)

    predictions = _concat_or_empty(prediction_frames)
    candidates = pd.DataFrame(candidate_rows)
    orders = _concat_or_empty(order_frames)
    ledger = _concat_or_empty(ledger_frames)
    summary = _concat_or_empty(summary_frames)
    report = _build_strategy_report(
        COST_AWARE_LINEAR_SCORE_STRATEGY,
        summary,
        selected_candidates=selected_candidates,
    )
    threshold_candidate_count = len(config.score_threshold_grid) + (
        len(config.quantile_top_fractions) if config.include_quantile_thresholds else 0
    )
    diagnostics = CostAwareLinearScoreDiagnostics(
        input_signal_rows=len(rows),
        output_prediction_rows=len(predictions),
        output_candidate_rows=len(candidates),
        output_order_rows=len(orders),
        output_ledger_rows=len(ledger),
        output_summary_rows=len(summary),
        output_report_rows=len(report),
        trading_dates=trading_dates,
        fold_count=fold_number,
        candidate_count_per_fold=(
            len(config.feature_sets)
            * threshold_candidate_count
            * len(config.cost_multiplier_grid)
            * len(config.cooldown_seconds_grid)
            * len(config.min_holding_seconds_grid)
        ),
        label_horizon=config.label_horizon,
        label_column=label_column,
        feature_sets=tuple(
            {"name": feature_set.name, "columns": feature_set.columns}
            for feature_set in config.feature_sets
        ),
        score_threshold_grid=config.score_threshold_grid,
        include_quantile_thresholds=config.include_quantile_thresholds,
        quantile_top_fractions=config.quantile_top_fractions,
        cost_multiplier_grid=config.cost_multiplier_grid,
        cooldown_seconds_grid=config.cooldown_seconds_grid,
        min_holding_seconds_grid=config.min_holding_seconds_grid,
        min_validation_trades=config.min_validation_trades,
        strategy_variant=COST_AWARE_LINEAR_SCORE_STRATEGY,
        base_score_column=MODEL_SCORE_COLUMN,
        signal_column=COST_AWARE_LINEAR_SCORE_SIGNAL_COLUMN,
        objective=COST_AWARE_OBJECTIVE,
        selected_candidates=tuple(selected_candidates),
    )
    return CostAwareLinearScoreResult(
        predictions=predictions,
        candidates=candidates,
        orders=orders,
        ledger=ledger,
        summary=summary,
        report=report,
        diagnostics=diagnostics,
    )


def build_cost_aware_linear_score_signals(
    rows: pd.DataFrame,
    *,
    threshold: float,
    cost_multiplier: float,
    cooldown_seconds: int,
    min_holding_seconds: int,
    fixed_bps: float = DEFAULT_FIXED_BPS,
    slippage_ticks: float = DEFAULT_SLIPPAGE_TICKS,
    tick_size: float = DEFAULT_TICK_SIZE,
) -> pd.DataFrame:
    """Convert existing linear-score output into cost-aware target signals."""

    _validate_cost_aware_signal_inputs(
        rows,
        threshold=threshold,
        cost_multiplier=cost_multiplier,
        cooldown_seconds=cooldown_seconds,
        min_holding_seconds=min_holding_seconds,
        fixed_bps=fixed_bps,
        slippage_ticks=slippage_ticks,
        tick_size=tick_size,
    )

    result = rows.copy()
    result["_cost_aware_order"] = range(len(result))
    score = pd.to_numeric(result[MODEL_SCORE_COLUMN], errors="coerce")
    midquote = pd.to_numeric(result["signal_midquote"], errors="coerce")
    spread = pd.to_numeric(result["signal_quoted_spread"], errors="coerce")
    raw_cost_bps = _estimated_round_trip_cost_bps(
        midquote=midquote,
        spread=spread,
        fixed_bps=fixed_bps,
        slippage_ticks=slippage_ticks,
        tick_size=tick_size,
    )
    estimated_cost_bps = raw_cost_bps * cost_multiplier
    predicted_edge_bps = score.abs()

    desired = pd.Series(0, index=result.index, dtype="int64")
    desired.loc[score.gt(threshold)] = 1
    desired.loc[score.lt(-threshold)] = -1
    cost_blocked = desired.ne(0) & (
        estimated_cost_bps.isna() | estimated_cost_bps.gt(predicted_edge_bps)
    )
    desired.loc[cost_blocked] = 0

    adjusted = pd.Series(0, index=result.index, dtype="int64")
    cooldown = pd.Timedelta(seconds=cooldown_seconds)
    min_holding = pd.Timedelta(seconds=min_holding_seconds)
    sorted_rows = result.sort_values(
        [SYMBOL, TRADING_DATE, EVENT_TIME, "_cost_aware_order"],
        kind="mergesort",
    )
    for _, group in sorted_rows.groupby([SYMBOL, TRADING_DATE], sort=False):
        current_position = 0
        last_change_time: pd.Timestamp | None = None
        for row_index, row in group.iterrows():
            event_time = pd.Timestamp(row[EVENT_TIME])
            desired_position = int(desired.loc[row_index])
            if desired_position == current_position:
                adjusted.loc[row_index] = current_position
                continue
            elapsed = (
                None
                if last_change_time is None
                else event_time - last_change_time
            )
            if current_position != 0 and elapsed is not None and elapsed < min_holding:
                adjusted.loc[row_index] = current_position
                continue
            if elapsed is not None and elapsed < cooldown:
                adjusted.loc[row_index] = current_position
                continue
            current_position = desired_position
            last_change_time = event_time
            adjusted.loc[row_index] = current_position

    result[COST_AWARE_PREDICTED_EDGE_BPS_COLUMN] = predicted_edge_bps
    result[COST_AWARE_ESTIMATED_COST_BPS_COLUMN] = estimated_cost_bps
    result[COST_AWARE_COST_BLOCKED_COLUMN] = cost_blocked
    result[COST_AWARE_LINEAR_SCORE_SIGNAL_COLUMN] = adjusted
    return result.drop(columns=["_cost_aware_order"])


def _train_linear_feature_model(
    train_rows: pd.DataFrame,
    *,
    feature_set: ModelFeatureSet,
    label_column: str,
    config: ModelTrainingV1Config,
) -> TrainedLinearFeatureModel:
    feature_frame = train_rows.loc[:, feature_set.columns].apply(
        pd.to_numeric,
        errors="coerce",
    )
    labels = pd.to_numeric(train_rows[label_column], errors="coerce")
    label_available = _bool_series(train_rows[_label_available_column(config.label_horizon)])
    fit_mask = feature_frame.notna().all(axis=1) & label_available & labels.notna()
    if int(fit_mask.sum()) < config.min_train_observations:
        raise ModelTrainingV1Error(
            f"Feature set {feature_set.name} has fewer than "
            f"{config.min_train_observations} train observations."
        )

    fit_features = feature_frame.loc[fit_mask]
    fit_labels = labels.loc[fit_mask]
    means = fit_features.mean()
    stds = fit_features.std(ddof=0).where(lambda values: values.gt(0), 1.0).fillna(1.0)
    z_features = (fit_features - means) / stds
    coefficients = z_features.mul(fit_labels, axis=0).mean()
    coefficient_norm = float(coefficients.abs().sum())
    if coefficient_norm > 0:
        coefficients = coefficients / coefficient_norm
    else:
        coefficients = coefficients * 0.0

    return TrainedLinearFeatureModel(
        feature_set_name=feature_set.name,
        feature_columns=feature_set.columns,
        label_column=label_column,
        train_rows=len(train_rows),
        train_fit_rows=int(fit_mask.sum()),
        feature_means={column: float(means[column]) for column in feature_set.columns},
        feature_stds={column: float(stds[column]) for column in feature_set.columns},
        coefficients={column: float(coefficients[column]) for column in feature_set.columns},
    )


def _score_rows(
    rows: pd.DataFrame,
    model: TrainedLinearFeatureModel,
    *,
    threshold: float,
) -> pd.DataFrame:
    result = rows.copy()
    feature_frame = result.loc[:, model.feature_columns].apply(pd.to_numeric, errors="coerce")
    valid_features = feature_frame.notna().all(axis=1)
    score = pd.Series(float("nan"), index=result.index, dtype="float64")
    if bool(valid_features.any()):
        means = pd.Series(model.feature_means)
        stds = pd.Series(model.feature_stds)
        coefficients = pd.Series(model.coefficients)
        z_features = (feature_frame.loc[valid_features] - means) / stds
        score.loc[valid_features] = z_features.mul(coefficients, axis=1).sum(axis=1)
    result[MODEL_SCORE_COLUMN] = score
    result[MODEL_SIGNAL_COLUMN] = _signal_from_score(score, threshold=threshold)
    return result


def _signal_from_score(score: pd.Series, *, threshold: float) -> pd.Series:
    signal = pd.Series(0, index=score.index, dtype="int64")
    signal.loc[score.gt(threshold)] = 1
    signal.loc[score.lt(-threshold)] = -1
    return signal


def _evaluate_scored_rows(
    rows: pd.DataFrame,
    *,
    config: ModelTrainingV1Config,
) -> dict[str, object]:
    result = run_target_position_accounting_v1(rows, config=_accounting_config(config))
    if result.summary.empty:
        raise ModelTrainingV1Error("Target-position accounting returned no summary.")
    return result.summary.iloc[0].to_dict()


def _accounting_config(config: ModelTrainingV1Config) -> TargetPositionAccountingConfig:
    return TargetPositionAccountingConfig(
        signal_column=MODEL_SIGNAL_COLUMN,
        max_position=config.max_position,
        cooldown=config.cooldown,
        max_trades_per_day=config.max_trades_per_day,
        fixed_bps=config.fixed_bps,
        slippage_ticks=config.slippage_ticks,
        tick_size=config.tick_size,
        flat_on_no_signal=config.flat_on_no_signal,
        eod_flat=config.eod_flat,
    )


def _cost_aware_accounting_config(
    config: CostAwareLinearScoreConfig,
) -> TargetPositionAccountingConfig:
    return TargetPositionAccountingConfig(
        signal_column=COST_AWARE_LINEAR_SCORE_SIGNAL_COLUMN,
        max_position=config.max_position,
        cooldown="0s",
        max_trades_per_day=config.max_trades_per_day,
        fixed_bps=config.fixed_bps,
        slippage_ticks=config.slippage_ticks,
        tick_size=config.tick_size,
        flat_on_no_signal=config.flat_on_no_signal,
        eod_flat=config.eod_flat,
    )


def _evaluate_cost_aware_scored_rows(
    rows: pd.DataFrame,
    *,
    config: CostAwareLinearScoreConfig,
) -> dict[str, object]:
    result = run_target_position_accounting_v1(
        rows,
        config=_cost_aware_accounting_config(config),
    )
    if result.summary.empty:
        raise ModelTrainingV1Error("Target-position accounting returned no summary.")
    return result.summary.iloc[0].to_dict()


def _evaluate_cost_aware_candidate_summary(
    rows: pd.DataFrame,
    *,
    threshold: float,
    cost_multiplier: float,
    cooldown_seconds: int,
    min_holding_seconds: int,
    config: CostAwareLinearScoreConfig,
) -> dict[str, object]:
    _validate_cost_aware_signal_inputs(
        rows,
        threshold=threshold,
        cost_multiplier=cost_multiplier,
        cooldown_seconds=cooldown_seconds,
        min_holding_seconds=min_holding_seconds,
        fixed_bps=config.fixed_bps,
        slippage_ticks=config.slippage_ticks,
        tick_size=config.tick_size,
    )
    prepared_groups = _prepare_cost_aware_groups(rows, config=config)
    desired_groups = _cost_aware_desired_groups(
        prepared_groups,
        threshold=threshold,
        cost_multiplier=cost_multiplier,
    )
    return _evaluate_cost_aware_desired_groups(
        desired_groups,
        input_rows=len(rows),
        threshold=threshold,
        cost_multiplier=cost_multiplier,
        cooldown_seconds=cooldown_seconds,
        min_holding_seconds=min_holding_seconds,
        config=config,
    )


def _prepare_cost_aware_groups(
    rows: pd.DataFrame,
    *,
    config: CostAwareLinearScoreConfig,
) -> tuple[_PreparedCostAwareGroup, ...]:
    work = rows.loc[
        :,
        [
            EVENT_TIME,
            SYMBOL,
            TRADING_DATE,
            MODEL_SCORE_COLUMN,
            "signal_midquote",
            "signal_quoted_spread",
        ],
    ].copy()
    work["_cost_aware_order"] = np.arange(len(work))
    work = work.sort_values(
        [SYMBOL, TRADING_DATE, EVENT_TIME, "_cost_aware_order"],
        kind="mergesort",
    )
    groups: list[_PreparedCostAwareGroup] = []
    for _, group in work.groupby([SYMBOL, TRADING_DATE], sort=False):
        times = pd.to_datetime(group[EVENT_TIME], format="mixed")
        time_ns = times.astype("int64").to_numpy()
        score = pd.to_numeric(group[MODEL_SCORE_COLUMN], errors="coerce").to_numpy()
        midquote = pd.to_numeric(group["signal_midquote"], errors="coerce").to_numpy()
        spread = pd.to_numeric(group["signal_quoted_spread"], errors="coerce").to_numpy()
        valid_price = (
            np.isfinite(midquote) & np.isfinite(spread) & (midquote > 0) & (spread >= 0)
        )
        raw_cost_bps = _estimated_round_trip_cost_bps(
            midquote=pd.Series(midquote),
            spread=pd.Series(spread),
            fixed_bps=config.fixed_bps,
            slippage_ticks=config.slippage_ticks,
            tick_size=config.tick_size,
        ).to_numpy()
        groups.append(
            _PreparedCostAwareGroup(
                time_ns=time_ns,
                score=score,
                midquote=midquote,
                spread=spread,
                valid_price=valid_price,
                raw_cost_bps=raw_cost_bps,
            )
        )
    return tuple(groups)


def _cost_aware_desired_groups(
    groups: tuple[_PreparedCostAwareGroup, ...],
    *,
    threshold: float,
    cost_multiplier: float,
) -> tuple[_DesiredCostAwareGroup, ...]:
    desired_groups: list[_DesiredCostAwareGroup] = []
    for group in groups:
        estimated_cost_bps = group.raw_cost_bps * cost_multiplier
        abs_score = np.abs(group.score)
        tradable_edge = np.isfinite(estimated_cost_bps) & np.isfinite(abs_score) & (
            estimated_cost_bps <= abs_score
        )
        desired = np.zeros(len(group.score), dtype=np.int8)
        desired[(group.score > threshold) & tradable_edge] = 1
        desired[(group.score < -threshold) & tradable_edge] = -1
        desired_groups.append(
            _DesiredCostAwareGroup(
                time_ns=group.time_ns,
                midquote=group.midquote,
                spread=group.spread,
                valid_price=group.valid_price,
                desired=desired,
                active_signal_rows=int(np.isin(desired, (1, -1)).sum()),
            )
        )
    return tuple(desired_groups)


def _evaluate_cost_aware_desired_groups(
    groups: tuple[_DesiredCostAwareGroup, ...],
    *,
    input_rows: int,
    threshold: float,
    cost_multiplier: float,
    cooldown_seconds: int,
    min_holding_seconds: int,
    config: CostAwareLinearScoreConfig,
) -> dict[str, object]:
    total = _empty_cost_aware_summary(
        input_rows=input_rows,
        simulation_id=_cost_aware_simulation_id(
            threshold=threshold,
            cost_multiplier=cost_multiplier,
            cooldown_seconds=cooldown_seconds,
            min_holding_seconds=min_holding_seconds,
            config=config,
        ),
        config=config,
        cooldown_seconds=cooldown_seconds,
    )
    for group in groups:
        _merge_cost_aware_group_summary(
            total,
            _evaluate_cost_aware_group(
                group,
                cooldown_seconds=cooldown_seconds,
                min_holding_seconds=min_holding_seconds,
                config=config,
            ),
        )

    num_trades = int(total["num_trades"])
    gross_pnl = float(total["gross_pnl"])
    cost = float(total["cost"])
    net_pnl = float(total["net_pnl"])
    total["order_rows"] = num_trades
    total["total_cost"] = cost
    total["final_equity"] = net_pnl
    total["gross_per_trade"] = _safe_metric_per_trade(gross_pnl, num_trades)
    total["cost_per_trade"] = _safe_metric_per_trade(cost, num_trades)
    total["net_per_trade"] = _safe_metric_per_trade(net_pnl, num_trades)
    return total


def _evaluate_cost_aware_group(
    group: _DesiredCostAwareGroup,
    *,
    cooldown_seconds: int,
    min_holding_seconds: int,
    config: CostAwareLinearScoreConfig,
) -> dict[str, object]:
    time_ns = group.time_ns
    midquote = group.midquote
    spread = group.spread
    valid_price = group.valid_price
    desired = group.desired
    if len(desired) == 0:
        return _empty_cost_aware_group_summary()

    segment_starts = np.flatnonzero(
        np.r_[True, desired[1:] != desired[:-1]]
    )
    segment_ends = np.r_[segment_starts[1:], len(desired)]
    cooldown_ns = pd.Timedelta(seconds=cooldown_seconds).value
    min_holding_ns = pd.Timedelta(seconds=min_holding_seconds).value

    position = 0.0
    cash = 0.0
    total_cost = 0.0
    total_turnover = 0.0
    order_count = 0
    skipped_missing_price_rows = 0
    skipped_max_trades_orders = 0
    target_change_candidate_rows = 0
    max_abs_position = 0.0
    abs_position_sum = 0.0
    last_change_ns: int | None = None

    for start, end in zip(segment_starts, segment_ends, strict=True):
        desired_position = float(desired[start]) * config.max_position
        if desired_position == 0.0 and not config.flat_on_no_signal:
            continue
        if desired_position == position:
            continue

        target_change_candidate_rows += 1
        earliest_ns = time_ns[start]
        if last_change_ns is not None:
            earliest_ns = max(earliest_ns, last_change_ns + cooldown_ns)
            if position != 0.0:
                earliest_ns = max(earliest_ns, last_change_ns + min_holding_ns)
        offset = int(np.searchsorted(time_ns[start:end], earliest_ns, side="left"))
        if start + offset >= end:
            continue
        fill_index = start + offset
        if not bool(valid_price[fill_index]):
            skipped_missing_price_rows += 1
            continue
        if (
            config.max_trades_per_day is not None
            and order_count >= config.max_trades_per_day
        ):
            skipped_max_trades_orders += 1
            continue

        event_cost, cash, position, turnover = _apply_cost_aware_order(
            previous_position=position,
            target_position=desired_position,
            fill_midquote=float(midquote[fill_index]),
            quoted_spread=float(spread[fill_index]),
            cash=cash,
            config=config,
        )
        total_cost += event_cost
        total_turnover += turnover
        order_count += 1
        last_change_ns = int(time_ns[fill_index])
        max_abs_position = max(max_abs_position, abs(position))
        abs_position_sum += abs(position)

    if config.eod_flat and position != 0.0:
        valid_indexes = np.flatnonzero(valid_price)
        if len(valid_indexes) == 0:
            skipped_missing_price_rows += 1
        else:
            fill_index = int(valid_indexes[-1])
            event_cost, cash, position, turnover = _apply_cost_aware_order(
                previous_position=position,
                target_position=0.0,
                fill_midquote=float(midquote[fill_index]),
                quoted_spread=float(spread[fill_index]),
                cash=cash,
                config=config,
            )
            total_cost += event_cost
            total_turnover += turnover
            order_count += 1
            max_abs_position = max(max_abs_position, abs(position))
            abs_position_sum += abs(position)

    net_pnl = cash
    gross_pnl = net_pnl + total_cost
    return {
        "active_signal_rows": group.active_signal_rows,
        "target_change_candidate_rows": target_change_candidate_rows,
        "num_trades": order_count,
        "num_position_changes": order_count,
        "gross_pnl": gross_pnl,
        "cost": total_cost,
        "net_pnl": net_pnl,
        "final_cash": cash,
        "final_position": position,
        "max_abs_position": max_abs_position,
        "mean_abs_position": None if order_count == 0 else abs_position_sum / order_count,
        "total_turnover": total_turnover,
        "skipped_missing_price_rows": skipped_missing_price_rows,
        "skipped_cooldown_orders": 0,
        "skipped_max_trades_orders": skipped_max_trades_orders,
    }


def _apply_cost_aware_order(
    *,
    previous_position: float,
    target_position: float,
    fill_midquote: float,
    quoted_spread: float,
    cash: float,
    config: CostAwareLinearScoreConfig,
) -> tuple[float, float, float, float]:
    order_quantity = target_position - previous_position
    event_cost = _cost_aware_order_cost(
        order_quantity=order_quantity,
        fill_midquote=fill_midquote,
        quoted_spread=quoted_spread,
        fixed_bps=config.fixed_bps,
        slippage_ticks=config.slippage_ticks,
        tick_size=config.tick_size,
    )
    cash_after = cash - order_quantity * fill_midquote - event_cost
    turnover = abs(order_quantity) * fill_midquote
    return event_cost, cash_after, target_position, turnover


def _cost_aware_order_cost(
    *,
    order_quantity: float,
    fill_midquote: float,
    quoted_spread: float,
    fixed_bps: float,
    slippage_ticks: float,
    tick_size: float,
) -> float:
    abs_quantity = abs(order_quantity)
    half_spread_cost = abs_quantity * quoted_spread / 2.0
    fixed_cost = abs_quantity * fill_midquote * fixed_bps / 10000.0
    slippage_cost = abs_quantity * slippage_ticks * tick_size
    return float(half_spread_cost + fixed_cost + slippage_cost)


def _empty_cost_aware_summary(
    *,
    input_rows: int,
    simulation_id: str,
    config: CostAwareLinearScoreConfig,
    cooldown_seconds: int,
) -> dict[str, object]:
    return {
        "simulation_id": simulation_id,
        "input_signal_rows": input_rows,
        "active_signal_rows": 0,
        "target_change_candidate_rows": 0,
        "order_rows": 0,
        "total_cost": 0.0,
        "gross_pnl": 0.0,
        "cost": 0.0,
        "net_pnl": 0.0,
        "num_trades": 0,
        "num_position_changes": 0,
        "gross_per_trade": None,
        "cost_per_trade": None,
        "net_per_trade": None,
        "final_position": 0.0,
        "final_cash": 0.0,
        "final_equity": 0.0,
        "max_abs_position": 0.0,
        "mean_abs_position": 0.0,
        "total_turnover": 0.0,
        "skipped_missing_price_rows": 0,
        "skipped_cooldown_orders": 0,
        "skipped_max_trades_orders": 0,
        "max_position": config.max_position,
        "cooldown": f"{cooldown_seconds}s",
        "max_trades_per_day": config.max_trades_per_day,
    }


def _empty_cost_aware_group_summary() -> dict[str, object]:
    return {
        "active_signal_rows": 0,
        "target_change_candidate_rows": 0,
        "num_trades": 0,
        "num_position_changes": 0,
        "gross_pnl": 0.0,
        "cost": 0.0,
        "net_pnl": 0.0,
        "final_cash": 0.0,
        "final_position": 0.0,
        "max_abs_position": 0.0,
        "mean_abs_position": 0.0,
        "total_turnover": 0.0,
        "skipped_missing_price_rows": 0,
        "skipped_cooldown_orders": 0,
        "skipped_max_trades_orders": 0,
    }


def _merge_cost_aware_group_summary(
    total: dict[str, object],
    group: dict[str, object],
) -> None:
    for column in (
        "active_signal_rows",
        "target_change_candidate_rows",
        "num_trades",
        "num_position_changes",
        "gross_pnl",
        "cost",
        "net_pnl",
        "final_cash",
        "total_turnover",
        "skipped_missing_price_rows",
        "skipped_cooldown_orders",
        "skipped_max_trades_orders",
    ):
        total[column] = total[column] + group[column]
    total["max_abs_position"] = max(total["max_abs_position"], group["max_abs_position"])
    if group["num_trades"]:
        total["mean_abs_position"] = group["mean_abs_position"]


def _cost_aware_simulation_id(
    *,
    threshold: float,
    cost_multiplier: float,
    cooldown_seconds: int,
    min_holding_seconds: int,
    config: CostAwareLinearScoreConfig,
) -> str:
    return (
        f"cost_aware_threshold_{threshold:g}_cost_mult_{cost_multiplier:g}"
        f"_cooldown_{cooldown_seconds}s_min_hold_{min_holding_seconds}s"
        f"_max_{config.max_position:g}"
    )


def _select_validation_candidate(candidate_rows: list[dict[str, object]]) -> dict[str, object]:
    if not candidate_rows:
        raise ModelTrainingV1Error("No model candidates are available for selection.")
    eligible_rows = [
        row for row in candidate_rows if bool(row.get("validation_selection_eligible"))
    ]
    if not eligible_rows:
        raise ModelTrainingV1Error(
            "No model candidates satisfy min_validation_orders."
        )
    return max(
        eligible_rows,
        key=lambda row: (
            _float_or_negative_inf(row.get("validation_final_equity")),
            -_float_or_inf(row.get("validation_total_cost")),
            -_float_or_inf(row.get("validation_order_rows")),
            -float(row["score_threshold"]),
        ),
    )


def _select_cost_aware_validation_candidate(
    candidate_rows: list[dict[str, object]],
) -> dict[str, object]:
    if not candidate_rows:
        raise ModelTrainingV1Error("No cost-aware candidates are available for selection.")
    eligible_rows = [
        row for row in candidate_rows if bool(row.get("validation_selection_eligible"))
    ]
    if not eligible_rows:
        raise ModelTrainingV1Error(
            "No cost-aware candidates satisfy min_validation_trades."
        )
    return max(
        eligible_rows,
        key=lambda row: (
            _float_or_negative_inf(row.get("validation_net_pnl")),
            -_float_or_inf(row.get("validation_cost")),
            -_float_or_inf(row.get("validation_num_position_changes")),
            _float_or_negative_inf(row.get("score_threshold")),
            -_float_or_inf(row.get("cost_multiplier")),
        ),
    )


def _metadata_from_candidate(candidate: dict[str, object]) -> dict[str, object]:
    fold_id = str(candidate["fold_id"])
    candidate_id = str(candidate["candidate_id"])
    test_date = str(candidate["test_date"])
    return {
        "model_backtest_id": f"{fold_id}_{candidate_id}_test_{test_date}",
        "fold_id": fold_id,
        "candidate_id": candidate_id,
        "feature_set": str(candidate["feature_set"]),
        "score_threshold": float(candidate["score_threshold"]),
        "train_start_date": str(candidate["train_start_date"]),
        "train_end_date": str(candidate["train_end_date"]),
        "validation_date": str(candidate["validation_date"]),
        "test_date": test_date,
        "parameter_source_policy": MODEL_SELECTION_POLICY,
    }


def _cost_aware_metadata_from_candidate(candidate: dict[str, object]) -> dict[str, object]:
    fold_id = str(candidate["fold_id"])
    candidate_id = str(candidate["candidate_id"])
    test_date = str(candidate["test_date"])
    threshold = float(candidate["score_threshold"])
    cost_multiplier = float(candidate["cost_multiplier"])
    return {
        "model_backtest_id": f"{fold_id}_{candidate_id}_test_{test_date}",
        "strategy_variant": COST_AWARE_LINEAR_SCORE_STRATEGY,
        "fold_id": fold_id,
        "candidate_id": candidate_id,
        "feature_set": str(candidate["feature_set"]),
        "score_threshold": threshold,
        "selected_threshold": threshold,
        "threshold_type": str(candidate["threshold_type"]),
        "threshold_label": str(candidate["threshold_label"]),
        "cost_multiplier": cost_multiplier,
        "selected_cost_multiplier": cost_multiplier,
        "cooldown_seconds": int(candidate["cooldown_seconds"]),
        "min_holding_seconds": int(candidate["min_holding_seconds"]),
        "train_start_date": str(candidate["train_start_date"]),
        "train_end_date": str(candidate["train_end_date"]),
        "validation_date": str(candidate["validation_date"]),
        "test_date": test_date,
        "parameter_source_policy": "cost_aware_validation_net_pnl_selection",
    }


def _selected_candidate_record(candidate: dict[str, object]) -> dict[str, object]:
    return {
        "fold_id": str(candidate["fold_id"]),
        "candidate_id": str(candidate["candidate_id"]),
        "feature_set": str(candidate["feature_set"]),
        "score_threshold": float(candidate["score_threshold"]),
        "validation_date": str(candidate["validation_date"]),
        "test_date": str(candidate["test_date"]),
        "validation_final_equity": float(candidate["validation_final_equity"]),
        "validation_total_cost": float(candidate["validation_total_cost"]),
        "validation_order_rows": int(candidate["validation_order_rows"]),
    }


def _cost_aware_selected_candidate_record(candidate: dict[str, object]) -> dict[str, object]:
    return {
        "fold_id": str(candidate["fold_id"]),
        "candidate_id": str(candidate["candidate_id"]),
        "strategy_variant": COST_AWARE_LINEAR_SCORE_STRATEGY,
        "feature_set": str(candidate["feature_set"]),
        "selected_threshold": float(candidate["score_threshold"]),
        "selected_threshold_type": str(candidate["threshold_type"]),
        "selected_threshold_label": str(candidate["threshold_label"]),
        "selected_cost_multiplier": float(candidate["cost_multiplier"]),
        "cooldown_seconds": int(candidate["cooldown_seconds"]),
        "min_holding_seconds": int(candidate["min_holding_seconds"]),
        "validation_date": str(candidate["validation_date"]),
        "test_date": str(candidate["test_date"]),
        "validation_net_pnl": float(candidate["validation_net_pnl"]),
        "validation_gross_pnl": float(candidate["validation_gross_pnl"]),
        "validation_cost": float(candidate["validation_cost"]),
        "validation_num_trades": int(candidate["validation_num_trades"]),
    }


def _prediction_frame(
    rows: pd.DataFrame,
    metadata: dict[str, object],
    *,
    config: ModelTrainingV1Config,
) -> pd.DataFrame:
    suffix = _horizon_suffix(config.label_horizon)
    prediction_columns = [
        EVENT_TIME,
        SYMBOL,
        TRADING_DATE,
        MODEL_SCORE_COLUMN,
        MODEL_SIGNAL_COLUMN,
        "signal_midquote",
        "signal_quoted_spread",
        f"future_midquote_direction_{suffix}",
        f"future_midquote_return_bps_{suffix}",
    ]
    available_columns = [column for column in prediction_columns if column in rows.columns]
    result = rows.loc[:, available_columns].copy()
    return _annotate_frame(result, metadata)


def _cost_aware_prediction_frame(
    rows: pd.DataFrame,
    metadata: dict[str, object],
    *,
    config: CostAwareLinearScoreConfig,
) -> pd.DataFrame:
    suffix = _horizon_suffix(config.label_horizon)
    prediction_columns = [
        EVENT_TIME,
        SYMBOL,
        TRADING_DATE,
        MODEL_SCORE_COLUMN,
        MODEL_SIGNAL_COLUMN,
        COST_AWARE_LINEAR_SCORE_SIGNAL_COLUMN,
        COST_AWARE_PREDICTED_EDGE_BPS_COLUMN,
        COST_AWARE_ESTIMATED_COST_BPS_COLUMN,
        COST_AWARE_COST_BLOCKED_COLUMN,
        "signal_midquote",
        "signal_quoted_spread",
        f"future_midquote_direction_{suffix}",
        f"future_midquote_return_bps_{suffix}",
    ]
    available_columns = [column for column in prediction_columns if column in rows.columns]
    result = rows.loc[:, available_columns].copy()
    return _annotate_frame(result, metadata)


def _annotate_frame(frame: pd.DataFrame, metadata: dict[str, object]) -> pd.DataFrame:
    result = frame.copy()
    for column, value in reversed(tuple(metadata.items())):
        result.insert(0, column, value)
    return result


def _concat_or_empty(frames: list[pd.DataFrame]) -> pd.DataFrame:
    non_empty = [frame for frame in frames if not frame.empty]
    if not non_empty:
        return pd.DataFrame()
    return pd.concat(non_empty, ignore_index=True)


def _build_strategy_report(
    strategy_variant: str,
    summary: pd.DataFrame,
    *,
    selected_candidates: list[dict[str, object]],
) -> pd.DataFrame:
    gross_pnl = _sum_summary_column(summary, "gross_pnl")
    cost = _sum_summary_column(summary, "cost")
    net_pnl = _sum_summary_column(summary, "net_pnl")
    num_trades = int(_sum_summary_column(summary, "num_trades"))
    num_position_changes = int(_sum_summary_column(summary, "num_position_changes"))
    return pd.DataFrame(
        [
            {
                "strategy": strategy_variant,
                "gross_pnl": gross_pnl,
                "cost": cost,
                "net_pnl": net_pnl,
                "num_trades": num_trades,
                "num_position_changes": num_position_changes,
                "gross_per_trade": _safe_metric_per_trade(gross_pnl, num_trades),
                "cost_per_trade": _safe_metric_per_trade(cost, num_trades),
                "net_per_trade": _safe_metric_per_trade(net_pnl, num_trades),
                "selected_threshold_by_fold": _selected_values_by_fold(
                    selected_candidates,
                    "selected_threshold",
                ),
                "selected_cost_multiplier_by_fold": _selected_values_by_fold(
                    selected_candidates,
                    "selected_cost_multiplier",
                ),
            }
        ]
    )


def _sum_summary_column(summary: pd.DataFrame, column: str) -> float:
    if summary.empty or column not in summary.columns:
        return 0.0
    return float(pd.to_numeric(summary[column], errors="coerce").fillna(0.0).sum())


def _selected_values_by_fold(
    selected_candidates: list[dict[str, object]],
    column: str,
) -> str:
    parts = []
    for candidate in selected_candidates:
        value = candidate.get(column)
        if value is None or pd.isna(value):
            continue
        parts.append(f"{candidate['fold_id']}={value}")
    return ";".join(parts)


def _safe_metric_per_trade(value: float, num_trades: int) -> float | None:
    if num_trades == 0:
        return None
    return value / num_trades


def _format_float_mapping(values: dict[str, float]) -> str:
    return ";".join(f"{key}={value:.10g}" for key, value in values.items())


def _prefix_metrics(metrics: dict[str, object], *, prefix: str) -> dict[str, object]:
    return {f"{prefix}_{key}": value for key, value in metrics.items()}


def _label_direction_column(horizon: str) -> str:
    return f"future_midquote_direction_{_horizon_suffix(horizon)}"


def _label_available_column(horizon: str) -> str:
    return f"label_available_{_horizon_suffix(horizon)}"


def _horizon_suffix(horizon: str) -> str:
    return horizon.lower().replace(" ", "").replace(".", "p")


def _bool_series(values: pd.Series) -> pd.Series:
    if pd.api.types.is_bool_dtype(values):
        return values.fillna(False)
    if pd.api.types.is_numeric_dtype(values):
        return pd.to_numeric(values, errors="coerce").fillna(0).ne(0)
    normalized = values.astype("string").str.strip().str.lower()
    return normalized.isin(("true", "1", "yes", "y"))


def _float_or_negative_inf(value: object) -> float:
    if value is None or pd.isna(value):
        return float("-inf")
    return float(value)


def _float_or_inf(value: object) -> float:
    if value is None or pd.isna(value):
        return float("inf")
    return float(value)


def _cost_aware_threshold_candidates(
    score: pd.Series,
    *,
    config: CostAwareLinearScoreConfig,
) -> tuple[dict[str, object], ...]:
    candidates: list[dict[str, object]] = []
    for threshold in config.score_threshold_grid:
        candidates.append(
            {
                "threshold_type": "absolute",
                "threshold_label": _format_threshold_value(threshold),
                "threshold_value": float(threshold),
                "quantile_top_fraction": None,
            }
        )
    if config.include_quantile_thresholds:
        abs_score = pd.to_numeric(score, errors="coerce").abs().dropna()
        for top_fraction in config.quantile_top_fractions:
            quantile = 1.0 - top_fraction
            threshold = (
                float(abs_score.quantile(quantile))
                if not abs_score.empty
                else float("inf")
            )
            candidates.append(
                {
                    "threshold_type": "quantile",
                    "threshold_label": f"top_{top_fraction:g}",
                    "threshold_value": threshold,
                    "quantile_top_fraction": float(top_fraction),
                }
            )
    return tuple(candidates)


def _estimated_round_trip_cost_bps(
    *,
    midquote: pd.Series,
    spread: pd.Series,
    fixed_bps: float,
    slippage_ticks: float,
    tick_size: float,
) -> pd.Series:
    valid_midquote = midquote.where(midquote.gt(0))
    half_spread_bps = (spread / 2.0) / valid_midquote * 10000.0
    slippage_bps = (slippage_ticks * tick_size) / valid_midquote * 10000.0
    one_way_cost_bps = half_spread_bps + fixed_bps + slippage_bps
    return 2.0 * one_way_cost_bps


def _format_threshold_value(value: float) -> str:
    return f"{value:g}"


def _validate_inputs(
    signal_rows: pd.DataFrame,
    *,
    config: ModelTrainingV1Config,
) -> None:
    label_column = _label_direction_column(config.label_horizon)
    label_available_column = _label_available_column(config.label_horizon)
    required = [
        EVENT_TIME,
        SYMBOL,
        TRADING_DATE,
        "signal_midquote",
        "signal_quoted_spread",
        label_column,
        label_available_column,
    ]
    for feature_set in config.feature_sets:
        required.extend(feature_set.columns)
    missing = [column for column in dict.fromkeys(required) if column not in signal_rows.columns]
    if missing:
        raise ModelTrainingV1Error(f"Signal rows are missing columns: {missing}")
    if not pd.api.types.is_datetime64_any_dtype(signal_rows[EVENT_TIME]):
        raise ModelTrainingV1Error("Signal rows must have datetime event_time values.")


def _validate_cost_aware_signal_inputs(
    rows: pd.DataFrame,
    *,
    threshold: float,
    cost_multiplier: float,
    cooldown_seconds: int,
    min_holding_seconds: int,
    fixed_bps: float,
    slippage_ticks: float,
    tick_size: float,
) -> None:
    required = [
        EVENT_TIME,
        SYMBOL,
        TRADING_DATE,
        MODEL_SCORE_COLUMN,
        "signal_midquote",
        "signal_quoted_spread",
    ]
    missing = [column for column in required if column not in rows.columns]
    if missing:
        raise ModelTrainingV1Error(f"Cost-aware rows are missing columns: {missing}")
    if not pd.api.types.is_datetime64_any_dtype(rows[EVENT_TIME]):
        raise ModelTrainingV1Error("Cost-aware rows must have datetime event_time values.")
    if threshold < 0:
        raise ModelTrainingV1Error("cost-aware threshold must be non-negative.")
    if cost_multiplier <= 0:
        raise ModelTrainingV1Error("cost_multiplier must be positive.")
    if cooldown_seconds < 0:
        raise ModelTrainingV1Error("cooldown_seconds must be non-negative.")
    if min_holding_seconds < 0:
        raise ModelTrainingV1Error("min_holding_seconds must be non-negative.")
    if fixed_bps < 0:
        raise ModelTrainingV1Error("fixed_bps must be non-negative.")
    if slippage_ticks < 0:
        raise ModelTrainingV1Error("slippage_ticks must be non-negative.")
    if tick_size <= 0:
        raise ModelTrainingV1Error("tick_size must be positive.")


def _validate_config(config: ModelTrainingV1Config) -> None:
    if config.min_train_dates < 1:
        raise ModelTrainingV1Error("min_train_dates must be at least 1.")
    if not config.feature_sets:
        raise ModelTrainingV1Error("At least one feature set is required.")
    if not config.score_threshold_grid:
        raise ModelTrainingV1Error("score_threshold_grid must not be empty.")
    if any(threshold < 0 for threshold in config.score_threshold_grid):
        raise ModelTrainingV1Error("score thresholds must be non-negative.")
    if config.max_position <= 0:
        raise ModelTrainingV1Error("max_position must be positive.")
    if pd.Timedelta(config.cooldown) < pd.Timedelta(0):
        raise ModelTrainingV1Error("cooldown must be non-negative.")
    if config.max_trades_per_day is not None and config.max_trades_per_day < 1:
        raise ModelTrainingV1Error("max_trades_per_day must be positive when set.")
    if config.fixed_bps < 0:
        raise ModelTrainingV1Error("fixed_bps must be non-negative.")
    if config.slippage_ticks < 0:
        raise ModelTrainingV1Error("slippage_ticks must be non-negative.")
    if config.tick_size <= 0:
        raise ModelTrainingV1Error("tick_size must be positive.")
    if config.min_train_observations < 1:
        raise ModelTrainingV1Error("min_train_observations must be positive.")
    if config.min_validation_orders < 1:
        raise ModelTrainingV1Error("min_validation_orders must be positive.")


def _validate_cost_aware_config(config: CostAwareLinearScoreConfig) -> None:
    if config.min_train_dates < 1:
        raise ModelTrainingV1Error("min_train_dates must be at least 1.")
    if not config.feature_sets:
        raise ModelTrainingV1Error("At least one feature set is required.")
    if not config.score_threshold_grid:
        raise ModelTrainingV1Error("cost-aware score_threshold_grid must not be empty.")
    if any(threshold < 0 for threshold in config.score_threshold_grid):
        raise ModelTrainingV1Error("cost-aware score thresholds must be non-negative.")
    if any(value <= 0 for value in config.cost_multiplier_grid):
        raise ModelTrainingV1Error("cost_multiplier_grid must contain positive values.")
    if any(value < 0 for value in config.cooldown_seconds_grid):
        raise ModelTrainingV1Error("cooldown_seconds_grid must contain non-negative values.")
    if any(value < 0 for value in config.min_holding_seconds_grid):
        raise ModelTrainingV1Error(
            "min_holding_seconds_grid must contain non-negative values."
        )
    if config.include_quantile_thresholds:
        if not config.quantile_top_fractions:
            raise ModelTrainingV1Error("quantile_top_fractions must not be empty.")
        if any(value <= 0 or value >= 1 for value in config.quantile_top_fractions):
            raise ModelTrainingV1Error(
                "quantile_top_fractions must contain values between 0 and 1."
            )
    if config.max_position <= 0:
        raise ModelTrainingV1Error("max_position must be positive.")
    if config.max_trades_per_day is not None and config.max_trades_per_day < 1:
        raise ModelTrainingV1Error("max_trades_per_day must be positive when set.")
    if config.fixed_bps < 0:
        raise ModelTrainingV1Error("fixed_bps must be non-negative.")
    if config.slippage_ticks < 0:
        raise ModelTrainingV1Error("slippage_ticks must be non-negative.")
    if config.tick_size <= 0:
        raise ModelTrainingV1Error("tick_size must be positive.")
    if config.min_train_observations < 1:
        raise ModelTrainingV1Error("min_train_observations must be positive.")
    if config.min_validation_trades < 1:
        raise ModelTrainingV1Error("min_validation_trades must be positive.")
