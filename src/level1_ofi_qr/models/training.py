"""AAPL prototype model training and held-out accounting backtest."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

import pandas as pd

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
