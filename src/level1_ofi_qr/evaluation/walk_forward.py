"""Walk-forward statistical evaluation for signal rows."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

import pandas as pd

from ..alignment import TRADING_DATE
from ..labeling import DEFAULT_LABEL_HORIZONS
from ..schema import EVENT_TIME, SYMBOL
from ..signals import SEQUENTIAL_GATE_SIGNAL

WALK_FORWARD_POLICY_NOTE: Final[str] = (
    "Walk-forward evaluation v1 evaluates precomputed signal rows against "
    "future midquote labels using expanding training-date context and next-date "
    "test folds. It does not optimize thresholds, fit models, apply transaction "
    "costs, or run backtests."
)
EVALUATION_POLICY: Final[str] = "expanding_train_dates_next_date_test"
SIGNAL_USAGE_POLICY: Final[str] = "evaluate_precomputed_signals_without_refitting"
LABEL_USAGE_POLICY: Final[str] = "labels_used_only_as_targets"


class WalkForwardEvaluationError(ValueError):
    """Raised when walk-forward evaluation cannot be completed."""


@dataclass(frozen=True)
class WalkForwardConfig:
    """Configuration for statistical walk-forward evaluation."""

    horizons: tuple[str, ...] = DEFAULT_LABEL_HORIZONS
    min_train_dates: int = 1
    signal_column: str = SEQUENTIAL_GATE_SIGNAL


@dataclass(frozen=True)
class WalkForwardDiagnostics:
    """Diagnostics for walk-forward evaluation v1."""

    input_signal_rows: int
    output_summary_rows: int
    horizons: tuple[str, ...]
    trading_dates: tuple[str, ...]
    fold_count: int
    min_train_dates: int
    signal_column: str
    evaluation_policy: str
    signal_usage_policy: str
    label_usage_policy: str
    threshold_optimization_implemented: bool = False
    model_training_implemented: bool = False
    cost_model_implemented: bool = False
    backtest_implemented: bool = False
    research_grade_strategy_result: bool = False


@dataclass(frozen=True)
class WalkForwardEvaluationResult:
    """Walk-forward summary table and diagnostics."""

    summary: pd.DataFrame
    diagnostics: WalkForwardDiagnostics


def evaluate_signals_walk_forward_v1(
    signal_rows: pd.DataFrame,
    *,
    config: WalkForwardConfig = WalkForwardConfig(),
) -> WalkForwardEvaluationResult:
    """Evaluate precomputed signals against future midquote direction labels."""

    _validate_inputs(signal_rows, config=config)
    dates = tuple(sorted(signal_rows[TRADING_DATE].dropna().astype(str).unique()))
    if len(dates) <= config.min_train_dates:
        raise WalkForwardEvaluationError(
            "Walk-forward evaluation needs more trading dates than min_train_dates."
        )

    rows: list[dict[str, object]] = []
    fold_ids: list[str] = []
    for fold_number, test_index in enumerate(range(config.min_train_dates, len(dates)), start=1):
        train_dates = dates[:test_index]
        test_date = dates[test_index]
        fold_id = f"fold_{fold_number:03d}"
        fold_ids.append(fold_id)
        train_rows = signal_rows.loc[signal_rows[TRADING_DATE].astype(str).isin(train_dates)]
        test_rows = signal_rows.loc[signal_rows[TRADING_DATE].astype(str) == test_date]
        for horizon in config.horizons:
            rows.append(
                _evaluate_subset(
                    test_rows,
                    horizon=horizon,
                    signal_column=config.signal_column,
                    fold_id=fold_id,
                    train_dates=train_dates,
                    test_date=test_date,
                    train_rows=len(train_rows),
                    test_rows=len(test_rows),
                )
            )

    evaluation_dates = dates[config.min_train_dates :]
    evaluation_rows = signal_rows.loc[signal_rows[TRADING_DATE].astype(str).isin(evaluation_dates)]
    train_dates = dates[: config.min_train_dates]
    for horizon in config.horizons:
        rows.append(
            _evaluate_subset(
                evaluation_rows,
                horizon=horizon,
                signal_column=config.signal_column,
                fold_id="ALL_EVALUATION_FOLDS",
                train_dates=train_dates,
                test_date="ALL_EVALUATION_DATES",
                train_rows=int((signal_rows[TRADING_DATE].astype(str).isin(train_dates)).sum()),
                test_rows=len(evaluation_rows),
            )
        )

    summary = pd.DataFrame(rows)
    diagnostics = WalkForwardDiagnostics(
        input_signal_rows=len(signal_rows),
        output_summary_rows=len(summary),
        horizons=config.horizons,
        trading_dates=dates,
        fold_count=len(fold_ids),
        min_train_dates=config.min_train_dates,
        signal_column=config.signal_column,
        evaluation_policy=EVALUATION_POLICY,
        signal_usage_policy=SIGNAL_USAGE_POLICY,
        label_usage_policy=LABEL_USAGE_POLICY,
    )
    return WalkForwardEvaluationResult(summary=summary, diagnostics=diagnostics)


def _validate_inputs(signal_rows: pd.DataFrame, *, config: WalkForwardConfig) -> None:
    base_columns = [EVENT_TIME, SYMBOL, TRADING_DATE, config.signal_column]
    missing = [column for column in base_columns if column not in signal_rows.columns]
    for horizon in config.horizons:
        suffix = _horizon_suffix(horizon)
        for column in (
            f"label_available_{suffix}",
            f"future_midquote_direction_{suffix}",
            f"future_midquote_return_bps_{suffix}",
        ):
            if column not in signal_rows.columns:
                missing.append(column)
    if missing:
        raise WalkForwardEvaluationError(f"Signal rows are missing columns: {missing}")
    if config.min_train_dates < 1:
        raise WalkForwardEvaluationError("min_train_dates must be at least 1.")
    if not config.horizons:
        raise WalkForwardEvaluationError("At least one evaluation horizon is required.")


def _evaluate_subset(
    rows: pd.DataFrame,
    *,
    horizon: str,
    signal_column: str,
    fold_id: str,
    train_dates: tuple[str, ...],
    test_date: str,
    train_rows: int,
    test_rows: int,
) -> dict[str, object]:
    suffix = _horizon_suffix(horizon)
    available_col = f"label_available_{suffix}"
    direction_col = f"future_midquote_direction_{suffix}"
    return_col = f"future_midquote_return_bps_{suffix}"

    label_available = rows[available_col].astype(bool) & rows[direction_col].notna()
    signals = pd.to_numeric(rows[signal_column], errors="coerce")
    directions = pd.to_numeric(rows[direction_col], errors="coerce")
    returns = pd.to_numeric(rows[return_col], errors="coerce")

    active_signal = label_available & signals.ne(0) & signals.notna()
    long_signal = active_signal & signals.eq(1)
    short_signal = active_signal & signals.eq(-1)
    correct = active_signal & signals.eq(directions)
    nonflat = active_signal & directions.ne(0)
    nonflat_correct = nonflat & signals.eq(directions)
    signed_aligned_return = signals.loc[active_signal] * returns.loc[active_signal]

    label_positive = label_available & directions.eq(1)
    label_flat = label_available & directions.eq(0)
    label_negative = label_available & directions.eq(-1)

    evaluated_signal_rows = int(active_signal.sum())
    nonflat_rows = int(nonflat.sum())
    long_rows = int(long_signal.sum())
    short_rows = int(short_signal.sum())

    return {
        "fold_id": fold_id,
        "horizon": horizon,
        "train_start_date": train_dates[0] if train_dates else None,
        "train_end_date": train_dates[-1] if train_dates else None,
        "test_date": test_date,
        "train_rows": train_rows,
        "test_rows": test_rows,
        "label_available_rows": int(label_available.sum()),
        "label_missing_rows": int((~label_available).sum()),
        "evaluated_signal_rows": evaluated_signal_rows,
        "long_signal_rows": long_rows,
        "short_signal_rows": short_rows,
        "signal_coverage": _safe_ratio(evaluated_signal_rows, int(label_available.sum())),
        "correct_signal_rows": int(correct.sum()),
        "signal_accuracy": _safe_ratio(int(correct.sum()), evaluated_signal_rows),
        "nonflat_evaluated_signal_rows": nonflat_rows,
        "nonflat_correct_signal_rows": int(nonflat_correct.sum()),
        "nonflat_signal_accuracy": _safe_ratio(int(nonflat_correct.sum()), nonflat_rows),
        "long_signal_accuracy": _safe_ratio(
            int((long_signal & directions.eq(1)).sum()),
            long_rows,
        ),
        "short_signal_accuracy": _safe_ratio(
            int((short_signal & directions.eq(-1)).sum()),
            short_rows,
        ),
        "label_positive_rows": int(label_positive.sum()),
        "label_flat_rows": int(label_flat.sum()),
        "label_negative_rows": int(label_negative.sum()),
        "mean_signal_aligned_return_bps": _series_mean(signed_aligned_return),
        "median_signal_aligned_return_bps": _series_median(signed_aligned_return),
        "mean_return_bps_when_long": _series_mean(returns.loc[long_signal]),
        "mean_return_bps_when_short": _series_mean(returns.loc[short_signal]),
    }


def _safe_ratio(numerator: int, denominator: int) -> float | None:
    if denominator == 0:
        return None
    return numerator / denominator


def _series_mean(values: pd.Series) -> float | None:
    non_null = values.dropna()
    if non_null.empty:
        return None
    return float(non_null.mean())


def _series_median(values: pd.Series) -> float | None:
    non_null = values.dropna()
    if non_null.empty:
        return None
    return float(non_null.median())


def _horizon_suffix(horizon: str) -> str:
    return horizon.lower().replace(" ", "").replace(".", "p")
