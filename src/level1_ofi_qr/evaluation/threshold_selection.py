"""Walk-forward threshold selection for sequential-gate signals."""

from __future__ import annotations

from dataclasses import dataclass
from itertools import product
from typing import Final

import pandas as pd

from ..alignment import TRADING_DATE
from ..labeling import DEFAULT_LABEL_HORIZONS
from ..schema import EVENT_TIME, SYMBOL
from ..signals import (
    DEFAULT_SIGNED_FLOW_COLUMN,
    SIGNAL_QUOTE_IMBALANCE,
    SIGNAL_QUOTE_REVISION_BPS,
)

THRESHOLD_SELECTION_POLICY_NOTE: Final[str] = (
    "Threshold selection v1 selects QI, signed-flow, and QR thresholds inside "
    "each walk-forward training window, then evaluates the selected thresholds "
    "on the next test date. It does not fit predictive models, apply "
    "transaction costs, or run backtests."
)
THRESHOLD_SELECTION_POLICY: Final[str] = "train_window_grid_search_next_date_test"
THRESHOLD_OBJECTIVE: Final[str] = "maximize_train_mean_signal_aligned_return_bps"
SIGNAL_CONSTRUCTION_POLICY: Final[str] = "recompute_sequential_gate_from_selected_thresholds"
LABEL_USAGE_POLICY: Final[str] = "train_labels_for_threshold_selection_test_labels_for_evaluation"
DEFAULT_QI_THRESHOLD_GRID: Final[tuple[float, ...]] = (0.0, 0.1, 0.25)
DEFAULT_SIGNED_FLOW_THRESHOLD_GRID: Final[tuple[float, ...]] = (0.0, 0.1, 0.25)
DEFAULT_QR_THRESHOLD_BPS_GRID: Final[tuple[float, ...]] = (0.0, 0.1, 0.25)


class ThresholdSelectionError(ValueError):
    """Raised when threshold selection cannot be completed."""


@dataclass(frozen=True)
class ThresholdSelectionConfig:
    """Configuration for threshold selection v1."""

    horizons: tuple[str, ...] = DEFAULT_LABEL_HORIZONS
    min_train_dates: int = 1
    signed_flow_column: str = DEFAULT_SIGNED_FLOW_COLUMN
    qi_threshold_grid: tuple[float, ...] = DEFAULT_QI_THRESHOLD_GRID
    signed_flow_threshold_grid: tuple[float, ...] = DEFAULT_SIGNED_FLOW_THRESHOLD_GRID
    qr_threshold_bps_grid: tuple[float, ...] = DEFAULT_QR_THRESHOLD_BPS_GRID
    min_train_signals: int = 100


@dataclass(frozen=True)
class ThresholdSelectionDiagnostics:
    """Diagnostics for threshold selection v1."""

    input_signal_rows: int
    output_summary_rows: int
    horizons: tuple[str, ...]
    trading_dates: tuple[str, ...]
    fold_count: int
    min_train_dates: int
    signed_flow_column: str
    qi_threshold_grid: tuple[float, ...]
    signed_flow_threshold_grid: tuple[float, ...]
    qr_threshold_bps_grid: tuple[float, ...]
    min_train_signals: int
    threshold_selection_policy: str
    threshold_objective: str
    signal_construction_policy: str
    label_usage_policy: str
    model_training_implemented: bool = False
    cost_model_implemented: bool = False
    backtest_implemented: bool = False
    research_grade_strategy_result: bool = False


@dataclass(frozen=True)
class ThresholdSelectionResult:
    """Threshold selection summary and diagnostics."""

    summary: pd.DataFrame
    diagnostics: ThresholdSelectionDiagnostics


def run_threshold_selection_v1(
    signal_rows: pd.DataFrame,
    *,
    config: ThresholdSelectionConfig = ThresholdSelectionConfig(),
) -> ThresholdSelectionResult:
    """Select thresholds on train dates and evaluate on next test dates."""

    _validate_inputs(signal_rows, config=config)
    dates = tuple(sorted(signal_rows[TRADING_DATE].dropna().astype(str).unique()))
    if len(dates) <= config.min_train_dates:
        raise ThresholdSelectionError(
            "Threshold selection needs more trading dates than min_train_dates."
        )

    threshold_grid = tuple(
        product(
            config.qi_threshold_grid,
            config.signed_flow_threshold_grid,
            config.qr_threshold_bps_grid,
        )
    )
    rows: list[dict[str, object]] = []
    for fold_number, test_index in enumerate(range(config.min_train_dates, len(dates)), start=1):
        train_dates = dates[:test_index]
        test_date = dates[test_index]
        train_rows = signal_rows.loc[signal_rows[TRADING_DATE].astype(str).isin(train_dates)]
        test_rows = signal_rows.loc[signal_rows[TRADING_DATE].astype(str) == test_date]
        for horizon in config.horizons:
            selected = _select_thresholds_for_horizon(
                train_rows,
                horizon=horizon,
                config=config,
                threshold_grid=threshold_grid,
            )
            test_metrics = _evaluate_thresholds(
                test_rows,
                horizon=horizon,
                signed_flow_column=config.signed_flow_column,
                qi_threshold=selected["selected_qi_threshold"],
                signed_flow_threshold=selected["selected_signed_flow_threshold"],
                qr_threshold_bps=selected["selected_qr_threshold_bps"],
            )
            rows.append(
                {
                    "fold_id": f"fold_{fold_number:03d}",
                    "horizon": horizon,
                    "train_start_date": train_dates[0],
                    "train_end_date": train_dates[-1],
                    "test_date": test_date,
                    "train_rows": len(train_rows),
                    "test_rows": len(test_rows),
                    **selected,
                    **_prefix_metrics(test_metrics, prefix="test"),
                }
            )

    summary = pd.DataFrame(rows)
    diagnostics = ThresholdSelectionDiagnostics(
        input_signal_rows=len(signal_rows),
        output_summary_rows=len(summary),
        horizons=config.horizons,
        trading_dates=dates,
        fold_count=len(dates) - config.min_train_dates,
        min_train_dates=config.min_train_dates,
        signed_flow_column=config.signed_flow_column,
        qi_threshold_grid=config.qi_threshold_grid,
        signed_flow_threshold_grid=config.signed_flow_threshold_grid,
        qr_threshold_bps_grid=config.qr_threshold_bps_grid,
        min_train_signals=config.min_train_signals,
        threshold_selection_policy=THRESHOLD_SELECTION_POLICY,
        threshold_objective=THRESHOLD_OBJECTIVE,
        signal_construction_policy=SIGNAL_CONSTRUCTION_POLICY,
        label_usage_policy=LABEL_USAGE_POLICY,
    )
    return ThresholdSelectionResult(summary=summary, diagnostics=diagnostics)


def _validate_inputs(signal_rows: pd.DataFrame, *, config: ThresholdSelectionConfig) -> None:
    missing = [
        column
        for column in (
            EVENT_TIME,
            SYMBOL,
            TRADING_DATE,
            SIGNAL_QUOTE_IMBALANCE,
            SIGNAL_QUOTE_REVISION_BPS,
            config.signed_flow_column,
        )
        if column not in signal_rows.columns
    ]
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
        raise ThresholdSelectionError(f"Signal rows are missing columns: {missing}")
    if config.min_train_dates < 1:
        raise ThresholdSelectionError("min_train_dates must be at least 1.")
    if config.min_train_signals < 1:
        raise ThresholdSelectionError("min_train_signals must be at least 1.")
    for grid_name, grid in (
        ("qi_threshold_grid", config.qi_threshold_grid),
        ("signed_flow_threshold_grid", config.signed_flow_threshold_grid),
        ("qr_threshold_bps_grid", config.qr_threshold_bps_grid),
    ):
        if not grid:
            raise ThresholdSelectionError(f"{grid_name} must not be empty.")
        if any(value < 0 for value in grid):
            raise ThresholdSelectionError(f"{grid_name} must contain non-negative values.")


def _select_thresholds_for_horizon(
    train_rows: pd.DataFrame,
    *,
    horizon: str,
    config: ThresholdSelectionConfig,
    threshold_grid: tuple[tuple[float, float, float], ...],
) -> dict[str, object]:
    candidates: list[dict[str, object]] = []
    for qi_threshold, signed_flow_threshold, qr_threshold_bps in threshold_grid:
        metrics = _evaluate_thresholds(
            train_rows,
            horizon=horizon,
            signed_flow_column=config.signed_flow_column,
            qi_threshold=qi_threshold,
            signed_flow_threshold=signed_flow_threshold,
            qr_threshold_bps=qr_threshold_bps,
        )
        candidate = {
            "selected_qi_threshold": qi_threshold,
            "selected_signed_flow_threshold": signed_flow_threshold,
            "selected_qr_threshold_bps": qr_threshold_bps,
            **_prefix_metrics(metrics, prefix="train"),
        }
        candidates.append(candidate)

    eligible = [
        candidate
        for candidate in candidates
        if candidate["train_evaluated_signal_rows"] >= config.min_train_signals
        and candidate["train_mean_signal_aligned_return_bps"] is not None
    ]
    if not eligible:
        selected = candidates[0]
        return {
            **selected,
            "selection_status": "fallback_first_grid_point_no_candidate_meets_min_train_signals",
        }

    selected = max(
        eligible,
        key=lambda candidate: (
            candidate["train_mean_signal_aligned_return_bps"],
            candidate["train_signal_accuracy"] if candidate["train_signal_accuracy"] is not None else -1,
            candidate["train_evaluated_signal_rows"],
            candidate["selected_qi_threshold"]
            + candidate["selected_signed_flow_threshold"]
            + candidate["selected_qr_threshold_bps"],
        ),
    )
    return {**selected, "selection_status": "selected_from_train_window"}


def _evaluate_thresholds(
    rows: pd.DataFrame,
    *,
    horizon: str,
    signed_flow_column: str,
    qi_threshold: float,
    signed_flow_threshold: float,
    qr_threshold_bps: float,
) -> dict[str, object]:
    suffix = _horizon_suffix(horizon)
    label_available = rows[f"label_available_{suffix}"].astype(bool)
    direction = pd.to_numeric(rows[f"future_midquote_direction_{suffix}"], errors="coerce")
    returns = pd.to_numeric(rows[f"future_midquote_return_bps_{suffix}"], errors="coerce")
    qi = pd.to_numeric(rows[SIGNAL_QUOTE_IMBALANCE], errors="coerce")
    signed_flow = pd.to_numeric(rows[signed_flow_column], errors="coerce")
    qr = pd.to_numeric(rows[SIGNAL_QUOTE_REVISION_BPS], errors="coerce")

    available = label_available & direction.notna() & qi.notna() & signed_flow.notna() & qr.notna()
    long_signal = (
        available
        & (qi > qi_threshold)
        & (signed_flow > signed_flow_threshold)
        & (qr > qr_threshold_bps)
    )
    short_signal = (
        available
        & (qi < -qi_threshold)
        & (signed_flow < -signed_flow_threshold)
        & (qr < -qr_threshold_bps)
    )
    signal = pd.Series(0, index=rows.index, dtype="int64")
    signal.loc[long_signal] = 1
    signal.loc[short_signal] = -1
    active = signal.ne(0)
    correct = active & signal.eq(direction)
    nonflat = active & direction.ne(0)
    nonflat_correct = nonflat & signal.eq(direction)
    aligned_returns = signal.loc[active] * returns.loc[active]

    evaluated = int(active.sum())
    nonflat_evaluated = int(nonflat.sum())
    long_rows = int(long_signal.sum())
    short_rows = int(short_signal.sum())
    available_rows = int(available.sum())
    return {
        "label_available_rows": available_rows,
        "evaluated_signal_rows": evaluated,
        "long_signal_rows": long_rows,
        "short_signal_rows": short_rows,
        "signal_coverage": _safe_ratio(evaluated, available_rows),
        "correct_signal_rows": int(correct.sum()),
        "signal_accuracy": _safe_ratio(int(correct.sum()), evaluated),
        "nonflat_evaluated_signal_rows": nonflat_evaluated,
        "nonflat_correct_signal_rows": int(nonflat_correct.sum()),
        "nonflat_signal_accuracy": _safe_ratio(
            int(nonflat_correct.sum()),
            nonflat_evaluated,
        ),
        "mean_signal_aligned_return_bps": _series_mean(aligned_returns),
        "median_signal_aligned_return_bps": _series_median(aligned_returns),
    }


def _prefix_metrics(metrics: dict[str, object], *, prefix: str) -> dict[str, object]:
    return {f"{prefix}_{key}": value for key, value in metrics.items()}


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
