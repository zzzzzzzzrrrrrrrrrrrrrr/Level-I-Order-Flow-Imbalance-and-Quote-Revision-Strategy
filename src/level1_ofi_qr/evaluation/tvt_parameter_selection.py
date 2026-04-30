"""Train-validation-test parameter selection for target-position accounting."""

from __future__ import annotations

from dataclasses import dataclass
from itertools import product
from typing import Final

import pandas as pd

from ..alignment import TRADING_DATE
from ..execution import TargetPositionAccountingConfig, run_target_position_accounting_v1
from ..schema import EVENT_TIME, SYMBOL
from ..signals import SEQUENTIAL_GATE_SIGNAL
from .parameter_sensitivity import (
    DEFAULT_COOLDOWN_GRID,
    DEFAULT_FIXED_BPS_GRID,
    DEFAULT_MAX_POSITION_GRID,
    DEFAULT_MAX_TRADES_PER_DAY_GRID,
    DEFAULT_SLIPPAGE_TICKS_GRID,
    DEFAULT_TICK_SIZE,
)

TVT_SELECTION_POLICY_NOTE: Final[str] = (
    "Train-validation-test parameter selection v1 records train dates, selects "
    "target-position accounting parameters on the validation date, and evaluates "
    "the frozen selected candidate on the next test date. It does not train a "
    "predictive model or use the test date for selection."
)
TVT_SPLIT_POLICY: Final[str] = "expanding_train_next_validation_next_test"
TVT_SELECTION_POLICY: Final[str] = "select_on_validation_evaluate_once_on_test"
TVT_OBJECTIVE: Final[str] = "maximize_validation_final_equity"
TVT_TIE_BREAK_POLICY: Final[str] = "higher_validation_equity_lower_cost_lower_order_count"


class TVTParameterSelectionError(ValueError):
    """Raised when TVT parameter selection cannot be completed."""


@dataclass(frozen=True)
class TVTParameterSelectionConfig:
    """Configuration for train-validation-test parameter selection v1."""

    signal_column: str = SEQUENTIAL_GATE_SIGNAL
    min_train_dates: int = 1
    max_position_grid: tuple[float, ...] = DEFAULT_MAX_POSITION_GRID
    cooldown_grid: tuple[str, ...] = DEFAULT_COOLDOWN_GRID
    max_trades_per_day_grid: tuple[int | None, ...] = DEFAULT_MAX_TRADES_PER_DAY_GRID
    fixed_bps_grid: tuple[float, ...] = DEFAULT_FIXED_BPS_GRID
    slippage_ticks_grid: tuple[float, ...] = DEFAULT_SLIPPAGE_TICKS_GRID
    tick_size: float = DEFAULT_TICK_SIZE
    flat_on_no_signal: bool = True
    eod_flat: bool = True


@dataclass(frozen=True)
class TVTParameterSelectionDiagnostics:
    """Diagnostics for TVT parameter selection v1."""

    input_signal_rows: int
    output_summary_rows: int
    trading_dates: tuple[str, ...]
    fold_count: int
    candidate_count: int
    signal_column: str
    min_train_dates: int
    max_position_grid: tuple[float, ...]
    cooldown_grid: tuple[str, ...]
    max_trades_per_day_grid: tuple[int | None, ...]
    fixed_bps_grid: tuple[float, ...]
    slippage_ticks_grid: tuple[float, ...]
    tick_size: float
    split_policy: str
    selection_policy: str
    objective: str
    tie_break_policy: str
    model_training_implemented: bool = False
    test_used_for_selection: bool = False
    final_hyperparameter_claim: bool = False
    research_grade_backtest: bool = False


@dataclass(frozen=True)
class TVTParameterSelectionResult:
    """TVT selection summary and diagnostics."""

    summary: pd.DataFrame
    diagnostics: TVTParameterSelectionDiagnostics


def run_tvt_parameter_selection_v1(
    signal_rows: pd.DataFrame,
    *,
    config: TVTParameterSelectionConfig = TVTParameterSelectionConfig(),
) -> TVTParameterSelectionResult:
    """Select parameters on validation dates and evaluate selected candidates on test dates."""

    _validate_inputs(signal_rows, config=config)
    _validate_config(config)

    rows = signal_rows.copy()
    rows[TRADING_DATE] = rows[TRADING_DATE].astype(str)
    trading_dates = tuple(sorted(rows[TRADING_DATE].dropna().unique()))
    if len(trading_dates) < config.min_train_dates + 2:
        raise TVTParameterSelectionError(
            "TVT selection needs at least min_train_dates + validation + test dates."
        )

    candidates = _candidate_configs(config)
    output_rows: list[dict[str, object]] = []
    fold_number = 0
    for validation_index in range(config.min_train_dates, len(trading_dates) - 1):
        fold_number += 1
        train_dates = trading_dates[:validation_index]
        validation_date = trading_dates[validation_index]
        test_date = trading_dates[validation_index + 1]
        validation_rows = rows.loc[rows[TRADING_DATE] == validation_date]
        test_rows = rows.loc[rows[TRADING_DATE] == test_date]
        candidate_rows = []
        for candidate_number, candidate in enumerate(candidates, start=1):
            candidate_id = f"candidate_{candidate_number:04d}"
            validation_summary = _evaluate_candidate(validation_rows, candidate)
            candidate_row = {
                "fold_id": f"fold_{fold_number:03d}",
                "candidate_id": candidate_id,
                "train_start_date": train_dates[0],
                "train_end_date": train_dates[-1],
                "validation_date": validation_date,
                "test_date": test_date,
                "train_rows": int(rows[TRADING_DATE].isin(train_dates).sum()),
                "validation_rows": len(validation_rows),
                "test_rows": len(test_rows),
                "max_position": candidate.max_position,
                "cooldown": candidate.cooldown,
                "max_trades_per_day": candidate.max_trades_per_day,
                "fixed_bps": candidate.fixed_bps,
                "slippage_ticks": candidate.slippage_ticks,
                "tick_size": candidate.tick_size,
                **_prefix_metrics(validation_summary, prefix="validation"),
            }
            candidate_rows.append(candidate_row)

        selected = _select_validation_candidate(candidate_rows)
        selected_candidate = candidates[int(selected["candidate_id"].split("_")[1]) - 1]
        test_summary = _evaluate_candidate(test_rows, selected_candidate)
        for candidate_row in candidate_rows:
            is_selected = candidate_row["candidate_id"] == selected["candidate_id"]
            row = {
                **candidate_row,
                "selection_status": "selected_on_validation" if is_selected else "not_selected",
                "selected_for_test": is_selected,
                "selection_objective": TVT_OBJECTIVE,
                "test_used_for_selection": False,
            }
            if is_selected:
                row.update(_prefix_metrics(test_summary, prefix="test"))
            output_rows.append(row)

    summary = pd.DataFrame(output_rows)
    diagnostics = TVTParameterSelectionDiagnostics(
        input_signal_rows=len(rows),
        output_summary_rows=len(summary),
        trading_dates=trading_dates,
        fold_count=fold_number,
        candidate_count=len(candidates),
        signal_column=config.signal_column,
        min_train_dates=config.min_train_dates,
        max_position_grid=config.max_position_grid,
        cooldown_grid=config.cooldown_grid,
        max_trades_per_day_grid=config.max_trades_per_day_grid,
        fixed_bps_grid=config.fixed_bps_grid,
        slippage_ticks_grid=config.slippage_ticks_grid,
        tick_size=config.tick_size,
        split_policy=TVT_SPLIT_POLICY,
        selection_policy=TVT_SELECTION_POLICY,
        objective=TVT_OBJECTIVE,
        tie_break_policy=TVT_TIE_BREAK_POLICY,
    )
    return TVTParameterSelectionResult(summary=summary, diagnostics=diagnostics)


def _evaluate_candidate(
    rows: pd.DataFrame,
    candidate: TargetPositionAccountingConfig,
) -> dict[str, object]:
    result = run_target_position_accounting_v1(rows, config=candidate)
    if result.summary.empty:
        raise TVTParameterSelectionError("Target-position accounting returned no summary.")
    return result.summary.iloc[0].to_dict()


def _select_validation_candidate(candidate_rows: list[dict[str, object]]) -> dict[str, object]:
    if not candidate_rows:
        raise TVTParameterSelectionError("No candidate rows available for selection.")
    return max(
        candidate_rows,
        key=lambda row: (
            _float_or_negative_inf(row.get("validation_final_equity")),
            -_float_or_inf(row.get("validation_total_cost")),
            -_float_or_inf(row.get("validation_order_rows")),
        ),
    )


def _candidate_configs(
    config: TVTParameterSelectionConfig,
) -> tuple[TargetPositionAccountingConfig, ...]:
    candidates = []
    for max_position, cooldown, max_trades_per_day, fixed_bps, slippage_ticks in product(
        config.max_position_grid,
        config.cooldown_grid,
        config.max_trades_per_day_grid,
        config.fixed_bps_grid,
        config.slippage_ticks_grid,
    ):
        candidates.append(
            TargetPositionAccountingConfig(
                signal_column=config.signal_column,
                max_position=max_position,
                cooldown=cooldown,
                max_trades_per_day=max_trades_per_day,
                fixed_bps=fixed_bps,
                slippage_ticks=slippage_ticks,
                tick_size=config.tick_size,
                flat_on_no_signal=config.flat_on_no_signal,
                eod_flat=config.eod_flat,
            )
        )
    return tuple(candidates)


def _prefix_metrics(metrics: dict[str, object], *, prefix: str) -> dict[str, object]:
    return {f"{prefix}_{key}": value for key, value in metrics.items()}


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
    config: TVTParameterSelectionConfig,
) -> None:
    required = [EVENT_TIME, SYMBOL, TRADING_DATE, config.signal_column]
    missing = [column for column in required if column not in signal_rows.columns]
    if missing:
        raise TVTParameterSelectionError(f"Signal rows are missing columns: {missing}")
    if not pd.api.types.is_datetime64_any_dtype(signal_rows[EVENT_TIME]):
        raise TVTParameterSelectionError("Signal rows must have datetime event_time values.")


def _validate_config(config: TVTParameterSelectionConfig) -> None:
    if config.min_train_dates < 1:
        raise TVTParameterSelectionError("min_train_dates must be at least 1.")
    for grid_name, grid in (
        ("max_position_grid", config.max_position_grid),
        ("cooldown_grid", config.cooldown_grid),
        ("max_trades_per_day_grid", config.max_trades_per_day_grid),
        ("fixed_bps_grid", config.fixed_bps_grid),
        ("slippage_ticks_grid", config.slippage_ticks_grid),
    ):
        if not grid:
            raise TVTParameterSelectionError(f"{grid_name} must not be empty.")
    if any(value <= 0 for value in config.max_position_grid):
        raise TVTParameterSelectionError("max_position_grid must contain positive values.")
    if any(pd.Timedelta(value) < pd.Timedelta(0) for value in config.cooldown_grid):
        raise TVTParameterSelectionError("cooldown_grid must contain non-negative durations.")
    if any(value is not None and value < 1 for value in config.max_trades_per_day_grid):
        raise TVTParameterSelectionError(
            "max_trades_per_day_grid must contain positive integers or None."
        )
    if any(value < 0 for value in config.fixed_bps_grid):
        raise TVTParameterSelectionError("fixed_bps_grid must contain non-negative values.")
    if any(value < 0 for value in config.slippage_ticks_grid):
        raise TVTParameterSelectionError(
            "slippage_ticks_grid must contain non-negative values."
        )
    if config.tick_size <= 0:
        raise TVTParameterSelectionError("tick_size must be positive.")
