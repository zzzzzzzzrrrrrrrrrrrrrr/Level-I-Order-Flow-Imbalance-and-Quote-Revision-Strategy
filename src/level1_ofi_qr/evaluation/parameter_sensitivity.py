"""Parameter sensitivity runner for target-position accounting."""

from __future__ import annotations

from dataclasses import dataclass
from itertools import product
from typing import Final

import pandas as pd

from ..execution import (
    TargetPositionAccountingConfig,
    run_target_position_accounting_v1,
)
from ..signals import SEQUENTIAL_GATE_SIGNAL

PARAMETER_SENSITIVITY_POLICY_NOTE: Final[str] = (
    "Parameter sensitivity v1 evaluates every configured target-position "
    "accounting parameter candidate and reports all results. It does not choose "
    "final hyperparameters, fit models, or make profitability claims."
)
PARAMETER_SENSITIVITY_POLICY: Final[str] = "exhaustive_grid_report_no_selection_v1"
PARAMETER_SELECTION_POLICY: Final[str] = "no_parameter_selection"

DEFAULT_MAX_POSITION_GRID: Final[tuple[float, ...]] = (1.0,)
DEFAULT_COOLDOWN_GRID: Final[tuple[str, ...]] = ("0ms",)
DEFAULT_MAX_TRADES_PER_DAY_GRID: Final[tuple[int | None, ...]] = (None,)
DEFAULT_FIXED_BPS_GRID: Final[tuple[float, ...]] = (0.0, 1.0)
DEFAULT_SLIPPAGE_TICKS_GRID: Final[tuple[float, ...]] = (0.0,)
DEFAULT_TICK_SIZE: Final[float] = 0.01


class ParameterSensitivityError(ValueError):
    """Raised when parameter sensitivity cannot be computed."""


@dataclass(frozen=True)
class ParameterSensitivityConfig:
    """Configuration for parameter sensitivity v1."""

    signal_column: str = SEQUENTIAL_GATE_SIGNAL
    max_position_grid: tuple[float, ...] = DEFAULT_MAX_POSITION_GRID
    cooldown_grid: tuple[str, ...] = DEFAULT_COOLDOWN_GRID
    max_trades_per_day_grid: tuple[int | None, ...] = DEFAULT_MAX_TRADES_PER_DAY_GRID
    fixed_bps_grid: tuple[float, ...] = DEFAULT_FIXED_BPS_GRID
    slippage_ticks_grid: tuple[float, ...] = DEFAULT_SLIPPAGE_TICKS_GRID
    tick_size: float = DEFAULT_TICK_SIZE
    flat_on_no_signal: bool = True
    eod_flat: bool = True


@dataclass(frozen=True)
class ParameterSensitivityDiagnostics:
    """Diagnostics for parameter sensitivity v1."""

    input_signal_rows: int
    output_summary_rows: int
    candidate_count: int
    signal_column: str
    max_position_grid: tuple[float, ...]
    cooldown_grid: tuple[str, ...]
    max_trades_per_day_grid: tuple[int | None, ...]
    fixed_bps_grid: tuple[float, ...]
    slippage_ticks_grid: tuple[float, ...]
    tick_size: float
    flat_on_no_signal: bool
    eod_flat: bool
    parameter_sensitivity_policy: str
    parameter_selection_policy: str
    train_window_selection_implemented: bool = False
    model_training_implemented: bool = False
    official_fee_model_implemented: bool = False
    passive_execution_implemented: bool = False
    research_grade_backtest: bool = False


@dataclass(frozen=True)
class ParameterSensitivityResult:
    """Parameter sensitivity summary and diagnostics."""

    summary: pd.DataFrame
    diagnostics: ParameterSensitivityDiagnostics


def run_parameter_sensitivity_v1(
    signal_rows: pd.DataFrame,
    *,
    config: ParameterSensitivityConfig = ParameterSensitivityConfig(),
) -> ParameterSensitivityResult:
    """Evaluate every configured accounting parameter candidate."""

    _validate_config(config)
    candidates = _candidate_configs(config)
    rows: list[dict[str, object]] = []

    for candidate_id, accounting_config in enumerate(candidates, start=1):
        result = run_target_position_accounting_v1(signal_rows, config=accounting_config)
        if result.summary.empty:
            raise ParameterSensitivityError("Target-position accounting returned no summary.")
        row = result.summary.iloc[0].to_dict()
        rows.append(
            {
                "candidate_id": f"candidate_{candidate_id:04d}",
                "signal_column": config.signal_column,
                "max_position": accounting_config.max_position,
                "cooldown": accounting_config.cooldown,
                "max_trades_per_day": accounting_config.max_trades_per_day,
                "fixed_bps": accounting_config.fixed_bps,
                "slippage_ticks": accounting_config.slippage_ticks,
                "tick_size": accounting_config.tick_size,
                "flat_on_no_signal": accounting_config.flat_on_no_signal,
                "eod_flat": accounting_config.eod_flat,
                **row,
            }
        )

    summary = pd.DataFrame(rows)
    diagnostics = ParameterSensitivityDiagnostics(
        input_signal_rows=len(signal_rows),
        output_summary_rows=len(summary),
        candidate_count=len(candidates),
        signal_column=config.signal_column,
        max_position_grid=config.max_position_grid,
        cooldown_grid=config.cooldown_grid,
        max_trades_per_day_grid=config.max_trades_per_day_grid,
        fixed_bps_grid=config.fixed_bps_grid,
        slippage_ticks_grid=config.slippage_ticks_grid,
        tick_size=config.tick_size,
        flat_on_no_signal=config.flat_on_no_signal,
        eod_flat=config.eod_flat,
        parameter_sensitivity_policy=PARAMETER_SENSITIVITY_POLICY,
        parameter_selection_policy=PARAMETER_SELECTION_POLICY,
    )
    return ParameterSensitivityResult(summary=summary, diagnostics=diagnostics)


def _candidate_configs(
    config: ParameterSensitivityConfig,
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


def _validate_config(config: ParameterSensitivityConfig) -> None:
    for grid_name, grid in (
        ("max_position_grid", config.max_position_grid),
        ("cooldown_grid", config.cooldown_grid),
        ("max_trades_per_day_grid", config.max_trades_per_day_grid),
        ("fixed_bps_grid", config.fixed_bps_grid),
        ("slippage_ticks_grid", config.slippage_ticks_grid),
    ):
        if not grid:
            raise ParameterSensitivityError(f"{grid_name} must not be empty.")
    if any(value <= 0 for value in config.max_position_grid):
        raise ParameterSensitivityError("max_position_grid must contain positive values.")
    if any(pd.Timedelta(value) < pd.Timedelta(0) for value in config.cooldown_grid):
        raise ParameterSensitivityError("cooldown_grid must contain non-negative durations.")
    if any(
        value is not None and value < 1
        for value in config.max_trades_per_day_grid
    ):
        raise ParameterSensitivityError(
            "max_trades_per_day_grid must contain positive integers or None."
        )
    if any(value < 0 for value in config.fixed_bps_grid):
        raise ParameterSensitivityError("fixed_bps_grid must contain non-negative values.")
    if any(value < 0 for value in config.slippage_ticks_grid):
        raise ParameterSensitivityError(
            "slippage_ticks_grid must contain non-negative values."
        )
    if config.tick_size <= 0:
        raise ParameterSensitivityError("tick_size must be positive.")
