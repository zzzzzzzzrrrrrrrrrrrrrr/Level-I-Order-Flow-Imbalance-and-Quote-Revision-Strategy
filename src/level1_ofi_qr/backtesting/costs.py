"""Cost diagnostics for signal rows.

Cost model v1 is intentionally narrower than a backtest. It estimates the
spread and stress costs that a signal would need to overcome, but it does not
simulate orders, inventory, exits, or realized PnL.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

import pandas as pd

from ..alignment import TRADING_DATE
from ..labeling import DEFAULT_LABEL_HORIZONS
from ..schema import EVENT_TIME, SYMBOL
from ..signals.rules import (
    SEQUENTIAL_GATE_SIGNAL,
    SIGNAL_MIDQUOTE,
    SIGNAL_QUOTED_SPREAD,
)

COST_MODEL_POLICY_NOTE: Final[str] = (
    "Cost model v1 computes spread-derived and stress-test cost diagnostics "
    "for active signal rows. It does not perform position accounting, order "
    "matching, passive fill simulation, risk management, or backtesting."
)
COST_MODEL_POLICY: Final[str] = "spread_and_stress_cost_diagnostics_v1"
EXECUTION_COST_POLICY: Final[str] = "aggressive_one_way_half_spread_proxy"
ROUND_TRIP_COST_POLICY: Final[str] = "aggressive_entry_exit_full_spread_proxy"
FIXED_BPS_POLICY: Final[str] = "stress_grid_not_official_fee_schedule"
SLIPPAGE_TICKS_POLICY: Final[str] = "tick_stress_grid_not_latency_model"

DEFAULT_FIXED_BPS_GRID: Final[tuple[float, ...]] = (0.0, 0.5, 1.0, 2.0, 5.0)
DEFAULT_SLIPPAGE_TICKS_GRID: Final[tuple[float, ...]] = (0.0, 0.5, 1.0)
DEFAULT_TICK_SIZE: Final[float] = 0.01


class CostModelError(ValueError):
    """Raised when cost diagnostics cannot be computed."""


@dataclass(frozen=True)
class CostModelConfig:
    """Configuration for cost model v1."""

    horizons: tuple[str, ...] = DEFAULT_LABEL_HORIZONS
    signal_column: str = SEQUENTIAL_GATE_SIGNAL
    midquote_column: str = SIGNAL_MIDQUOTE
    spread_column: str = SIGNAL_QUOTED_SPREAD
    fixed_bps_grid: tuple[float, ...] = DEFAULT_FIXED_BPS_GRID
    slippage_ticks_grid: tuple[float, ...] = DEFAULT_SLIPPAGE_TICKS_GRID
    tick_size: float = DEFAULT_TICK_SIZE


@dataclass(frozen=True)
class CostModelDiagnostics:
    """Diagnostics emitted by cost model v1."""

    input_signal_rows: int
    active_signal_rows: int
    skipped_no_signal_rows: int
    costable_signal_rows: int
    skipped_missing_cost_rows: int
    output_summary_rows: int
    horizons: tuple[str, ...]
    signal_column: str
    midquote_column: str
    spread_column: str
    fixed_bps_grid: tuple[float, ...]
    slippage_ticks_grid: tuple[float, ...]
    tick_size: float
    cost_model_policy: str
    execution_cost_policy: str
    round_trip_cost_policy: str
    fixed_bps_policy: str
    slippage_ticks_policy: str
    one_way_cost_components: tuple[str, ...]
    round_trip_cost_components: tuple[str, ...]
    position_accounting_implemented: bool = False
    broker_fee_model_implemented: bool = False
    sec_finra_fee_model_implemented: bool = False
    passive_fill_simulation_implemented: bool = False
    backtest_implemented: bool = False
    research_grade_pnl: bool = False


@dataclass(frozen=True)
class CostModelResult:
    """Cost summary and diagnostics."""

    summary: pd.DataFrame
    diagnostics: CostModelDiagnostics


def run_cost_model_v1(
    signal_rows: pd.DataFrame,
    *,
    config: CostModelConfig = CostModelConfig(),
) -> CostModelResult:
    """Compute cost and edge diagnostics for active signal rows."""

    _validate_inputs(signal_rows, config=config)
    _validate_config(config)

    rows = signal_rows.copy()
    signal = pd.to_numeric(rows[config.signal_column], errors="coerce")
    midquote = pd.to_numeric(rows[config.midquote_column], errors="coerce")
    spread = pd.to_numeric(rows[config.spread_column], errors="coerce")
    active = signal.isin((1, -1))
    costable = active & midquote.gt(0) & spread.ge(0) & midquote.notna() & spread.notna()

    half_spread_cost_bps = (spread / 2.0) / midquote * 10000.0
    full_spread_round_trip_cost_bps = spread / midquote * 10000.0
    tick_cost_bps = config.tick_size / midquote * 10000.0

    summary_rows: list[dict[str, object]] = []
    for horizon in config.horizons:
        suffix = _horizon_suffix(horizon)
        label_available = _bool_series(rows[f"label_available_{suffix}"])
        future_return_bps = pd.to_numeric(
            rows[f"future_midquote_return_bps_{suffix}"],
            errors="coerce",
        )
        evaluable = costable & label_available & future_return_bps.notna()
        signed_future_return_bps = signal.loc[evaluable] * future_return_bps.loc[evaluable]

        for fixed_bps in config.fixed_bps_grid:
            for slippage_ticks in config.slippage_ticks_grid:
                one_way_cost = (
                    half_spread_cost_bps.loc[evaluable]
                    + fixed_bps
                    + slippage_ticks * tick_cost_bps.loc[evaluable]
                )
                round_trip_cost = (
                    full_spread_round_trip_cost_bps.loc[evaluable]
                    + 2.0 * fixed_bps
                    + 2.0 * slippage_ticks * tick_cost_bps.loc[evaluable]
                )
                after_one_way = signed_future_return_bps - one_way_cost
                after_round_trip = signed_future_return_bps - round_trip_cost
                summary_rows.append(
                    {
                        "horizon": horizon,
                        "fixed_bps": fixed_bps,
                        "slippage_ticks": slippage_ticks,
                        "tick_size": config.tick_size,
                        "active_signal_rows": int(active.sum()),
                        "costable_signal_rows": int(costable.sum()),
                        "label_available_signal_rows": int((costable & label_available).sum()),
                        "evaluated_signal_rows": int(evaluable.sum()),
                        "long_signal_rows": int((evaluable & signal.eq(1)).sum()),
                        "short_signal_rows": int((evaluable & signal.eq(-1)).sum()),
                        "mean_signed_future_return_bps": _series_mean(
                            signed_future_return_bps
                        ),
                        "median_signed_future_return_bps": _series_median(
                            signed_future_return_bps
                        ),
                        "mean_half_spread_cost_bps": _series_mean(
                            half_spread_cost_bps.loc[evaluable]
                        ),
                        "mean_full_spread_round_trip_cost_bps": _series_mean(
                            full_spread_round_trip_cost_bps.loc[evaluable]
                        ),
                        "mean_one_way_total_cost_bps": _series_mean(one_way_cost),
                        "mean_round_trip_total_cost_bps": _series_mean(round_trip_cost),
                        "mean_after_one_way_cost_bps": _series_mean(after_one_way),
                        "mean_after_round_trip_cost_bps": _series_mean(after_round_trip),
                        "share_beating_one_way_cost": _share_positive(after_one_way),
                        "share_beating_round_trip_cost": _share_positive(after_round_trip),
                    }
                )

    summary = pd.DataFrame(summary_rows)
    diagnostics = CostModelDiagnostics(
        input_signal_rows=len(rows),
        active_signal_rows=int(active.sum()),
        skipped_no_signal_rows=int((~active).sum()),
        costable_signal_rows=int(costable.sum()),
        skipped_missing_cost_rows=int((active & ~costable).sum()),
        output_summary_rows=len(summary),
        horizons=config.horizons,
        signal_column=config.signal_column,
        midquote_column=config.midquote_column,
        spread_column=config.spread_column,
        fixed_bps_grid=config.fixed_bps_grid,
        slippage_ticks_grid=config.slippage_ticks_grid,
        tick_size=config.tick_size,
        cost_model_policy=COST_MODEL_POLICY,
        execution_cost_policy=EXECUTION_COST_POLICY,
        round_trip_cost_policy=ROUND_TRIP_COST_POLICY,
        fixed_bps_policy=FIXED_BPS_POLICY,
        slippage_ticks_policy=SLIPPAGE_TICKS_POLICY,
        one_way_cost_components=("half_spread", "fixed_bps", "slippage_ticks"),
        round_trip_cost_components=(
            "full_spread_round_trip",
            "two_way_fixed_bps",
            "two_way_slippage_ticks",
        ),
    )
    return CostModelResult(summary=summary, diagnostics=diagnostics)


def _validate_inputs(signal_rows: pd.DataFrame, *, config: CostModelConfig) -> None:
    required = [
        EVENT_TIME,
        SYMBOL,
        TRADING_DATE,
        config.signal_column,
        config.midquote_column,
        config.spread_column,
    ]
    for horizon in config.horizons:
        suffix = _horizon_suffix(horizon)
        required.extend(
            [
                f"label_available_{suffix}",
                f"future_midquote_return_bps_{suffix}",
            ]
        )
    missing = [column for column in required if column not in signal_rows.columns]
    if missing:
        raise CostModelError(f"Signal rows are missing columns: {missing}")


def _validate_config(config: CostModelConfig) -> None:
    if not config.horizons:
        raise CostModelError("At least one horizon is required.")
    if config.tick_size <= 0:
        raise CostModelError("tick_size must be positive.")
    for grid_name, grid in (
        ("fixed_bps_grid", config.fixed_bps_grid),
        ("slippage_ticks_grid", config.slippage_ticks_grid),
    ):
        if not grid:
            raise CostModelError(f"{grid_name} must not be empty.")
        if any(value < 0 for value in grid):
            raise CostModelError(f"{grid_name} must contain non-negative values.")


def _bool_series(values: pd.Series) -> pd.Series:
    if pd.api.types.is_bool_dtype(values):
        return values.fillna(False)
    if pd.api.types.is_numeric_dtype(values):
        return pd.to_numeric(values, errors="coerce").fillna(0).ne(0)
    normalized = values.astype("string").str.strip().str.lower()
    return normalized.isin(("true", "1", "yes", "y"))


def _horizon_suffix(horizon: str) -> str:
    return horizon.lower().replace(" ", "").replace(".", "p")


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


def _share_positive(values: pd.Series) -> float | None:
    non_null = values.dropna()
    if non_null.empty:
        return None
    return float(non_null.gt(0).mean())
