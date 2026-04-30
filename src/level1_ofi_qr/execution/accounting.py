"""Execution accounting scaffold for signal rows.

Execution accounting v1 turns active signal rows into simple fixed-horizon
round trips and a ledger. It is an accounting scaffold, not a strategy
profitability test.
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

EXECUTION_ACCOUNTING_POLICY_NOTE: Final[str] = (
    "Execution accounting v1 converts active signal rows into independent "
    "one-share fixed-horizon round trips and a cash/inventory ledger. It is a "
    "mechanical accounting scaffold, not a complete execution simulator or "
    "profitability claim."
)
ACCOUNTING_POLICY: Final[str] = "independent_fixed_horizon_round_trip_accounting_v1"
ENTRY_EXECUTION_POLICY: Final[str] = "entry_at_signal_midquote_with_half_spread_cost"
EXIT_EXECUTION_POLICY: Final[str] = "exit_at_future_midquote_with_entry_spread_proxy"
POSITION_POLICY: Final[str] = "independent_unit_round_trips_no_position_limit"
FILL_ORDER_POLICY: Final[str] = "exit_before_entry_when_fill_timestamps_tie"

DEFAULT_QUANTITY: Final[float] = 1.0
DEFAULT_FIXED_BPS: Final[float] = 0.0
DEFAULT_SLIPPAGE_TICKS: Final[float] = 0.0
DEFAULT_TICK_SIZE: Final[float] = 0.01

TRADE_COLUMNS: Final[tuple[str, ...]] = (
    "simulation_id",
    "horizon",
    "signal_row_index",
    "entry_time",
    "exit_time",
    "symbol",
    "trading_date",
    "side",
    "quantity",
    "entry_midquote",
    "exit_midquote",
    "entry_spread",
    "gross_pnl",
    "entry_cost",
    "exit_cost",
    "total_cost",
    "net_pnl",
)

LEDGER_COLUMNS: Final[tuple[str, ...]] = (
    "simulation_id",
    "horizon",
    "signal_row_index",
    "fill_time",
    "event_type",
    "symbol",
    "trading_date",
    "side",
    "quantity_delta",
    "fill_midquote",
    "event_cost",
    "cash_delta",
    "realized_gross_pnl",
    "realized_net_pnl",
    "position_after",
    "cash_after",
    "inventory_value_after",
    "equity_after",
)


class ExecutionAccountingError(ValueError):
    """Raised when execution accounting cannot be computed."""


@dataclass(frozen=True)
class ExecutionAccountingConfig:
    """Configuration for execution accounting v1."""

    horizons: tuple[str, ...] = DEFAULT_LABEL_HORIZONS
    signal_column: str = SEQUENTIAL_GATE_SIGNAL
    midquote_column: str = SIGNAL_MIDQUOTE
    spread_column: str = SIGNAL_QUOTED_SPREAD
    quantity: float = DEFAULT_QUANTITY
    fixed_bps: float = DEFAULT_FIXED_BPS
    slippage_ticks: float = DEFAULT_SLIPPAGE_TICKS
    tick_size: float = DEFAULT_TICK_SIZE


@dataclass(frozen=True)
class ExecutionAccountingDiagnostics:
    """Diagnostics for execution accounting v1."""

    input_signal_rows: int
    active_signal_rows: int
    skipped_no_signal_rows: int
    costable_signal_rows: int
    skipped_missing_cost_rows: int
    output_trade_rows: int
    output_ledger_rows: int
    output_summary_rows: int
    horizons: tuple[str, ...]
    signal_column: str
    quantity: float
    fixed_bps: float
    slippage_ticks: float
    tick_size: float
    accounting_policy: str
    entry_execution_policy: str
    exit_execution_policy: str
    position_policy: str
    fill_order_policy: str
    label_available_trade_rows: dict[str, int]
    skipped_missing_label_rows: dict[str, int]
    final_position_by_horizon: dict[str, float]
    position_accounting_implemented: bool = True
    cash_accounting_implemented: bool = True
    inventory_accounting_implemented: bool = True
    pnl_attribution_implemented: bool = True
    passive_fill_simulation_implemented: bool = False
    order_book_fill_simulation_implemented: bool = False
    broker_fee_model_implemented: bool = False
    sec_finra_fee_model_implemented: bool = False
    risk_controls_implemented: bool = False
    parameter_optimization_implemented: bool = False
    research_grade_backtest: bool = False


@dataclass(frozen=True)
class ExecutionAccountingResult:
    """Execution accounting outputs and diagnostics."""

    trades: pd.DataFrame
    ledger: pd.DataFrame
    summary: pd.DataFrame
    diagnostics: ExecutionAccountingDiagnostics


def run_execution_accounting_v1(
    signal_rows: pd.DataFrame,
    *,
    config: ExecutionAccountingConfig = ExecutionAccountingConfig(),
) -> ExecutionAccountingResult:
    """Build fixed-horizon round-trip trades, ledger, and summary."""

    _validate_inputs(signal_rows, config=config)
    _validate_config(config)

    rows = signal_rows.copy()
    rows["_signal_row_index"] = rows.index
    signal = pd.to_numeric(rows[config.signal_column], errors="coerce")
    midquote = pd.to_numeric(rows[config.midquote_column], errors="coerce")
    spread = pd.to_numeric(rows[config.spread_column], errors="coerce")
    active = signal.isin((1, -1))
    costable = active & midquote.gt(0) & spread.ge(0) & midquote.notna() & spread.notna()

    trade_frames: list[pd.DataFrame] = []
    ledger_frames: list[pd.DataFrame] = []
    summary_rows: list[dict[str, object]] = []
    label_available_trade_rows: dict[str, int] = {}
    skipped_missing_label_rows: dict[str, int] = {}
    final_position_by_horizon: dict[str, float] = {}

    for horizon in config.horizons:
        suffix = _horizon_suffix(horizon)
        label_available = _bool_series(rows[f"label_available_{suffix}"])
        future_midquote = pd.to_numeric(rows[f"future_midquote_{suffix}"], errors="coerce")
        future_time = pd.to_datetime(
            rows[f"future_midquote_event_time_{suffix}"],
            format="mixed",
        )
        evaluable = costable & label_available & future_midquote.notna() & future_time.notna()
        label_available_trade_rows[suffix] = int((costable & label_available).sum())
        skipped_missing_label_rows[suffix] = int((costable & ~evaluable).sum())

        trades = _build_trade_frame(
            rows.loc[evaluable],
            signal=signal.loc[evaluable],
            entry_midquote=midquote.loc[evaluable],
            entry_spread=spread.loc[evaluable],
            exit_midquote=future_midquote.loc[evaluable],
            exit_time=future_time.loc[evaluable],
            horizon=horizon,
            config=config,
        )
        ledger = _build_ledger_frame(trades)
        if not ledger.empty:
            ledger = _apply_running_accounting(ledger)

        trade_frames.append(trades)
        ledger_frames.append(ledger)
        summary = _summarize_horizon(
            horizon=horizon,
            simulation_id=_simulation_id(
                horizon=horizon,
                fixed_bps=config.fixed_bps,
                slippage_ticks=config.slippage_ticks,
            ),
            trades=trades,
            ledger=ledger,
            active_rows=int(active.sum()),
            costable_rows=int(costable.sum()),
        )
        summary_rows.append(summary)
        final_position_by_horizon[_horizon_suffix(horizon)] = float(summary["final_position"])

    all_trades = _concat_or_empty(trade_frames, TRADE_COLUMNS)
    all_ledger = _concat_or_empty(ledger_frames, LEDGER_COLUMNS)
    summary = pd.DataFrame(summary_rows)
    diagnostics = ExecutionAccountingDiagnostics(
        input_signal_rows=len(rows),
        active_signal_rows=int(active.sum()),
        skipped_no_signal_rows=int((~active).sum()),
        costable_signal_rows=int(costable.sum()),
        skipped_missing_cost_rows=int((active & ~costable).sum()),
        output_trade_rows=len(all_trades),
        output_ledger_rows=len(all_ledger),
        output_summary_rows=len(summary),
        horizons=config.horizons,
        signal_column=config.signal_column,
        quantity=config.quantity,
        fixed_bps=config.fixed_bps,
        slippage_ticks=config.slippage_ticks,
        tick_size=config.tick_size,
        accounting_policy=ACCOUNTING_POLICY,
        entry_execution_policy=ENTRY_EXECUTION_POLICY,
        exit_execution_policy=EXIT_EXECUTION_POLICY,
        position_policy=POSITION_POLICY,
        fill_order_policy=FILL_ORDER_POLICY,
        label_available_trade_rows=label_available_trade_rows,
        skipped_missing_label_rows=skipped_missing_label_rows,
        final_position_by_horizon=final_position_by_horizon,
    )
    return ExecutionAccountingResult(
        trades=all_trades,
        ledger=all_ledger,
        summary=summary,
        diagnostics=diagnostics,
    )


def _validate_inputs(signal_rows: pd.DataFrame, *, config: ExecutionAccountingConfig) -> None:
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
                f"future_midquote_{suffix}",
                f"future_midquote_event_time_{suffix}",
            ]
        )
    missing = [column for column in required if column not in signal_rows.columns]
    if missing:
        raise ExecutionAccountingError(f"Signal rows are missing columns: {missing}")


def _validate_config(config: ExecutionAccountingConfig) -> None:
    if not config.horizons:
        raise ExecutionAccountingError("At least one horizon is required.")
    if config.quantity <= 0:
        raise ExecutionAccountingError("quantity must be positive.")
    if config.fixed_bps < 0:
        raise ExecutionAccountingError("fixed_bps must be non-negative.")
    if config.slippage_ticks < 0:
        raise ExecutionAccountingError("slippage_ticks must be non-negative.")
    if config.tick_size <= 0:
        raise ExecutionAccountingError("tick_size must be positive.")


def _build_trade_frame(
    rows: pd.DataFrame,
    *,
    signal: pd.Series,
    entry_midquote: pd.Series,
    entry_spread: pd.Series,
    exit_midquote: pd.Series,
    exit_time: pd.Series,
    horizon: str,
    config: ExecutionAccountingConfig,
) -> pd.DataFrame:
    if rows.empty:
        return pd.DataFrame(columns=TRADE_COLUMNS)

    side = signal.astype("int64")
    quantity = config.quantity
    entry_cost = _side_cost(
        midquote=entry_midquote,
        half_spread=entry_spread / 2.0,
        quantity=quantity,
        fixed_bps=config.fixed_bps,
        slippage_ticks=config.slippage_ticks,
        tick_size=config.tick_size,
    )
    exit_cost = _side_cost(
        midquote=exit_midquote,
        half_spread=entry_spread / 2.0,
        quantity=quantity,
        fixed_bps=config.fixed_bps,
        slippage_ticks=config.slippage_ticks,
        tick_size=config.tick_size,
    )
    gross_pnl = side * quantity * (exit_midquote - entry_midquote)
    total_cost = entry_cost + exit_cost
    simulation_id = _simulation_id(
        horizon=horizon,
        fixed_bps=config.fixed_bps,
        slippage_ticks=config.slippage_ticks,
    )

    trades = pd.DataFrame(
        {
            "simulation_id": simulation_id,
            "horizon": horizon,
            "signal_row_index": rows["_signal_row_index"].astype("int64"),
            "entry_time": rows[EVENT_TIME],
            "exit_time": exit_time,
            "symbol": rows[SYMBOL].astype(str),
            "trading_date": rows[TRADING_DATE].astype(str),
            "side": side,
            "quantity": quantity,
            "entry_midquote": entry_midquote,
            "exit_midquote": exit_midquote,
            "entry_spread": entry_spread,
            "gross_pnl": gross_pnl,
            "entry_cost": entry_cost,
            "exit_cost": exit_cost,
            "total_cost": total_cost,
            "net_pnl": gross_pnl - total_cost,
        }
    )
    return trades.loc[:, TRADE_COLUMNS]


def _side_cost(
    *,
    midquote: pd.Series,
    half_spread: pd.Series,
    quantity: float,
    fixed_bps: float,
    slippage_ticks: float,
    tick_size: float,
) -> pd.Series:
    fixed_cost = quantity * midquote * fixed_bps / 10000.0
    slippage_cost = quantity * slippage_ticks * tick_size
    return quantity * half_spread + fixed_cost + slippage_cost


def _build_ledger_frame(trades: pd.DataFrame) -> pd.DataFrame:
    if trades.empty:
        return pd.DataFrame(columns=LEDGER_COLUMNS)

    entry = pd.DataFrame(
        {
            "simulation_id": trades["simulation_id"],
            "horizon": trades["horizon"],
            "signal_row_index": trades["signal_row_index"],
            "fill_time": trades["entry_time"],
            "event_type": "entry",
            "symbol": trades["symbol"],
            "trading_date": trades["trading_date"],
            "side": trades["side"],
            "quantity_delta": trades["side"] * trades["quantity"],
            "fill_midquote": trades["entry_midquote"],
            "event_cost": trades["entry_cost"],
            "cash_delta": -trades["side"] * trades["quantity"] * trades["entry_midquote"]
            - trades["entry_cost"],
            "realized_gross_pnl": 0.0,
            "realized_net_pnl": -trades["entry_cost"],
            "_event_order": 1,
        }
    )
    exit_frame = pd.DataFrame(
        {
            "simulation_id": trades["simulation_id"],
            "horizon": trades["horizon"],
            "signal_row_index": trades["signal_row_index"],
            "fill_time": trades["exit_time"],
            "event_type": "exit",
            "symbol": trades["symbol"],
            "trading_date": trades["trading_date"],
            "side": trades["side"],
            "quantity_delta": -trades["side"] * trades["quantity"],
            "fill_midquote": trades["exit_midquote"],
            "event_cost": trades["exit_cost"],
            "cash_delta": trades["side"] * trades["quantity"] * trades["exit_midquote"]
            - trades["exit_cost"],
            "realized_gross_pnl": trades["gross_pnl"],
            "realized_net_pnl": trades["gross_pnl"] - trades["exit_cost"],
            "_event_order": 0,
        }
    )
    ledger = pd.concat([entry, exit_frame], ignore_index=True)
    ledger = ledger.sort_values(
        ["simulation_id", "fill_time", "_event_order", "signal_row_index"],
        kind="mergesort",
    ).reset_index(drop=True)
    return ledger


def _apply_running_accounting(ledger: pd.DataFrame) -> pd.DataFrame:
    result = ledger.copy()
    grouped = result.groupby("simulation_id", sort=False)
    result["position_after"] = grouped["quantity_delta"].cumsum()
    result["cash_after"] = grouped["cash_delta"].cumsum()
    result["inventory_value_after"] = result["position_after"] * result["fill_midquote"]
    result["equity_after"] = result["cash_after"] + result["inventory_value_after"]
    result = result.drop(columns=["_event_order"])
    return result.loc[:, LEDGER_COLUMNS]


def _summarize_horizon(
    *,
    horizon: str,
    simulation_id: str,
    trades: pd.DataFrame,
    ledger: pd.DataFrame,
    active_rows: int,
    costable_rows: int,
) -> dict[str, object]:
    if trades.empty:
        return {
            "simulation_id": simulation_id,
            "horizon": horizon,
            "active_signal_rows": active_rows,
            "costable_signal_rows": costable_rows,
            "accounted_round_trips": 0,
            "ledger_rows": 0,
            "total_gross_pnl": 0.0,
            "total_cost": 0.0,
            "total_net_pnl": 0.0,
            "mean_net_pnl_per_round_trip": None,
            "win_rate_net_positive": None,
            "total_turnover": 0.0,
            "final_position": 0.0,
            "final_cash": 0.0,
            "final_equity": 0.0,
            "max_abs_position": 0.0,
            "mean_abs_position": 0.0,
        }
    turnover = (ledger["quantity_delta"].abs() * ledger["fill_midquote"]).sum()
    final = ledger.iloc[-1]
    return {
        "simulation_id": trades["simulation_id"].iloc[0],
        "horizon": horizon,
        "active_signal_rows": active_rows,
        "costable_signal_rows": costable_rows,
        "accounted_round_trips": len(trades),
        "ledger_rows": len(ledger),
        "total_gross_pnl": float(trades["gross_pnl"].sum()),
        "total_cost": float(trades["total_cost"].sum()),
        "total_net_pnl": float(trades["net_pnl"].sum()),
        "mean_net_pnl_per_round_trip": _series_mean(trades["net_pnl"]),
        "win_rate_net_positive": _series_positive_share(trades["net_pnl"]),
        "total_turnover": float(turnover),
        "final_position": float(final["position_after"]),
        "final_cash": float(final["cash_after"]),
        "final_equity": float(final["equity_after"]),
        "max_abs_position": float(ledger["position_after"].abs().max()),
        "mean_abs_position": float(ledger["position_after"].abs().mean()),
    }


def _bool_series(values: pd.Series) -> pd.Series:
    if pd.api.types.is_bool_dtype(values):
        return values.fillna(False)
    if pd.api.types.is_numeric_dtype(values):
        return pd.to_numeric(values, errors="coerce").fillna(0).ne(0)
    normalized = values.astype("string").str.strip().str.lower()
    return normalized.isin(("true", "1", "yes", "y"))


def _concat_or_empty(frames: list[pd.DataFrame], columns: tuple[str, ...]) -> pd.DataFrame:
    non_empty = [frame for frame in frames if not frame.empty]
    if not non_empty:
        return pd.DataFrame(columns=columns)
    return pd.concat(non_empty, ignore_index=True).loc[:, columns]


def _simulation_id(*, horizon: str, fixed_bps: float, slippage_ticks: float) -> str:
    suffix = _horizon_suffix(horizon)
    return f"{suffix}_fixed_bps_{fixed_bps:g}_slip_ticks_{slippage_ticks:g}"


def _horizon_suffix(horizon: str) -> str:
    return horizon.lower().replace(" ", "").replace(".", "p")


def _series_mean(values: pd.Series) -> float | None:
    non_null = values.dropna()
    if non_null.empty:
        return None
    return float(non_null.mean())


def _series_positive_share(values: pd.Series) -> float | None:
    non_null = values.dropna()
    if non_null.empty:
        return None
    return float(non_null.gt(0).mean())
