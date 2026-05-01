"""Target-position execution accounting for signal rows.

Target-position accounting v1 converts signal rows into target positions,
applies basic account constraints, and records order/cash/inventory/equity
state. It is still an accounting scaffold, not a research-grade backtest.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

import pandas as pd

from ..alignment import TRADING_DATE
from ..schema import EVENT_TIME, SYMBOL
from ..signals.rules import (
    SEQUENTIAL_GATE_SIGNAL,
    SIGNAL_MIDQUOTE,
    SIGNAL_QUOTED_SPREAD,
)

TARGET_POSITION_POLICY_NOTE: Final[str] = (
    "Target-position accounting v1 maps signal rows to bounded target "
    "positions, applies cooldown / max-trades / EOD-flat account constraints, "
    "and records order-level cash, position, inventory, and equity. It is an "
    "accounting scaffold, not parameter optimization or a research-grade "
    "backtest."
)
TARGET_POSITION_POLICY: Final[str] = "signal_to_bounded_target_position_v1"
TARGET_MAPPING_POLICY: Final[str] = "long_short_flat_signal_to_target_position"
ORDER_EXECUTION_POLICY: Final[str] = "fill_at_signal_midquote_with_cost_deduction"
POSITION_LIMIT_POLICY: Final[str] = "clip_target_to_max_abs_position"
NO_SIGNAL_POLICY: Final[str] = "flat_on_no_signal"
EOD_POLICY: Final[str] = "force_flat_at_last_valid_row_per_symbol_date"

DEFAULT_MAX_POSITION: Final[float] = 1.0
DEFAULT_FIXED_BPS: Final[float] = 0.0
DEFAULT_SLIPPAGE_TICKS: Final[float] = 0.0
DEFAULT_TICK_SIZE: Final[float] = 0.01
DEFAULT_COOLDOWN: Final[str] = "0ms"

ORDER_COLUMNS: Final[tuple[str, ...]] = (
    "simulation_id",
    "event_time",
    "symbol",
    "trading_date",
    "signal_row_index",
    "signal",
    "order_reason",
    "previous_position",
    "target_position",
    "order_quantity",
    "fill_midquote",
    "quoted_spread",
    "event_cost",
    "cash_delta",
)

LEDGER_COLUMNS: Final[tuple[str, ...]] = (
    *ORDER_COLUMNS,
    "position_after",
    "cash_after",
    "inventory_value_after",
    "equity_after",
)


class TargetPositionAccountingError(ValueError):
    """Raised when target-position accounting cannot be computed."""


@dataclass(frozen=True)
class TargetPositionAccountingConfig:
    """Configuration for target-position accounting v1."""

    signal_column: str = SEQUENTIAL_GATE_SIGNAL
    midquote_column: str = SIGNAL_MIDQUOTE
    spread_column: str = SIGNAL_QUOTED_SPREAD
    max_position: float = DEFAULT_MAX_POSITION
    fixed_bps: float = DEFAULT_FIXED_BPS
    slippage_ticks: float = DEFAULT_SLIPPAGE_TICKS
    tick_size: float = DEFAULT_TICK_SIZE
    cooldown: str = DEFAULT_COOLDOWN
    flat_on_no_signal: bool = True
    eod_flat: bool = True
    max_trades_per_day: int | None = None


@dataclass(frozen=True)
class TargetPositionAccountingDiagnostics:
    """Diagnostics for target-position accounting v1."""

    input_signal_rows: int
    valid_price_rows: int
    active_signal_rows: int
    target_change_candidate_rows: int
    output_order_rows: int
    output_ledger_rows: int
    output_summary_rows: int
    signal_column: str
    max_position: float
    fixed_bps: float
    slippage_ticks: float
    tick_size: float
    cooldown: str
    flat_on_no_signal: bool
    eod_flat: bool
    max_trades_per_day: int | None
    target_position_policy: str
    target_mapping_policy: str
    order_execution_policy: str
    position_limit_policy: str
    no_signal_policy: str
    eod_policy: str
    skipped_missing_price_rows: int
    skipped_cooldown_orders: int
    skipped_max_trades_orders: int
    final_position_by_symbol_date: dict[str, float]
    position_limit_implemented: bool = True
    cooldown_implemented: bool = True
    max_trades_per_day_implemented: bool = True
    eod_flat_implemented: bool = True
    risk_controls_implemented: bool = True
    passive_fill_simulation_implemented: bool = False
    order_book_fill_simulation_implemented: bool = False
    broker_fee_model_implemented: bool = False
    sec_finra_fee_model_implemented: bool = False
    parameter_optimization_implemented: bool = False
    research_grade_backtest: bool = False


@dataclass(frozen=True)
class TargetPositionAccountingResult:
    """Target-position accounting outputs and diagnostics."""

    orders: pd.DataFrame
    ledger: pd.DataFrame
    summary: pd.DataFrame
    diagnostics: TargetPositionAccountingDiagnostics


def run_target_position_accounting_v1(
    signal_rows: pd.DataFrame,
    *,
    config: TargetPositionAccountingConfig = TargetPositionAccountingConfig(),
) -> TargetPositionAccountingResult:
    """Convert signal rows to bounded target-position accounting."""

    _validate_inputs(signal_rows, config=config)
    _validate_config(config)

    rows = signal_rows.copy()
    rows["signal_row_index"] = rows.index
    rows["signal_numeric"] = pd.to_numeric(rows[config.signal_column], errors="coerce").fillna(0)
    rows["midquote_numeric"] = pd.to_numeric(rows[config.midquote_column], errors="coerce")
    rows["spread_numeric"] = pd.to_numeric(rows[config.spread_column], errors="coerce")
    rows["valid_price"] = rows["midquote_numeric"].gt(0) & rows["spread_numeric"].ge(0)
    rows = rows.sort_values(
        [SYMBOL, TRADING_DATE, EVENT_TIME, "signal_row_index"],
        kind="mergesort",
    ).reset_index(drop=True)

    cooldown = pd.Timedelta(config.cooldown)
    simulation_id = _simulation_id(config)
    orders: list[dict[str, object]] = []
    skipped_missing_price_rows = 0
    skipped_cooldown_orders = 0
    skipped_max_trades_orders = 0
    target_change_candidate_rows = 0
    final_position_by_symbol_date: dict[str, float] = {}

    for (symbol, trading_date), group in rows.groupby([SYMBOL, TRADING_DATE], sort=False):
        state = _GroupState()
        group_orders, group_diagnostics = _process_group(
            group,
            symbol=str(symbol),
            trading_date=str(trading_date),
            config=config,
            cooldown=cooldown,
            simulation_id=simulation_id,
            state=state,
        )
        orders.extend(group_orders)
        skipped_missing_price_rows += group_diagnostics["skipped_missing_price_rows"]
        skipped_cooldown_orders += group_diagnostics["skipped_cooldown_orders"]
        skipped_max_trades_orders += group_diagnostics["skipped_max_trades_orders"]
        target_change_candidate_rows += group_diagnostics["target_change_candidate_rows"]
        final_position_by_symbol_date[f"{symbol}|{trading_date}"] = state.position

    orders_frame = pd.DataFrame(orders, columns=ORDER_COLUMNS)
    ledger = _build_ledger(orders_frame)
    summary = _build_summary(
        rows,
        ledger,
        simulation_id=simulation_id,
        config=config,
        skipped_missing_price_rows=skipped_missing_price_rows,
        skipped_cooldown_orders=skipped_cooldown_orders,
        skipped_max_trades_orders=skipped_max_trades_orders,
        target_change_candidate_rows=target_change_candidate_rows,
    )
    diagnostics = TargetPositionAccountingDiagnostics(
        input_signal_rows=len(rows),
        valid_price_rows=int(rows["valid_price"].sum()),
        active_signal_rows=int(rows["signal_numeric"].isin((1, -1)).sum()),
        target_change_candidate_rows=target_change_candidate_rows,
        output_order_rows=len(orders_frame),
        output_ledger_rows=len(ledger),
        output_summary_rows=len(summary),
        signal_column=config.signal_column,
        max_position=config.max_position,
        fixed_bps=config.fixed_bps,
        slippage_ticks=config.slippage_ticks,
        tick_size=config.tick_size,
        cooldown=config.cooldown,
        flat_on_no_signal=config.flat_on_no_signal,
        eod_flat=config.eod_flat,
        max_trades_per_day=config.max_trades_per_day,
        target_position_policy=TARGET_POSITION_POLICY,
        target_mapping_policy=TARGET_MAPPING_POLICY,
        order_execution_policy=ORDER_EXECUTION_POLICY,
        position_limit_policy=POSITION_LIMIT_POLICY,
        no_signal_policy=NO_SIGNAL_POLICY if config.flat_on_no_signal else "hold_on_no_signal",
        eod_policy=EOD_POLICY if config.eod_flat else "no_forced_eod_flat",
        skipped_missing_price_rows=skipped_missing_price_rows,
        skipped_cooldown_orders=skipped_cooldown_orders,
        skipped_max_trades_orders=skipped_max_trades_orders,
        final_position_by_symbol_date=final_position_by_symbol_date,
    )
    return TargetPositionAccountingResult(
        orders=orders_frame,
        ledger=ledger,
        summary=summary,
        diagnostics=diagnostics,
    )


@dataclass
class _GroupState:
    position: float = 0.0
    cash: float = 0.0
    next_allowed_time: pd.Timestamp | None = None
    trade_count: int = 0


def _process_group(
    group: pd.DataFrame,
    *,
    symbol: str,
    trading_date: str,
    config: TargetPositionAccountingConfig,
    cooldown: pd.Timedelta,
    simulation_id: str,
    state: _GroupState,
) -> tuple[list[dict[str, object]], dict[str, int]]:
    orders: list[dict[str, object]] = []
    skipped_missing_price_rows = 0
    skipped_cooldown_orders = 0
    skipped_max_trades_orders = 0
    target_change_candidate_rows = 0

    for row in group.itertuples(index=False):
        event_time = getattr(row, EVENT_TIME)
        signal_value = float(row.signal_numeric)
        if signal_value == 0 and not config.flat_on_no_signal:
            continue
        target_position = _target_position_from_signal(signal_value, config=config)
        if target_position == state.position:
            continue
        target_change_candidate_rows += 1
        if not bool(row.valid_price):
            skipped_missing_price_rows += 1
            continue
        if state.next_allowed_time is not None and event_time < state.next_allowed_time:
            skipped_cooldown_orders += 1
            continue
        if (
            config.max_trades_per_day is not None
            and state.trade_count >= config.max_trades_per_day
        ):
            skipped_max_trades_orders += 1
            continue
        orders.append(
            _make_order(
                row,
                simulation_id=simulation_id,
                symbol=symbol,
                trading_date=trading_date,
                target_position=target_position,
                order_reason="signal_target_change",
                config=config,
                state=state,
            )
        )
        state.next_allowed_time = event_time + cooldown
        state.trade_count += 1

    if config.eod_flat and state.position != 0:
        valid_group = group.loc[group["valid_price"]]
        if valid_group.empty:
            skipped_missing_price_rows += 1
        else:
            last_row = valid_group.iloc[-1]
            orders.append(
                _make_order(
                    last_row,
                    simulation_id=simulation_id,
                    symbol=symbol,
                    trading_date=trading_date,
                    target_position=0.0,
                    order_reason="eod_flat",
                    config=config,
                    state=state,
                )
            )
            state.trade_count += 1

    return orders, {
        "skipped_missing_price_rows": skipped_missing_price_rows,
        "skipped_cooldown_orders": skipped_cooldown_orders,
        "skipped_max_trades_orders": skipped_max_trades_orders,
        "target_change_candidate_rows": target_change_candidate_rows,
    }


def _target_position_from_signal(
    signal_value: float,
    *,
    config: TargetPositionAccountingConfig,
) -> float:
    if signal_value > 0:
        return config.max_position
    if signal_value < 0:
        return -config.max_position
    if config.flat_on_no_signal:
        return 0.0
    return 0.0


def _make_order(
    row: pd.Series | object,
    *,
    simulation_id: str,
    symbol: str,
    trading_date: str,
    target_position: float,
    order_reason: str,
    config: TargetPositionAccountingConfig,
    state: _GroupState,
) -> dict[str, object]:
    event_time = getattr(row, EVENT_TIME) if not isinstance(row, pd.Series) else row[EVENT_TIME]
    signal_value = (
        float(getattr(row, "signal_numeric"))
        if not isinstance(row, pd.Series)
        else float(row["signal_numeric"])
    )
    row_index = (
        int(getattr(row, "signal_row_index"))
        if not isinstance(row, pd.Series)
        else int(row["signal_row_index"])
    )
    fill_midquote = (
        float(getattr(row, "midquote_numeric"))
        if not isinstance(row, pd.Series)
        else float(row["midquote_numeric"])
    )
    quoted_spread = (
        float(getattr(row, "spread_numeric"))
        if not isinstance(row, pd.Series)
        else float(row["spread_numeric"])
    )
    previous_position = state.position
    order_quantity = target_position - previous_position
    event_cost = _order_cost(
        order_quantity=order_quantity,
        fill_midquote=fill_midquote,
        quoted_spread=quoted_spread,
        fixed_bps=config.fixed_bps,
        slippage_ticks=config.slippage_ticks,
        tick_size=config.tick_size,
    )
    cash_delta = -order_quantity * fill_midquote - event_cost
    state.position = target_position
    state.cash += cash_delta
    return {
        "simulation_id": simulation_id,
        "event_time": event_time,
        "symbol": symbol,
        "trading_date": trading_date,
        "signal_row_index": row_index,
        "signal": signal_value,
        "order_reason": order_reason,
        "previous_position": previous_position,
        "target_position": target_position,
        "order_quantity": order_quantity,
        "fill_midquote": fill_midquote,
        "quoted_spread": quoted_spread,
        "event_cost": event_cost,
        "cash_delta": cash_delta,
    }


def _order_cost(
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


def _build_ledger(orders: pd.DataFrame) -> pd.DataFrame:
    if orders.empty:
        return pd.DataFrame(columns=LEDGER_COLUMNS)
    ledger = orders.copy()
    grouped = ledger.groupby("simulation_id", sort=False)
    ledger["position_after"] = grouped["order_quantity"].cumsum()
    ledger["cash_after"] = grouped["cash_delta"].cumsum()
    ledger["inventory_value_after"] = ledger["position_after"] * ledger["fill_midquote"]
    ledger["equity_after"] = ledger["cash_after"] + ledger["inventory_value_after"]
    return ledger.loc[:, LEDGER_COLUMNS]


def _build_summary(
    rows: pd.DataFrame,
    ledger: pd.DataFrame,
    *,
    simulation_id: str,
    config: TargetPositionAccountingConfig,
    skipped_missing_price_rows: int,
    skipped_cooldown_orders: int,
    skipped_max_trades_orders: int,
    target_change_candidate_rows: int,
) -> pd.DataFrame:
    if ledger.empty:
        gross_pnl = 0.0
        cost = 0.0
        net_pnl = 0.0
        num_trades = 0
        return pd.DataFrame(
            [
                {
                    "simulation_id": simulation_id,
                    "input_signal_rows": len(rows),
                    "active_signal_rows": int(rows["signal_numeric"].isin((1, -1)).sum()),
                    "target_change_candidate_rows": target_change_candidate_rows,
                    "order_rows": 0,
                    "total_cost": 0.0,
                    "gross_pnl": gross_pnl,
                    "cost": cost,
                    "net_pnl": net_pnl,
                    "num_trades": num_trades,
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
                    "skipped_missing_price_rows": skipped_missing_price_rows,
                    "skipped_cooldown_orders": skipped_cooldown_orders,
                    "skipped_max_trades_orders": skipped_max_trades_orders,
                    "max_position": config.max_position,
                    "cooldown": config.cooldown,
                    "max_trades_per_day": config.max_trades_per_day,
                }
            ]
        )
    final = ledger.iloc[-1]
    turnover = (ledger["order_quantity"].abs() * ledger["fill_midquote"]).sum()
    cost = float(ledger["event_cost"].sum())
    net_pnl = float(final["equity_after"])
    gross_pnl = net_pnl + cost
    num_trades = len(ledger)
    return pd.DataFrame(
        [
            {
                "simulation_id": simulation_id,
                "input_signal_rows": len(rows),
                "active_signal_rows": int(rows["signal_numeric"].isin((1, -1)).sum()),
                "target_change_candidate_rows": target_change_candidate_rows,
                "order_rows": len(ledger),
                "total_cost": cost,
                "gross_pnl": gross_pnl,
                "cost": cost,
                "net_pnl": net_pnl,
                "num_trades": num_trades,
                "num_position_changes": len(ledger),
                "gross_per_trade": _safe_per_trade(gross_pnl, num_trades),
                "cost_per_trade": _safe_per_trade(cost, num_trades),
                "net_per_trade": _safe_per_trade(net_pnl, num_trades),
                "final_position": float(final["position_after"]),
                "final_cash": float(final["cash_after"]),
                "final_equity": net_pnl,
                "max_abs_position": float(ledger["position_after"].abs().max()),
                "mean_abs_position": float(ledger["position_after"].abs().mean()),
                "total_turnover": float(turnover),
                "skipped_missing_price_rows": skipped_missing_price_rows,
                "skipped_cooldown_orders": skipped_cooldown_orders,
                "skipped_max_trades_orders": skipped_max_trades_orders,
                "max_position": config.max_position,
                "cooldown": config.cooldown,
                "max_trades_per_day": config.max_trades_per_day,
            }
        ]
    )


def _safe_per_trade(value: float, num_trades: int) -> float | None:
    if num_trades == 0:
        return None
    return value / num_trades


def _validate_inputs(
    signal_rows: pd.DataFrame,
    *,
    config: TargetPositionAccountingConfig,
) -> None:
    required = [
        EVENT_TIME,
        SYMBOL,
        TRADING_DATE,
        config.signal_column,
        config.midquote_column,
        config.spread_column,
    ]
    missing = [column for column in required if column not in signal_rows.columns]
    if missing:
        raise TargetPositionAccountingError(f"Signal rows are missing columns: {missing}")
    if not pd.api.types.is_datetime64_any_dtype(signal_rows[EVENT_TIME]):
        raise TargetPositionAccountingError("Signal rows must have datetime event_time values.")


def _validate_config(config: TargetPositionAccountingConfig) -> None:
    if config.max_position <= 0:
        raise TargetPositionAccountingError("max_position must be positive.")
    if config.fixed_bps < 0:
        raise TargetPositionAccountingError("fixed_bps must be non-negative.")
    if config.slippage_ticks < 0:
        raise TargetPositionAccountingError("slippage_ticks must be non-negative.")
    if config.tick_size <= 0:
        raise TargetPositionAccountingError("tick_size must be positive.")
    if pd.Timedelta(config.cooldown) < pd.Timedelta(0):
        raise TargetPositionAccountingError("cooldown must be non-negative.")
    if config.max_trades_per_day is not None and config.max_trades_per_day < 1:
        raise TargetPositionAccountingError("max_trades_per_day must be positive when set.")


def _simulation_id(config: TargetPositionAccountingConfig) -> str:
    cooldown = config.cooldown.lower().replace(" ", "")
    max_trades = "none" if config.max_trades_per_day is None else str(config.max_trades_per_day)
    return (
        f"target_position_max_{config.max_position:g}_fixed_bps_{config.fixed_bps:g}"
        f"_slip_ticks_{config.slippage_ticks:g}_cooldown_{cooldown}"
        f"_max_trades_{max_trades}"
    )
