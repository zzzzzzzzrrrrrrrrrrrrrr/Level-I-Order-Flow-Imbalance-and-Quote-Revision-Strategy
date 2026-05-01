"""Microstructure diagnostics for selected cost-aware linear-score trades."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

import numpy as np
import pandas as pd

from ..alignment import TRADING_DATE
from ..features.quotes import (
    MIDQUOTE,
    QUOTE_EVENT_INTERVAL_MS,
    QUOTE_REVISION_BPS,
    QUOTED_SPREAD,
    RELATIVE_SPREAD,
)
from ..models import (
    COST_AWARE_ESTIMATED_COST_BPS_COLUMN,
    COST_AWARE_PREDICTED_EDGE_BPS_COLUMN,
    MODEL_SCORE_COLUMN,
)
from ..schema import ASK, ASK_SIZE, BID, BID_SIZE, EVENT_TIME, SYMBOL
from ..signals.rules import (
    SIGNAL_QUOTE_IMBALANCE,
    SIGNAL_QUOTE_REVISION_BPS,
    SIGNAL_QUOTED_SPREAD,
    SIGNAL_RELATIVE_SPREAD,
)

DEFAULT_DIAGNOSTIC_HORIZONS: Final[tuple[str, ...]] = (
    "100ms",
    "250ms",
    "500ms",
    "1s",
    "2s",
    "5s",
)
DEFAULT_COST_STRESS_MULTIPLIERS: Final[tuple[float, ...]] = (0.0, 1.0, 1.5, 2.0)
SIGNED_FLOW_COLUMNS: Final[tuple[str, ...]] = (
    "signed_flow_imbalance_10_trades",
    "signed_flow_imbalance_50_trades",
    "signed_flow_imbalance_100_trades",
    "signed_flow_imbalance_100ms",
    "signed_flow_imbalance_500ms",
    "signed_flow_imbalance_1s",
)


class MicrostructureDiagnosticsError(ValueError):
    """Raised when microstructure diagnostics cannot be built."""


@dataclass(frozen=True)
class MicrostructureDiagnosticsConfig:
    """Configuration for post-selection microstructure diagnostics."""

    horizons: tuple[str, ...] = DEFAULT_DIAGNOSTIC_HORIZONS
    cost_stress_multipliers: tuple[float, ...] = DEFAULT_COST_STRESS_MULTIPLIERS
    trailing_window: str = "1s"
    passive_entry_timeout: str = "5s"


@dataclass(frozen=True)
class MicrostructureDiagnosticsResult:
    """Diagnostic tables for selected cost-aware trades."""

    trades: pd.DataFrame
    fold_summary: pd.DataFrame
    breakdown: pd.DataFrame
    horizon: pd.DataFrame
    horizon_summary: pd.DataFrame
    execution_trades: pd.DataFrame
    execution: pd.DataFrame
    cost_stress: pd.DataFrame
    strategy_variants: pd.DataFrame


@dataclass(frozen=True)
class _MarketState:
    times: np.ndarray
    event_times: np.ndarray
    midquote: np.ndarray
    microprice: np.ndarray
    bid: np.ndarray
    ask: np.ndarray
    bid_size: np.ndarray
    ask_size: np.ndarray
    quoted_spread: np.ndarray
    relative_spread: np.ndarray
    quote_revision_bps: np.ndarray
    quote_event_interval_ms: np.ndarray


@dataclass(frozen=True)
class _TradeState:
    times: np.ndarray
    event_times: np.ndarray
    trade_price: np.ndarray
    trade_size: np.ndarray
    trade_sign: np.ndarray
    signed_trade_size: np.ndarray


def build_cost_aware_microstructure_diagnostics(
    ledger: pd.DataFrame,
    quote_features: pd.DataFrame,
    *,
    trade_rows: pd.DataFrame | None = None,
    signal_rows: pd.DataFrame | None = None,
    prediction_rows: pd.DataFrame | None = None,
    config: MicrostructureDiagnosticsConfig = MicrostructureDiagnosticsConfig(),
) -> MicrostructureDiagnosticsResult:
    """Build diagnostics without changing model or selection logic."""

    _validate_config(config)
    _validate_ledger(ledger)
    _validate_quote_features(quote_features)

    normalized_ledger = _normalize_time_frame(ledger, EVENT_TIME)
    normalized_quotes = _normalize_time_frame(quote_features, EVENT_TIME)
    normalized_trades = (
        _normalize_time_frame(trade_rows, EVENT_TIME)
        if trade_rows is not None and not trade_rows.empty
        else pd.DataFrame()
    )
    normalized_signals = (
        _normalize_time_frame(signal_rows, EVENT_TIME)
        if signal_rows is not None and not signal_rows.empty
        else pd.DataFrame()
    )
    normalized_predictions = (
        _normalize_time_frame(prediction_rows, EVENT_TIME)
        if prediction_rows is not None and not prediction_rows.empty
        else pd.DataFrame()
    )

    market_by_group = _build_market_state(normalized_quotes)
    trades_by_group = _build_trade_state(normalized_trades)
    round_trips = _build_round_trips(normalized_ledger)
    if round_trips.empty:
        empty = pd.DataFrame()
        return MicrostructureDiagnosticsResult(
            trades=empty,
            fold_summary=empty,
            breakdown=empty,
            horizon=empty,
            horizon_summary=empty,
            execution_trades=empty,
            execution=empty,
            cost_stress=empty,
            strategy_variants=empty,
        )

    enriched = _attach_signal_and_prediction_rows(
        round_trips,
        signal_rows=normalized_signals,
        prediction_rows=normalized_predictions,
    )
    enriched = _attach_entry_market_state(
        enriched,
        market_by_group=market_by_group,
        trades_by_group=trades_by_group,
        trailing_window=pd.Timedelta(config.trailing_window),
    )
    enriched = _add_trade_buckets(enriched)
    horizon = _build_horizon_diagnostics(
        enriched,
        market_by_group=market_by_group,
        horizons=tuple(pd.Timedelta(value) for value in config.horizons),
        horizon_labels=config.horizons,
    )
    execution_trades = _build_execution_trade_diagnostics(
        enriched,
        market_by_group=market_by_group,
        trades_by_group=trades_by_group,
        passive_entry_timeout=pd.Timedelta(config.passive_entry_timeout),
    )
    return MicrostructureDiagnosticsResult(
        trades=enriched,
        fold_summary=_build_fold_summary(enriched, normalized_ledger),
        breakdown=_build_breakdowns(enriched),
        horizon=horizon,
        horizon_summary=_build_horizon_summary(horizon),
        execution_trades=execution_trades,
        execution=_summarize_execution_trades(execution_trades),
        cost_stress=_build_cost_stress(enriched, config.cost_stress_multipliers),
        strategy_variants=_build_strategy_variant_summary(
            enriched,
            horizon=horizon,
            execution_trades=execution_trades,
        ),
    )


def _build_round_trips(ledger: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    group_columns = [column for column in ("model_backtest_id", "fold_id") if column in ledger.columns]
    if not group_columns:
        group_columns = [TRADING_DATE]
    trade_index = 0
    for _, group in ledger.sort_values(EVENT_TIME, kind="mergesort").groupby(
        group_columns,
        sort=False,
        dropna=False,
    ):
        active: dict[str, object] | None = None
        for _, event in group.sort_values(EVENT_TIME, kind="mergesort").iterrows():
            previous_position = float(event.get("previous_position", 0.0))
            target_position = float(event.get("target_position", event.get("position_after", 0.0)))
            close_event = event
            open_event = event
            if (
                active is not None
                and previous_position != 0.0
                and target_position != 0.0
                and np.sign(previous_position) != np.sign(target_position)
            ):
                close_event, open_event = _split_reversal_event_cost(
                    event,
                    previous_position=previous_position,
                    target_position=target_position,
                )
            if active is not None and (
                target_position == 0.0 or np.sign(target_position) != active["side"]
            ):
                trade_index += 1
                rows.append(_close_round_trip(active, close_event, trade_index))
                active = None
            if target_position != 0.0 and (
                previous_position == 0.0
                or active is None and np.sign(target_position) != np.sign(previous_position)
            ):
                active = _open_round_trip(open_event)
    return pd.DataFrame(rows)


def _split_reversal_event_cost(
    event: pd.Series,
    *,
    previous_position: float,
    target_position: float,
) -> tuple[pd.Series, pd.Series]:
    order_quantity = abs(target_position - previous_position)
    if order_quantity == 0:
        return event, event
    total_cost = _zero_if_nan(event.get("event_cost"))
    close_quantity = abs(previous_position)
    open_quantity = abs(target_position)
    close_event = event.copy()
    open_event = event.copy()
    close_event["event_cost"] = total_cost * close_quantity / order_quantity
    open_event["event_cost"] = total_cost * open_quantity / order_quantity
    return close_event, open_event


def _open_round_trip(event: pd.Series) -> dict[str, object]:
    side = int(np.sign(float(event["target_position"])))
    return {
        "fold_id": event.get("fold_id"),
        "model_backtest_id": event.get("model_backtest_id"),
        "candidate_id": event.get("candidate_id"),
        "feature_set": event.get("feature_set"),
        "score_threshold": _as_float_or_nan(event.get("score_threshold")),
        "selected_threshold": _as_float_or_nan(event.get("selected_threshold")),
        "threshold_type": event.get("threshold_type"),
        "cost_multiplier": _as_float_or_nan(event.get("cost_multiplier")),
        "selected_cost_multiplier": _as_float_or_nan(event.get("selected_cost_multiplier")),
        "cooldown_seconds": _as_float_or_nan(event.get("cooldown_seconds")),
        "min_holding_seconds": _as_float_or_nan(event.get("min_holding_seconds")),
        "validation_date": event.get("validation_date"),
        "test_date": event.get("test_date"),
        "symbol": event[SYMBOL],
        "trading_date": event[TRADING_DATE],
        "entry_time": event[EVENT_TIME],
        "entry_signal_row_index": event.get("signal_row_index"),
        "side": side,
        "side_label": "long" if side > 0 else "short",
        "entry_midquote": float(event["fill_midquote"]),
        "entry_spread": _as_float_or_nan(event.get("quoted_spread")),
        "entry_event_cost": _as_float_or_nan(event.get("event_cost")),
    }


def _close_round_trip(
    active: dict[str, object],
    event: pd.Series,
    trade_index: int,
) -> dict[str, object]:
    row = dict(active)
    side = int(row["side"])
    entry_midquote = float(row["entry_midquote"])
    exit_midquote = float(event["fill_midquote"])
    entry_cost = _zero_if_nan(row.get("entry_event_cost"))
    exit_cost = _zero_if_nan(event.get("event_cost"))
    gross_pnl = side * (exit_midquote - entry_midquote)
    cost = entry_cost + exit_cost
    row.update(
        {
            "trade_id": f"cost_aware_trade_{trade_index:06d}",
            "exit_time": event[EVENT_TIME],
            "exit_signal_row_index": event.get("signal_row_index"),
            "exit_midquote": exit_midquote,
            "exit_spread": _as_float_or_nan(event.get("quoted_spread")),
            "exit_event_cost": exit_cost,
            "gross_pnl": gross_pnl,
            "cost": cost,
            "net_pnl": gross_pnl - cost,
            "holding_seconds": (
                event[EVENT_TIME] - row["entry_time"]
            ).total_seconds(),
        }
    )
    return row


def _attach_signal_and_prediction_rows(
    trades: pd.DataFrame,
    *,
    signal_rows: pd.DataFrame,
    prediction_rows: pd.DataFrame,
) -> pd.DataFrame:
    result = trades.copy()
    if not signal_rows.empty:
        signal_columns = [
            EVENT_TIME,
            SYMBOL,
            TRADING_DATE,
            SIGNAL_QUOTE_IMBALANCE,
            SIGNAL_QUOTE_REVISION_BPS,
            SIGNAL_QUOTED_SPREAD,
            SIGNAL_RELATIVE_SPREAD,
            *[column for column in SIGNED_FLOW_COLUMNS if column in signal_rows.columns],
        ]
        result = result.merge(
            signal_rows.loc[:, [column for column in signal_columns if column in signal_rows.columns]]
            .drop_duplicates([EVENT_TIME, SYMBOL, TRADING_DATE])
            .rename(columns={EVENT_TIME: "entry_time"}),
            how="left",
            on=["entry_time", SYMBOL, TRADING_DATE],
        )
    if not prediction_rows.empty:
        prediction_columns = [
            EVENT_TIME,
            SYMBOL,
            TRADING_DATE,
            MODEL_SCORE_COLUMN,
            COST_AWARE_ESTIMATED_COST_BPS_COLUMN,
            COST_AWARE_PREDICTED_EDGE_BPS_COLUMN,
        ]
        result = result.merge(
            prediction_rows.loc[
                :,
                [column for column in prediction_columns if column in prediction_rows.columns],
            ]
            .drop_duplicates([EVENT_TIME, SYMBOL, TRADING_DATE])
            .rename(columns={EVENT_TIME: "entry_time"}),
            how="left",
            on=["entry_time", SYMBOL, TRADING_DATE],
        )
    return result


def _attach_entry_market_state(
    trades: pd.DataFrame,
    *,
    market_by_group: dict[tuple[str, str], _MarketState],
    trades_by_group: dict[tuple[str, str], _TradeState],
    trailing_window: pd.Timedelta,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    trailing_ns = int(trailing_window.value)
    for _, trade in trades.iterrows():
        row = trade.to_dict()
        key = (str(trade[SYMBOL]), str(trade[TRADING_DATE]))
        market = market_by_group.get(key)
        entry_ns = _timestamp_ns(trade["entry_time"])
        if market is not None:
            index = _last_index_at_or_before(market.times, entry_ns)
            if index is not None:
                _update_entry_quote_state(row, market, index)
                _update_quote_window_state(row, market, entry_ns, trailing_ns, int(trade["side"]))
        trade_state = trades_by_group.get(key)
        if trade_state is not None:
            _update_trade_window_state(row, trade_state, entry_ns, trailing_ns)
        _update_edge_and_interactions(row)
        rows.append(row)
    return pd.DataFrame(rows)


def _update_entry_quote_state(row: dict[str, object], market: _MarketState, index: int) -> None:
    row.update(
        {
            "entry_quote_time": market.event_times[index],
            "entry_bid": market.bid[index],
            "entry_ask": market.ask[index],
            "entry_bid_size": market.bid_size[index],
            "entry_ask_size": market.ask_size[index],
            "entry_depth": market.bid_size[index] + market.ask_size[index],
            "entry_depth_imbalance": _safe_divide_scalar(
                market.bid_size[index] - market.ask_size[index],
                market.bid_size[index] + market.ask_size[index],
            ),
            "entry_microprice": market.microprice[index],
            "entry_microprice_gap": market.microprice[index] - market.midquote[index],
            "entry_microprice_gap_bps": _safe_divide_scalar(
                market.microprice[index] - market.midquote[index],
                market.midquote[index],
            )
            * 10000.0,
            "entry_quoted_spread": market.quoted_spread[index],
            "entry_relative_spread": market.relative_spread[index],
            "entry_quote_revision_bps": market.quote_revision_bps[index],
            "entry_quote_event_interval_ms": market.quote_event_interval_ms[index],
            "entry_quote_arrival_rate_hz": _safe_divide_scalar(
                1000.0,
                market.quote_event_interval_ms[index],
            ),
        }
    )


def _update_quote_window_state(
    row: dict[str, object],
    market: _MarketState,
    entry_ns: int,
    trailing_ns: int,
    side: int,
) -> None:
    start = np.searchsorted(market.times, entry_ns - trailing_ns, side="left")
    end = np.searchsorted(market.times, entry_ns, side="right")
    window_mid = market.midquote[start:end]
    window_qr = market.quote_revision_bps[start:end]
    if len(window_mid) > 1:
        returns = np.diff(window_mid) / window_mid[:-1] * 10000.0
        row["short_horizon_volatility_bps"] = float(np.nanstd(returns))
    else:
        row["short_horizon_volatility_bps"] = np.nan
    row["quote_arrival_rate_1s"] = len(window_mid) / (trailing_ns / 1_000_000_000.0)
    valid_qr = window_qr[~np.isnan(window_qr)]
    if len(valid_qr):
        aligned = np.sign(valid_qr) == side
        row["qr_persistence_count_1s"] = int(aligned.sum())
        row["qr_persistence_fraction_1s"] = float(aligned.mean())
    else:
        row["qr_persistence_count_1s"] = 0
        row["qr_persistence_fraction_1s"] = np.nan


def _update_trade_window_state(
    row: dict[str, object],
    trades: _TradeState,
    entry_ns: int,
    trailing_ns: int,
) -> None:
    start = np.searchsorted(trades.times, entry_ns - trailing_ns, side="left")
    end = np.searchsorted(trades.times, entry_ns, side="right")
    window_size = trades.trade_size[start:end]
    window_signed_size = trades.signed_trade_size[start:end]
    row["trade_arrival_rate_1s"] = len(window_size) / (trailing_ns / 1_000_000_000.0)
    row["recent_trade_volume_1s"] = float(np.nansum(window_size))
    row["recent_signed_volume_1s"] = float(np.nansum(window_signed_size))


def _update_edge_and_interactions(row: dict[str, object]) -> None:
    model_score = _as_float_or_nan(row.get(MODEL_SCORE_COLUMN))
    side = _as_float_or_nan(row.get("side"))
    midquote = _as_float_or_nan(row.get("entry_midquote"))
    spread = _as_float_or_nan(row.get("entry_quoted_spread", row.get("entry_spread")))
    expected_cost_bps = _as_float_or_nan(row.get(COST_AWARE_ESTIMATED_COST_BPS_COLUMN))
    if np.isnan(expected_cost_bps):
        expected_cost_bps = _safe_divide_scalar(spread, midquote) * 10000.0
    predicted_move_bps = side * model_score if not np.isnan(model_score) else np.nan
    row["predicted_move_bps"] = predicted_move_bps
    row["expected_cost_bps"] = expected_cost_bps
    row["expected_net_edge_bps"] = abs(predicted_move_bps) - expected_cost_bps
    row["edge_ratio"] = _safe_divide_scalar(abs(predicted_move_bps), expected_cost_bps)
    row["score_x_spread"] = model_score * spread
    row["score_x_depth_imbalance"] = model_score * _as_float_or_nan(
        row.get("entry_depth_imbalance")
    )

    available_ofi = [
        _as_float_or_nan(row.get(column))
        for column in SIGNED_FLOW_COLUMNS
        if column in row and not np.isnan(_as_float_or_nan(row.get(column)))
    ]
    if available_ofi and not np.isnan(side):
        aligned = [np.sign(value) == side for value in available_ofi]
        row["ofi_persistence_count"] = int(sum(aligned))
        row["ofi_persistence_fraction"] = float(sum(aligned) / len(aligned))
    else:
        row["ofi_persistence_count"] = 0
        row["ofi_persistence_fraction"] = np.nan
    row["ofi_acceleration_time"] = _as_float_or_nan(
        row.get("signed_flow_imbalance_100ms")
    ) - _as_float_or_nan(row.get("signed_flow_imbalance_1s"))
    row["ofi_acceleration_trades"] = _as_float_or_nan(
        row.get("signed_flow_imbalance_10_trades")
    ) - _as_float_or_nan(row.get("signed_flow_imbalance_100_trades"))


def _add_trade_buckets(trades: pd.DataFrame) -> pd.DataFrame:
    result = trades.copy()
    result["holding_bucket"] = pd.cut(
        pd.to_numeric(result["holding_seconds"], errors="coerce"),
        bins=[-0.001, 1, 3, 5, 10, 30, 60, np.inf],
        labels=("0-1s", "1-3s", "3-5s", "5-10s", "10-30s", "30-60s", "60s+"),
    ).astype("string")
    entry_time = pd.to_datetime(result["entry_time"], format="mixed")
    result["intraday_time_bucket"] = entry_time.dt.floor("30min").dt.strftime("%H:%M")
    spread = pd.to_numeric(result["entry_quoted_spread"], errors="coerce")
    try:
        result["spread_bucket"] = pd.qcut(
            spread.rank(method="first"),
            q=4,
            labels=("spread_q1", "spread_q2", "spread_q3", "spread_q4"),
        ).astype("string")
    except ValueError:
        result["spread_bucket"] = "spread_all"
    return result


def _build_horizon_diagnostics(
    trades: pd.DataFrame,
    *,
    market_by_group: dict[tuple[str, str], _MarketState],
    horizons: tuple[pd.Timedelta, ...],
    horizon_labels: tuple[str, ...],
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for _, trade in trades.iterrows():
        key = (str(trade[SYMBOL]), str(trade[TRADING_DATE]))
        market = market_by_group.get(key)
        if market is None:
            continue
        entry_ns = _timestamp_ns(trade["entry_time"])
        entry_midquote = _as_float_or_nan(trade.get("entry_midquote"))
        side = int(trade["side"])
        cost_threshold = _zero_if_nan(trade.get("cost"))
        for label, horizon in zip(horizon_labels, horizons, strict=True):
            rows.append(
                _horizon_row(
                    trade,
                    market=market,
                    entry_ns=entry_ns,
                    entry_midquote=entry_midquote,
                    side=side,
                    horizon_label=label,
                    horizon_ns=int(horizon.value),
                    cost_threshold=cost_threshold,
                )
            )
    return pd.DataFrame(rows)


def _horizon_row(
    trade: pd.Series,
    *,
    market: _MarketState,
    entry_ns: int,
    entry_midquote: float,
    side: int,
    horizon_label: str,
    horizon_ns: int,
    cost_threshold: float,
) -> dict[str, object]:
    start = np.searchsorted(market.times, entry_ns, side="left")
    end = np.searchsorted(market.times, entry_ns + horizon_ns, side="right")
    window_mid = market.midquote[start:end]
    future_index = np.searchsorted(market.times, entry_ns + horizon_ns, side="left")
    if future_index >= len(market.midquote):
        future_midquote = np.nan
    else:
        future_midquote = market.midquote[future_index]
    favorable = side * (window_mid - entry_midquote) if len(window_mid) else np.array([])
    time_to_profit = np.nan
    if len(favorable):
        hit_indices = np.flatnonzero(favorable >= cost_threshold)
        if len(hit_indices):
            time_to_profit = (market.times[start + hit_indices[0]] - entry_ns) / 1_000_000_000.0
    gross = side * (future_midquote - entry_midquote) if not np.isnan(future_midquote) else np.nan
    cost = _zero_if_nan(trade.get("cost"))
    return {
        "trade_id": trade["trade_id"],
        "fold_id": trade.get("fold_id"),
        "symbol": trade[SYMBOL],
        "trading_date": trade[TRADING_DATE],
        "side_label": trade["side_label"],
        "horizon": horizon_label,
        "gross_pnl": gross,
        "cost": cost,
        "net_pnl": gross - cost if not np.isnan(gross) else np.nan,
        "mfe": float(np.nanmax(favorable)) if len(favorable) else np.nan,
        "mae": float(np.nanmin(favorable)) if len(favorable) else np.nan,
        "time_to_profit_seconds": time_to_profit,
        "hit_cost_before_horizon": not np.isnan(time_to_profit),
    }


def _build_horizon_summary(horizon: pd.DataFrame) -> pd.DataFrame:
    if horizon.empty:
        return pd.DataFrame()
    rows = []
    for horizon_label, group in horizon.groupby("horizon", sort=False):
        rows.append(_summary_row(group, bucket=str(horizon_label), prefix={"horizon": horizon_label}))
    return pd.DataFrame(rows)


def _build_fold_summary(trades: pd.DataFrame, ledger: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for fold_id, ledger_group in ledger.groupby("fold_id", sort=False, dropna=False):
        trade_group = trades.loc[trades["fold_id"] == fold_id]
        first = ledger_group.iloc[0]
        row = _summary_row(trade_group, bucket=str(fold_id), prefix={"fold_id": fold_id})
        row.update(
            {
                "candidate_id": first.get("candidate_id"),
                "feature_set": first.get("feature_set"),
                "selected_threshold": first.get("selected_threshold", first.get("score_threshold")),
                "selected_cost_multiplier": first.get(
                    "selected_cost_multiplier",
                    first.get("cost_multiplier"),
                ),
                "cooldown_seconds": first.get("cooldown_seconds"),
                "min_holding_seconds": first.get("min_holding_seconds"),
                "test_date": first.get("test_date"),
                "num_order_events": len(ledger_group),
                "num_position_changes": len(ledger_group),
                "gross_per_order_event": _safe_divide_scalar(
                    row["gross_pnl"],
                    len(ledger_group),
                ),
                "cost_per_order_event": _safe_divide_scalar(row["cost"], len(ledger_group)),
                "net_per_order_event": _safe_divide_scalar(row["net_pnl"], len(ledger_group)),
            }
        )
        rows.append(row)
    return pd.DataFrame(rows)


def _build_breakdowns(trades: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for column, name in (
        ("side_label", "side"),
        ("holding_bucket", "holding_horizon"),
        ("intraday_time_bucket", "intraday_time"),
        ("spread_bucket", "spread_bucket"),
    ):
        if column not in trades.columns:
            continue
        for bucket, group in trades.groupby(column, sort=False, dropna=False):
            rows.append(
                _summary_row(
                    group,
                    bucket=str(bucket),
                    prefix={"breakdown_type": name, "bucket": str(bucket)},
                )
            )
    return pd.DataFrame(rows)


def _build_cost_stress(
    trades: pd.DataFrame,
    multipliers: tuple[float, ...],
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    groups: list[tuple[str, pd.DataFrame]] = [("ALL", trades)]
    groups.extend((str(fold), group) for fold, group in trades.groupby("fold_id", sort=False))
    for fold_id, group in groups:
        gross = float(pd.to_numeric(group["gross_pnl"], errors="coerce").fillna(0.0).sum())
        base_cost = float(pd.to_numeric(group["cost"], errors="coerce").fillna(0.0).sum())
        count = len(group)
        for multiplier in multipliers:
            cost = base_cost * multiplier
            rows.append(
                {
                    "fold_id": fold_id,
                    "cost_multiplier": multiplier,
                    "gross_pnl": gross,
                    "cost": cost,
                    "net_pnl": gross - cost,
                    "num_round_trips": count,
                    "gross_per_round_trip": _safe_divide_scalar(gross, count),
                    "cost_per_round_trip": _safe_divide_scalar(cost, count),
                    "net_per_round_trip": _safe_divide_scalar(gross - cost, count),
                }
            )
    return pd.DataFrame(rows)


def _build_execution_trade_diagnostics(
    trades: pd.DataFrame,
    *,
    market_by_group: dict[tuple[str, str], _MarketState],
    trades_by_group: dict[tuple[str, str], _TradeState],
    passive_entry_timeout: pd.Timedelta,
) -> pd.DataFrame:
    scenario_rows: list[dict[str, object]] = []
    for _, trade in trades.iterrows():
        scenario_rows.extend(
            _execution_rows_for_trade(
                trade,
                market_by_group=market_by_group,
                trades_by_group=trades_by_group,
                passive_entry_timeout=passive_entry_timeout,
            )
        )
    if not scenario_rows:
        return pd.DataFrame()
    return pd.DataFrame(scenario_rows)


def _summarize_execution_trades(scenarios: pd.DataFrame) -> pd.DataFrame:
    if scenarios.empty:
        return pd.DataFrame()
    rows = []
    for scenario, group in scenarios.groupby("execution_scenario", sort=False):
        pnl = pd.to_numeric(group["execution_net_pnl"], errors="coerce").fillna(0.0)
        filled = group["entry_filled"].astype(bool)
        rows.append(
            {
                "execution_scenario": scenario,
                "attempted_round_trips": len(group),
                "filled_round_trips": int(filled.sum()),
                "passive_exit_fills": int(group["passive_exit_filled"].astype(bool).sum()),
                "net_pnl": float(pnl.sum()),
                "net_per_attempt": _safe_divide_scalar(float(pnl.sum()), len(group)),
                "net_per_filled": _safe_divide_scalar(float(pnl.sum()), int(filled.sum())),
                "fill_rate": _safe_divide_scalar(int(filled.sum()), len(group)),
            }
        )
    return pd.DataFrame(rows)


def _build_strategy_variant_summary(
    trades: pd.DataFrame,
    *,
    horizon: pd.DataFrame,
    execution_trades: pd.DataFrame,
) -> pd.DataFrame:
    """Summarize diagnostic v2 shapes without changing selection logic."""

    rows = []
    all_trade_ids = set(trades["trade_id"])
    spread_gate = set(
        trades.loc[
            (trades["spread_bucket"] == "spread_q1")
            & (pd.to_numeric(trades["expected_net_edge_bps"], errors="coerce") > 0),
            "trade_id",
        ]
    )
    microprice_gate = set(
        trades.loc[
            (trades["spread_bucket"] == "spread_q1")
            & (pd.to_numeric(trades["expected_net_edge_bps"], errors="coerce") > 0)
            & (
                pd.to_numeric(trades["side"], errors="coerce")
                * pd.to_numeric(trades["entry_microprice_gap_bps"], errors="coerce")
                > 0
            ),
            "trade_id",
        ]
    )
    rows.append(
        _variant_from_trade_rows(
            trades,
            trade_ids=all_trade_ids,
            variant="v1_market_entry_market_exit",
            variant_family="baseline",
            diagnostic_rule="all selected cost-aware trades with market-style accounting",
        )
    )
    rows.append(
        _variant_from_trade_rows(
            trades,
            trade_ids=spread_gate,
            variant="v2_spread_q1_edge_gate_market_exit",
            variant_family="state_gating",
            diagnostic_rule="spread_q1 and expected_net_edge_bps > 0, original market exit",
        )
    )
    rows.append(
        _variant_from_trade_rows(
            trades,
            trade_ids=microprice_gate,
            variant="v2_spread_q1_edge_microprice_gate_market_exit",
            variant_family="state_gating",
            diagnostic_rule=(
                "spread_q1, expected_net_edge_bps > 0, and microprice gap aligned "
                "with trade side"
            ),
        )
    )
    rows.append(
        _variant_from_horizon_rows(
            horizon,
            trade_ids=microprice_gate,
            horizon_label="5s",
            variant="v2_spread_q1_edge_microprice_gate_5s_exit",
            variant_family="state_gating_horizon",
            diagnostic_rule=(
                "spread_q1, expected_net_edge_bps > 0, microprice aligned, fixed "
                "5s markout exit"
            ),
        )
    )
    rows.extend(
        _variant_from_execution_rows(
            execution_trades,
            trade_ids=microprice_gate,
            scenario="limit_entry_market_exit",
            variant="v2_spread_q1_microprice_passive_entry_market_exit",
            variant_family="passive_first",
            diagnostic_rule=(
                "spread_q1, expected_net_edge_bps > 0, microprice aligned, "
                "conservative passive entry then market exit"
            ),
        )
    )
    rows.extend(
        _variant_from_execution_rows(
            execution_trades,
            trade_ids=microprice_gate,
            scenario="limit_entry_limit_or_timeout_exit",
            variant="v2_spread_q1_microprice_passive_entry_limit_timeout_exit",
            variant_family="passive_first",
            diagnostic_rule=(
                "spread_q1, expected_net_edge_bps > 0, microprice aligned, "
                "conservative passive entry then passive-or-timeout exit"
            ),
        )
    )
    return pd.DataFrame(rows)


def _variant_from_trade_rows(
    trades: pd.DataFrame,
    *,
    trade_ids: set[object],
    variant: str,
    variant_family: str,
    diagnostic_rule: str,
) -> dict[str, object]:
    group = trades.loc[trades["trade_id"].isin(trade_ids)]
    gross = float(pd.to_numeric(group["gross_pnl"], errors="coerce").fillna(0.0).sum())
    cost = float(pd.to_numeric(group["cost"], errors="coerce").fillna(0.0).sum())
    net = float(pd.to_numeric(group["net_pnl"], errors="coerce").fillna(0.0).sum())
    count = len(group)
    return {
        "variant": variant,
        "variant_family": variant_family,
        "diagnostic_rule": diagnostic_rule,
        "attempted_round_trips": count,
        "filled_round_trips": count,
        "fill_rate": 1.0 if count else None,
        "gross_pnl": gross,
        "cost": cost,
        "net_pnl": net,
        "gross_per_round_trip": _safe_divide_scalar(gross, count),
        "cost_per_round_trip": _safe_divide_scalar(cost, count),
        "net_per_round_trip": _safe_divide_scalar(net, count),
    }


def _variant_from_horizon_rows(
    horizon: pd.DataFrame,
    *,
    trade_ids: set[object],
    horizon_label: str,
    variant: str,
    variant_family: str,
    diagnostic_rule: str,
) -> dict[str, object]:
    group = horizon.loc[
        horizon["trade_id"].isin(trade_ids) & (horizon["horizon"] == horizon_label)
    ]
    gross = float(pd.to_numeric(group["gross_pnl"], errors="coerce").fillna(0.0).sum())
    cost = float(pd.to_numeric(group["cost"], errors="coerce").fillna(0.0).sum())
    net = float(pd.to_numeric(group["net_pnl"], errors="coerce").fillna(0.0).sum())
    count = len(group)
    return {
        "variant": variant,
        "variant_family": variant_family,
        "diagnostic_rule": diagnostic_rule,
        "attempted_round_trips": count,
        "filled_round_trips": count,
        "fill_rate": 1.0 if count else None,
        "gross_pnl": gross,
        "cost": cost,
        "net_pnl": net,
        "gross_per_round_trip": _safe_divide_scalar(gross, count),
        "cost_per_round_trip": _safe_divide_scalar(cost, count),
        "net_per_round_trip": _safe_divide_scalar(net, count),
    }


def _variant_from_execution_rows(
    execution_trades: pd.DataFrame,
    *,
    trade_ids: set[object],
    scenario: str,
    variant: str,
    variant_family: str,
    diagnostic_rule: str,
) -> list[dict[str, object]]:
    group = execution_trades.loc[
        execution_trades["trade_id"].isin(trade_ids)
        & (execution_trades["execution_scenario"] == scenario)
    ]
    attempted = len(group)
    filled = int(group["entry_filled"].astype(bool).sum()) if attempted else 0
    net = float(pd.to_numeric(group["execution_net_pnl"], errors="coerce").fillna(0.0).sum())
    return [
        {
            "variant": variant,
            "variant_family": variant_family,
            "diagnostic_rule": diagnostic_rule,
            "attempted_round_trips": attempted,
            "filled_round_trips": filled,
            "fill_rate": _safe_divide_scalar(filled, attempted),
            "gross_pnl": None,
            "cost": None,
            "net_pnl": net,
            "gross_per_round_trip": None,
            "cost_per_round_trip": None,
            "net_per_round_trip": _safe_divide_scalar(net, attempted),
        }
    ]


def _execution_rows_for_trade(
    trade: pd.Series,
    *,
    market_by_group: dict[tuple[str, str], _MarketState],
    trades_by_group: dict[tuple[str, str], _TradeState],
    passive_entry_timeout: pd.Timedelta,
) -> list[dict[str, object]]:
    key = (str(trade[SYMBOL]), str(trade[TRADING_DATE]))
    market = market_by_group.get(key)
    if market is None:
        return []
    trade_state = trades_by_group.get(key)
    side = int(trade["side"])
    entry_ns = _timestamp_ns(trade["entry_time"])
    exit_ns = _timestamp_ns(trade["exit_time"])
    entry_index = _last_index_at_or_before(market.times, entry_ns)
    exit_index = _last_index_at_or_before(market.times, exit_ns)
    if entry_index is None or exit_index is None:
        return []

    market_entry_price = market.ask[entry_index] if side > 0 else market.bid[entry_index]
    market_exit_price = market.bid[exit_index] if side > 0 else market.ask[exit_index]
    market_pnl = side * (market_exit_price - market_entry_price)
    rows = [
        _execution_row(
            trade,
            "market_entry_market_exit",
            entry_filled=True,
            passive_exit_filled=False,
            pnl=market_pnl,
        )
    ]

    entry_limit_price = market.bid[entry_index] if side > 0 else market.ask[entry_index]
    fill = _find_entry_limit_fill(
        side=side,
        limit_price=entry_limit_price,
        start_ns=entry_ns,
        end_ns=min(exit_ns, entry_ns + int(passive_entry_timeout.value)),
        market=market,
        trades=trade_state,
    )
    if fill is None:
        rows.append(
            _execution_row(
                trade,
                "limit_entry_market_exit",
                entry_filled=False,
                passive_exit_filled=False,
                pnl=0.0,
            )
        )
        rows.append(
            _execution_row(
                trade,
                "limit_entry_limit_or_timeout_exit",
                entry_filled=False,
                passive_exit_filled=False,
                pnl=0.0,
            )
        )
        return rows

    fill_ns, _ = fill
    passive_entry_market_exit_pnl = side * (market_exit_price - entry_limit_price)
    rows.append(
        _execution_row(
            trade,
            "limit_entry_market_exit",
            entry_filled=True,
            passive_exit_filled=False,
            pnl=passive_entry_market_exit_pnl,
        )
    )

    fill_index = _last_index_at_or_before(market.times, fill_ns)
    exit_limit_price = (
        market.ask[fill_index] if side > 0 and fill_index is not None else np.nan
    )
    if side < 0 and fill_index is not None:
        exit_limit_price = market.bid[fill_index]
    passive_exit = None
    if not np.isnan(exit_limit_price):
        passive_exit = _find_exit_limit_fill(
            side=side,
            limit_price=exit_limit_price,
            start_ns=fill_ns,
            end_ns=exit_ns,
            market=market,
            trades=trade_state,
        )
    if passive_exit is None:
        hybrid_pnl = passive_entry_market_exit_pnl
        passive_exit_filled = False
    else:
        hybrid_pnl = side * (exit_limit_price - entry_limit_price)
        passive_exit_filled = True
    rows.append(
        _execution_row(
            trade,
            "limit_entry_limit_or_timeout_exit",
            entry_filled=True,
            passive_exit_filled=passive_exit_filled,
            pnl=hybrid_pnl,
        )
    )
    return rows


def _execution_row(
    trade: pd.Series,
    scenario: str,
    *,
    entry_filled: bool,
    passive_exit_filled: bool,
    pnl: float,
) -> dict[str, object]:
    return {
        "trade_id": trade["trade_id"],
        "fold_id": trade.get("fold_id"),
        "execution_scenario": scenario,
        "entry_filled": entry_filled,
        "passive_exit_filled": passive_exit_filled,
        "execution_net_pnl": pnl,
    }


def _find_entry_limit_fill(
    *,
    side: int,
    limit_price: float,
    start_ns: int,
    end_ns: int,
    market: _MarketState,
    trades: _TradeState | None,
) -> tuple[int, float] | None:
    quote_price = market.ask if side > 0 else market.bid
    quote_cross = quote_price <= limit_price if side > 0 else quote_price >= limit_price
    trade_cross = None
    if trades is not None:
        trade_cross = trades.trade_price <= limit_price if side > 0 else trades.trade_price >= limit_price
    return _first_cross(limit_price, start_ns, end_ns, market, trades, quote_cross, trade_cross)


def _find_exit_limit_fill(
    *,
    side: int,
    limit_price: float,
    start_ns: int,
    end_ns: int,
    market: _MarketState,
    trades: _TradeState | None,
) -> tuple[int, float] | None:
    quote_price = market.bid if side > 0 else market.ask
    quote_cross = quote_price >= limit_price if side > 0 else quote_price <= limit_price
    trade_cross = None
    if trades is not None:
        trade_cross = trades.trade_price >= limit_price if side > 0 else trades.trade_price <= limit_price
    return _first_cross(limit_price, start_ns, end_ns, market, trades, quote_cross, trade_cross)


def _first_cross(
    limit_price: float,
    start_ns: int,
    end_ns: int,
    market: _MarketState,
    trades: _TradeState | None,
    quote_cross: np.ndarray,
    trade_cross: np.ndarray | None,
) -> tuple[int, float] | None:
    quote_start = np.searchsorted(market.times, start_ns, side="right")
    quote_end = np.searchsorted(market.times, end_ns, side="right")
    quote_hits = np.flatnonzero(quote_cross[quote_start:quote_end])
    candidates: list[tuple[int, float]] = []
    if len(quote_hits):
        candidates.append((int(market.times[quote_start + quote_hits[0]]), limit_price))
    if trades is not None and trade_cross is not None:
        trade_start = np.searchsorted(trades.times, start_ns, side="right")
        trade_end = np.searchsorted(trades.times, end_ns, side="right")
        trade_hits = np.flatnonzero(trade_cross[trade_start:trade_end])
        if len(trade_hits):
            candidates.append((int(trades.times[trade_start + trade_hits[0]]), limit_price))
    if not candidates:
        return None
    return min(candidates, key=lambda item: item[0])


def _summary_row(
    group: pd.DataFrame,
    *,
    bucket: str,
    prefix: dict[str, object],
) -> dict[str, object]:
    gross = float(pd.to_numeric(group.get("gross_pnl", pd.Series(dtype=float)), errors="coerce").fillna(0.0).sum())
    cost = float(pd.to_numeric(group.get("cost", pd.Series(dtype=float)), errors="coerce").fillna(0.0).sum())
    net = float(pd.to_numeric(group.get("net_pnl", pd.Series(dtype=float)), errors="coerce").fillna(0.0).sum())
    count = len(group)
    wins = pd.to_numeric(group.get("net_pnl", pd.Series(dtype=float)), errors="coerce") > 0
    row = dict(prefix)
    row.update(
        {
            "bucket": bucket,
            "num_round_trips": count,
            "gross_pnl": gross,
            "cost": cost,
            "net_pnl": net,
            "gross_per_round_trip": _safe_divide_scalar(gross, count),
            "cost_per_round_trip": _safe_divide_scalar(cost, count),
            "net_per_round_trip": _safe_divide_scalar(net, count),
            "win_rate": _safe_divide_scalar(int(wins.sum()), count),
        }
    )
    if "mfe" in group.columns:
        row["mfe_mean"] = float(pd.to_numeric(group["mfe"], errors="coerce").mean())
    if "mae" in group.columns:
        row["mae_mean"] = float(pd.to_numeric(group["mae"], errors="coerce").mean())
    if "time_to_profit_seconds" in group.columns:
        row["time_to_profit_seconds_mean"] = float(
            pd.to_numeric(group["time_to_profit_seconds"], errors="coerce").mean()
        )
    return row


def _build_market_state(quotes: pd.DataFrame) -> dict[tuple[str, str], _MarketState]:
    result: dict[tuple[str, str], _MarketState] = {}
    sorted_quotes = quotes.sort_values([SYMBOL, TRADING_DATE, EVENT_TIME], kind="mergesort")
    for key, group in sorted_quotes.groupby([SYMBOL, TRADING_DATE], sort=False):
        result[(str(key[0]), str(key[1]))] = _MarketState(
            times=_ns_array(group[EVENT_TIME]),
            event_times=group[EVENT_TIME].to_numpy(),
            midquote=_numeric_array(group[MIDQUOTE]),
            microprice=_microprice_array(group),
            bid=_numeric_array(group[BID]),
            ask=_numeric_array(group[ASK]),
            bid_size=_numeric_array(group[BID_SIZE]),
            ask_size=_numeric_array(group[ASK_SIZE]),
            quoted_spread=_numeric_array(group[QUOTED_SPREAD]),
            relative_spread=_numeric_array(group[RELATIVE_SPREAD]),
            quote_revision_bps=_numeric_array(group[QUOTE_REVISION_BPS]),
            quote_event_interval_ms=_numeric_array(group[QUOTE_EVENT_INTERVAL_MS]),
        )
    return result


def _microprice_array(group: pd.DataFrame) -> np.ndarray:
    bid = _numeric_array(group[BID])
    ask = _numeric_array(group[ASK])
    bid_size = _numeric_array(group[BID_SIZE])
    ask_size = _numeric_array(group[ASK_SIZE])
    denominator = bid_size + ask_size
    with np.errstate(divide="ignore", invalid="ignore"):
        microprice = (bid * ask_size + ask * bid_size) / denominator
    microprice[denominator == 0] = np.nan
    return microprice


def _build_trade_state(trades: pd.DataFrame) -> dict[tuple[str, str], _TradeState]:
    if trades.empty:
        return {}
    result: dict[tuple[str, str], _TradeState] = {}
    sorted_trades = trades.sort_values([SYMBOL, TRADING_DATE, EVENT_TIME], kind="mergesort")
    for key, group in sorted_trades.groupby([SYMBOL, TRADING_DATE], sort=False):
        result[(str(key[0]), str(key[1]))] = _TradeState(
            times=_ns_array(group[EVENT_TIME]),
            event_times=group[EVENT_TIME].to_numpy(),
            trade_price=_numeric_array(group["trade_price"]),
            trade_size=_numeric_array(group.get("trade_size", pd.Series(0.0, index=group.index))),
            trade_sign=_numeric_array(group.get("trade_sign", pd.Series(0.0, index=group.index))),
            signed_trade_size=_numeric_array(
                group.get("signed_trade_size", pd.Series(0.0, index=group.index))
            ),
        )
    return result


def _normalize_time_frame(frame: pd.DataFrame | None, column: str) -> pd.DataFrame:
    if frame is None:
        return pd.DataFrame()
    result = frame.copy()
    if column in result.columns and not pd.api.types.is_datetime64_any_dtype(result[column]):
        result[column] = pd.to_datetime(result[column], format="mixed")
    return result


def _ns_array(series: pd.Series) -> np.ndarray:
    return pd.to_datetime(series, format="mixed").astype("int64").to_numpy()


def _numeric_array(series: pd.Series) -> np.ndarray:
    return pd.to_numeric(series, errors="coerce").to_numpy(dtype="float64")


def _timestamp_ns(value: object) -> int:
    return int(pd.Timestamp(value).value)


def _last_index_at_or_before(values: np.ndarray, timestamp_ns: int) -> int | None:
    index = int(np.searchsorted(values, timestamp_ns, side="right") - 1)
    if index < 0:
        return None
    return index


def _safe_divide_scalar(numerator: object, denominator: object) -> float:
    numerator_value = _as_float_or_nan(numerator)
    denominator_value = _as_float_or_nan(denominator)
    if np.isnan(numerator_value) or np.isnan(denominator_value) or denominator_value == 0:
        return np.nan
    return numerator_value / denominator_value


def _as_float_or_nan(value: object) -> float:
    try:
        if value is None or pd.isna(value):
            return np.nan
        return float(value)
    except (TypeError, ValueError):
        return np.nan


def _zero_if_nan(value: object) -> float:
    numeric = _as_float_or_nan(value)
    return 0.0 if np.isnan(numeric) else numeric


def _validate_config(config: MicrostructureDiagnosticsConfig) -> None:
    if not config.horizons:
        raise MicrostructureDiagnosticsError("At least one horizon is required.")
    if not config.cost_stress_multipliers:
        raise MicrostructureDiagnosticsError("At least one cost stress multiplier is required.")
    if any(value < 0 for value in config.cost_stress_multipliers):
        raise MicrostructureDiagnosticsError("Cost stress multipliers must be non-negative.")
    pd.Timedelta(config.trailing_window)
    pd.Timedelta(config.passive_entry_timeout)
    for horizon in config.horizons:
        pd.Timedelta(horizon)


def _validate_ledger(ledger: pd.DataFrame) -> None:
    required = {
        EVENT_TIME,
        SYMBOL,
        TRADING_DATE,
        "previous_position",
        "target_position",
        "fill_midquote",
        "event_cost",
        "fold_id",
    }
    missing = sorted(required - set(ledger.columns))
    if missing:
        raise MicrostructureDiagnosticsError(f"Ledger is missing columns: {missing}")


def _validate_quote_features(quote_features: pd.DataFrame) -> None:
    required = {
        EVENT_TIME,
        SYMBOL,
        TRADING_DATE,
        BID,
        ASK,
        BID_SIZE,
        ASK_SIZE,
        MIDQUOTE,
        QUOTED_SPREAD,
        RELATIVE_SPREAD,
        QUOTE_REVISION_BPS,
        QUOTE_EVENT_INTERVAL_MS,
    }
    missing = sorted(required - set(quote_features.columns))
    if missing:
        raise MicrostructureDiagnosticsError(
            f"Quote feature rows are missing columns: {missing}"
        )
