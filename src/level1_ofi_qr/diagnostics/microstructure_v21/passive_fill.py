"""Conservative passive-fill utilities for microstructure v2.1."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class MarketArrays:
    """Sorted quote/trade arrays for one symbol-date."""

    quote_time_ns: np.ndarray
    bid: np.ndarray
    ask: np.ndarray
    midquote: np.ndarray
    microprice_gap_bps: np.ndarray
    quote_revision_bps: np.ndarray
    quoted_spread: np.ndarray
    trade_time_ns: np.ndarray
    trade_price: np.ndarray


@dataclass(frozen=True)
class PassiveFillResult:
    """Passive-fill result for one submitted order."""

    filled: bool
    fill_time_ns: int | None
    fill_price: float | None
    fill_evidence: str


def find_passive_fill(
    *,
    side: int,
    submission_time_ns: int,
    limit_price: float,
    cancel_time_ns: int,
    market: MarketArrays,
    queue_haircut: str,
    tick_size: float,
) -> PassiveFillResult:
    """Find the first passive fill strictly after order submission."""

    if cancel_time_ns <= submission_time_ns:
        return PassiveFillResult(False, None, None, "not_enough_time")
    quote_start = np.searchsorted(market.quote_time_ns, submission_time_ns, side="right")
    quote_end = np.searchsorted(market.quote_time_ns, cancel_time_ns, side="right")
    trade_start = np.searchsorted(market.trade_time_ns, submission_time_ns, side="right")
    trade_end = np.searchsorted(market.trade_time_ns, cancel_time_ns, side="right")

    candidates: list[tuple[int, str]] = []
    trade_prices = market.trade_price[trade_start:trade_end]
    if len(trade_prices):
        if side > 0:
            trade_hits = np.flatnonzero(trade_prices <= limit_price)
        else:
            trade_hits = np.flatnonzero(trade_prices >= limit_price)
        if len(trade_hits):
            candidates.append(
                (int(market.trade_time_ns[trade_start + trade_hits[0]]), "trade_cross")
            )

    quote_bid = market.bid[quote_start:quote_end]
    quote_ask = market.ask[quote_start:quote_end]
    if queue_haircut == "conservative":
        pass
    elif queue_haircut == "base":
        quote_hits = (
            np.flatnonzero(quote_ask < limit_price - tick_size / 2.0)
            if side > 0
            else np.flatnonzero(quote_bid > limit_price + tick_size / 2.0)
        )
        if len(quote_hits):
            candidates.append(
                (int(market.quote_time_ns[quote_start + quote_hits[0]]), "quote_through")
            )
    elif queue_haircut == "optimistic":
        quote_hits = (
            np.flatnonzero(quote_ask <= limit_price)
            if side > 0
            else np.flatnonzero(quote_bid >= limit_price)
        )
        if len(quote_hits):
            candidates.append(
                (int(market.quote_time_ns[quote_start + quote_hits[0]]), "quote_touch")
            )
    else:
        raise ValueError(f"Unknown queue_haircut: {queue_haircut}")

    if not candidates:
        return PassiveFillResult(False, None, None, "no_cross")
    fill_time_ns, evidence = min(candidates, key=lambda item: item[0])
    return PassiveFillResult(True, fill_time_ns, limit_price, evidence)


def market_entry_price(*, side: int, quote_index: int, market: MarketArrays) -> float:
    """Aggressive entry price at the prevailing top of book."""

    return float(market.ask[quote_index] if side > 0 else market.bid[quote_index])


def market_exit_price(*, side: int, quote_index: int, market: MarketArrays) -> float:
    """Aggressive exit price at the prevailing top of book."""

    return float(market.bid[quote_index] if side > 0 else market.ask[quote_index])


def passive_exit_price(*, side: int, quote_index: int, market: MarketArrays) -> float:
    """Passive exit limit price at the favorable side."""

    return float(market.ask[quote_index] if side > 0 else market.bid[quote_index])


def quote_index_at_or_before(time_ns: int, market: MarketArrays) -> int | None:
    """Return quote index at or before a timestamp."""

    index = int(np.searchsorted(market.quote_time_ns, time_ns, side="right") - 1)
    if index < 0:
        return None
    return index


def quote_index_at_or_after(time_ns: int, market: MarketArrays) -> int | None:
    """Return quote index at or after a timestamp."""

    index = int(np.searchsorted(market.quote_time_ns, time_ns, side="left"))
    if index >= len(market.quote_time_ns):
        return None
    return index


def to_ns(value: object) -> int:
    """Convert timestamp-like value to nanoseconds."""

    return int(pd.Timestamp(value).value)
