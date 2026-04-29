"""Scope filters for symbol, trading date, and regular-market-hours constraints."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, time
from typing import Iterable

import pandas as pd

from ..schema import EVENT_TIME, SYMBOL

REGULAR_MARKET_OPEN = time.fromisoformat("09:30:00")
REGULAR_MARKET_CLOSE = time.fromisoformat("16:00:00")


@dataclass(frozen=True)
class ScopeFilterDiagnostics:
    """Row-count diagnostics for scope-based filtering."""

    input_rows: int
    removed_symbol_rows: int
    removed_date_rows: int
    removed_session_rows: int
    output_rows: int


def filter_frame_to_scope(
    frame: pd.DataFrame,
    *,
    symbols: Iterable[str],
    trading_dates: Iterable[date | str],
    market_open: time = REGULAR_MARKET_OPEN,
    market_close: time = REGULAR_MARKET_CLOSE,
    market_timezone: str = "America/New_York",
) -> tuple[pd.DataFrame, ScopeFilterDiagnostics]:
    """Filter a normalized frame to the configured research slice."""

    symbol_set = {symbol.strip() for symbol in symbols}
    trading_date_set = {
        current if isinstance(current, date) else date.fromisoformat(current)
        for current in trading_dates
    }

    input_rows = len(frame)

    symbol_filtered = frame.loc[frame[SYMBOL].isin(symbol_set)].reset_index(drop=True)
    removed_symbol_rows = input_rows - len(symbol_filtered)

    localized = _localize_timestamps(symbol_filtered[EVENT_TIME], market_timezone)
    date_mask = localized.dt.date.isin(trading_date_set)
    date_filtered = symbol_filtered.loc[date_mask].reset_index(drop=True)
    removed_date_rows = len(symbol_filtered) - len(date_filtered)

    localized_date_filtered = _localize_timestamps(date_filtered[EVENT_TIME], market_timezone)
    session_mask = localized_date_filtered.dt.time.map(
        lambda current: market_open <= current <= market_close
    )
    session_filtered = date_filtered.loc[session_mask].reset_index(drop=True)
    removed_session_rows = len(date_filtered) - len(session_filtered)

    diagnostics = ScopeFilterDiagnostics(
        input_rows=input_rows,
        removed_symbol_rows=removed_symbol_rows,
        removed_date_rows=removed_date_rows,
        removed_session_rows=removed_session_rows,
        output_rows=len(session_filtered),
    )
    return session_filtered, diagnostics


def _localize_timestamps(timestamps: pd.Series, market_timezone: str) -> pd.Series:
    if timestamps.dt.tz is None:
        return timestamps
    return timestamps.dt.tz_convert(market_timezone)
