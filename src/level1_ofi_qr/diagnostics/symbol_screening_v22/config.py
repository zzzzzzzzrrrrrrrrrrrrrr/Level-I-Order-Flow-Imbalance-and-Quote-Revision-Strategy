"""Configuration for v2.2 cross-symbol screening diagnostics."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping


@dataclass(frozen=True)
class SymbolScreenV22Config:
    """Config for cheap symbol-level alpha kill-tests."""

    universe_name: str = "configured_universe"
    date_window_name: str | None = None
    date_window_start: str | None = None
    date_window_end: str | None = None
    date_window_purpose: str | None = None
    expected_trading_dates: tuple[str, ...] = ()
    session_filter: str | None = None
    symbol_metadata: Mapping[str, Mapping[str, str]] = field(default_factory=dict)
    horizons: tuple[str, ...] = ("1s", "5s", "10s", "30s", "60s")
    decile_horizons: tuple[str, ...] = ("1s", "5s")
    signal_buckets: tuple[str, ...] = ("all", "top10pct_score", "top5pct_score", "top1pct_score")
    validation_min_dates: int = 2
    pass_move_over_cost: float = 1.0
    strong_pass_move_over_cost: float = 1.5
    fail_move_over_cost: float = 0.5
    candidate_source: str = "auto"
