"""Configuration for v2.2 cross-symbol screening diagnostics."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SymbolScreenV22Config:
    """Config for cheap symbol-level alpha kill-tests."""

    universe_name: str = "configured_universe"
    horizons: tuple[str, ...] = ("1s", "5s", "10s", "30s", "60s")
    decile_horizons: tuple[str, ...] = ("1s", "5s")
    signal_buckets: tuple[str, ...] = ("all", "top10pct_score", "top5pct_score", "top1pct_score")
    validation_min_dates: int = 2
    pass_move_over_cost: float = 1.0
    strong_pass_move_over_cost: float = 1.5
    fail_move_over_cost: float = 0.5
    candidate_source: str = "auto"
