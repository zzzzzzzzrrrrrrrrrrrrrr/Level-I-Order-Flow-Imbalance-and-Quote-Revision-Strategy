"""Execution-mode selection for microstructure v2.1."""

from __future__ import annotations

import pandas as pd


def select_execution_mode(
    row: pd.Series,
    *,
    edge_threshold_passed: bool,
    microprice_usage: str,
    adverse_selection_buffer_bps: float,
    safety_margin_bps: float,
) -> str:
    """Select market, passive, or no-trade action from edge and state."""

    predicted_edge = float(row["predicted_edge_bps"])
    full_market_cost = float(row["expected_cost_bps"])
    tradable_edge = float(row["tradable_edge_bps"])
    strong_cutoff = full_market_cost + adverse_selection_buffer_bps + safety_margin_bps
    if predicted_edge > strong_cutoff:
        return "market_entry"
    if tradable_edge <= 0 or not edge_threshold_passed:
        return "no_trade"
    if not passive_state_is_safe(row, microprice_usage=microprice_usage):
        return "no_trade"
    return "passive_entry"


def passive_state_is_safe(row: pd.Series, *, microprice_usage: str) -> bool:
    """Evaluate whether a passive order can be submitted under a microprice policy."""

    if microprice_usage == "entry_gate":
        return bool(row.get("microprice_aligned", False))
    if microprice_usage in {
        "cancellation_only",
        "leaning_or_adverse_selection_score",
    }:
        return True
    raise ValueError(f"Unknown microprice_usage: {microprice_usage}")
