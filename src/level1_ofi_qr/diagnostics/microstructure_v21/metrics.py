"""Metrics for microstructure v2.1 diagnostics."""

from __future__ import annotations

import numpy as np
import pandas as pd


REQUIRED_METRIC_COLUMNS = (
    "candidate_signals",
    "submitted_orders",
    "filled_orders",
    "fill_rate",
    "unfilled_rate",
    "gross_pnl",
    "cost",
    "net_pnl",
    "net_pnl_per_filled_order",
    "net_pnl_per_submitted_order",
    "daily_net_pnl_mean",
    "daily_net_pnl_std",
    "daily_sharpe",
    "max_daily_loss",
    "post_fill_mid_move_100ms_bps",
    "post_fill_mid_move_500ms_bps",
    "post_fill_mid_move_1s_bps",
    "post_fill_mid_move_5s_bps",
    "realized_spread_bps",
    "adverse_selection_bps",
    "unfilled_opportunity_cost",
)


def summarize_orders(orders: pd.DataFrame, *, group_columns: tuple[str, ...]) -> pd.DataFrame:
    """Summarize submitted and filled-order diagnostics by variant."""

    rows = []
    if orders.empty:
        return pd.DataFrame(columns=(*group_columns, *REQUIRED_METRIC_COLUMNS))
    for key, group in orders.groupby(list(group_columns), sort=False, dropna=False):
        key_values = key if isinstance(key, tuple) else (key,)
        row = dict(zip(group_columns, key_values, strict=True))
        row.update(_metric_row(group))
        rows.append(row)
    return pd.DataFrame(rows)


def _metric_row(group: pd.DataFrame) -> dict[str, object]:
    submitted = len(group)
    filled_group = group.loc[group["filled"] == True]
    filled = len(filled_group)
    gross = float(pd.to_numeric(filled_group["gross_pnl"], errors="coerce").fillna(0.0).sum())
    cost = float(pd.to_numeric(filled_group["cost"], errors="coerce").fillna(0.0).sum())
    net = float(pd.to_numeric(filled_group["net_pnl"], errors="coerce").fillna(0.0).sum())
    daily = (
        filled_group.groupby("trading_date", sort=False)["net_pnl"].sum()
        if not filled_group.empty
        else pd.Series(dtype=float)
    )
    daily_std = float(daily.std(ddof=0)) if len(daily) else 0.0
    daily_mean = float(daily.mean()) if len(daily) else 0.0
    return {
        "candidate_signals": int(group["candidate_signal"].sum()),
        "submitted_orders": submitted,
        "filled_orders": filled,
        "fill_rate": _safe_ratio(filled, submitted),
        "unfilled_rate": 1.0 - _safe_ratio(filled, submitted) if submitted else None,
        "gross_pnl": gross,
        "cost": cost,
        "net_pnl": net,
        "net_pnl_per_filled_order": _safe_ratio(net, filled),
        "net_pnl_per_submitted_order": _safe_ratio(net, submitted),
        "daily_net_pnl_mean": daily_mean,
        "daily_net_pnl_std": daily_std,
        "daily_sharpe": None if daily_std == 0 else daily_mean / daily_std,
        "max_daily_loss": float(daily.min()) if len(daily) else 0.0,
        "post_fill_mid_move_100ms_bps": _mean(filled_group, "post_fill_mid_move_100ms_bps"),
        "post_fill_mid_move_500ms_bps": _mean(filled_group, "post_fill_mid_move_500ms_bps"),
        "post_fill_mid_move_1s_bps": _mean(filled_group, "post_fill_mid_move_1s_bps"),
        "post_fill_mid_move_5s_bps": _mean(filled_group, "post_fill_mid_move_5s_bps"),
        "realized_spread_bps": _mean(filled_group, "realized_spread_bps"),
        "adverse_selection_bps": _mean(filled_group, "adverse_selection_bps"),
        "unfilled_opportunity_cost": float(
            pd.to_numeric(
                group.loc[group["filled"] == False, "unfilled_opportunity_cost"],
                errors="coerce",
            )
            .fillna(0.0)
            .sum()
        ),
    }


def _mean(frame: pd.DataFrame, column: str) -> float | None:
    if frame.empty or column not in frame.columns:
        return None
    value = pd.to_numeric(frame[column], errors="coerce").mean()
    return None if pd.isna(value) else float(value)


def _safe_ratio(numerator: float, denominator: float) -> float | None:
    if denominator == 0:
        return None
    return float(numerator) / float(denominator)
