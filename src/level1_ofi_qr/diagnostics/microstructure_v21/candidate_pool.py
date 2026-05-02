"""Candidate event construction for microstructure v2.1 diagnostics."""

from __future__ import annotations

import numpy as np
import pandas as pd

from ...alignment import TRADING_DATE
from ...models import MODEL_SCORE_COLUMN
from ...schema import EVENT_TIME, SYMBOL


def microprice(
    bid: pd.Series,
    ask: pd.Series,
    bid_size: pd.Series,
    ask_size: pd.Series,
) -> pd.Series:
    """Compute Level-I microprice from best prices and displayed sizes."""

    bid_numeric = pd.to_numeric(bid, errors="coerce")
    ask_numeric = pd.to_numeric(ask, errors="coerce")
    bid_size_numeric = pd.to_numeric(bid_size, errors="coerce")
    ask_size_numeric = pd.to_numeric(ask_size, errors="coerce")
    denominator = bid_size_numeric + ask_size_numeric
    result = (bid_numeric * ask_size_numeric + ask_numeric * bid_size_numeric) / denominator
    return result.where(denominator != 0)


def build_candidate_events(
    prediction_rows: pd.DataFrame,
    quote_state: pd.DataFrame,
    *,
    tick_size: float,
) -> pd.DataFrame:
    """Build submitted-order candidates from positive-edge side-change events.

    This keeps v2.1 as a frequency-expansion diagnostic without submitting an
    order for every row in the full signal file.
    """

    rows = prediction_rows.copy()
    rows[EVENT_TIME] = pd.to_datetime(rows[EVENT_TIME], format="mixed")
    rows = rows.sort_values([SYMBOL, TRADING_DATE, EVENT_TIME], kind="mergesort")
    rows["predicted_edge_bps"] = pd.to_numeric(rows[MODEL_SCORE_COLUMN], errors="coerce").abs()
    rows["side"] = np.sign(
        pd.to_numeric(rows[MODEL_SCORE_COLUMN], errors="coerce").fillna(0.0)
    ).astype(int)
    rows["expected_cost_bps"] = pd.to_numeric(
        rows["cost_aware_estimated_cost_bps"],
        errors="coerce",
    ).fillna(0.0)
    rows["tradable_edge_bps"] = rows["predicted_edge_bps"] - rows["expected_cost_bps"]
    rows["selected_threshold_numeric"] = pd.to_numeric(
        rows.get("selected_threshold", np.nan),
        errors="coerce",
    )
    rows["desired_side"] = rows["side"].where(rows["tradable_edge_bps"] > 0, 0)
    groups = rows.groupby([SYMBOL, TRADING_DATE], sort=False)
    previous_side = groups["desired_side"].shift(1).fillna(0).astype(int)
    candidates = rows.loc[
        (rows["desired_side"] != 0) & (rows["desired_side"] != previous_side)
    ].copy()
    if candidates.empty:
        return candidates

    candidates = attach_quote_state(candidates, quote_state)
    candidates["signal_id"] = [
        f"v21_signal_{index:08d}" for index in range(1, len(candidates) + 1)
    ]
    candidates["spread_bps"] = (
        candidates["quoted_spread"] / candidates["midquote"] * 10000.0
    )
    candidates["microprice_gap"] = candidates["microprice"] - candidates["midquote"]
    candidates["microprice_gap_bps"] = (
        candidates["microprice_gap"] / candidates["midquote"] * 10000.0
    )
    candidates["microprice_aligned"] = (
        pd.to_numeric(candidates["side"], errors="coerce")
        * pd.to_numeric(candidates["microprice_gap_bps"], errors="coerce")
        > 0
    )
    candidates["one_tick_spread"] = candidates["quoted_spread"] <= tick_size + 1e-12
    candidates["displayed_depth"] = candidates["bid_size"] + candidates["ask_size"]
    return candidates


def attach_quote_state(candidates: pd.DataFrame, quote_state: pd.DataFrame) -> pd.DataFrame:
    """Attach latest quote state at or before each candidate timestamp."""

    frames = []
    quote_state = quote_state.sort_values([SYMBOL, TRADING_DATE, EVENT_TIME], kind="mergesort")
    for key, candidate_group in candidates.groupby([SYMBOL, TRADING_DATE], sort=False):
        quote_group = quote_state.loc[
            (quote_state[SYMBOL] == key[0]) & (quote_state[TRADING_DATE] == key[1])
        ].sort_values(EVENT_TIME, kind="mergesort")
        if quote_group.empty:
            continue
        merged = pd.merge_asof(
            candidate_group.sort_values(EVENT_TIME, kind="mergesort"),
            quote_group.sort_values(EVENT_TIME, kind="mergesort"),
            on=EVENT_TIME,
            direction="backward",
            suffixes=("", "_quote"),
        )
        if f"{SYMBOL}_quote" in merged.columns or f"{TRADING_DATE}_quote" in merged.columns:
            merged = merged.drop(
                columns=[f"{SYMBOL}_quote", f"{TRADING_DATE}_quote"],
                errors="ignore",
            )
        frames.append(merged)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def spread_thresholds(validation_rows: pd.DataFrame) -> dict[str, float]:
    """Compute spread quantiles from validation/calibration rows only."""

    spreads = pd.to_numeric(validation_rows["quoted_spread"], errors="coerce").dropna()
    if spreads.empty:
        return {"q1": np.inf, "q2": np.inf}
    return {
        "q1": float(spreads.quantile(0.25)),
        "q2": float(spreads.quantile(0.50)),
    }


def candidate_pool_mask(
    rows: pd.DataFrame,
    *,
    candidate_pool: str,
    thresholds: dict[str, float],
    tick_size: float,
    min_depth: float,
) -> pd.Series:
    """Return rows included by a named candidate-pool policy."""

    spread = pd.to_numeric(rows["quoted_spread"], errors="coerce")
    depth = pd.to_numeric(rows["displayed_depth"], errors="coerce")
    if candidate_pool == "spread_q1":
        return spread <= thresholds["q1"]
    if candidate_pool == "spread_q1_or_q2":
        return spread <= thresholds["q2"]
    if candidate_pool == "one_tick_spread":
        return spread <= tick_size + 1e-12
    if candidate_pool == "one_tick_spread_with_min_depth":
        return (spread <= tick_size + 1e-12) & (depth >= min_depth)
    raise ValueError(f"Unknown candidate_pool: {candidate_pool}")


def edge_threshold_mask(rows: pd.DataFrame, *, edge_threshold: str) -> pd.Series:
    """Return rows passing a named tradable-edge threshold."""

    edge = pd.to_numeric(rows["tradable_edge_bps"], errors="coerce")
    predicted = pd.to_numeric(rows["predicted_edge_bps"], errors="coerce")
    selected = pd.to_numeric(rows["selected_threshold_numeric"], errors="coerce")
    if edge_threshold == "edge_gt_0":
        return edge > 0
    if edge_threshold == "edge_gt_0p25":
        return edge > 0.25
    if edge_threshold == "edge_gt_0p50":
        return edge > 0.50
    if edge_threshold == "existing_threshold":
        return predicted >= selected
    raise ValueError(f"Unknown edge_threshold: {edge_threshold}")
