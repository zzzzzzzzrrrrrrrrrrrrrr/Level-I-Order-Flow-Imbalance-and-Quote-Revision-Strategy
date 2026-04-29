"""Cleaning rules and diagnostics for normalized quote data."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from ..schema import ASK, ASK_SIZE, BID, BID_SIZE, validate_quote_frame


@dataclass(frozen=True)
class QuoteHardConstraintDiagnostics:
    """Row-count diagnostics for hard quote constraints."""

    input_rows: int
    removed_nonpositive_bid_rows: int
    removed_nonpositive_ask_rows: int
    removed_negative_bid_size_rows: int
    removed_negative_ask_size_rows: int
    removed_crossed_quote_rows: int
    output_rows: int


@dataclass(frozen=True)
class QuoteQualityWarnings:
    """Summary counts for soft quote-quality warnings."""

    input_rows: int
    wide_relative_spread_rows: int
    zero_depth_rows: int
    locked_quote_rows: int


def filter_quote_hard_constraints(
    quotes: pd.DataFrame,
) -> tuple[pd.DataFrame, QuoteHardConstraintDiagnostics]:
    """Filter rows that violate hard Level-I quote constraints."""

    validate_quote_frame(quotes)

    input_rows = len(quotes)

    positive_bid = quotes.loc[quotes[BID] > 0].reset_index(drop=True)
    removed_nonpositive_bid_rows = input_rows - len(positive_bid)

    positive_ask = positive_bid.loc[positive_bid[ASK] > 0].reset_index(drop=True)
    removed_nonpositive_ask_rows = len(positive_bid) - len(positive_ask)

    nonnegative_bid_size = positive_ask.loc[positive_ask[BID_SIZE] >= 0].reset_index(drop=True)
    removed_negative_bid_size_rows = len(positive_ask) - len(nonnegative_bid_size)

    nonnegative_ask_size = nonnegative_bid_size.loc[
        nonnegative_bid_size[ASK_SIZE] >= 0
    ].reset_index(drop=True)
    removed_negative_ask_size_rows = len(nonnegative_bid_size) - len(nonnegative_ask_size)

    noncrossed = nonnegative_ask_size.loc[
        nonnegative_ask_size[ASK] >= nonnegative_ask_size[BID]
    ].reset_index(drop=True)
    removed_crossed_quote_rows = len(nonnegative_ask_size) - len(noncrossed)

    diagnostics = QuoteHardConstraintDiagnostics(
        input_rows=input_rows,
        removed_nonpositive_bid_rows=removed_nonpositive_bid_rows,
        removed_nonpositive_ask_rows=removed_nonpositive_ask_rows,
        removed_negative_bid_size_rows=removed_negative_bid_size_rows,
        removed_negative_ask_size_rows=removed_negative_ask_size_rows,
        removed_crossed_quote_rows=removed_crossed_quote_rows,
        output_rows=len(noncrossed),
    )
    return noncrossed, diagnostics


def summarize_quote_quality_warnings(
    quotes: pd.DataFrame,
    *,
    relative_spread_warn_threshold: float = 0.05,
) -> QuoteQualityWarnings:
    """Summarize soft quote-quality warnings without dropping rows."""

    validate_quote_frame(quotes)

    midquote = (quotes[BID] + quotes[ASK]) / 2.0
    relative_spread = (quotes[ASK] - quotes[BID]) / midquote

    zero_depth_rows = ((quotes[BID_SIZE] == 0) | (quotes[ASK_SIZE] == 0)).sum()
    locked_quote_rows = (quotes[ASK] == quotes[BID]).sum()
    wide_relative_spread_rows = (relative_spread > relative_spread_warn_threshold).sum()

    return QuoteQualityWarnings(
        input_rows=len(quotes),
        wide_relative_spread_rows=int(wide_relative_spread_rows),
        zero_depth_rows=int(zero_depth_rows),
        locked_quote_rows=int(locked_quote_rows),
    )
