"""Cleaning rules and diagnostics for normalized trade data."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from ..schema import TRADE_PRICE, TRADE_SIZE, validate_trade_frame


@dataclass(frozen=True)
class TradeHardConstraintDiagnostics:
    """Row-count diagnostics for hard trade constraints."""

    input_rows: int
    removed_nonpositive_trade_price_rows: int
    removed_nonpositive_trade_size_rows: int
    output_rows: int


@dataclass(frozen=True)
class TradeQualityWarnings:
    """Summary counts for soft trade-quality warnings."""

    input_rows: int
    large_trade_size_rows: int
    size_quantile: float
    size_threshold_value: float | None


def filter_trade_hard_constraints(
    trades: pd.DataFrame,
) -> tuple[pd.DataFrame, TradeHardConstraintDiagnostics]:
    """Filter rows that violate hard trade constraints."""

    validate_trade_frame(trades)

    input_rows = len(trades)

    positive_trade_price = trades.loc[trades[TRADE_PRICE] > 0].reset_index(drop=True)
    removed_nonpositive_trade_price_rows = input_rows - len(positive_trade_price)

    positive_trade_size = positive_trade_price.loc[
        positive_trade_price[TRADE_SIZE] > 0
    ].reset_index(drop=True)
    removed_nonpositive_trade_size_rows = len(positive_trade_price) - len(positive_trade_size)

    diagnostics = TradeHardConstraintDiagnostics(
        input_rows=input_rows,
        removed_nonpositive_trade_price_rows=removed_nonpositive_trade_price_rows,
        removed_nonpositive_trade_size_rows=removed_nonpositive_trade_size_rows,
        output_rows=len(positive_trade_size),
    )
    return positive_trade_size, diagnostics


def summarize_trade_quality_warnings(
    trades: pd.DataFrame,
    *,
    size_quantile: float = 0.999,
) -> TradeQualityWarnings:
    """Summarize soft trade-quality warnings without dropping rows."""

    validate_trade_frame(trades)

    if trades.empty:
        return TradeQualityWarnings(
            input_rows=0,
            large_trade_size_rows=0,
            size_quantile=size_quantile,
            size_threshold_value=None,
        )

    size_threshold_value = float(trades[TRADE_SIZE].quantile(size_quantile))
    large_trade_size_rows = (trades[TRADE_SIZE] > size_threshold_value).sum()

    return TradeQualityWarnings(
        input_rows=len(trades),
        large_trade_size_rows=int(large_trade_size_rows),
        size_quantile=size_quantile,
        size_threshold_value=size_threshold_value,
    )
