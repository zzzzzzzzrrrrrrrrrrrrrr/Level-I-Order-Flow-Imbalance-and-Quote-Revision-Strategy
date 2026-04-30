"""Trade signing v1 for aligned Level-I trade rows."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

import pandas as pd

from ..alignment import IS_QUOTE_MATCHED, QUOTE_ASK, QUOTE_BID, TRADING_DATE
from ..schema import (
    EVENT_TIME,
    SYMBOL,
    TRADE_PRICE,
    TRADE_SEQUENCE_NUMBER,
    TRADE_SIZE,
    validate_trade_frame,
)

MATCHED_MIDQUOTE: Final[str] = "matched_midquote"
QUOTE_RULE_SIGN: Final[str] = "quote_rule_sign"
TICK_RULE_SIGN: Final[str] = "tick_rule_sign"
TRADE_SIGN: Final[str] = "trade_sign"
TRADE_SIGN_SOURCE: Final[str] = "trade_sign_source"
SIGNED_TRADE_SIZE: Final[str] = "signed_trade_size"

TRADE_SIGN_COLUMNS: Final[tuple[str, ...]] = (
    MATCHED_MIDQUOTE,
    QUOTE_RULE_SIGN,
    TICK_RULE_SIGN,
    TRADE_SIGN,
    TRADE_SIGN_SOURCE,
    SIGNED_TRADE_SIZE,
)

TRADE_SIGNING_SCOPE_NOTE: Final[str] = (
    "Trade signing v1 classifies trade direction using quote rule with "
    "tick-rule fallback on aligned trade rows. It does not apply sale-condition "
    "filters, NBBO condition filters, OFI aggregation, labels, or backtest "
    "signals."
)
PRICE_COMPARISON_EPSILON: Final[float] = 1e-12

REQUIRED_ALIGNED_TRADE_COLUMNS: Final[tuple[str, ...]] = (
    TRADING_DATE,
    IS_QUOTE_MATCHED,
    QUOTE_BID,
    QUOTE_ASK,
)


class TradeSigningError(ValueError):
    """Raised when trade signing cannot be completed."""


@dataclass(frozen=True)
class TradeSigningDiagnostics:
    """Diagnostics for trade signing v1."""

    input_aligned_trade_rows: int
    output_signed_trade_rows: int
    row_preserving: bool
    trade_signing_method: str
    trade_sign_columns: tuple[str, ...]
    trade_sign_group_keys: tuple[str, str]
    quote_matched_rows: int
    quote_unmatched_rows: int
    quote_rule_signed_rows: int
    tick_rule_signed_rows: int
    tick_rule_available_rows: int
    unknown_sign_rows: int
    unknown_sign_ratio: float
    buy_sign_rows: int
    sell_sign_rows: int
    zero_sign_rows: int
    quote_midpoint_tie_rows: int
    matched_midquote_null_rows: int
    quote_tick_conflict_rows: int
    condition_filters_applied: bool = False
    sale_condition_filters_applied: bool = False
    nbbo_quote_condition_filters_applied: bool = False
    ofi_features_implemented: bool = False
    labels_implemented: bool = False
    backtest_implemented: bool = False
    research_grade_signed_sample: bool = False


@dataclass(frozen=True)
class TradeSigningResult:
    """Signed trade rows and diagnostics."""

    signed_trades: pd.DataFrame
    diagnostics: TradeSigningDiagnostics


def build_trade_signs_v1(aligned_trades: pd.DataFrame) -> TradeSigningResult:
    """Classify trade direction without dropping aligned trade rows."""

    _validate_aligned_trade_frame(aligned_trades)

    signed = aligned_trades.copy()
    signed["_trade_order"] = range(len(signed))
    signed[MATCHED_MIDQUOTE] = _safe_midquote(signed)
    signed[QUOTE_RULE_SIGN] = _quote_rule_sign(signed)
    signed[TICK_RULE_SIGN] = _tick_rule_sign(signed)

    quote_rule_mask = signed[QUOTE_RULE_SIGN].notna()
    tick_rule_mask = ~quote_rule_mask & signed[TICK_RULE_SIGN].notna()

    final_sign = pd.Series(0, index=signed.index, dtype="int64")
    final_sign.loc[quote_rule_mask] = signed.loc[quote_rule_mask, QUOTE_RULE_SIGN].astype("int64")
    final_sign.loc[tick_rule_mask] = signed.loc[tick_rule_mask, TICK_RULE_SIGN].astype("int64")
    signed[TRADE_SIGN] = final_sign

    source = pd.Series("unknown", index=signed.index, dtype="string")
    source.loc[quote_rule_mask] = "quote_rule"
    source.loc[tick_rule_mask] = "tick_rule"
    signed[TRADE_SIGN_SOURCE] = source
    signed[SIGNED_TRADE_SIZE] = signed[TRADE_SIGN] * pd.to_numeric(
        signed[TRADE_SIZE],
        errors="coerce",
    )

    diagnostics = _build_diagnostics(
        signed,
        quote_rule_mask=quote_rule_mask,
        tick_rule_mask=tick_rule_mask,
        input_rows=len(aligned_trades),
    )
    signed = signed.drop(columns=["_trade_order"])
    signed = _order_signed_columns(signed, tuple(aligned_trades.columns))
    return TradeSigningResult(signed_trades=signed, diagnostics=diagnostics)


def _validate_aligned_trade_frame(aligned_trades: pd.DataFrame) -> None:
    validate_trade_frame(aligned_trades)
    missing_columns = [
        column for column in REQUIRED_ALIGNED_TRADE_COLUMNS if column not in aligned_trades.columns
    ]
    if missing_columns:
        raise TradeSigningError(
            "Trade signing v1 requires quote-trade alignment columns. "
            f"Missing columns: {missing_columns}"
        )
    if not pd.api.types.is_datetime64_any_dtype(aligned_trades[EVENT_TIME]):
        raise TradeSigningError("Aligned trades must have datetime event_time values.")


def _safe_midquote(frame: pd.DataFrame) -> pd.Series:
    bid = pd.to_numeric(frame[QUOTE_BID], errors="coerce")
    ask = pd.to_numeric(frame[QUOTE_ASK], errors="coerce")
    return (bid + ask) / 2.0


def _quote_rule_sign(frame: pd.DataFrame) -> pd.Series:
    trade_price = pd.to_numeric(frame[TRADE_PRICE], errors="coerce")
    midquote = frame[MATCHED_MIDQUOTE]
    is_matched = _coerce_bool(frame[IS_QUOTE_MATCHED])
    valid = is_matched & trade_price.notna() & midquote.notna()
    price_vs_midquote = trade_price - midquote

    signs = pd.Series(pd.NA, index=frame.index, dtype="Int64")
    signs.loc[valid & (price_vs_midquote > PRICE_COMPARISON_EPSILON)] = 1
    signs.loc[valid & (price_vs_midquote < -PRICE_COMPARISON_EPSILON)] = -1
    return signs


def _tick_rule_sign(frame: pd.DataFrame) -> pd.Series:
    signs = pd.Series(pd.NA, index=frame.index, dtype="Int64")
    sort_columns = [SYMBOL, TRADING_DATE, EVENT_TIME, TRADE_SEQUENCE_NUMBER, "_trade_order"]
    sorted_frame = frame.sort_values(sort_columns, kind="mergesort")

    for _, group in sorted_frame.groupby([SYMBOL, TRADING_DATE], sort=False):
        prices = pd.to_numeric(group[TRADE_PRICE], errors="coerce")
        price_change = prices.diff()
        group_signs = pd.Series(pd.NA, index=group.index, dtype="Int64")
        group_signs.loc[price_change > 0] = 1
        group_signs.loc[price_change < 0] = -1
        group_signs = group_signs.ffill()
        signs.loc[group.index] = group_signs

    return signs


def _build_diagnostics(
    signed: pd.DataFrame,
    *,
    quote_rule_mask: pd.Series,
    tick_rule_mask: pd.Series,
    input_rows: int,
) -> TradeSigningDiagnostics:
    is_matched = _coerce_bool(signed[IS_QUOTE_MATCHED])
    final_sign = signed[TRADE_SIGN]
    trade_price = pd.to_numeric(signed[TRADE_PRICE], errors="coerce")
    valid_midquote = is_matched & trade_price.notna() & signed[MATCHED_MIDQUOTE].notna()
    midpoint_ties = (
        valid_midquote
        & ((trade_price - signed[MATCHED_MIDQUOTE]).abs() <= PRICE_COMPARISON_EPSILON)
    )
    conflict_mask = (
        signed[QUOTE_RULE_SIGN].notna()
        & signed[TICK_RULE_SIGN].notna()
        & (signed[QUOTE_RULE_SIGN] != signed[TICK_RULE_SIGN])
    )
    unknown_sign_rows = int((final_sign == 0).sum())

    return TradeSigningDiagnostics(
        input_aligned_trade_rows=input_rows,
        output_signed_trade_rows=len(signed),
        row_preserving=len(signed) == input_rows,
        trade_signing_method="quote_rule_with_tick_rule_fallback_v1",
        trade_sign_columns=TRADE_SIGN_COLUMNS,
        trade_sign_group_keys=(SYMBOL, TRADING_DATE),
        quote_matched_rows=int(is_matched.sum()),
        quote_unmatched_rows=int((~is_matched).sum()),
        quote_rule_signed_rows=int(quote_rule_mask.sum()),
        tick_rule_signed_rows=int(tick_rule_mask.sum()),
        tick_rule_available_rows=int(signed[TICK_RULE_SIGN].notna().sum()),
        unknown_sign_rows=unknown_sign_rows,
        unknown_sign_ratio=unknown_sign_rows / input_rows if input_rows else 0.0,
        buy_sign_rows=int((final_sign == 1).sum()),
        sell_sign_rows=int((final_sign == -1).sum()),
        zero_sign_rows=unknown_sign_rows,
        quote_midpoint_tie_rows=int(midpoint_ties.sum()),
        matched_midquote_null_rows=int(signed.loc[is_matched, MATCHED_MIDQUOTE].isna().sum()),
        quote_tick_conflict_rows=int(conflict_mask.sum()),
    )


def _coerce_bool(values: pd.Series) -> pd.Series:
    if pd.api.types.is_bool_dtype(values):
        return values.fillna(False).astype(bool)
    if pd.api.types.is_numeric_dtype(values):
        return values.fillna(0).astype(float) != 0
    normalized = values.astype("string").str.strip().str.lower()
    return normalized.isin(["true", "1", "t", "yes", "y"])


def _order_signed_columns(signed: pd.DataFrame, aligned_columns: tuple[str, ...]) -> pd.DataFrame:
    preferred_columns = (*aligned_columns, *TRADE_SIGN_COLUMNS)
    ordered_columns: list[str] = []
    for column in preferred_columns:
        if column in signed.columns and column not in ordered_columns:
            ordered_columns.append(column)
    ordered_columns.extend(column for column in signed.columns if column not in ordered_columns)
    return signed.loc[:, ordered_columns]
