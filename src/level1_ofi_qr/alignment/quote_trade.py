"""Quote-trade alignment for cleaned Level-I datasets."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

import pandas as pd

from ..schema import (
    ASK,
    ASK_EXCHANGE,
    ASK_SIZE,
    BID,
    BID_EXCHANGE,
    BID_SIZE,
    EVENT_TIME,
    NBBO_QUOTE_CONDITION,
    SOURCE,
    SYMBOL,
    validate_quote_frame,
    validate_trade_frame,
)

RAW_ROW_INDEX: Final[str] = "raw_row_index"
TRADING_DATE: Final[str] = "trading_date"
IS_QUOTE_MATCHED: Final[str] = "is_quote_matched"
MATCHED_QUOTE_EVENT_TIME: Final[str] = "matched_quote_event_time"
MATCHED_QUOTE_TRADING_DATE: Final[str] = "matched_quote_trading_date"
QUOTE_LAG_MS: Final[str] = "quote_lag_ms"
QUOTE_SOURCE: Final[str] = "quote_source"
QUOTE_RAW_ROW_INDEX: Final[str] = "quote_raw_row_index"
QUOTE_BID_EXCHANGE: Final[str] = "quote_bid_exchange"
QUOTE_ASK_EXCHANGE: Final[str] = "quote_ask_exchange"
QUOTE_NBBO_CONDITION: Final[str] = "quote_nbbo_quote_condition"
QUOTE_BID: Final[str] = "quote_bid"
QUOTE_ASK: Final[str] = "quote_ask"
QUOTE_BID_SIZE: Final[str] = "quote_bid_size"
QUOTE_ASK_SIZE: Final[str] = "quote_ask_size"

QUOTE_FIELD_RENAMES: Final[dict[str, str]] = {
    EVENT_TIME: MATCHED_QUOTE_EVENT_TIME,
    TRADING_DATE: MATCHED_QUOTE_TRADING_DATE,
    SOURCE: QUOTE_SOURCE,
    RAW_ROW_INDEX: QUOTE_RAW_ROW_INDEX,
    BID_EXCHANGE: QUOTE_BID_EXCHANGE,
    ASK_EXCHANGE: QUOTE_ASK_EXCHANGE,
    NBBO_QUOTE_CONDITION: QUOTE_NBBO_CONDITION,
    BID: QUOTE_BID,
    ASK: QUOTE_ASK,
    BID_SIZE: QUOTE_BID_SIZE,
    ASK_SIZE: QUOTE_ASK_SIZE,
}

ALIGNED_QUOTE_COLUMNS: Final[tuple[str, ...]] = (
    TRADING_DATE,
    IS_QUOTE_MATCHED,
    MATCHED_QUOTE_EVENT_TIME,
    MATCHED_QUOTE_TRADING_DATE,
    QUOTE_LAG_MS,
    QUOTE_SOURCE,
    QUOTE_RAW_ROW_INDEX,
    QUOTE_BID_EXCHANGE,
    QUOTE_ASK_EXCHANGE,
    QUOTE_NBBO_CONDITION,
    QUOTE_BID,
    QUOTE_ASK,
    QUOTE_BID_SIZE,
    QUOTE_ASK_SIZE,
)


class AlignmentError(ValueError):
    """Raised when quote-trade alignment cannot be completed."""


@dataclass(frozen=True)
class QuoteTradeAlignmentDiagnostics:
    """Diagnostics for strict prior-quote alignment."""

    input_trade_rows: int
    input_quote_rows: int
    aligned_trade_rows: int
    matched_trade_rows: int
    unmatched_trade_rows: int
    matched_ratio: float
    allow_exact_matches: bool
    tolerance: str | None
    tolerance_policy: str
    min_quote_lag_ms: float | None
    median_quote_lag_ms: float | None
    p95_quote_lag_ms: float | None
    p99_quote_lag_ms: float | None
    max_quote_lag_ms: float | None
    matched_locked_quote_count: int
    matched_locked_quote_ratio: float
    session_boundary_policy: str = "same_symbol_same_trading_date_only"
    alignment_group_keys: tuple[str, str] = (SYMBOL, TRADING_DATE)
    cross_session_match_count: int = 0
    alignment_rule: str = "latest_quote_strictly_before_trade"
    condition_filters_applied: bool = False
    trade_signing_applied: bool = False


@dataclass(frozen=True)
class QuoteTradeAlignmentResult:
    """Aligned trade rows and alignment diagnostics."""

    aligned_trades: pd.DataFrame
    diagnostics: QuoteTradeAlignmentDiagnostics


def align_trades_to_prior_quotes(
    trades: pd.DataFrame,
    quotes: pd.DataFrame,
    *,
    tolerance: str | pd.Timedelta | None = None,
    market_timezone: str = "America/New_York",
) -> QuoteTradeAlignmentResult:
    """Attach the latest quote strictly before each trade.

    The function uses cleaned quote/trade inputs and does not apply condition-code
    filters or trade signing. Trades with no strictly prior quote remain in the
    output with null quote fields.
    """

    validate_trade_frame(trades)
    validate_quote_frame(quotes)
    _validate_comparable_event_time_dtypes(trades, quotes)

    tolerance_delta = _coerce_tolerance(tolerance)
    trade_work = _add_trading_date(trades.copy(), market_timezone=market_timezone)
    trade_work["_trade_order"] = range(len(trade_work))
    quote_work = _add_trading_date(quotes.copy(), market_timezone=market_timezone)
    quote_payload = _build_quote_payload(quote_work)

    aligned_parts: list[pd.DataFrame] = []
    for (symbol, trading_date), trade_group in trade_work.groupby(
        [SYMBOL, TRADING_DATE],
        sort=False,
    ):
        trade_group = trade_group.sort_values(
            [EVENT_TIME, "_trade_order"],
            kind="mergesort",
        )
        quote_group = quote_payload.loc[
            (quote_payload[SYMBOL] == symbol)
            & (quote_payload[MATCHED_QUOTE_TRADING_DATE] == trading_date)
        ].drop(
            columns=[SYMBOL],
        )
        quote_group = quote_group.sort_values(MATCHED_QUOTE_EVENT_TIME, kind="mergesort")

        aligned_group = pd.merge_asof(
            trade_group,
            quote_group,
            left_on=EVENT_TIME,
            right_on=MATCHED_QUOTE_EVENT_TIME,
            direction="backward",
            allow_exact_matches=False,
            tolerance=tolerance_delta,
        )
        aligned_parts.append(aligned_group)

    if aligned_parts:
        aligned = pd.concat(aligned_parts, ignore_index=True)
        aligned = aligned.sort_values("_trade_order", kind="mergesort").reset_index(drop=True)
        aligned = aligned.drop(columns=["_trade_order"])
    else:
        aligned = _empty_aligned_frame(trades, quote_payload)

    aligned = _add_match_and_lag_fields(aligned)
    aligned = _order_aligned_columns(aligned, tuple(trades.columns))
    diagnostics = _build_diagnostics(
        aligned,
        input_quote_rows=len(quotes),
        input_trade_rows=len(trades),
        tolerance_delta=tolerance_delta,
    )
    return QuoteTradeAlignmentResult(aligned_trades=aligned, diagnostics=diagnostics)


def _build_quote_payload(quotes: pd.DataFrame) -> pd.DataFrame:
    quote_columns = [
        SYMBOL,
        TRADING_DATE,
        EVENT_TIME,
        SOURCE,
        RAW_ROW_INDEX,
        BID_EXCHANGE,
        ASK_EXCHANGE,
        NBBO_QUOTE_CONDITION,
        BID,
        ASK,
        BID_SIZE,
        ASK_SIZE,
    ]
    available_columns = [column for column in quote_columns if column in quotes.columns]
    return quotes.loc[:, available_columns].rename(columns=QUOTE_FIELD_RENAMES)


def _add_trading_date(frame: pd.DataFrame, *, market_timezone: str) -> pd.DataFrame:
    frame = frame.copy()
    timestamps = frame[EVENT_TIME]
    if timestamps.dt.tz is None:
        localized = timestamps.dt.tz_localize(market_timezone)
    else:
        localized = timestamps.dt.tz_convert(market_timezone)
    frame[TRADING_DATE] = localized.dt.strftime("%Y-%m-%d")
    return frame


def _add_match_and_lag_fields(aligned: pd.DataFrame) -> pd.DataFrame:
    aligned = aligned.copy()
    aligned[IS_QUOTE_MATCHED] = aligned[MATCHED_QUOTE_EVENT_TIME].notna()
    lag = aligned[EVENT_TIME] - aligned[MATCHED_QUOTE_EVENT_TIME]
    aligned[QUOTE_LAG_MS] = lag.dt.total_seconds() * 1000.0
    nonpositive_lag = aligned.loc[aligned[IS_QUOTE_MATCHED], QUOTE_LAG_MS] <= 0
    if bool(nonpositive_lag.any()):
        raise AlignmentError(
            "Matched quote lags must be strictly positive because exact timestamp "
            "matches are disabled."
        )
    return aligned


def _build_diagnostics(
    aligned: pd.DataFrame,
    *,
    input_quote_rows: int,
    input_trade_rows: int,
    tolerance_delta: pd.Timedelta | None,
) -> QuoteTradeAlignmentDiagnostics:
    matched_mask = aligned[IS_QUOTE_MATCHED].astype(bool)
    matched_trade_rows = int(matched_mask.sum())
    unmatched_trade_rows = int(len(aligned) - matched_trade_rows)
    lag_ms = aligned.loc[matched_mask, QUOTE_LAG_MS].astype(float)
    locked_quotes = aligned.loc[matched_mask, QUOTE_BID] == aligned.loc[matched_mask, QUOTE_ASK]
    matched_locked_quote_count = int(locked_quotes.sum())
    cross_session_matches = aligned.loc[
        matched_mask
        & (aligned[TRADING_DATE] != aligned[MATCHED_QUOTE_TRADING_DATE])
    ]

    return QuoteTradeAlignmentDiagnostics(
        input_trade_rows=input_trade_rows,
        input_quote_rows=input_quote_rows,
        aligned_trade_rows=len(aligned),
        matched_trade_rows=matched_trade_rows,
        unmatched_trade_rows=unmatched_trade_rows,
        matched_ratio=matched_trade_rows / input_trade_rows if input_trade_rows else 0.0,
        allow_exact_matches=False,
        tolerance=_format_tolerance(tolerance_delta),
        tolerance_policy=(
            "retain_trade_unmatched_if_quote_lag_exceeds_tolerance"
            if tolerance_delta is not None
            else "no_maximum_lag"
        ),
        min_quote_lag_ms=_series_min(lag_ms),
        median_quote_lag_ms=_series_quantile(lag_ms, 0.50),
        p95_quote_lag_ms=_series_quantile(lag_ms, 0.95),
        p99_quote_lag_ms=_series_quantile(lag_ms, 0.99),
        max_quote_lag_ms=_series_max(lag_ms),
        matched_locked_quote_count=matched_locked_quote_count,
        matched_locked_quote_ratio=(
            matched_locked_quote_count / matched_trade_rows if matched_trade_rows else 0.0
        ),
        cross_session_match_count=len(cross_session_matches),
    )


def _series_min(values: pd.Series) -> float | None:
    if values.empty:
        return None
    return float(values.min())


def _series_max(values: pd.Series) -> float | None:
    if values.empty:
        return None
    return float(values.max())


def _series_quantile(values: pd.Series, quantile: float) -> float | None:
    if values.empty:
        return None
    return float(values.quantile(quantile))


def _format_tolerance(tolerance_delta: pd.Timedelta | None) -> str | None:
    if tolerance_delta is None:
        return None
    tolerance_ms = tolerance_delta.total_seconds() * 1000.0
    return f"{tolerance_ms:g}ms"


def _coerce_tolerance(tolerance: str | pd.Timedelta | None) -> pd.Timedelta | None:
    if tolerance is None:
        return None
    tolerance_delta = pd.Timedelta(tolerance)
    if tolerance_delta < pd.Timedelta(0):
        raise AlignmentError("Alignment tolerance must be non-negative.")
    return tolerance_delta


def _validate_comparable_event_time_dtypes(
    trades: pd.DataFrame,
    quotes: pd.DataFrame,
) -> None:
    trade_dtype = trades[EVENT_TIME].dtype
    quote_dtype = quotes[EVENT_TIME].dtype
    if trade_dtype != quote_dtype:
        raise AlignmentError(
            "Trade and quote event_time columns must use the same datetime dtype. "
            f"Got trades={trade_dtype!s}, quotes={quote_dtype!s}."
        )


def _empty_aligned_frame(trades: pd.DataFrame, quote_payload: pd.DataFrame) -> pd.DataFrame:
    quote_columns = [column for column in quote_payload.columns if column != SYMBOL]
    aligned = trades.copy()
    for column in quote_columns:
        aligned[column] = pd.Series(dtype=quote_payload[column].dtype)
    return aligned


def _order_aligned_columns(aligned: pd.DataFrame, trade_columns: tuple[str, ...]) -> pd.DataFrame:
    preferred_columns = (*trade_columns, *ALIGNED_QUOTE_COLUMNS)
    ordered_columns: list[str] = []
    for column in preferred_columns:
        if column in aligned.columns and column not in ordered_columns:
            ordered_columns.append(column)
    ordered_columns.extend(column for column in aligned.columns if column not in ordered_columns)
    return aligned.loc[:, ordered_columns]
