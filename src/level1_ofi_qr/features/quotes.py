"""Quote-only Level-I feature generation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

import pandas as pd

from ..schema import (
    ASK,
    ASK_SIZE,
    BID,
    BID_SIZE,
    EVENT_TIME,
    SYMBOL,
    validate_quote_frame,
)

TRADING_DATE: Final[str] = "trading_date"
MIDQUOTE: Final[str] = "midquote"
QUOTED_SPREAD: Final[str] = "quoted_spread"
RELATIVE_SPREAD: Final[str] = "relative_spread"
QUOTED_DEPTH: Final[str] = "quoted_depth"
QUOTE_IMBALANCE: Final[str] = "quote_imbalance"
PREVIOUS_MIDQUOTE: Final[str] = "previous_midquote"
QUOTE_REVISION: Final[str] = "quote_revision"
QUOTE_REVISION_BPS: Final[str] = "quote_revision_bps"
QUOTE_EVENT_INTERVAL_MS: Final[str] = "quote_event_interval_ms"

QUOTE_FEATURE_COLUMNS: Final[tuple[str, ...]] = (
    TRADING_DATE,
    MIDQUOTE,
    QUOTED_SPREAD,
    RELATIVE_SPREAD,
    QUOTED_DEPTH,
    QUOTE_IMBALANCE,
    PREVIOUS_MIDQUOTE,
    QUOTE_REVISION,
    QUOTE_REVISION_BPS,
    QUOTE_EVENT_INTERVAL_MS,
)

QUOTE_FEATURE_SCOPE_NOTE: Final[str] = (
    "Quote feature v1 computes row-preserving, quote-only features from "
    "cleaned Level-I quotes. It supports spread, depth, signed top-of-book "
    "imbalance, quote revision, and quote event interval diagnostics. It does "
    "not compute trade signing, signed order flow imbalance, labels, or "
    "backtest signals."
)


@dataclass(frozen=True)
class QuoteFeatureDiagnostics:
    """Diagnostics for quote-only feature generation."""

    input_quote_rows: int
    output_feature_rows: int
    feature_columns: tuple[str, ...]
    feature_group_keys: tuple[str, str]
    trading_date_count: int
    zero_quoted_depth_rows: int
    quote_imbalance_null_rows: int
    quote_revision_null_rows: int
    quote_revision_bps_null_rows: int
    max_abs_quote_revision_bps: float | None
    condition_filters_applied: bool = False
    trade_signing_applied: bool = False
    ofi_from_signed_trades_implemented: bool = False
    labels_implemented: bool = False
    backtest_implemented: bool = False


@dataclass(frozen=True)
class QuoteFeatureResult:
    """Quote feature frame and diagnostics."""

    quote_features: pd.DataFrame
    diagnostics: QuoteFeatureDiagnostics


def build_quote_features_v1(
    quotes: pd.DataFrame,
    *,
    market_timezone: str = "America/New_York",
) -> QuoteFeatureResult:
    """Build quote-only features from cleaned normalized quotes."""

    validate_quote_frame(quotes)
    features = quotes.copy()
    features[TRADING_DATE] = _derive_trading_date(
        features[EVENT_TIME],
        market_timezone=market_timezone,
    )
    features["_quote_order"] = range(len(features))
    features = features.sort_values(
        [SYMBOL, TRADING_DATE, EVENT_TIME, "_quote_order"],
        kind="mergesort",
    ).reset_index(drop=True)

    features[MIDQUOTE] = (features[BID] + features[ASK]) / 2.0
    features[QUOTED_SPREAD] = features[ASK] - features[BID]
    features[RELATIVE_SPREAD] = _safe_divide(features[QUOTED_SPREAD], features[MIDQUOTE])
    features[QUOTED_DEPTH] = features[BID_SIZE] + features[ASK_SIZE]
    features[QUOTE_IMBALANCE] = _safe_divide(
        features[BID_SIZE] - features[ASK_SIZE],
        features[QUOTED_DEPTH],
    )

    feature_groups = features.groupby([SYMBOL, TRADING_DATE], sort=False)
    features[PREVIOUS_MIDQUOTE] = feature_groups[MIDQUOTE].shift(1)
    features[QUOTE_REVISION] = features[MIDQUOTE] - features[PREVIOUS_MIDQUOTE]
    features[QUOTE_REVISION_BPS] = _safe_divide(
        features[QUOTE_REVISION],
        features[PREVIOUS_MIDQUOTE],
    ) * 10000.0
    features[QUOTE_EVENT_INTERVAL_MS] = (
        feature_groups[EVENT_TIME].diff().dt.total_seconds() * 1000.0
    )
    features = features.drop(columns=["_quote_order"])

    diagnostics = QuoteFeatureDiagnostics(
        input_quote_rows=len(quotes),
        output_feature_rows=len(features),
        feature_columns=QUOTE_FEATURE_COLUMNS,
        feature_group_keys=(SYMBOL, TRADING_DATE),
        trading_date_count=features[TRADING_DATE].nunique(),
        zero_quoted_depth_rows=int((features[QUOTED_DEPTH] == 0).sum()),
        quote_imbalance_null_rows=int(features[QUOTE_IMBALANCE].isna().sum()),
        quote_revision_null_rows=int(features[QUOTE_REVISION].isna().sum()),
        quote_revision_bps_null_rows=int(features[QUOTE_REVISION_BPS].isna().sum()),
        max_abs_quote_revision_bps=_series_abs_max(features[QUOTE_REVISION_BPS]),
    )
    return QuoteFeatureResult(quote_features=features, diagnostics=diagnostics)


def _derive_trading_date(timestamps: pd.Series, *, market_timezone: str) -> pd.Series:
    if timestamps.dt.tz is None:
        localized = timestamps.dt.tz_localize(market_timezone)
    else:
        localized = timestamps.dt.tz_convert(market_timezone)
    return localized.dt.strftime("%Y-%m-%d")


def _safe_divide(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    return numerator.where(denominator != 0) / denominator.where(denominator != 0)


def _series_abs_max(values: pd.Series) -> float | None:
    non_null = values.dropna()
    if non_null.empty:
        return None
    return float(non_null.abs().max())
