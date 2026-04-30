"""Signed trade-flow feature generation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

import pandas as pd

from ..alignment import TRADING_DATE
from ..schema import (
    EVENT_TIME,
    SYMBOL,
    TRADE_PRICE,
    TRADE_SEQUENCE_NUMBER,
    TRADE_SIZE,
    validate_trade_frame,
)
from ..trade_signing import SIGNED_TRADE_SIZE, TRADE_SIGN

BUY_TRADE_SIZE: Final[str] = "buy_trade_size"
SELL_TRADE_SIZE: Final[str] = "sell_trade_size"
UNKNOWN_TRADE_SIZE: Final[str] = "unknown_trade_size"
SIGNED_TRADE_VALUE: Final[str] = "signed_trade_value"

SIGNED_FLOW_FEATURE_SCOPE_NOTE: Final[str] = (
    "Signed-flow feature v1 computes row-preserving trade-flow features from "
    "trade_signing_v1 output. It uses event-count and clock-time trailing "
    "windows within symbol and trading_date groups. It does not apply "
    "condition-code filters, construct labels, or run backtests. These are "
    "trade-signed-flow features, not a final research-grade OFI sample."
)
WINDOW_INCLUSION_POLICY: Final[str] = "trailing_windows_include_current_trade"
UNKNOWN_SIGN_POLICY: Final[str] = (
    "unknown_sign_trades_contribute_zero_signed_flow_and_remain_in_trade_volume"
)

DEFAULT_TRADE_COUNT_WINDOWS: Final[tuple[int, ...]] = (10, 50, 100)
DEFAULT_TIME_WINDOWS: Final[tuple[str, ...]] = ("100ms", "500ms", "1s")
IMBALANCE_BOUND_EPSILON: Final[float] = 1e-9


class SignedFlowFeatureError(ValueError):
    """Raised when signed-flow feature generation cannot be completed."""


@dataclass(frozen=True)
class SignedFlowFeatureDiagnostics:
    """Diagnostics for signed-flow feature generation."""

    input_signed_trade_rows: int
    output_feature_rows: int
    row_preserving: bool
    feature_columns: tuple[str, ...]
    feature_group_keys: tuple[str, str]
    trade_count_windows: tuple[int, ...]
    time_windows: tuple[str, ...]
    window_inclusion_policy: str
    unknown_sign_policy: str
    signed_trade_rows: int
    unknown_sign_rows: int
    buy_sign_rows: int
    sell_sign_rows: int
    zero_volume_window_rows: dict[str, int]
    signed_flow_imbalance_null_rows: dict[str, int]
    max_abs_signed_flow_imbalance: dict[str, float | None]
    condition_filters_applied: bool = False
    sale_condition_filters_applied: bool = False
    nbbo_quote_condition_filters_applied: bool = False
    labels_implemented: bool = False
    backtest_implemented: bool = False
    research_grade_strategy_sample: bool = False


@dataclass(frozen=True)
class SignedFlowFeatureResult:
    """Signed-flow feature frame and diagnostics."""

    signed_flow_features: pd.DataFrame
    diagnostics: SignedFlowFeatureDiagnostics


def build_signed_flow_features_v1(
    signed_trades: pd.DataFrame,
    *,
    trade_count_windows: tuple[int, ...] = DEFAULT_TRADE_COUNT_WINDOWS,
    time_windows: tuple[str, ...] = DEFAULT_TIME_WINDOWS,
) -> SignedFlowFeatureResult:
    """Build row-preserving signed trade-flow features.

    Window features are computed only within symbol and trading_date groups.
    The current trade is included in each trailing window.
    """

    _validate_signed_trade_frame(signed_trades)
    _validate_windows(trade_count_windows=trade_count_windows, time_windows=time_windows)

    features = signed_trades.copy()
    features["_trade_order"] = range(len(features))
    features = features.sort_values(
        [SYMBOL, TRADING_DATE, EVENT_TIME, TRADE_SEQUENCE_NUMBER, "_trade_order"],
        kind="mergesort",
    ).reset_index(drop=True)

    features[TRADE_SIZE] = pd.to_numeric(features[TRADE_SIZE], errors="coerce")
    features[TRADE_PRICE] = pd.to_numeric(features[TRADE_PRICE], errors="coerce")
    features[TRADE_SIGN] = pd.to_numeric(features[TRADE_SIGN], errors="coerce").fillna(0)
    features[SIGNED_TRADE_SIZE] = pd.to_numeric(
        features[SIGNED_TRADE_SIZE],
        errors="coerce",
    ).fillna(0)
    features[BUY_TRADE_SIZE] = features[TRADE_SIZE].where(features[TRADE_SIGN] > 0, 0)
    features[SELL_TRADE_SIZE] = features[TRADE_SIZE].where(features[TRADE_SIGN] < 0, 0)
    features[UNKNOWN_TRADE_SIZE] = features[TRADE_SIZE].where(features[TRADE_SIGN] == 0, 0)
    features[SIGNED_TRADE_VALUE] = features[SIGNED_TRADE_SIZE] * features[TRADE_PRICE]

    feature_columns = [
        BUY_TRADE_SIZE,
        SELL_TRADE_SIZE,
        UNKNOWN_TRADE_SIZE,
        SIGNED_TRADE_VALUE,
    ]
    imbalance_columns: list[str] = []
    zero_volume_rows: dict[str, int] = {}
    imbalance_null_rows: dict[str, int] = {}
    max_abs_imbalance: dict[str, float | None] = {}

    for window in trade_count_windows:
        suffix = f"{window}_trades"
        added_columns = _add_trade_count_window_features(features, window=window, suffix=suffix)
        feature_columns.extend(added_columns)
        imbalance_columns.append(f"signed_flow_imbalance_{suffix}")

    for window in time_windows:
        suffix = _time_window_suffix(window)
        added_columns = _add_time_window_features(features, window=window, suffix=suffix)
        feature_columns.extend(added_columns)
        imbalance_columns.append(f"signed_flow_imbalance_{suffix}")

    for column in imbalance_columns:
        volume_column = column.replace("signed_flow_imbalance_", "trade_volume_")
        zero_volume_rows[column] = int((features[volume_column] == 0).sum())
        imbalance_null_rows[column] = int(features[column].isna().sum())
        max_abs_imbalance[column] = _series_abs_max(features[column])

    features = features.drop(columns=["_trade_order"])
    diagnostics = SignedFlowFeatureDiagnostics(
        input_signed_trade_rows=len(signed_trades),
        output_feature_rows=len(features),
        row_preserving=len(signed_trades) == len(features),
        feature_columns=tuple(feature_columns),
        feature_group_keys=(SYMBOL, TRADING_DATE),
        trade_count_windows=trade_count_windows,
        time_windows=time_windows,
        window_inclusion_policy=WINDOW_INCLUSION_POLICY,
        unknown_sign_policy=UNKNOWN_SIGN_POLICY,
        signed_trade_rows=int((features[TRADE_SIGN] != 0).sum()),
        unknown_sign_rows=int((features[TRADE_SIGN] == 0).sum()),
        buy_sign_rows=int((features[TRADE_SIGN] > 0).sum()),
        sell_sign_rows=int((features[TRADE_SIGN] < 0).sum()),
        zero_volume_window_rows=zero_volume_rows,
        signed_flow_imbalance_null_rows=imbalance_null_rows,
        max_abs_signed_flow_imbalance=max_abs_imbalance,
    )
    return SignedFlowFeatureResult(
        signed_flow_features=_order_feature_columns(features, tuple(signed_trades.columns), feature_columns),
        diagnostics=diagnostics,
    )


def _validate_signed_trade_frame(signed_trades: pd.DataFrame) -> None:
    validate_trade_frame(signed_trades)
    missing_columns = [
        column
        for column in (TRADING_DATE, TRADE_SIGN, SIGNED_TRADE_SIZE)
        if column not in signed_trades.columns
    ]
    if missing_columns:
        raise SignedFlowFeatureError(
            "Signed-flow features require trade_signing_v1 columns. "
            f"Missing columns: {missing_columns}"
        )
    if not pd.api.types.is_datetime64_any_dtype(signed_trades[EVENT_TIME]):
        raise SignedFlowFeatureError("Signed trades must have datetime event_time values.")


def _validate_windows(
    *,
    trade_count_windows: tuple[int, ...],
    time_windows: tuple[str, ...],
) -> None:
    if not trade_count_windows and not time_windows:
        raise SignedFlowFeatureError("At least one signed-flow feature window is required.")
    for window in trade_count_windows:
        if window <= 0:
            raise SignedFlowFeatureError("Trade-count windows must be positive integers.")
    for window in time_windows:
        if pd.Timedelta(window) <= pd.Timedelta(0):
            raise SignedFlowFeatureError("Clock-time windows must be positive durations.")


def _add_trade_count_window_features(
    features: pd.DataFrame,
    *,
    window: int,
    suffix: str,
) -> tuple[str, ...]:
    group = features.groupby([SYMBOL, TRADING_DATE], sort=False)
    signed_flow_column = f"signed_flow_{suffix}"
    trade_volume_column = f"trade_volume_{suffix}"
    buy_volume_column = f"buy_volume_{suffix}"
    sell_volume_column = f"sell_volume_{suffix}"
    trade_count_column = f"trade_count_{suffix}"
    imbalance_column = f"signed_flow_imbalance_{suffix}"

    features[signed_flow_column] = _rolling_count_sum(group[SIGNED_TRADE_SIZE], window)
    features[trade_volume_column] = _rolling_count_sum(group[TRADE_SIZE], window)
    features[buy_volume_column] = _rolling_count_sum(group[BUY_TRADE_SIZE], window)
    features[sell_volume_column] = _rolling_count_sum(group[SELL_TRADE_SIZE], window)
    features[trade_count_column] = _rolling_count_sum(group[TRADE_SIZE], window, count=True)
    features[imbalance_column] = _safe_divide(
        features[signed_flow_column],
        features[trade_volume_column],
    )
    return (
        signed_flow_column,
        trade_volume_column,
        buy_volume_column,
        sell_volume_column,
        trade_count_column,
        imbalance_column,
    )


def _add_time_window_features(
    features: pd.DataFrame,
    *,
    window: str,
    suffix: str,
) -> tuple[str, ...]:
    signed_flow_column = f"signed_flow_{suffix}"
    trade_volume_column = f"trade_volume_{suffix}"
    buy_volume_column = f"buy_volume_{suffix}"
    sell_volume_column = f"sell_volume_{suffix}"
    trade_count_column = f"trade_count_{suffix}"
    imbalance_column = f"signed_flow_imbalance_{suffix}"

    features[signed_flow_column] = _rolling_time_sum(features, SIGNED_TRADE_SIZE, window)
    features[trade_volume_column] = _rolling_time_sum(features, TRADE_SIZE, window)
    features[buy_volume_column] = _rolling_time_sum(features, BUY_TRADE_SIZE, window)
    features[sell_volume_column] = _rolling_time_sum(features, SELL_TRADE_SIZE, window)
    features[trade_count_column] = _rolling_time_count(features, TRADE_SIZE, window)
    features[imbalance_column] = _safe_divide(
        features[signed_flow_column],
        features[trade_volume_column],
    )
    return (
        signed_flow_column,
        trade_volume_column,
        buy_volume_column,
        sell_volume_column,
        trade_count_column,
        imbalance_column,
    )


def _rolling_count_sum(
    grouped_series: pd.core.groupby.SeriesGroupBy,
    window: int,
    *,
    count: bool = False,
) -> pd.Series:
    rolling = grouped_series.rolling(window=window, min_periods=1)
    values = rolling.count() if count else rolling.sum()
    return values.reset_index(level=[0, 1], drop=True)


def _rolling_time_sum(features: pd.DataFrame, column: str, window: str) -> pd.Series:
    result = pd.Series(index=features.index, dtype="float64")
    for _, group in features.groupby([SYMBOL, TRADING_DATE], sort=False):
        result.loc[group.index] = (
            group.set_index(EVENT_TIME)[column]
            .rolling(window=pd.Timedelta(window), min_periods=1, closed="both")
            .sum()
            .to_numpy()
        )
    return result


def _rolling_time_count(features: pd.DataFrame, column: str, window: str) -> pd.Series:
    result = pd.Series(index=features.index, dtype="float64")
    for _, group in features.groupby([SYMBOL, TRADING_DATE], sort=False):
        result.loc[group.index] = (
            group.set_index(EVENT_TIME)[column]
            .rolling(window=pd.Timedelta(window), min_periods=1, closed="both")
            .count()
            .to_numpy()
        )
    return result


def _safe_divide(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    ratio = numerator.where(denominator != 0) / denominator.where(denominator != 0)
    ratio = ratio.mask(
        (ratio > 1.0) & (ratio <= 1.0 + IMBALANCE_BOUND_EPSILON),
        1.0,
    )
    ratio = ratio.mask(
        (ratio < -1.0) & (ratio >= -1.0 - IMBALANCE_BOUND_EPSILON),
        -1.0,
    )
    return ratio


def _time_window_suffix(window: str) -> str:
    return window.lower().replace(" ", "").replace(".", "p")


def _series_abs_max(values: pd.Series) -> float | None:
    non_null = values.dropna()
    if non_null.empty:
        return None
    return float(non_null.abs().max())


def _order_feature_columns(
    features: pd.DataFrame,
    signed_trade_columns: tuple[str, ...],
    feature_columns: list[str],
) -> pd.DataFrame:
    preferred_columns = (*signed_trade_columns, *feature_columns)
    ordered_columns: list[str] = []
    for column in preferred_columns:
        if column in features.columns and column not in ordered_columns:
            ordered_columns.append(column)
    ordered_columns.extend(column for column in features.columns if column not in ordered_columns)
    return features.loc[:, ordered_columns]
