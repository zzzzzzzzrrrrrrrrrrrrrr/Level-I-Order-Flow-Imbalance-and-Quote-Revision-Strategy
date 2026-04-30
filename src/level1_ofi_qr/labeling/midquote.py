"""Future midquote label construction."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

import pandas as pd

from ..alignment import TRADING_DATE
from ..features.quotes import MIDQUOTE
from ..schema import EVENT_TIME, SYMBOL

DECISION_TIME: Final[str] = "decision_time"
CURRENT_MIDQUOTE_EVENT_TIME: Final[str] = "current_midquote_event_time"
CURRENT_MIDQUOTE: Final[str] = "current_midquote"
CURRENT_MIDQUOTE_LAG_MS: Final[str] = "current_midquote_lag_ms"

DEFAULT_LABEL_HORIZONS: Final[tuple[str, ...]] = ("100ms", "500ms", "1s", "5s")
DEFAULT_DEAD_ZONE_BPS: Final[float] = 0.0

CURRENT_QUOTE_POLICY: Final[str] = "latest_quote_at_or_before_decision_time"
FUTURE_QUOTE_POLICY: Final[str] = "first_quote_at_or_after_decision_time_plus_horizon"
SESSION_BOUNDARY_POLICY: Final[str] = "same_symbol_same_trading_date_only"
LABEL_USAGE_POLICY: Final[str] = "labels_are_targets_not_features"
LABELING_SCOPE_NOTE: Final[str] = (
    "Labeling v1 creates future midquote return and direction targets from "
    "quote feature data. Labels are computed strictly after decision_time and "
    "must not be used as features. This step does not create trading signals, "
    "run walk-forward evaluation, or run backtests."
)


class MidquoteLabelError(ValueError):
    """Raised when midquote labels cannot be constructed."""


@dataclass(frozen=True)
class MidquoteLabelDiagnostics:
    """Diagnostics for future midquote label construction."""

    input_feature_rows: int
    input_quote_rows: int
    output_labeled_rows: int
    row_preserving: bool
    horizons: tuple[str, ...]
    dead_zone_bps: float
    label_columns: tuple[str, ...]
    label_group_keys: tuple[str, str]
    current_quote_policy: str
    future_quote_policy: str
    session_boundary_policy: str
    label_usage_policy: str
    current_midquote_missing_rows: int
    current_midquote_lag_missing_rows: int
    label_available_rows: dict[str, int]
    label_missing_rows: dict[str, int]
    positive_direction_rows: dict[str, int]
    flat_direction_rows: dict[str, int]
    negative_direction_rows: dict[str, int]
    min_return_bps: dict[str, float | None]
    median_return_bps: dict[str, float | None]
    max_return_bps: dict[str, float | None]
    cross_session_label_count: int = 0
    signals_implemented: bool = False
    walk_forward_implemented: bool = False
    backtest_implemented: bool = False
    research_grade_strategy_sample: bool = False


@dataclass(frozen=True)
class MidquoteLabelResult:
    """Labeled feature rows and diagnostics."""

    labeled_features: pd.DataFrame
    diagnostics: MidquoteLabelDiagnostics


def build_midquote_labels_v1(
    feature_rows: pd.DataFrame,
    quote_features: pd.DataFrame,
    *,
    horizons: tuple[str, ...] = DEFAULT_LABEL_HORIZONS,
    dead_zone_bps: float = DEFAULT_DEAD_ZONE_BPS,
) -> MidquoteLabelResult:
    """Attach future midquote labels to feature rows without dropping rows."""

    _validate_feature_rows(feature_rows)
    _validate_quote_features(quote_features)
    _validate_horizons(horizons)
    if dead_zone_bps < 0:
        raise MidquoteLabelError("dead_zone_bps must be non-negative.")

    labeled = feature_rows.copy()
    labeled[DECISION_TIME] = labeled[EVENT_TIME]
    labeled["_decision_order"] = range(len(labeled))
    labeled = labeled.sort_values(
        [SYMBOL, TRADING_DATE, DECISION_TIME, "_decision_order"],
        kind="mergesort",
    ).reset_index(drop=True)

    quotes = quote_features.loc[:, [SYMBOL, TRADING_DATE, EVENT_TIME, MIDQUOTE]].copy()
    quotes = quotes.sort_values([SYMBOL, TRADING_DATE, EVENT_TIME], kind="mergesort")

    labeled = _attach_current_midquote(labeled, quotes)

    label_columns = [DECISION_TIME, CURRENT_MIDQUOTE_EVENT_TIME, CURRENT_MIDQUOTE, CURRENT_MIDQUOTE_LAG_MS]
    label_available_rows: dict[str, int] = {}
    label_missing_rows: dict[str, int] = {}
    positive_direction_rows: dict[str, int] = {}
    flat_direction_rows: dict[str, int] = {}
    negative_direction_rows: dict[str, int] = {}
    min_return_bps: dict[str, float | None] = {}
    median_return_bps: dict[str, float | None] = {}
    max_return_bps: dict[str, float | None] = {}

    for horizon in horizons:
        horizon_delta = pd.Timedelta(horizon)
        suffix = _horizon_suffix(horizon)
        columns = _label_columns_for_suffix(suffix)
        labeled[columns["target_time"]] = labeled[DECISION_TIME] + horizon_delta
        labeled = _attach_future_midquote(
            labeled,
            quotes,
            target_time_column=columns["target_time"],
            future_time_column=columns["future_time"],
            future_midquote_column=columns["future_midquote"],
        )
        labeled[columns["future_lag_ms"]] = (
            labeled[columns["future_time"]] - labeled[columns["target_time"]]
        ).dt.total_seconds() * 1000.0
        labeled[columns["return"]] = labeled[columns["future_midquote"]] - labeled[CURRENT_MIDQUOTE]
        labeled[columns["return_bps"]] = _safe_return_bps(
            labeled[columns["return"]],
            labeled[CURRENT_MIDQUOTE],
        )
        labeled[columns["available"]] = (
            labeled[CURRENT_MIDQUOTE].notna() & labeled[columns["future_midquote"]].notna()
        )
        labeled[columns["direction"]] = _direction_from_return_bps(
            labeled[columns["return_bps"]],
            dead_zone_bps=dead_zone_bps,
        )
        label_columns.extend(columns.values())

        label_available_rows[suffix] = int(labeled[columns["available"]].sum())
        label_missing_rows[suffix] = int((~labeled[columns["available"]]).sum())
        positive_direction_rows[suffix] = int((labeled[columns["direction"]] == 1).sum())
        flat_direction_rows[suffix] = int((labeled[columns["direction"]] == 0).sum())
        negative_direction_rows[suffix] = int((labeled[columns["direction"]] == -1).sum())
        returns = labeled.loc[labeled[columns["available"]], columns["return_bps"]]
        min_return_bps[suffix] = _series_min(returns)
        median_return_bps[suffix] = _series_quantile(returns, 0.50)
        max_return_bps[suffix] = _series_max(returns)

    labeled = labeled.sort_values("_decision_order", kind="mergesort").reset_index(drop=True)
    labeled = labeled.drop(columns=["_decision_order"])
    diagnostics = MidquoteLabelDiagnostics(
        input_feature_rows=len(feature_rows),
        input_quote_rows=len(quote_features),
        output_labeled_rows=len(labeled),
        row_preserving=len(feature_rows) == len(labeled),
        horizons=horizons,
        dead_zone_bps=dead_zone_bps,
        label_columns=tuple(label_columns),
        label_group_keys=(SYMBOL, TRADING_DATE),
        current_quote_policy=CURRENT_QUOTE_POLICY,
        future_quote_policy=FUTURE_QUOTE_POLICY,
        session_boundary_policy=SESSION_BOUNDARY_POLICY,
        label_usage_policy=LABEL_USAGE_POLICY,
        current_midquote_missing_rows=int(labeled[CURRENT_MIDQUOTE].isna().sum()),
        current_midquote_lag_missing_rows=int(labeled[CURRENT_MIDQUOTE_LAG_MS].isna().sum()),
        label_available_rows=label_available_rows,
        label_missing_rows=label_missing_rows,
        positive_direction_rows=positive_direction_rows,
        flat_direction_rows=flat_direction_rows,
        negative_direction_rows=negative_direction_rows,
        min_return_bps=min_return_bps,
        median_return_bps=median_return_bps,
        max_return_bps=max_return_bps,
    )
    return MidquoteLabelResult(labeled_features=labeled, diagnostics=diagnostics)


def _validate_feature_rows(feature_rows: pd.DataFrame) -> None:
    missing_columns = [
        column for column in (EVENT_TIME, SYMBOL, TRADING_DATE) if column not in feature_rows.columns
    ]
    if missing_columns:
        raise MidquoteLabelError(f"Feature rows are missing columns: {missing_columns}")
    if not pd.api.types.is_datetime64_any_dtype(feature_rows[EVENT_TIME]):
        raise MidquoteLabelError("Feature rows must have datetime event_time values.")


def _validate_quote_features(quote_features: pd.DataFrame) -> None:
    missing_columns = [
        column
        for column in (EVENT_TIME, SYMBOL, TRADING_DATE, MIDQUOTE)
        if column not in quote_features.columns
    ]
    if missing_columns:
        raise MidquoteLabelError(f"Quote features are missing columns: {missing_columns}")
    if not pd.api.types.is_datetime64_any_dtype(quote_features[EVENT_TIME]):
        raise MidquoteLabelError("Quote features must have datetime event_time values.")


def _validate_horizons(horizons: tuple[str, ...]) -> None:
    if not horizons:
        raise MidquoteLabelError("At least one label horizon is required.")
    for horizon in horizons:
        if pd.Timedelta(horizon) <= pd.Timedelta(0):
            raise MidquoteLabelError("Label horizons must be positive durations.")


def _attach_current_midquote(labeled: pd.DataFrame, quotes: pd.DataFrame) -> pd.DataFrame:
    matches: list[pd.DataFrame] = []
    for (symbol, trading_date), decision_group in labeled.groupby(
        [SYMBOL, TRADING_DATE],
        sort=False,
    ):
        quote_group = _quote_group_for(quotes, symbol=symbol, trading_date=trading_date)
        left = decision_group.loc[:, ["_decision_order", DECISION_TIME]].sort_values(
            DECISION_TIME,
            kind="mergesort",
        )
        if quote_group.empty:
            left[CURRENT_MIDQUOTE_EVENT_TIME] = pd.NaT
            left[CURRENT_MIDQUOTE] = pd.NA
            matches.append(left)
            continue
        right = quote_group.loc[:, [EVENT_TIME, MIDQUOTE]].rename(
            columns={
                EVENT_TIME: CURRENT_MIDQUOTE_EVENT_TIME,
                MIDQUOTE: CURRENT_MIDQUOTE,
            }
        )
        matched = pd.merge_asof(
            left,
            right,
            left_on=DECISION_TIME,
            right_on=CURRENT_MIDQUOTE_EVENT_TIME,
            direction="backward",
            allow_exact_matches=True,
        )
        matches.append(matched)

    current = pd.concat(matches, ignore_index=True).set_index("_decision_order")
    result = labeled.join(
        current.loc[:, [CURRENT_MIDQUOTE_EVENT_TIME, CURRENT_MIDQUOTE]],
        on="_decision_order",
    )
    result[CURRENT_MIDQUOTE_LAG_MS] = (
        result[DECISION_TIME] - result[CURRENT_MIDQUOTE_EVENT_TIME]
    ).dt.total_seconds() * 1000.0
    return result


def _attach_future_midquote(
    labeled: pd.DataFrame,
    quotes: pd.DataFrame,
    *,
    target_time_column: str,
    future_time_column: str,
    future_midquote_column: str,
) -> pd.DataFrame:
    matches: list[pd.DataFrame] = []
    for (symbol, trading_date), decision_group in labeled.groupby(
        [SYMBOL, TRADING_DATE],
        sort=False,
    ):
        quote_group = _quote_group_for(quotes, symbol=symbol, trading_date=trading_date)
        left = decision_group.loc[:, ["_decision_order", target_time_column]].sort_values(
            target_time_column,
            kind="mergesort",
        )
        if quote_group.empty:
            left[future_time_column] = pd.NaT
            left[future_midquote_column] = pd.NA
            matches.append(left)
            continue
        right = quote_group.loc[:, [EVENT_TIME, MIDQUOTE]].rename(
            columns={
                EVENT_TIME: future_time_column,
                MIDQUOTE: future_midquote_column,
            }
        )
        matched = pd.merge_asof(
            left,
            right,
            left_on=target_time_column,
            right_on=future_time_column,
            direction="forward",
            allow_exact_matches=True,
        )
        matches.append(matched)

    future = pd.concat(matches, ignore_index=True).set_index("_decision_order")
    return labeled.join(
        future.loc[:, [future_time_column, future_midquote_column]],
        on="_decision_order",
    )


def _quote_group_for(quotes: pd.DataFrame, *, symbol: str, trading_date: str) -> pd.DataFrame:
    return quotes.loc[
        (quotes[SYMBOL] == symbol) & (quotes[TRADING_DATE] == trading_date)
    ].sort_values(EVENT_TIME, kind="mergesort")


def _label_columns_for_suffix(suffix: str) -> dict[str, str]:
    return {
        "target_time": f"label_target_time_{suffix}",
        "future_time": f"future_midquote_event_time_{suffix}",
        "future_midquote": f"future_midquote_{suffix}",
        "future_lag_ms": f"future_midquote_lag_ms_{suffix}",
        "return": f"future_midquote_return_{suffix}",
        "return_bps": f"future_midquote_return_bps_{suffix}",
        "direction": f"future_midquote_direction_{suffix}",
        "available": f"label_available_{suffix}",
    }


def _horizon_suffix(horizon: str) -> str:
    return horizon.lower().replace(" ", "").replace(".", "p")


def _safe_return_bps(midquote_change: pd.Series, current_midquote: pd.Series) -> pd.Series:
    return midquote_change.where(current_midquote != 0) / current_midquote.where(
        current_midquote != 0
    ) * 10000.0


def _direction_from_return_bps(
    return_bps: pd.Series,
    *,
    dead_zone_bps: float,
) -> pd.Series:
    direction = pd.Series(pd.NA, index=return_bps.index, dtype="Int64")
    valid = return_bps.notna()
    direction.loc[valid & (return_bps > dead_zone_bps)] = 1
    direction.loc[valid & (return_bps < -dead_zone_bps)] = -1
    direction.loc[valid & (return_bps.abs() <= dead_zone_bps)] = 0
    return direction


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
