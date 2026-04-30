"""Interpretable signal rules for Level-I feature rows."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

import pandas as pd

from ..alignment import TRADING_DATE
from ..features.quotes import (
    MIDQUOTE,
    QUOTE_IMBALANCE,
    QUOTE_REVISION_BPS,
    QUOTED_SPREAD,
    RELATIVE_SPREAD,
)
from ..schema import EVENT_TIME, SYMBOL

SIGNAL_QUOTE_EVENT_TIME: Final[str] = "signal_quote_event_time"
SIGNAL_QUOTE_LAG_MS: Final[str] = "signal_quote_lag_ms"
SIGNAL_MIDQUOTE: Final[str] = "signal_midquote"
SIGNAL_QUOTE_IMBALANCE: Final[str] = "signal_quote_imbalance"
SIGNAL_QUOTE_REVISION_BPS: Final[str] = "signal_quote_revision_bps"
SIGNAL_QUOTED_SPREAD: Final[str] = "signal_quoted_spread"
SIGNAL_RELATIVE_SPREAD: Final[str] = "signal_relative_spread"
QI_SIGNAL: Final[str] = "qi_signal"
SIGNED_FLOW_SIGNAL: Final[str] = "signed_flow_signal"
QR_SIGNAL: Final[str] = "qr_signal"
SIGNAL_INPUT_AVAILABLE: Final[str] = "signal_input_available"
SEQUENTIAL_GATE_SIGNAL: Final[str] = "sequential_gate_signal"
SIGNAL_REASON: Final[str] = "signal_reason"

DEFAULT_SIGNED_FLOW_COLUMN: Final[str] = "signed_flow_imbalance_500ms"
DEFAULT_QI_THRESHOLD: Final[float] = 0.0
DEFAULT_SIGNED_FLOW_THRESHOLD: Final[float] = 0.0
DEFAULT_QR_THRESHOLD_BPS: Final[float] = 0.0

SIGNAL_POLICY_NOTE: Final[str] = (
    "Signals v1 builds an interpretable sequential gate from current quote "
    "imbalance, signed-flow imbalance, and quote revision. Default thresholds "
    "are diagnostic sign-agreement defaults, not optimized trading thresholds. "
    "Labels may be retained in the output for later evaluation, but labels are "
    "not used to compute signals. This step does not run walk-forward "
    "evaluation or backtests."
)
THRESHOLD_SELECTION_POLICY: Final[str] = "diagnostic_defaults_not_optimized"
LABEL_USAGE_POLICY: Final[str] = "labels_retained_for_evaluation_not_used_for_signal"
SIGNAL_SESSION_POLICY: Final[str] = "same_symbol_same_trading_date_quote_state"


class SignalRuleError(ValueError):
    """Raised when signal rules cannot be applied."""


@dataclass(frozen=True)
class SignalRuleConfig:
    """Config for sequential gate signals."""

    signed_flow_column: str = DEFAULT_SIGNED_FLOW_COLUMN
    qi_threshold: float = DEFAULT_QI_THRESHOLD
    signed_flow_threshold: float = DEFAULT_SIGNED_FLOW_THRESHOLD
    qr_threshold_bps: float = DEFAULT_QR_THRESHOLD_BPS


@dataclass(frozen=True)
class SignalDiagnostics:
    """Diagnostics for signals v1."""

    input_feature_rows: int
    input_quote_rows: int
    output_signal_rows: int
    row_preserving: bool
    signal_rule: str
    signal_columns: tuple[str, ...]
    signal_group_keys: tuple[str, str]
    signed_flow_column: str
    qi_threshold: float
    signed_flow_threshold: float
    qr_threshold_bps: float
    threshold_selection_policy: str
    label_usage_policy: str
    signal_session_policy: str
    signal_input_available_rows: int
    signal_input_missing_rows: int
    long_signal_rows: int
    short_signal_rows: int
    no_trade_rows: int
    qi_long_rows: int
    qi_short_rows: int
    signed_flow_long_rows: int
    signed_flow_short_rows: int
    qr_long_rows: int
    qr_short_rows: int
    labels_retained: bool
    labels_used_for_signal: bool = False
    walk_forward_implemented: bool = False
    backtest_implemented: bool = False
    threshold_optimization_implemented: bool = False
    research_grade_strategy_sample: bool = False


@dataclass(frozen=True)
class SignalResult:
    """Signal rows and diagnostics."""

    signals: pd.DataFrame
    diagnostics: SignalDiagnostics


def build_sequential_gate_signals_v1(
    feature_rows: pd.DataFrame,
    quote_features: pd.DataFrame,
    *,
    config: SignalRuleConfig = SignalRuleConfig(),
) -> SignalResult:
    """Build row-preserving sequential gate signal rows."""

    _validate_inputs(feature_rows, quote_features, config=config)
    _validate_config(config)

    signals = feature_rows.copy()
    signals["_signal_order"] = range(len(signals))
    signals = signals.sort_values(
        [SYMBOL, TRADING_DATE, EVENT_TIME, "_signal_order"],
        kind="mergesort",
    ).reset_index(drop=True)

    quote_state = quote_features.loc[
        :,
        [
            SYMBOL,
            TRADING_DATE,
            EVENT_TIME,
            MIDQUOTE,
            QUOTE_IMBALANCE,
            QUOTE_REVISION_BPS,
            QUOTED_SPREAD,
            RELATIVE_SPREAD,
        ],
    ].copy()
    quote_state = quote_state.sort_values([SYMBOL, TRADING_DATE, EVENT_TIME], kind="mergesort")
    signals = _attach_current_quote_state(signals, quote_state)

    signals[QI_SIGNAL] = _component_signal(
        signals[SIGNAL_QUOTE_IMBALANCE],
        threshold=config.qi_threshold,
    )
    signals[SIGNED_FLOW_SIGNAL] = _component_signal(
        pd.to_numeric(signals[config.signed_flow_column], errors="coerce"),
        threshold=config.signed_flow_threshold,
    )
    signals[QR_SIGNAL] = _component_signal(
        signals[SIGNAL_QUOTE_REVISION_BPS],
        threshold=config.qr_threshold_bps,
    )
    signals[SIGNAL_INPUT_AVAILABLE] = (
        signals[QI_SIGNAL].notna()
        & signals[SIGNED_FLOW_SIGNAL].notna()
        & signals[QR_SIGNAL].notna()
    )

    signals[SEQUENTIAL_GATE_SIGNAL] = 0
    long_mask = (
        signals[SIGNAL_INPUT_AVAILABLE]
        & (signals[QI_SIGNAL] == 1)
        & (signals[SIGNED_FLOW_SIGNAL] == 1)
        & (signals[QR_SIGNAL] == 1)
    )
    short_mask = (
        signals[SIGNAL_INPUT_AVAILABLE]
        & (signals[QI_SIGNAL] == -1)
        & (signals[SIGNED_FLOW_SIGNAL] == -1)
        & (signals[QR_SIGNAL] == -1)
    )
    signals.loc[long_mask, SEQUENTIAL_GATE_SIGNAL] = 1
    signals.loc[short_mask, SEQUENTIAL_GATE_SIGNAL] = -1
    signals[SIGNAL_REASON] = "gates_not_aligned"
    signals.loc[~signals[SIGNAL_INPUT_AVAILABLE], SIGNAL_REASON] = "inputs_missing"
    signals.loc[long_mask, SIGNAL_REASON] = "long_all_gates"
    signals.loc[short_mask, SIGNAL_REASON] = "short_all_gates"

    diagnostics = _build_diagnostics(signals, quote_features, config=config)
    signals = signals.sort_values("_signal_order", kind="mergesort").reset_index(drop=True)
    signals = signals.drop(columns=["_signal_order"])
    signals = _order_signal_columns(signals, tuple(feature_rows.columns))
    return SignalResult(signals=signals, diagnostics=diagnostics)


def _validate_inputs(
    feature_rows: pd.DataFrame,
    quote_features: pd.DataFrame,
    *,
    config: SignalRuleConfig,
) -> None:
    missing_feature_columns = [
        column
        for column in (EVENT_TIME, SYMBOL, TRADING_DATE, config.signed_flow_column)
        if column not in feature_rows.columns
    ]
    if missing_feature_columns:
        raise SignalRuleError(f"Feature rows are missing columns: {missing_feature_columns}")
    missing_quote_columns = [
        column
        for column in (
            EVENT_TIME,
            SYMBOL,
            TRADING_DATE,
            MIDQUOTE,
            QUOTE_IMBALANCE,
            QUOTE_REVISION_BPS,
            QUOTED_SPREAD,
            RELATIVE_SPREAD,
        )
        if column not in quote_features.columns
    ]
    if missing_quote_columns:
        raise SignalRuleError(f"Quote features are missing columns: {missing_quote_columns}")
    if not pd.api.types.is_datetime64_any_dtype(feature_rows[EVENT_TIME]):
        raise SignalRuleError("Feature rows must have datetime event_time values.")
    if not pd.api.types.is_datetime64_any_dtype(quote_features[EVENT_TIME]):
        raise SignalRuleError("Quote features must have datetime event_time values.")


def _validate_config(config: SignalRuleConfig) -> None:
    for name, value in (
        ("qi_threshold", config.qi_threshold),
        ("signed_flow_threshold", config.signed_flow_threshold),
        ("qr_threshold_bps", config.qr_threshold_bps),
    ):
        if value < 0:
            raise SignalRuleError(f"{name} must be non-negative.")


def _attach_current_quote_state(signals: pd.DataFrame, quote_state: pd.DataFrame) -> pd.DataFrame:
    matches: list[pd.DataFrame] = []
    for (symbol, trading_date), signal_group in signals.groupby(
        [SYMBOL, TRADING_DATE],
        sort=False,
    ):
        quotes = quote_state.loc[
            (quote_state[SYMBOL] == symbol) & (quote_state[TRADING_DATE] == trading_date)
        ].sort_values(EVENT_TIME, kind="mergesort")
        left = signal_group.loc[:, ["_signal_order", EVENT_TIME]].sort_values(
            EVENT_TIME,
            kind="mergesort",
        )
        if quotes.empty:
            for column in _quote_signal_columns():
                left[column] = pd.NA
            matches.append(left)
            continue
        right = quotes.rename(
            columns={
                EVENT_TIME: SIGNAL_QUOTE_EVENT_TIME,
                MIDQUOTE: SIGNAL_MIDQUOTE,
                QUOTE_IMBALANCE: SIGNAL_QUOTE_IMBALANCE,
                QUOTE_REVISION_BPS: SIGNAL_QUOTE_REVISION_BPS,
                QUOTED_SPREAD: SIGNAL_QUOTED_SPREAD,
                RELATIVE_SPREAD: SIGNAL_RELATIVE_SPREAD,
            }
        ).loc[:, [SIGNAL_QUOTE_EVENT_TIME, *_quote_signal_value_columns()]]
        matched = pd.merge_asof(
            left,
            right,
            left_on=EVENT_TIME,
            right_on=SIGNAL_QUOTE_EVENT_TIME,
            direction="backward",
            allow_exact_matches=True,
        )
        matches.append(matched)

    quote_matches = pd.concat(matches, ignore_index=True).set_index("_signal_order")
    result = signals.join(
        quote_matches.loc[:, _quote_signal_columns()],
        on="_signal_order",
    )
    result[SIGNAL_QUOTE_LAG_MS] = (
        result[EVENT_TIME] - result[SIGNAL_QUOTE_EVENT_TIME]
    ).dt.total_seconds() * 1000.0
    return result


def _component_signal(values: pd.Series, *, threshold: float) -> pd.Series:
    numeric = pd.to_numeric(values, errors="coerce")
    result = pd.Series(pd.NA, index=values.index, dtype="Int64")
    result.loc[numeric.notna() & (numeric > threshold)] = 1
    result.loc[numeric.notna() & (numeric < -threshold)] = -1
    result.loc[numeric.notna() & (numeric.abs() <= threshold)] = 0
    return result


def _build_diagnostics(
    signals: pd.DataFrame,
    quote_features: pd.DataFrame,
    *,
    config: SignalRuleConfig,
) -> SignalDiagnostics:
    signal_available = signals[SIGNAL_INPUT_AVAILABLE].astype(bool)
    long_mask = signals[SEQUENTIAL_GATE_SIGNAL] == 1
    short_mask = signals[SEQUENTIAL_GATE_SIGNAL] == -1
    label_columns = [column for column in signals.columns if column.startswith("future_midquote_")]
    return SignalDiagnostics(
        input_feature_rows=len(signals),
        input_quote_rows=len(quote_features),
        output_signal_rows=len(signals),
        row_preserving=True,
        signal_rule="sequential_gate_qi_signed_flow_qr_v1",
        signal_columns=(
            SIGNAL_QUOTE_EVENT_TIME,
            SIGNAL_QUOTE_LAG_MS,
            SIGNAL_MIDQUOTE,
            SIGNAL_QUOTE_IMBALANCE,
            SIGNAL_QUOTE_REVISION_BPS,
            SIGNAL_QUOTED_SPREAD,
            SIGNAL_RELATIVE_SPREAD,
            QI_SIGNAL,
            SIGNED_FLOW_SIGNAL,
            QR_SIGNAL,
            SIGNAL_INPUT_AVAILABLE,
            SEQUENTIAL_GATE_SIGNAL,
            SIGNAL_REASON,
        ),
        signal_group_keys=(SYMBOL, TRADING_DATE),
        signed_flow_column=config.signed_flow_column,
        qi_threshold=config.qi_threshold,
        signed_flow_threshold=config.signed_flow_threshold,
        qr_threshold_bps=config.qr_threshold_bps,
        threshold_selection_policy=THRESHOLD_SELECTION_POLICY,
        label_usage_policy=LABEL_USAGE_POLICY,
        signal_session_policy=SIGNAL_SESSION_POLICY,
        signal_input_available_rows=int(signal_available.sum()),
        signal_input_missing_rows=int((~signal_available).sum()),
        long_signal_rows=int(long_mask.sum()),
        short_signal_rows=int(short_mask.sum()),
        no_trade_rows=int((~(long_mask | short_mask)).sum()),
        qi_long_rows=int((signals[QI_SIGNAL] == 1).sum()),
        qi_short_rows=int((signals[QI_SIGNAL] == -1).sum()),
        signed_flow_long_rows=int((signals[SIGNED_FLOW_SIGNAL] == 1).sum()),
        signed_flow_short_rows=int((signals[SIGNED_FLOW_SIGNAL] == -1).sum()),
        qr_long_rows=int((signals[QR_SIGNAL] == 1).sum()),
        qr_short_rows=int((signals[QR_SIGNAL] == -1).sum()),
        labels_retained=bool(label_columns),
    )


def _quote_signal_columns() -> tuple[str, ...]:
    return (SIGNAL_QUOTE_EVENT_TIME, *_quote_signal_value_columns())


def _quote_signal_value_columns() -> tuple[str, ...]:
    return (
        SIGNAL_MIDQUOTE,
        SIGNAL_QUOTE_IMBALANCE,
        SIGNAL_QUOTE_REVISION_BPS,
        SIGNAL_QUOTED_SPREAD,
        SIGNAL_RELATIVE_SPREAD,
    )


def _order_signal_columns(signals: pd.DataFrame, feature_columns: tuple[str, ...]) -> pd.DataFrame:
    signal_columns = (
        SIGNAL_QUOTE_EVENT_TIME,
        SIGNAL_QUOTE_LAG_MS,
        SIGNAL_MIDQUOTE,
        SIGNAL_QUOTE_IMBALANCE,
        SIGNAL_QUOTE_REVISION_BPS,
        SIGNAL_QUOTED_SPREAD,
        SIGNAL_RELATIVE_SPREAD,
        QI_SIGNAL,
        SIGNED_FLOW_SIGNAL,
        QR_SIGNAL,
        SIGNAL_INPUT_AVAILABLE,
        SEQUENTIAL_GATE_SIGNAL,
        SIGNAL_REASON,
    )
    preferred_columns = (*feature_columns, *signal_columns)
    ordered_columns: list[str] = []
    for column in preferred_columns:
        if column in signals.columns and column not in ordered_columns:
            ordered_columns.append(column)
    ordered_columns.extend(column for column in signals.columns if column not in ordered_columns)
    return signals.loc[:, ordered_columns]
