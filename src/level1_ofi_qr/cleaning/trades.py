"""Cleaning rules and diagnostics for normalized trade data."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from ..schema import TRADE_CORRECTION, TRADE_PRICE, TRADE_SIZE, validate_trade_frame
from .audit import (
    AuditedCleaningResult,
    CleaningRule,
    CleaningRuleDiagnostics,
    apply_drop_rule,
    empty_rejected_frame,
)

TRADE_CLEANING_RULES_V2: tuple[CleaningRule, ...] = (
    CleaningRule(
        rule_id="T001_non_positive_price_or_size",
        description="Remove trades with trade_price <= 0 or trade_size <= 0.",
        input_columns=(TRADE_PRICE, TRADE_SIZE),
    ),
    CleaningRule(
        rule_id="T002_trade_correction",
        description="Keep only uncorrected trades where trade_correction is 0/00.",
        input_columns=(TRADE_CORRECTION,),
    ),
)


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

    audited = clean_trades_v2(trades)
    positive_trade_price = trades.loc[trades[TRADE_PRICE] > 0]
    positive_trade_size = positive_trade_price.loc[positive_trade_price[TRADE_SIZE] > 0]

    diagnostics = TradeHardConstraintDiagnostics(
        input_rows=len(trades),
        removed_nonpositive_trade_price_rows=len(trades) - len(positive_trade_price),
        removed_nonpositive_trade_size_rows=len(positive_trade_price) - len(positive_trade_size),
        output_rows=len(audited.cleaned),
    )
    return audited.cleaned, diagnostics


def clean_trades_v2(trades: pd.DataFrame) -> AuditedCleaningResult:
    """Apply auditable trade cleaning rules and retain rejected rows."""

    validate_trade_frame(trades)

    current = trades.copy()
    rejected_frames: list[pd.DataFrame] = []
    diagnostics: list[CleaningRuleDiagnostics] = []

    for rule in TRADE_CLEANING_RULES_V2:
        if rule.rule_id == "T001_non_positive_price_or_size":
            keep_mask = (current[TRADE_PRICE] > 0) & (current[TRADE_SIZE] > 0)
        elif rule.rule_id == "T002_trade_correction":
            corrections = current[TRADE_CORRECTION].astype("string").str.strip()
            keep_mask = corrections.isin({"0", "00", "0.0"})
        else:  # pragma: no cover - guarded by the static rule list
            raise ValueError(f"Unsupported trade cleaning rule: {rule.rule_id}")

        current, rejected, rule_diagnostics = apply_drop_rule(
            current,
            rule=rule,
            keep_mask=keep_mask,
        )
        if not rejected.empty:
            rejected_frames.append(rejected)
        diagnostics.append(rule_diagnostics)

    rejected_rows = (
        pd.concat(rejected_frames, ignore_index=True)
        if rejected_frames
        else empty_rejected_frame(tuple(trades.columns))
    )
    return AuditedCleaningResult(
        cleaned=current.reset_index(drop=True),
        rejected=rejected_rows,
        diagnostics=tuple(diagnostics),
    )


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
