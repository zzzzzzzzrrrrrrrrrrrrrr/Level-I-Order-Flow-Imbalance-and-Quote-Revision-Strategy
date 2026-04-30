"""Cleaning rules and diagnostics for normalized quote data."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from ..schema import ASK, ASK_SIZE, BID, BID_SIZE, validate_quote_frame
from .audit import (
    AuditedCleaningResult,
    CleaningRule,
    CleaningRuleDiagnostics,
    apply_drop_rule,
    empty_rejected_frame,
)

QUOTE_CLEANING_RULES_V2: tuple[CleaningRule, ...] = (
    CleaningRule(
        rule_id="Q001_non_positive_prices",
        description="Remove quotes with bid <= 0 or ask <= 0.",
        input_columns=(BID, ASK),
    ),
    CleaningRule(
        rule_id="Q002_negative_depth",
        description="Remove quotes with bid_size < 0 or ask_size < 0.",
        input_columns=(BID_SIZE, ASK_SIZE),
    ),
    CleaningRule(
        rule_id="Q003_crossed_market",
        description="Remove quotes with ask < bid.",
        input_columns=(BID, ASK),
    ),
)


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

    audited = clean_quotes_v2(quotes)
    positive_bid = quotes.loc[quotes[BID] > 0]
    positive_ask = positive_bid.loc[positive_bid[ASK] > 0]
    nonnegative_bid_size = positive_ask.loc[positive_ask[BID_SIZE] >= 0]
    nonnegative_ask_size = nonnegative_bid_size.loc[nonnegative_bid_size[ASK_SIZE] >= 0]

    diagnostics = QuoteHardConstraintDiagnostics(
        input_rows=len(quotes),
        removed_nonpositive_bid_rows=len(quotes) - len(positive_bid),
        removed_nonpositive_ask_rows=len(positive_bid) - len(positive_ask),
        removed_negative_bid_size_rows=len(positive_ask) - len(nonnegative_bid_size),
        removed_negative_ask_size_rows=len(nonnegative_bid_size) - len(nonnegative_ask_size),
        removed_crossed_quote_rows=len(nonnegative_ask_size) - len(audited.cleaned),
        output_rows=len(audited.cleaned),
    )
    return audited.cleaned, diagnostics


def clean_quotes_v2(quotes: pd.DataFrame) -> AuditedCleaningResult:
    """Apply auditable quote cleaning rules and retain rejected rows."""

    validate_quote_frame(quotes)

    current = quotes.copy()
    rejected_frames: list[pd.DataFrame] = []
    diagnostics: list[CleaningRuleDiagnostics] = []

    for rule in QUOTE_CLEANING_RULES_V2:
        if rule.rule_id == "Q001_non_positive_prices":
            keep_mask = (current[BID] > 0) & (current[ASK] > 0)
        elif rule.rule_id == "Q002_negative_depth":
            keep_mask = (current[BID_SIZE] >= 0) & (current[ASK_SIZE] >= 0)
        elif rule.rule_id == "Q003_crossed_market":
            keep_mask = current[ASK] >= current[BID]
        else:  # pragma: no cover - guarded by the static rule list
            raise ValueError(f"Unsupported quote cleaning rule: {rule.rule_id}")

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
        else empty_rejected_frame(tuple(quotes.columns))
    )
    return AuditedCleaningResult(
        cleaned=current.reset_index(drop=True),
        rejected=rejected_rows,
        diagnostics=tuple(diagnostics),
    )


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
