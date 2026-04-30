"""Auditable cleaning-rule application helpers."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass(frozen=True)
class CleaningRule:
    """One explicit cleaning rule in the data contract."""

    rule_id: str
    description: str
    input_columns: tuple[str, ...]
    action: str = "drop"
    severity: str = "fail"
    diagnostics: bool = True
    unresolved: bool = False


@dataclass(frozen=True)
class CleaningRuleDiagnostics:
    """Row-count diagnostics for one cleaning-rule application."""

    rule_id: str
    description: str
    input_columns: tuple[str, ...]
    action: str
    severity: str
    before: int
    dropped_count: int
    after: int


@dataclass(frozen=True)
class AuditedCleaningResult:
    """Cleaned rows, rejected rows, and per-rule diagnostics."""

    cleaned: pd.DataFrame
    rejected: pd.DataFrame
    diagnostics: tuple[CleaningRuleDiagnostics, ...]


REJECT_RULE_ID = "rule_id"
REJECT_REASON = "reject_reason"
REJECT_ACTION = "rule_action"
REJECT_SEVERITY = "rule_severity"
RAW_ROW_INDEX = "raw_row_index"


def apply_drop_rule(
    frame: pd.DataFrame,
    *,
    rule: CleaningRule,
    keep_mask: pd.Series,
) -> tuple[pd.DataFrame, pd.DataFrame, CleaningRuleDiagnostics]:
    """Apply one drop rule and return kept rows, rejected rows, and diagnostics."""

    if rule.action != "drop":
        raise ValueError(f"Unsupported cleaning action for {rule.rule_id}: {rule.action!r}.")

    before = len(frame)
    keep_mask = keep_mask.fillna(False).astype(bool)
    rejected = frame.loc[~keep_mask].copy()
    if not rejected.empty:
        if RAW_ROW_INDEX not in rejected.columns:
            rejected[RAW_ROW_INDEX] = rejected.index
        rejected[REJECT_RULE_ID] = rule.rule_id
        rejected[REJECT_REASON] = rule.description
        rejected[REJECT_ACTION] = rule.action
        rejected[REJECT_SEVERITY] = rule.severity
        rejected = _order_rejected_columns(rejected, rule.input_columns)

    kept = frame.loc[keep_mask].reset_index(drop=True)
    diagnostics = CleaningRuleDiagnostics(
        rule_id=rule.rule_id,
        description=rule.description,
        input_columns=rule.input_columns,
        action=rule.action,
        severity=rule.severity,
        before=before,
        dropped_count=len(rejected),
        after=len(kept),
    )
    return kept, rejected.reset_index(drop=True), diagnostics


def empty_rejected_frame(columns: tuple[str, ...]) -> pd.DataFrame:
    """Create an empty rejected-row frame with audit columns."""

    return pd.DataFrame(columns=_ordered_rejected_columns(columns, ()))


def _order_rejected_columns(rejected: pd.DataFrame, input_columns: tuple[str, ...]) -> pd.DataFrame:
    return rejected.loc[:, _ordered_rejected_columns(tuple(rejected.columns), input_columns)]


def _ordered_rejected_columns(
    columns: tuple[str, ...],
    input_columns: tuple[str, ...],
) -> list[str]:
    priority = (
        "event_time",
        "symbol",
        "source",
        RAW_ROW_INDEX,
        REJECT_RULE_ID,
        REJECT_REASON,
        REJECT_ACTION,
        REJECT_SEVERITY,
        *input_columns,
    )
    ordered: list[str] = []
    for column in priority:
        if column in columns and column not in ordered:
            ordered.append(column)
    ordered.extend(column for column in columns if column not in ordered)
    return ordered
