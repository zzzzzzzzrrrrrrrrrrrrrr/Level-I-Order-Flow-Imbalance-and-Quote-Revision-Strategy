"""Backtest v1 orchestration over frozen TVT-selected parameters.

Backtest v1 is a thin evaluation layer: it reads the candidate selected on a
validation date by TVT parameter selection and runs target-position accounting
only on that candidate's test date.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

import pandas as pd

from ..alignment import TRADING_DATE
from ..execution import TargetPositionAccountingConfig, run_target_position_accounting_v1
from ..schema import EVENT_TIME, SYMBOL
from ..signals import SEQUENTIAL_GATE_SIGNAL

BACKTEST_V1_POLICY_NOTE: Final[str] = (
    "Backtest v1 evaluates frozen TVT-selected target-position accounting "
    "parameters on the held-out test date. It does not reselect parameters on "
    "test data, train a predictive model, simulate passive fills, or include "
    "official broker / SEC / FINRA / exchange fees."
)
BACKTEST_V1_POLICY: Final[str] = "tvt_selected_candidate_test_accounting_v1"
BACKTEST_PARAMETER_SOURCE_POLICY: Final[str] = "frozen_candidate_selected_on_validation"
BACKTEST_EVALUATION_POLICY: Final[str] = "evaluate_selected_candidate_on_test_date_only"
BACKTEST_SPLIT_SOURCE_POLICY: Final[str] = "tvt_parameter_selection_v1"

BACKTEST_METADATA_COLUMNS: Final[tuple[str, ...]] = (
    "backtest_id",
    "fold_id",
    "candidate_id",
    "train_start_date",
    "train_end_date",
    "validation_date",
    "test_date",
    "parameter_source_policy",
)


class BacktestV1Error(ValueError):
    """Raised when backtest v1 cannot be computed."""


@dataclass(frozen=True)
class BacktestV1Config:
    """Configuration for backtest v1."""

    signal_column: str = SEQUENTIAL_GATE_SIGNAL
    selected_column: str = "selected_for_test"
    fold_id: str | None = None


@dataclass(frozen=True)
class BacktestV1Diagnostics:
    """Diagnostics for backtest v1."""

    input_signal_rows: int
    input_tvt_summary_rows: int
    selected_candidate_rows: int
    output_order_rows: int
    output_ledger_rows: int
    output_summary_rows: int
    evaluated_test_dates: tuple[str, ...]
    fold_ids: tuple[str, ...]
    candidate_ids: tuple[str, ...]
    signal_column: str
    backtest_policy: str
    parameter_source_policy: str
    evaluation_policy: str
    split_source_policy: str
    selected_candidate_parameters: tuple[dict[str, object], ...]
    target_position_accounting_used: bool = True
    parameter_reselection_on_test: bool = False
    test_used_for_selection: bool = False
    model_training_implemented: bool = False
    passive_fill_simulation_implemented: bool = False
    order_book_fill_simulation_implemented: bool = False
    broker_fee_model_implemented: bool = False
    sec_finra_fee_model_implemented: bool = False
    exchange_fee_rebate_model_implemented: bool = False
    latency_model_implemented: bool = False
    research_grade_backtest: bool = False


@dataclass(frozen=True)
class BacktestV1Result:
    """Backtest v1 outputs and diagnostics."""

    orders: pd.DataFrame
    ledger: pd.DataFrame
    summary: pd.DataFrame
    diagnostics: BacktestV1Diagnostics


def run_backtest_v1(
    signal_rows: pd.DataFrame,
    tvt_summary: pd.DataFrame,
    *,
    config: BacktestV1Config = BacktestV1Config(),
) -> BacktestV1Result:
    """Evaluate TVT-selected parameters on held-out test dates."""

    _validate_inputs(signal_rows, tvt_summary, config=config)
    rows = signal_rows.copy()
    rows[TRADING_DATE] = rows[TRADING_DATE].astype(str)
    selected_rows = _selected_tvt_rows(tvt_summary, config=config)
    _assert_no_test_selection_leakage(selected_rows)

    order_frames: list[pd.DataFrame] = []
    ledger_frames: list[pd.DataFrame] = []
    summary_frames: list[pd.DataFrame] = []
    evaluated_test_dates: list[str] = []
    fold_ids: list[str] = []
    candidate_ids: list[str] = []
    selected_parameters: list[dict[str, object]] = []

    for selected in selected_rows.to_dict(orient="records"):
        test_date = str(selected["test_date"])
        test_rows = rows.loc[rows[TRADING_DATE] == test_date]
        accounting_config = _accounting_config_from_tvt_row(selected, config=config)
        accounting_result = run_target_position_accounting_v1(
            test_rows,
            config=accounting_config,
        )
        metadata = _metadata_from_tvt_row(selected)
        order_frames.append(_annotate_frame(accounting_result.orders, metadata))
        ledger_frames.append(_annotate_frame(accounting_result.ledger, metadata))
        summary_frames.append(_annotate_frame(accounting_result.summary, metadata))
        evaluated_test_dates.append(test_date)
        fold_ids.append(str(selected["fold_id"]))
        candidate_ids.append(str(selected["candidate_id"]))
        selected_parameters.append(_parameter_record(selected))

    orders = _concat_or_empty(order_frames)
    ledger = _concat_or_empty(ledger_frames)
    summary = _concat_or_empty(summary_frames)
    diagnostics = BacktestV1Diagnostics(
        input_signal_rows=len(rows),
        input_tvt_summary_rows=len(tvt_summary),
        selected_candidate_rows=len(selected_rows),
        output_order_rows=len(orders),
        output_ledger_rows=len(ledger),
        output_summary_rows=len(summary),
        evaluated_test_dates=tuple(evaluated_test_dates),
        fold_ids=tuple(fold_ids),
        candidate_ids=tuple(candidate_ids),
        signal_column=config.signal_column,
        backtest_policy=BACKTEST_V1_POLICY,
        parameter_source_policy=BACKTEST_PARAMETER_SOURCE_POLICY,
        evaluation_policy=BACKTEST_EVALUATION_POLICY,
        split_source_policy=BACKTEST_SPLIT_SOURCE_POLICY,
        selected_candidate_parameters=tuple(selected_parameters),
    )
    return BacktestV1Result(
        orders=orders,
        ledger=ledger,
        summary=summary,
        diagnostics=diagnostics,
    )


def _selected_tvt_rows(
    tvt_summary: pd.DataFrame,
    *,
    config: BacktestV1Config,
) -> pd.DataFrame:
    selected = tvt_summary.loc[_bool_series(tvt_summary[config.selected_column])].copy()
    if config.fold_id is not None:
        selected = selected.loc[selected["fold_id"].astype(str) == config.fold_id]
    if selected.empty:
        raise BacktestV1Error("No TVT-selected candidate rows are available for backtest v1.")
    return selected.reset_index(drop=True)


def _assert_no_test_selection_leakage(selected_rows: pd.DataFrame) -> None:
    if "test_used_for_selection" not in selected_rows.columns:
        return
    if _bool_series(selected_rows["test_used_for_selection"]).any():
        raise BacktestV1Error("Selected TVT rows indicate test data was used for selection.")


def _accounting_config_from_tvt_row(
    row: dict[str, object],
    *,
    config: BacktestV1Config,
) -> TargetPositionAccountingConfig:
    return TargetPositionAccountingConfig(
        signal_column=config.signal_column,
        max_position=float(row["max_position"]),
        cooldown=str(row["cooldown"]),
        max_trades_per_day=_optional_int(row["max_trades_per_day"]),
        fixed_bps=float(row["fixed_bps"]),
        slippage_ticks=float(row["slippage_ticks"]),
        tick_size=float(row["tick_size"]),
    )


def _metadata_from_tvt_row(row: dict[str, object]) -> dict[str, object]:
    fold_id = str(row["fold_id"])
    candidate_id = str(row["candidate_id"])
    test_date = str(row["test_date"])
    return {
        "backtest_id": f"{fold_id}_{candidate_id}_test_{test_date}",
        "fold_id": fold_id,
        "candidate_id": candidate_id,
        "train_start_date": str(row["train_start_date"]),
        "train_end_date": str(row["train_end_date"]),
        "validation_date": str(row["validation_date"]),
        "test_date": test_date,
        "parameter_source_policy": BACKTEST_PARAMETER_SOURCE_POLICY,
    }


def _parameter_record(row: dict[str, object]) -> dict[str, object]:
    return {
        "fold_id": str(row["fold_id"]),
        "candidate_id": str(row["candidate_id"]),
        "test_date": str(row["test_date"]),
        "max_position": float(row["max_position"]),
        "cooldown": str(row["cooldown"]),
        "max_trades_per_day": _optional_int(row["max_trades_per_day"]),
        "fixed_bps": float(row["fixed_bps"]),
        "slippage_ticks": float(row["slippage_ticks"]),
        "tick_size": float(row["tick_size"]),
    }


def _annotate_frame(frame: pd.DataFrame, metadata: dict[str, object]) -> pd.DataFrame:
    result = frame.copy()
    for column, value in reversed(tuple(metadata.items())):
        result.insert(0, column, value)
    return result


def _concat_or_empty(frames: list[pd.DataFrame]) -> pd.DataFrame:
    non_empty = [frame for frame in frames if not frame.empty]
    if not non_empty:
        return pd.DataFrame(columns=BACKTEST_METADATA_COLUMNS)
    return pd.concat(non_empty, ignore_index=True)


def _bool_series(values: pd.Series) -> pd.Series:
    if pd.api.types.is_bool_dtype(values):
        return values.fillna(False)
    if pd.api.types.is_numeric_dtype(values):
        return pd.to_numeric(values, errors="coerce").fillna(0).ne(0)
    normalized = values.astype("string").str.strip().str.lower()
    return normalized.isin(("true", "1", "yes", "y"))


def _optional_int(value: object) -> int | None:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip().lower()
    if text in ("", "none", "nan", "null"):
        return None
    return int(float(text))


def _validate_inputs(
    signal_rows: pd.DataFrame,
    tvt_summary: pd.DataFrame,
    *,
    config: BacktestV1Config,
) -> None:
    required_signal = [EVENT_TIME, SYMBOL, TRADING_DATE, config.signal_column]
    missing_signal = [column for column in required_signal if column not in signal_rows.columns]
    if missing_signal:
        raise BacktestV1Error(f"Signal rows are missing columns: {missing_signal}")
    if not pd.api.types.is_datetime64_any_dtype(signal_rows[EVENT_TIME]):
        raise BacktestV1Error("Signal rows must have datetime event_time values.")

    required_tvt = [
        config.selected_column,
        "fold_id",
        "candidate_id",
        "train_start_date",
        "train_end_date",
        "validation_date",
        "test_date",
        "max_position",
        "cooldown",
        "max_trades_per_day",
        "fixed_bps",
        "slippage_ticks",
        "tick_size",
    ]
    missing_tvt = [column for column in required_tvt if column not in tvt_summary.columns]
    if missing_tvt:
        raise BacktestV1Error(f"TVT summary is missing columns: {missing_tvt}")
