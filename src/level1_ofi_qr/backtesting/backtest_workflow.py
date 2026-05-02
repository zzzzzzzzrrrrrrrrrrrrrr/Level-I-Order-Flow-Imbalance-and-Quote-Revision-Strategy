"""File-based workflow for backtest v1."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import json
from pathlib import Path

import pandas as pd

from ..alignment import TRADING_DATE
from ..schema import EVENT_TIME, SYMBOL
from ..signals.rules import SIGNAL_MIDQUOTE, SIGNAL_QUOTED_SPREAD
from ..utils import DataSliceConfig
from .backtest import (
    BACKTEST_V1_POLICY_NOTE,
    BacktestV1Config,
    BacktestV1Diagnostics,
    run_backtest_v1,
)


class BacktestV1WorkflowError(ValueError):
    """Raised when backtest v1 workflow inputs cannot be resolved."""


@dataclass(frozen=True)
class BacktestV1InputPaths:
    """Input paths for backtest v1."""

    signal_path: Path
    tvt_summary_path: Path


@dataclass(frozen=True)
class BacktestV1OutputPaths:
    """Output paths written by backtest v1."""

    orders_csv_path: Path
    ledger_csv_path: Path
    summary_csv_path: Path
    manifest_path: Path


@dataclass(frozen=True)
class BacktestV1BuildResult:
    """Backtest v1 frames, paths, and diagnostics."""

    orders: pd.DataFrame
    ledger: pd.DataFrame
    summary: pd.DataFrame
    paths: BacktestV1OutputPaths
    diagnostics: BacktestV1Diagnostics


def find_backtest_v1_inputs(
    config: DataSliceConfig,
    *,
    processed_dir: str | Path | None = None,
    tvt_summary_path: str | Path | None = None,
) -> BacktestV1InputPaths:
    """Find signal rows and TVT summary inputs for backtest v1."""

    root = Path(processed_dir or config.storage["processed_dir"]) / config.slice_name
    signal_path = root / f"{config.slice_name}_signals_v1.csv"
    resolved_tvt_summary_path = (
        Path(tvt_summary_path)
        if tvt_summary_path is not None
        else root / f"{config.slice_name}_tvt_parameter_selection_v1.csv"
    )
    missing = []
    if not signal_path.exists():
        missing.append(str(signal_path))
    if not resolved_tvt_summary_path.exists():
        missing.append(str(resolved_tvt_summary_path))
    if missing:
        raise BacktestV1WorkflowError(
            "Backtest v1 input files are missing: "
            f"{missing}. Run scripts/build_signals.py and "
            "scripts/run_tvt_parameter_selection.py first."
        )
    return BacktestV1InputPaths(
        signal_path=signal_path,
        tvt_summary_path=resolved_tvt_summary_path,
    )


def build_backtest_v1(
    config: DataSliceConfig,
    *,
    processed_dir: str | Path | None = None,
    output_dir: str | Path | None = None,
    tvt_summary_path: str | Path | None = None,
    backtest_config: BacktestV1Config = BacktestV1Config(),
) -> BacktestV1BuildResult:
    """Run backtest v1 from signal rows and TVT-selected parameters."""

    inputs = find_backtest_v1_inputs(
        config,
        processed_dir=processed_dir,
        tvt_summary_path=tvt_summary_path,
    )
    signal_rows = _read_signal_csv(inputs.signal_path, config=backtest_config)
    tvt_summary = pd.read_csv(inputs.tvt_summary_path)
    result = run_backtest_v1(signal_rows, tvt_summary, config=backtest_config)
    paths = _write_backtest_outputs(
        config,
        inputs=inputs,
        orders=result.orders,
        ledger=result.ledger,
        summary=result.summary,
        diagnostics=result.diagnostics,
        output_dir=output_dir or processed_dir,
    )
    return BacktestV1BuildResult(
        orders=result.orders,
        ledger=result.ledger,
        summary=result.summary,
        paths=paths,
        diagnostics=result.diagnostics,
    )


def _read_signal_csv(path: Path, *, config: BacktestV1Config) -> pd.DataFrame:
    usecols = [
        EVENT_TIME,
        SYMBOL,
        TRADING_DATE,
        config.signal_column,
        SIGNAL_MIDQUOTE,
        SIGNAL_QUOTED_SPREAD,
    ]
    frame = pd.read_csv(path, usecols=tuple(dict.fromkeys(usecols)))
    frame[EVENT_TIME] = pd.to_datetime(frame[EVENT_TIME], format="mixed")
    return frame


def _write_backtest_outputs(
    config: DataSliceConfig,
    *,
    inputs: BacktestV1InputPaths,
    orders: pd.DataFrame,
    ledger: pd.DataFrame,
    summary: pd.DataFrame,
    diagnostics: BacktestV1Diagnostics,
    output_dir: str | Path | None,
) -> BacktestV1OutputPaths:
    output_root = Path(output_dir or config.storage["processed_dir"]) / config.slice_name
    output_root.mkdir(parents=True, exist_ok=True)

    orders_csv_path = output_root / f"{config.slice_name}_backtest_v1_orders.csv"
    ledger_csv_path = output_root / f"{config.slice_name}_backtest_v1_ledger.csv"
    summary_csv_path = output_root / f"{config.slice_name}_backtest_v1_summary.csv"
    manifest_path = output_root / f"{config.slice_name}_backtest_v1_manifest.json"

    orders.to_csv(orders_csv_path, index=False)
    ledger.to_csv(ledger_csv_path, index=False)
    summary.to_csv(summary_csv_path, index=False)
    manifest = {
        "slice_name": config.slice_name,
        "inputs": {
            "signal_path": str(inputs.signal_path),
            "tvt_summary_path": str(inputs.tvt_summary_path),
        },
        "outputs": {
            "orders_csv_path": str(orders_csv_path),
            "ledger_csv_path": str(ledger_csv_path),
            "summary_csv_path": str(summary_csv_path),
            "manifest_path": str(manifest_path),
        },
        "backtest_v1_status": {
            "backtest_implemented": "v1_tvt_selected_test_accounting",
            "backtest_policy": diagnostics.backtest_policy,
            "parameter_source_policy": diagnostics.parameter_source_policy,
            "evaluation_policy": diagnostics.evaluation_policy,
            "split_source_policy": diagnostics.split_source_policy,
            "target_position_accounting_used": diagnostics.target_position_accounting_used,
            "parameter_reselection_on_test": diagnostics.parameter_reselection_on_test,
            "test_used_for_selection": diagnostics.test_used_for_selection,
            "model_training_implemented": diagnostics.model_training_implemented,
            "passive_fill_simulation_implemented": (
                diagnostics.passive_fill_simulation_implemented
            ),
            "order_book_fill_simulation_implemented": (
                diagnostics.order_book_fill_simulation_implemented
            ),
            "broker_fee_model_implemented": diagnostics.broker_fee_model_implemented,
            "sec_finra_fee_model_implemented": diagnostics.sec_finra_fee_model_implemented,
            "exchange_fee_rebate_model_implemented": (
                diagnostics.exchange_fee_rebate_model_implemented
            ),
            "latency_model_implemented": diagnostics.latency_model_implemented,
            "research_grade_backtest": diagnostics.research_grade_backtest,
        },
        "backtest_v1_scope_note": BACKTEST_V1_POLICY_NOTE,
        "diagnostics": asdict(diagnostics),
        "summary": summary.to_dict(orient="records"),
    }
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return BacktestV1OutputPaths(
        orders_csv_path=orders_csv_path,
        ledger_csv_path=ledger_csv_path,
        summary_csv_path=summary_csv_path,
        manifest_path=manifest_path,
    )
