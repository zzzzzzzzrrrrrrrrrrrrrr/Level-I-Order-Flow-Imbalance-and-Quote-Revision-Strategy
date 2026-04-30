"""File-based workflow for execution accounting v1."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import json
from pathlib import Path

import pandas as pd

from ..schema import EVENT_TIME
from ..utils import DataSliceConfig
from .accounting import (
    EXECUTION_ACCOUNTING_POLICY_NOTE,
    ExecutionAccountingConfig,
    ExecutionAccountingDiagnostics,
    run_execution_accounting_v1,
)


class ExecutionAccountingWorkflowError(ValueError):
    """Raised when execution accounting workflow inputs cannot be resolved."""


@dataclass(frozen=True)
class ExecutionAccountingInputPaths:
    """Input paths for execution accounting v1."""

    signal_path: Path


@dataclass(frozen=True)
class ExecutionAccountingOutputPaths:
    """Output paths written by execution accounting v1."""

    trades_csv_path: Path
    ledger_csv_path: Path
    summary_csv_path: Path
    manifest_path: Path


@dataclass(frozen=True)
class ExecutionAccountingBuildResult:
    """Execution accounting frames, paths, and diagnostics."""

    trades: pd.DataFrame
    ledger: pd.DataFrame
    summary: pd.DataFrame
    paths: ExecutionAccountingOutputPaths
    diagnostics: ExecutionAccountingDiagnostics


def find_execution_accounting_input(
    config: DataSliceConfig,
    *,
    processed_dir: str | Path | None = None,
) -> ExecutionAccountingInputPaths:
    """Find signal input rows for execution accounting v1."""

    root = Path(processed_dir or config.storage["processed_dir"]) / config.slice_name
    signal_path = root / f"{config.slice_name}_signals_v1.csv"
    if not signal_path.exists():
        raise ExecutionAccountingWorkflowError(
            f"Execution accounting input file is missing: {signal_path}. "
            "Run scripts/build_signals.py first."
        )
    return ExecutionAccountingInputPaths(signal_path=signal_path)


def build_execution_accounting(
    config: DataSliceConfig,
    *,
    processed_dir: str | Path | None = None,
    output_dir: str | Path | None = None,
    accounting_config: ExecutionAccountingConfig = ExecutionAccountingConfig(),
) -> ExecutionAccountingBuildResult:
    """Run execution accounting v1 from signal rows."""

    inputs = find_execution_accounting_input(config, processed_dir=processed_dir)
    signal_rows = _read_signal_csv(inputs.signal_path)
    result = run_execution_accounting_v1(signal_rows, config=accounting_config)
    paths = _write_execution_accounting_outputs(
        config,
        inputs=inputs,
        trades=result.trades,
        ledger=result.ledger,
        summary=result.summary,
        diagnostics=result.diagnostics,
        output_dir=output_dir or processed_dir,
    )
    return ExecutionAccountingBuildResult(
        trades=result.trades,
        ledger=result.ledger,
        summary=result.summary,
        paths=paths,
        diagnostics=result.diagnostics,
    )


def _read_signal_csv(path: Path) -> pd.DataFrame:
    frame = pd.read_csv(path)
    frame[EVENT_TIME] = pd.to_datetime(frame[EVENT_TIME], format="mixed")
    return frame


def _write_execution_accounting_outputs(
    config: DataSliceConfig,
    *,
    inputs: ExecutionAccountingInputPaths,
    trades: pd.DataFrame,
    ledger: pd.DataFrame,
    summary: pd.DataFrame,
    diagnostics: ExecutionAccountingDiagnostics,
    output_dir: str | Path | None,
) -> ExecutionAccountingOutputPaths:
    output_root = Path(output_dir or config.storage["processed_dir"]) / config.slice_name
    output_root.mkdir(parents=True, exist_ok=True)

    trades_csv_path = output_root / f"{config.slice_name}_execution_accounting_v1_trades.csv"
    ledger_csv_path = output_root / f"{config.slice_name}_execution_accounting_v1_ledger.csv"
    summary_csv_path = output_root / f"{config.slice_name}_execution_accounting_v1_summary.csv"
    manifest_path = output_root / f"{config.slice_name}_execution_accounting_v1_manifest.json"

    trades.to_csv(trades_csv_path, index=False)
    ledger.to_csv(ledger_csv_path, index=False)
    summary.to_csv(summary_csv_path, index=False)
    manifest = {
        "slice_name": config.slice_name,
        "inputs": {
            "signal_path": str(inputs.signal_path),
        },
        "outputs": {
            "trades_csv_path": str(trades_csv_path),
            "ledger_csv_path": str(ledger_csv_path),
            "summary_csv_path": str(summary_csv_path),
            "manifest_path": str(manifest_path),
        },
        "execution_accounting_status": {
            "execution_accounting_implemented": "v1_scaffold",
            "accounting_policy": diagnostics.accounting_policy,
            "entry_execution_policy": diagnostics.entry_execution_policy,
            "exit_execution_policy": diagnostics.exit_execution_policy,
            "position_policy": diagnostics.position_policy,
            "position_accounting_implemented": diagnostics.position_accounting_implemented,
            "cash_accounting_implemented": diagnostics.cash_accounting_implemented,
            "inventory_accounting_implemented": diagnostics.inventory_accounting_implemented,
            "pnl_attribution_implemented": diagnostics.pnl_attribution_implemented,
            "passive_fill_simulation_implemented": (
                diagnostics.passive_fill_simulation_implemented
            ),
            "order_book_fill_simulation_implemented": (
                diagnostics.order_book_fill_simulation_implemented
            ),
            "broker_fee_model_implemented": diagnostics.broker_fee_model_implemented,
            "sec_finra_fee_model_implemented": diagnostics.sec_finra_fee_model_implemented,
            "risk_controls_implemented": diagnostics.risk_controls_implemented,
            "parameter_optimization_implemented": (
                diagnostics.parameter_optimization_implemented
            ),
            "research_grade_backtest": diagnostics.research_grade_backtest,
        },
        "execution_accounting_scope_note": EXECUTION_ACCOUNTING_POLICY_NOTE,
        "diagnostics": asdict(diagnostics),
        "summary": summary.to_dict(orient="records"),
    }
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return ExecutionAccountingOutputPaths(
        trades_csv_path=trades_csv_path,
        ledger_csv_path=ledger_csv_path,
        summary_csv_path=summary_csv_path,
        manifest_path=manifest_path,
    )
