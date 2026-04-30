"""File-based workflow for target-position accounting v1."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import json
from pathlib import Path

import pandas as pd

from ..schema import EVENT_TIME
from ..utils import DataSliceConfig
from .target_position import (
    TARGET_POSITION_POLICY_NOTE,
    TargetPositionAccountingConfig,
    TargetPositionAccountingDiagnostics,
    run_target_position_accounting_v1,
)


class TargetPositionAccountingWorkflowError(ValueError):
    """Raised when target-position accounting workflow inputs cannot be resolved."""


@dataclass(frozen=True)
class TargetPositionAccountingInputPaths:
    """Input paths for target-position accounting v1."""

    signal_path: Path


@dataclass(frozen=True)
class TargetPositionAccountingOutputPaths:
    """Output paths written by target-position accounting v1."""

    orders_csv_path: Path
    ledger_csv_path: Path
    summary_csv_path: Path
    manifest_path: Path


@dataclass(frozen=True)
class TargetPositionAccountingBuildResult:
    """Target-position accounting frames, paths, and diagnostics."""

    orders: pd.DataFrame
    ledger: pd.DataFrame
    summary: pd.DataFrame
    paths: TargetPositionAccountingOutputPaths
    diagnostics: TargetPositionAccountingDiagnostics


def find_target_position_accounting_input(
    config: DataSliceConfig,
    *,
    processed_dir: str | Path | None = None,
) -> TargetPositionAccountingInputPaths:
    """Find signal input rows for target-position accounting v1."""

    root = Path(processed_dir or config.storage["processed_dir"]) / config.slice_name
    signal_path = root / f"{config.slice_name}_signals_v1.csv"
    if not signal_path.exists():
        raise TargetPositionAccountingWorkflowError(
            f"Target-position accounting input file is missing: {signal_path}. "
            "Run scripts/build_signals.py first."
        )
    return TargetPositionAccountingInputPaths(signal_path=signal_path)


def build_target_position_accounting(
    config: DataSliceConfig,
    *,
    processed_dir: str | Path | None = None,
    output_dir: str | Path | None = None,
    accounting_config: TargetPositionAccountingConfig = TargetPositionAccountingConfig(),
) -> TargetPositionAccountingBuildResult:
    """Run target-position accounting v1 from signal rows."""

    inputs = find_target_position_accounting_input(config, processed_dir=processed_dir)
    signal_rows = _read_signal_csv(inputs.signal_path)
    result = run_target_position_accounting_v1(signal_rows, config=accounting_config)
    paths = _write_target_position_outputs(
        config,
        inputs=inputs,
        orders=result.orders,
        ledger=result.ledger,
        summary=result.summary,
        diagnostics=result.diagnostics,
        output_dir=output_dir or processed_dir,
    )
    return TargetPositionAccountingBuildResult(
        orders=result.orders,
        ledger=result.ledger,
        summary=result.summary,
        paths=paths,
        diagnostics=result.diagnostics,
    )


def _read_signal_csv(path: Path) -> pd.DataFrame:
    frame = pd.read_csv(path)
    frame[EVENT_TIME] = pd.to_datetime(frame[EVENT_TIME], format="mixed")
    return frame


def _write_target_position_outputs(
    config: DataSliceConfig,
    *,
    inputs: TargetPositionAccountingInputPaths,
    orders: pd.DataFrame,
    ledger: pd.DataFrame,
    summary: pd.DataFrame,
    diagnostics: TargetPositionAccountingDiagnostics,
    output_dir: str | Path | None,
) -> TargetPositionAccountingOutputPaths:
    output_root = Path(output_dir or config.storage["processed_dir"]) / config.slice_name
    output_root.mkdir(parents=True, exist_ok=True)

    orders_csv_path = output_root / f"{config.slice_name}_target_position_accounting_v1_orders.csv"
    ledger_csv_path = output_root / f"{config.slice_name}_target_position_accounting_v1_ledger.csv"
    summary_csv_path = output_root / f"{config.slice_name}_target_position_accounting_v1_summary.csv"
    manifest_path = output_root / f"{config.slice_name}_target_position_accounting_v1_manifest.json"

    orders.to_csv(orders_csv_path, index=False)
    ledger.to_csv(ledger_csv_path, index=False)
    summary.to_csv(summary_csv_path, index=False)
    manifest = {
        "slice_name": config.slice_name,
        "inputs": {
            "signal_path": str(inputs.signal_path),
        },
        "outputs": {
            "orders_csv_path": str(orders_csv_path),
            "ledger_csv_path": str(ledger_csv_path),
            "summary_csv_path": str(summary_csv_path),
            "manifest_path": str(manifest_path),
        },
        "target_position_accounting_status": {
            "target_position_accounting_implemented": "v1_scaffold",
            "target_position_policy": diagnostics.target_position_policy,
            "target_mapping_policy": diagnostics.target_mapping_policy,
            "order_execution_policy": diagnostics.order_execution_policy,
            "position_limit_policy": diagnostics.position_limit_policy,
            "no_signal_policy": diagnostics.no_signal_policy,
            "eod_policy": diagnostics.eod_policy,
            "position_limit_implemented": diagnostics.position_limit_implemented,
            "cooldown_implemented": diagnostics.cooldown_implemented,
            "max_trades_per_day_implemented": diagnostics.max_trades_per_day_implemented,
            "eod_flat_implemented": diagnostics.eod_flat_implemented,
            "risk_controls_implemented": diagnostics.risk_controls_implemented,
            "passive_fill_simulation_implemented": (
                diagnostics.passive_fill_simulation_implemented
            ),
            "order_book_fill_simulation_implemented": (
                diagnostics.order_book_fill_simulation_implemented
            ),
            "broker_fee_model_implemented": diagnostics.broker_fee_model_implemented,
            "sec_finra_fee_model_implemented": diagnostics.sec_finra_fee_model_implemented,
            "parameter_optimization_implemented": (
                diagnostics.parameter_optimization_implemented
            ),
            "research_grade_backtest": diagnostics.research_grade_backtest,
        },
        "target_position_accounting_scope_note": TARGET_POSITION_POLICY_NOTE,
        "diagnostics": asdict(diagnostics),
        "summary": summary.to_dict(orient="records"),
    }
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return TargetPositionAccountingOutputPaths(
        orders_csv_path=orders_csv_path,
        ledger_csv_path=ledger_csv_path,
        summary_csv_path=summary_csv_path,
        manifest_path=manifest_path,
    )
