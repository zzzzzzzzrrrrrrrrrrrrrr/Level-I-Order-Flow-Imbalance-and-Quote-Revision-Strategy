"""File-based workflow for cost model v1."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import json
from pathlib import Path

import pandas as pd

from ..schema import EVENT_TIME
from ..utils import DataSliceConfig
from .costs import (
    COST_MODEL_POLICY_NOTE,
    CostModelConfig,
    CostModelDiagnostics,
    run_cost_model_v1,
)


class CostModelWorkflowError(ValueError):
    """Raised when cost model workflow inputs cannot be resolved."""


@dataclass(frozen=True)
class CostModelInputPaths:
    """Input paths used for cost diagnostics."""

    signal_path: Path


@dataclass(frozen=True)
class CostModelOutputPaths:
    """Output paths written by cost model v1."""

    summary_csv_path: Path
    manifest_path: Path


@dataclass(frozen=True)
class CostModelBuildResult:
    """Cost summary, paths, and diagnostics."""

    summary: pd.DataFrame
    paths: CostModelOutputPaths
    diagnostics: CostModelDiagnostics


def find_cost_model_input(
    config: DataSliceConfig,
    *,
    processed_dir: str | Path | None = None,
) -> CostModelInputPaths:
    """Find signal input rows for cost model v1."""

    root = Path(processed_dir or config.storage["processed_dir"]) / config.slice_name
    signal_path = root / f"{config.slice_name}_signals_v1.csv"
    if not signal_path.exists():
        raise CostModelWorkflowError(
            f"Cost model input file is missing: {signal_path}. "
            "Run scripts/build_signals.py first."
        )
    return CostModelInputPaths(signal_path=signal_path)


def build_cost_model_diagnostics(
    config: DataSliceConfig,
    *,
    processed_dir: str | Path | None = None,
    output_dir: str | Path | None = None,
    cost_config: CostModelConfig = CostModelConfig(),
) -> CostModelBuildResult:
    """Run cost model v1 from signal rows."""

    inputs = find_cost_model_input(config, processed_dir=processed_dir)
    signal_rows = _read_signal_csv(inputs.signal_path)
    result = run_cost_model_v1(signal_rows, config=cost_config)
    paths = _write_cost_model_outputs(
        config,
        inputs=inputs,
        summary=result.summary,
        diagnostics=result.diagnostics,
        output_dir=output_dir or processed_dir,
    )
    return CostModelBuildResult(
        summary=result.summary,
        paths=paths,
        diagnostics=result.diagnostics,
    )


def _read_signal_csv(path: Path) -> pd.DataFrame:
    frame = pd.read_csv(path)
    frame[EVENT_TIME] = pd.to_datetime(frame[EVENT_TIME], format="mixed")
    return frame


def _write_cost_model_outputs(
    config: DataSliceConfig,
    *,
    inputs: CostModelInputPaths,
    summary: pd.DataFrame,
    diagnostics: CostModelDiagnostics,
    output_dir: str | Path | None,
) -> CostModelOutputPaths:
    output_root = Path(output_dir or config.storage["processed_dir"]) / config.slice_name
    output_root.mkdir(parents=True, exist_ok=True)

    summary_csv_path = output_root / f"{config.slice_name}_cost_model_v1.csv"
    manifest_path = output_root / f"{config.slice_name}_cost_model_v1_manifest.json"

    summary.to_csv(summary_csv_path, index=False)
    manifest = {
        "slice_name": config.slice_name,
        "inputs": {
            "signal_path": str(inputs.signal_path),
        },
        "outputs": {
            "summary_csv_path": str(summary_csv_path),
            "manifest_path": str(manifest_path),
        },
        "cost_model_status": {
            "cost_model_implemented": "v1_diagnostic",
            "cost_model_policy": diagnostics.cost_model_policy,
            "execution_cost_policy": diagnostics.execution_cost_policy,
            "round_trip_cost_policy": diagnostics.round_trip_cost_policy,
            "broker_fee_model_implemented": diagnostics.broker_fee_model_implemented,
            "sec_finra_fee_model_implemented": diagnostics.sec_finra_fee_model_implemented,
            "position_accounting_implemented": diagnostics.position_accounting_implemented,
            "passive_fill_simulation_implemented": (
                diagnostics.passive_fill_simulation_implemented
            ),
            "backtest_implemented": diagnostics.backtest_implemented,
            "research_grade_pnl": diagnostics.research_grade_pnl,
        },
        "cost_model_scope_note": COST_MODEL_POLICY_NOTE,
        "diagnostics": asdict(diagnostics),
        "summary": summary.to_dict(orient="records"),
    }
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    return CostModelOutputPaths(
        summary_csv_path=summary_csv_path,
        manifest_path=manifest_path,
    )

