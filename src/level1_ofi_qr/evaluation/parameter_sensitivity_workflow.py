"""File-based workflow for parameter sensitivity v1."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import json
from pathlib import Path

import pandas as pd

from ..alignment import TRADING_DATE
from ..schema import EVENT_TIME, SYMBOL
from ..signals.rules import SIGNAL_MIDQUOTE, SIGNAL_QUOTED_SPREAD
from ..utils import DataSliceConfig
from .parameter_sensitivity import (
    PARAMETER_SENSITIVITY_POLICY_NOTE,
    ParameterSensitivityConfig,
    ParameterSensitivityDiagnostics,
    run_parameter_sensitivity_v1,
)


class ParameterSensitivityWorkflowError(ValueError):
    """Raised when parameter sensitivity workflow inputs cannot be resolved."""


@dataclass(frozen=True)
class ParameterSensitivityInputPaths:
    """Input paths for parameter sensitivity v1."""

    signal_path: Path


@dataclass(frozen=True)
class ParameterSensitivityOutputPaths:
    """Output paths written by parameter sensitivity v1."""

    summary_csv_path: Path
    manifest_path: Path


@dataclass(frozen=True)
class ParameterSensitivityBuildResult:
    """Parameter sensitivity summary, paths, and diagnostics."""

    summary: pd.DataFrame
    paths: ParameterSensitivityOutputPaths
    diagnostics: ParameterSensitivityDiagnostics


def find_parameter_sensitivity_input(
    config: DataSliceConfig,
    *,
    processed_dir: str | Path | None = None,
) -> ParameterSensitivityInputPaths:
    """Find signal input rows for parameter sensitivity v1."""

    root = Path(processed_dir or config.storage["processed_dir"]) / config.slice_name
    signal_path = root / f"{config.slice_name}_signals_v1.csv"
    if not signal_path.exists():
        raise ParameterSensitivityWorkflowError(
            f"Parameter sensitivity input file is missing: {signal_path}. "
            "Run scripts/build_signals.py first."
        )
    return ParameterSensitivityInputPaths(signal_path=signal_path)


def build_parameter_sensitivity(
    config: DataSliceConfig,
    *,
    processed_dir: str | Path | None = None,
    output_dir: str | Path | None = None,
    sensitivity_config: ParameterSensitivityConfig = ParameterSensitivityConfig(),
) -> ParameterSensitivityBuildResult:
    """Run parameter sensitivity v1 from signal rows."""

    inputs = find_parameter_sensitivity_input(config, processed_dir=processed_dir)
    signal_rows = _read_signal_csv(inputs.signal_path, config=sensitivity_config)
    result = run_parameter_sensitivity_v1(signal_rows, config=sensitivity_config)
    paths = _write_parameter_sensitivity_outputs(
        config,
        inputs=inputs,
        summary=result.summary,
        diagnostics=result.diagnostics,
        output_dir=output_dir or processed_dir,
    )
    return ParameterSensitivityBuildResult(
        summary=result.summary,
        paths=paths,
        diagnostics=result.diagnostics,
    )


def _read_signal_csv(path: Path, *, config: ParameterSensitivityConfig) -> pd.DataFrame:
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


def _write_parameter_sensitivity_outputs(
    config: DataSliceConfig,
    *,
    inputs: ParameterSensitivityInputPaths,
    summary: pd.DataFrame,
    diagnostics: ParameterSensitivityDiagnostics,
    output_dir: str | Path | None,
) -> ParameterSensitivityOutputPaths:
    output_root = Path(output_dir or config.storage["processed_dir"]) / config.slice_name
    output_root.mkdir(parents=True, exist_ok=True)

    summary_csv_path = output_root / f"{config.slice_name}_parameter_sensitivity_v1.csv"
    manifest_path = output_root / f"{config.slice_name}_parameter_sensitivity_v1_manifest.json"

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
        "parameter_sensitivity_status": {
            "parameter_sensitivity_implemented": "v1_grid_report",
            "parameter_sensitivity_policy": diagnostics.parameter_sensitivity_policy,
            "parameter_selection_policy": diagnostics.parameter_selection_policy,
            "train_window_selection_implemented": (
                diagnostics.train_window_selection_implemented
            ),
            "model_training_implemented": diagnostics.model_training_implemented,
            "official_fee_model_implemented": diagnostics.official_fee_model_implemented,
            "passive_execution_implemented": diagnostics.passive_execution_implemented,
            "research_grade_backtest": diagnostics.research_grade_backtest,
        },
        "parameter_sensitivity_scope_note": PARAMETER_SENSITIVITY_POLICY_NOTE,
        "diagnostics": asdict(diagnostics),
        "summary": summary.to_dict(orient="records"),
    }
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return ParameterSensitivityOutputPaths(
        summary_csv_path=summary_csv_path,
        manifest_path=manifest_path,
    )
