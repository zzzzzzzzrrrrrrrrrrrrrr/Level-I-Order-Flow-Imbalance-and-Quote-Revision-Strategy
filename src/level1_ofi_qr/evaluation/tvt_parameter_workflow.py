"""File-based workflow for train-validation-test parameter selection v1."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import json
from pathlib import Path

import pandas as pd

from ..alignment import TRADING_DATE
from ..schema import EVENT_TIME, SYMBOL
from ..signals.rules import SIGNAL_MIDQUOTE, SIGNAL_QUOTED_SPREAD
from ..utils import DataSliceConfig
from .tvt_parameter_selection import (
    TVT_SELECTION_POLICY_NOTE,
    TVTParameterSelectionConfig,
    TVTParameterSelectionDiagnostics,
    run_tvt_parameter_selection_v1,
)


class TVTParameterSelectionWorkflowError(ValueError):
    """Raised when TVT parameter selection workflow inputs cannot be resolved."""


@dataclass(frozen=True)
class TVTParameterSelectionInputPaths:
    """Input paths for TVT parameter selection v1."""

    signal_path: Path


@dataclass(frozen=True)
class TVTParameterSelectionOutputPaths:
    """Output paths written by TVT parameter selection v1."""

    summary_csv_path: Path
    manifest_path: Path


@dataclass(frozen=True)
class TVTParameterSelectionBuildResult:
    """TVT parameter selection summary, paths, and diagnostics."""

    summary: pd.DataFrame
    paths: TVTParameterSelectionOutputPaths
    diagnostics: TVTParameterSelectionDiagnostics


def find_tvt_parameter_selection_input(
    config: DataSliceConfig,
    *,
    processed_dir: str | Path | None = None,
) -> TVTParameterSelectionInputPaths:
    """Find signal input rows for TVT parameter selection v1."""

    root = Path(processed_dir or config.storage["processed_dir"]) / config.slice_name
    signal_path = root / f"{config.slice_name}_signals_v1.csv"
    if not signal_path.exists():
        raise TVTParameterSelectionWorkflowError(
            f"TVT parameter selection input file is missing: {signal_path}. "
            "Run scripts/build_signals.py first."
        )
    return TVTParameterSelectionInputPaths(signal_path=signal_path)


def build_tvt_parameter_selection(
    config: DataSliceConfig,
    *,
    processed_dir: str | Path | None = None,
    output_dir: str | Path | None = None,
    selection_config: TVTParameterSelectionConfig = TVTParameterSelectionConfig(),
) -> TVTParameterSelectionBuildResult:
    """Run TVT parameter selection v1 from signal rows."""

    inputs = find_tvt_parameter_selection_input(config, processed_dir=processed_dir)
    signal_rows = _read_signal_csv(inputs.signal_path, config=selection_config)
    result = run_tvt_parameter_selection_v1(signal_rows, config=selection_config)
    paths = _write_tvt_parameter_selection_outputs(
        config,
        inputs=inputs,
        summary=result.summary,
        diagnostics=result.diagnostics,
        output_dir=output_dir or processed_dir,
    )
    return TVTParameterSelectionBuildResult(
        summary=result.summary,
        paths=paths,
        diagnostics=result.diagnostics,
    )


def _read_signal_csv(path: Path, *, config: TVTParameterSelectionConfig) -> pd.DataFrame:
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


def _write_tvt_parameter_selection_outputs(
    config: DataSliceConfig,
    *,
    inputs: TVTParameterSelectionInputPaths,
    summary: pd.DataFrame,
    diagnostics: TVTParameterSelectionDiagnostics,
    output_dir: str | Path | None,
) -> TVTParameterSelectionOutputPaths:
    output_root = Path(output_dir or config.storage["processed_dir"]) / config.slice_name
    output_root.mkdir(parents=True, exist_ok=True)

    summary_csv_path = output_root / f"{config.slice_name}_tvt_parameter_selection_v1.csv"
    manifest_path = output_root / f"{config.slice_name}_tvt_parameter_selection_v1_manifest.json"

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
        "tvt_parameter_selection_status": {
            "tvt_parameter_selection_implemented": "v1_validation_select_test_evaluate",
            "split_policy": diagnostics.split_policy,
            "selection_policy": diagnostics.selection_policy,
            "objective": diagnostics.objective,
            "tie_break_policy": diagnostics.tie_break_policy,
            "model_training_implemented": diagnostics.model_training_implemented,
            "test_used_for_selection": diagnostics.test_used_for_selection,
            "final_hyperparameter_claim": diagnostics.final_hyperparameter_claim,
            "research_grade_backtest": diagnostics.research_grade_backtest,
        },
        "tvt_parameter_selection_scope_note": TVT_SELECTION_POLICY_NOTE,
        "diagnostics": asdict(diagnostics),
        "summary": summary.to_dict(orient="records"),
    }
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return TVTParameterSelectionOutputPaths(
        summary_csv_path=summary_csv_path,
        manifest_path=manifest_path,
    )
