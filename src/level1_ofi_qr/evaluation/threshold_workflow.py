"""File-based workflow for threshold selection v1."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import json
from pathlib import Path

import pandas as pd

from ..alignment import TRADING_DATE
from ..schema import EVENT_TIME, SYMBOL
from ..signals import SIGNAL_QUOTE_IMBALANCE, SIGNAL_QUOTE_REVISION_BPS
from ..utils import DataSliceConfig
from .threshold_selection import (
    THRESHOLD_SELECTION_POLICY_NOTE,
    ThresholdSelectionConfig,
    ThresholdSelectionDiagnostics,
    run_threshold_selection_v1,
)
from .workflow import WalkForwardWorkflowError, find_walk_forward_input


@dataclass(frozen=True)
class ThresholdSelectionOutputPaths:
    """Output paths written by threshold selection."""

    summary_csv_path: Path
    manifest_path: Path


@dataclass(frozen=True)
class ThresholdSelectionBuildResult:
    """Threshold selection summary, paths, and diagnostics."""

    summary: pd.DataFrame
    paths: ThresholdSelectionOutputPaths
    diagnostics: ThresholdSelectionDiagnostics


def build_threshold_selection(
    config: DataSliceConfig,
    *,
    processed_dir: str | Path | None = None,
    output_dir: str | Path | None = None,
    selection_config: ThresholdSelectionConfig = ThresholdSelectionConfig(),
) -> ThresholdSelectionBuildResult:
    """Run threshold selection v1 from signal rows."""

    inputs = find_walk_forward_input(config, processed_dir=processed_dir)
    signal_rows = _read_signal_csv(inputs.signal_path, config=selection_config)
    selection = run_threshold_selection_v1(signal_rows, config=selection_config)
    paths = _write_threshold_selection_outputs(
        config,
        signal_path=inputs.signal_path,
        summary=selection.summary,
        diagnostics=selection.diagnostics,
        output_dir=output_dir or processed_dir,
    )
    return ThresholdSelectionBuildResult(
        summary=selection.summary,
        paths=paths,
        diagnostics=selection.diagnostics,
    )


def _read_signal_csv(path: Path, *, config: ThresholdSelectionConfig) -> pd.DataFrame:
    if not path.exists():
        raise WalkForwardWorkflowError(f"Signal input file is missing: {path}")
    usecols = [
        EVENT_TIME,
        SYMBOL,
        TRADING_DATE,
        config.signed_flow_column,
        SIGNAL_QUOTE_IMBALANCE,
        SIGNAL_QUOTE_REVISION_BPS,
    ]
    for horizon in config.horizons:
        suffix = horizon.lower().replace(" ", "").replace(".", "p")
        usecols.extend(
            [
                f"label_available_{suffix}",
                f"future_midquote_direction_{suffix}",
                f"future_midquote_return_bps_{suffix}",
            ]
        )
    frame = pd.read_csv(path, usecols=tuple(dict.fromkeys(usecols)))
    frame[EVENT_TIME] = pd.to_datetime(frame[EVENT_TIME], format="mixed")
    return frame


def _write_threshold_selection_outputs(
    config: DataSliceConfig,
    *,
    signal_path: Path,
    summary: pd.DataFrame,
    diagnostics: ThresholdSelectionDiagnostics,
    output_dir: str | Path | None,
) -> ThresholdSelectionOutputPaths:
    output_root = Path(output_dir or config.storage["processed_dir"]) / config.slice_name
    output_root.mkdir(parents=True, exist_ok=True)

    summary_csv_path = output_root / f"{config.slice_name}_threshold_selection_v1.csv"
    manifest_path = output_root / f"{config.slice_name}_threshold_selection_v1_manifest.json"

    summary.to_csv(summary_csv_path, index=False)
    manifest = {
        "slice_name": config.slice_name,
        "inputs": {
            "signal_path": str(signal_path),
        },
        "outputs": {
            "summary_csv_path": str(summary_csv_path),
            "manifest_path": str(manifest_path),
        },
        "threshold_selection_status": {
            "threshold_selection_implemented": "v1_statistical",
            "threshold_selection_policy": diagnostics.threshold_selection_policy,
            "threshold_objective": diagnostics.threshold_objective,
            "signal_construction_policy": diagnostics.signal_construction_policy,
            "label_usage_policy": diagnostics.label_usage_policy,
            "model_training_implemented": diagnostics.model_training_implemented,
            "cost_model_implemented": diagnostics.cost_model_implemented,
            "backtest_implemented": diagnostics.backtest_implemented,
            "research_grade_strategy_result": diagnostics.research_grade_strategy_result,
        },
        "threshold_selection_scope_note": THRESHOLD_SELECTION_POLICY_NOTE,
        "diagnostics": asdict(diagnostics),
        "summary": summary.to_dict(orient="records"),
    }
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    return ThresholdSelectionOutputPaths(
        summary_csv_path=summary_csv_path,
        manifest_path=manifest_path,
    )
