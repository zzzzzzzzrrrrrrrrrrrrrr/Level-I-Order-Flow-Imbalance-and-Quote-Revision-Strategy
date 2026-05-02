"""File-based workflow for walk-forward evaluation v1."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import json
from pathlib import Path

import pandas as pd

from ..alignment import TRADING_DATE
from ..schema import EVENT_TIME, SYMBOL
from ..utils import DataSliceConfig
from .walk_forward import (
    WALK_FORWARD_POLICY_NOTE,
    WalkForwardConfig,
    WalkForwardDiagnostics,
    evaluate_signals_walk_forward_v1,
)


class WalkForwardWorkflowError(ValueError):
    """Raised when walk-forward workflow inputs cannot be resolved."""


@dataclass(frozen=True)
class WalkForwardInputPaths:
    """Input path used for walk-forward evaluation."""

    signal_path: Path


@dataclass(frozen=True)
class WalkForwardOutputPaths:
    """Output paths written by walk-forward evaluation."""

    summary_csv_path: Path
    manifest_path: Path


@dataclass(frozen=True)
class WalkForwardBuildResult:
    """Walk-forward summary, paths, and diagnostics."""

    summary: pd.DataFrame
    paths: WalkForwardOutputPaths
    diagnostics: WalkForwardDiagnostics


def find_walk_forward_input(
    config: DataSliceConfig,
    *,
    processed_dir: str | Path | None = None,
) -> WalkForwardInputPaths:
    """Find signal v1 input for walk-forward evaluation."""

    root = Path(processed_dir or config.storage["processed_dir"]) / config.slice_name
    signal_path = root / f"{config.slice_name}_signals_v1.csv"
    if not signal_path.exists():
        raise WalkForwardWorkflowError(
            "Signal input file is missing: "
            f"{signal_path}. Run scripts/build_signals.py first or pass --processed-dir."
        )
    return WalkForwardInputPaths(signal_path=signal_path)


def build_walk_forward_evaluation(
    config: DataSliceConfig,
    *,
    processed_dir: str | Path | None = None,
    output_dir: str | Path | None = None,
    evaluation_config: WalkForwardConfig = WalkForwardConfig(),
) -> WalkForwardBuildResult:
    """Build walk-forward statistical evaluation from signal v1 rows."""

    inputs = find_walk_forward_input(config, processed_dir=processed_dir)
    signal_rows = _read_signal_csv(inputs.signal_path, config=evaluation_config)
    evaluation = evaluate_signals_walk_forward_v1(signal_rows, config=evaluation_config)
    paths = _write_walk_forward_outputs(
        config,
        inputs=inputs,
        summary=evaluation.summary,
        diagnostics=evaluation.diagnostics,
        output_dir=output_dir or processed_dir,
    )
    return WalkForwardBuildResult(
        summary=evaluation.summary,
        paths=paths,
        diagnostics=evaluation.diagnostics,
    )


def _read_signal_csv(path: Path, *, config: WalkForwardConfig) -> pd.DataFrame:
    usecols = [EVENT_TIME, SYMBOL, TRADING_DATE, config.signal_column]
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


def _write_walk_forward_outputs(
    config: DataSliceConfig,
    *,
    inputs: WalkForwardInputPaths,
    summary: pd.DataFrame,
    diagnostics: WalkForwardDiagnostics,
    output_dir: str | Path | None,
) -> WalkForwardOutputPaths:
    output_root = Path(output_dir or config.storage["processed_dir"]) / config.slice_name
    output_root.mkdir(parents=True, exist_ok=True)

    summary_csv_path = output_root / f"{config.slice_name}_walk_forward_evaluation_v1.csv"
    manifest_path = output_root / f"{config.slice_name}_walk_forward_evaluation_v1_manifest.json"

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
        "evaluation_status": {
            "walk_forward_implemented": "v1_statistical",
            "evaluation_policy": diagnostics.evaluation_policy,
            "signal_usage_policy": diagnostics.signal_usage_policy,
            "label_usage_policy": diagnostics.label_usage_policy,
            "threshold_optimization_implemented": (
                diagnostics.threshold_optimization_implemented
            ),
            "model_training_implemented": diagnostics.model_training_implemented,
            "cost_model_implemented": diagnostics.cost_model_implemented,
            "backtest_implemented": diagnostics.backtest_implemented,
            "research_grade_strategy_result": diagnostics.research_grade_strategy_result,
        },
        "evaluation_scope_note": WALK_FORWARD_POLICY_NOTE,
        "diagnostics": asdict(diagnostics),
        "summary": summary.to_dict(orient="records"),
    }
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    return WalkForwardOutputPaths(summary_csv_path=summary_csv_path, manifest_path=manifest_path)
