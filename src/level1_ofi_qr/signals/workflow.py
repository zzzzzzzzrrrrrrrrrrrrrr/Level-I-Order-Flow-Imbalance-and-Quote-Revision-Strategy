"""File-based workflow for signal v1 outputs."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import json
from pathlib import Path

import pandas as pd

from ..schema import EVENT_TIME
from ..utils import DataSliceConfig
from .rules import (
    SIGNAL_POLICY_NOTE,
    SignalDiagnostics,
    SignalRuleConfig,
    build_sequential_gate_signals_v1,
)


class SignalWorkflowError(ValueError):
    """Raised when signal workflow inputs cannot be resolved."""


@dataclass(frozen=True)
class SignalInputPaths:
    """Input paths used for signal generation."""

    labeled_feature_path: Path
    quote_feature_path: Path


@dataclass(frozen=True)
class SignalOutputPaths:
    """Output paths written by the signal workflow."""

    signal_path: Path
    manifest_path: Path


@dataclass(frozen=True)
class SignalBuildResult:
    """Signal frame, paths, and diagnostics produced by the workflow."""

    signals: pd.DataFrame
    paths: SignalOutputPaths
    diagnostics: SignalDiagnostics


def find_signal_inputs(
    config: DataSliceConfig,
    *,
    processed_dir: str | Path | None = None,
) -> SignalInputPaths:
    """Find labeled feature and quote feature inputs for signal v1."""

    root = Path(processed_dir or config.storage["processed_dir"]) / config.slice_name
    labeled_feature_path = root / f"{config.slice_name}_labeled_features_v1.csv"
    quote_feature_path = root / f"{config.slice_name}_quote_features_v1.csv"
    missing_paths = [
        path for path in (labeled_feature_path, quote_feature_path) if not path.exists()
    ]
    if missing_paths:
        missing_list = ", ".join(str(path) for path in missing_paths)
        raise SignalWorkflowError(
            "Signal input file(s) are missing: "
            f"{missing_list}. Run scripts/build_labels.py and quote features first."
        )
    return SignalInputPaths(
        labeled_feature_path=labeled_feature_path,
        quote_feature_path=quote_feature_path,
    )


def build_signal_dataset(
    config: DataSliceConfig,
    *,
    processed_dir: str | Path | None = None,
    output_dir: str | Path | None = None,
    signal_config: SignalRuleConfig = SignalRuleConfig(),
) -> SignalBuildResult:
    """Build signal v1 output from labeled features and quote features."""

    inputs = find_signal_inputs(config, processed_dir=processed_dir)
    feature_rows = _read_market_data_csv(inputs.labeled_feature_path)
    quote_features = _read_market_data_csv(inputs.quote_feature_path)
    signal_result = build_sequential_gate_signals_v1(
        feature_rows,
        quote_features,
        config=signal_config,
    )
    paths = _write_signal_outputs(
        config,
        inputs=inputs,
        signals=signal_result.signals,
        diagnostics=signal_result.diagnostics,
        output_dir=output_dir or processed_dir,
    )
    return SignalBuildResult(
        signals=signal_result.signals,
        paths=paths,
        diagnostics=signal_result.diagnostics,
    )


def _read_market_data_csv(path: Path) -> pd.DataFrame:
    frame = pd.read_csv(path)
    frame[EVENT_TIME] = pd.to_datetime(frame[EVENT_TIME], format="mixed")
    return frame


def _write_signal_outputs(
    config: DataSliceConfig,
    *,
    inputs: SignalInputPaths,
    signals: pd.DataFrame,
    diagnostics: SignalDiagnostics,
    output_dir: str | Path | None,
) -> SignalOutputPaths:
    output_root = Path(output_dir or config.storage["processed_dir"]) / config.slice_name
    output_root.mkdir(parents=True, exist_ok=True)

    signal_path = output_root / f"{config.slice_name}_signals_v1.csv"
    manifest_path = output_root / f"{config.slice_name}_signals_v1_manifest.json"

    signals.to_csv(signal_path, index=False)
    manifest = {
        "slice_name": config.slice_name,
        "inputs": {
            "labeled_feature_path": str(inputs.labeled_feature_path),
            "quote_feature_path": str(inputs.quote_feature_path),
        },
        "outputs": {
            "signal_path": str(signal_path),
            "manifest_path": str(manifest_path),
        },
        "signal_status": {
            "signals_implemented": "v1",
            "signal_rule": diagnostics.signal_rule,
            "threshold_selection_policy": diagnostics.threshold_selection_policy,
            "label_usage_policy": diagnostics.label_usage_policy,
            "labels_used_for_signal": diagnostics.labels_used_for_signal,
            "walk_forward_implemented": diagnostics.walk_forward_implemented,
            "backtest_implemented": diagnostics.backtest_implemented,
            "threshold_optimization_implemented": (
                diagnostics.threshold_optimization_implemented
            ),
            "research_grade_strategy_sample": diagnostics.research_grade_strategy_sample,
        },
        "signal_scope_note": SIGNAL_POLICY_NOTE,
        "diagnostics": asdict(diagnostics),
    }
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    return SignalOutputPaths(signal_path=signal_path, manifest_path=manifest_path)
