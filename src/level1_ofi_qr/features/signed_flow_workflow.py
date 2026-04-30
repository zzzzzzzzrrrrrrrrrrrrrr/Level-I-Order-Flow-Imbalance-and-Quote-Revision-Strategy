"""File-based workflow for signed-flow feature outputs."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import json
from pathlib import Path

import pandas as pd

from ..alignment import MATCHED_QUOTE_EVENT_TIME
from ..schema import EVENT_TIME
from ..utils import DataSliceConfig
from .signed_flow import (
    DEFAULT_TIME_WINDOWS,
    DEFAULT_TRADE_COUNT_WINDOWS,
    SIGNED_FLOW_FEATURE_SCOPE_NOTE,
    SignedFlowFeatureDiagnostics,
    build_signed_flow_features_v1,
)


class SignedFlowFeatureWorkflowError(ValueError):
    """Raised when signed-flow feature workflow inputs cannot be resolved."""


@dataclass(frozen=True)
class SignedFlowFeatureInputPaths:
    """Signed trade input path used for signed-flow features."""

    signed_trade_path: Path


@dataclass(frozen=True)
class SignedFlowFeatureOutputPaths:
    """Output paths written by the signed-flow feature workflow."""

    signed_flow_feature_path: Path
    manifest_path: Path


@dataclass(frozen=True)
class SignedFlowFeatureBuildResult:
    """Signed-flow features, paths, and diagnostics produced by the workflow."""

    signed_flow_features: pd.DataFrame
    paths: SignedFlowFeatureOutputPaths
    diagnostics: SignedFlowFeatureDiagnostics


def find_signed_flow_feature_input(
    config: DataSliceConfig,
    *,
    processed_dir: str | Path | None = None,
) -> SignedFlowFeatureInputPaths:
    """Find signed trade input for a configured slice."""

    root = Path(processed_dir or config.storage["processed_dir"]) / config.slice_name
    signed_trade_path = root / f"{config.slice_name}_trades_signed_v1.csv"
    if not signed_trade_path.exists():
        raise SignedFlowFeatureWorkflowError(
            "Signed trade input file is missing: "
            f"{signed_trade_path}. Run scripts/sign_trades.py first or pass --processed-dir."
        )
    return SignedFlowFeatureInputPaths(signed_trade_path=signed_trade_path)


def build_signed_flow_feature_dataset(
    config: DataSliceConfig,
    *,
    processed_dir: str | Path | None = None,
    output_dir: str | Path | None = None,
    trade_count_windows: tuple[int, ...] = DEFAULT_TRADE_COUNT_WINDOWS,
    time_windows: tuple[str, ...] = DEFAULT_TIME_WINDOWS,
) -> SignedFlowFeatureBuildResult:
    """Build signed-flow feature dataset from trade signing v1 output."""

    inputs = find_signed_flow_feature_input(config, processed_dir=processed_dir)
    signed_trades = _read_signed_trades_csv(inputs.signed_trade_path)
    feature_result = build_signed_flow_features_v1(
        signed_trades,
        trade_count_windows=trade_count_windows,
        time_windows=time_windows,
    )
    paths = _write_signed_flow_feature_outputs(
        config,
        inputs=inputs,
        signed_flow_features=feature_result.signed_flow_features,
        diagnostics=feature_result.diagnostics,
        output_dir=output_dir or processed_dir,
    )
    return SignedFlowFeatureBuildResult(
        signed_flow_features=feature_result.signed_flow_features,
        paths=paths,
        diagnostics=feature_result.diagnostics,
    )


def _read_signed_trades_csv(path: Path) -> pd.DataFrame:
    frame = pd.read_csv(path)
    frame[EVENT_TIME] = pd.to_datetime(frame[EVENT_TIME], format="mixed")
    if MATCHED_QUOTE_EVENT_TIME in frame.columns:
        frame[MATCHED_QUOTE_EVENT_TIME] = pd.to_datetime(
            frame[MATCHED_QUOTE_EVENT_TIME],
            format="mixed",
        )
    return frame


def _write_signed_flow_feature_outputs(
    config: DataSliceConfig,
    *,
    inputs: SignedFlowFeatureInputPaths,
    signed_flow_features: pd.DataFrame,
    diagnostics: SignedFlowFeatureDiagnostics,
    output_dir: str | Path | None,
) -> SignedFlowFeatureOutputPaths:
    output_root = Path(output_dir or config.storage["processed_dir"]) / config.slice_name
    output_root.mkdir(parents=True, exist_ok=True)

    signed_flow_feature_path = output_root / f"{config.slice_name}_signed_flow_features_v1.csv"
    manifest_path = output_root / f"{config.slice_name}_signed_flow_features_v1_manifest.json"

    signed_flow_features.to_csv(signed_flow_feature_path, index=False)
    manifest = {
        "slice_name": config.slice_name,
        "inputs": {
            "signed_trade_path": str(inputs.signed_trade_path),
        },
        "outputs": {
            "signed_flow_feature_path": str(signed_flow_feature_path),
            "manifest_path": str(manifest_path),
        },
        "feature_status": {
            "signed_flow_features_implemented": "v1",
            "window_inclusion_policy": diagnostics.window_inclusion_policy,
            "unknown_sign_policy": diagnostics.unknown_sign_policy,
            "condition_filters_applied": diagnostics.condition_filters_applied,
            "sale_condition_filters_applied": diagnostics.sale_condition_filters_applied,
            "nbbo_quote_condition_filters_applied": (
                diagnostics.nbbo_quote_condition_filters_applied
            ),
            "labels_implemented": diagnostics.labels_implemented,
            "backtest_implemented": diagnostics.backtest_implemented,
            "research_grade_strategy_sample": diagnostics.research_grade_strategy_sample,
        },
        "feature_scope_note": SIGNED_FLOW_FEATURE_SCOPE_NOTE,
        "diagnostics": asdict(diagnostics),
    }
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    return SignedFlowFeatureOutputPaths(
        signed_flow_feature_path=signed_flow_feature_path,
        manifest_path=manifest_path,
    )
