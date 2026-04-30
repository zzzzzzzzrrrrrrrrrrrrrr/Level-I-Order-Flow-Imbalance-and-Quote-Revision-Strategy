"""File-based workflow for future midquote label outputs."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import json
from pathlib import Path

import pandas as pd

from ..schema import EVENT_TIME
from ..utils import DataSliceConfig
from .midquote import (
    DEFAULT_DEAD_ZONE_BPS,
    DEFAULT_LABEL_HORIZONS,
    LABELING_SCOPE_NOTE,
    MidquoteLabelDiagnostics,
    build_midquote_labels_v1,
)


class LabelingWorkflowError(ValueError):
    """Raised when labeling workflow inputs cannot be resolved."""


@dataclass(frozen=True)
class MidquoteLabelInputPaths:
    """Feature and quote input paths used for labeling."""

    signed_flow_feature_path: Path
    quote_feature_path: Path


@dataclass(frozen=True)
class MidquoteLabelOutputPaths:
    """Output paths written by the labeling workflow."""

    labeled_feature_path: Path
    manifest_path: Path


@dataclass(frozen=True)
class MidquoteLabelBuildResult:
    """Labeled features, paths, and diagnostics produced by the workflow."""

    labeled_features: pd.DataFrame
    paths: MidquoteLabelOutputPaths
    diagnostics: MidquoteLabelDiagnostics


def find_midquote_label_inputs(
    config: DataSliceConfig,
    *,
    processed_dir: str | Path | None = None,
) -> MidquoteLabelInputPaths:
    """Find signed-flow feature and quote-feature inputs for labeling."""

    root = Path(processed_dir or config.storage["processed_dir"]) / config.slice_name
    signed_flow_feature_path = root / f"{config.slice_name}_signed_flow_features_v1.csv"
    quote_feature_path = root / f"{config.slice_name}_quote_features_v1.csv"
    missing_paths = [
        path for path in (signed_flow_feature_path, quote_feature_path) if not path.exists()
    ]
    if missing_paths:
        missing_list = ", ".join(str(path) for path in missing_paths)
        raise LabelingWorkflowError(
            "Labeling input file(s) are missing: "
            f"{missing_list}. Run quote and signed-flow feature scripts first."
        )
    return MidquoteLabelInputPaths(
        signed_flow_feature_path=signed_flow_feature_path,
        quote_feature_path=quote_feature_path,
    )


def build_midquote_label_dataset(
    config: DataSliceConfig,
    *,
    processed_dir: str | Path | None = None,
    output_dir: str | Path | None = None,
    horizons: tuple[str, ...] = DEFAULT_LABEL_HORIZONS,
    dead_zone_bps: float = DEFAULT_DEAD_ZONE_BPS,
) -> MidquoteLabelBuildResult:
    """Build future midquote labels for signed-flow feature rows."""

    inputs = find_midquote_label_inputs(config, processed_dir=processed_dir)
    feature_rows = _read_market_data_csv(inputs.signed_flow_feature_path)
    quote_features = _read_market_data_csv(inputs.quote_feature_path)
    label_result = build_midquote_labels_v1(
        feature_rows,
        quote_features,
        horizons=horizons,
        dead_zone_bps=dead_zone_bps,
    )
    paths = _write_midquote_label_outputs(
        config,
        inputs=inputs,
        labeled_features=label_result.labeled_features,
        diagnostics=label_result.diagnostics,
        output_dir=output_dir or processed_dir,
    )
    return MidquoteLabelBuildResult(
        labeled_features=label_result.labeled_features,
        paths=paths,
        diagnostics=label_result.diagnostics,
    )


def _read_market_data_csv(path: Path) -> pd.DataFrame:
    frame = pd.read_csv(path)
    frame[EVENT_TIME] = pd.to_datetime(frame[EVENT_TIME], format="mixed")
    return frame


def _write_midquote_label_outputs(
    config: DataSliceConfig,
    *,
    inputs: MidquoteLabelInputPaths,
    labeled_features: pd.DataFrame,
    diagnostics: MidquoteLabelDiagnostics,
    output_dir: str | Path | None,
) -> MidquoteLabelOutputPaths:
    output_root = Path(output_dir or config.storage["processed_dir"]) / config.slice_name
    output_root.mkdir(parents=True, exist_ok=True)

    labeled_feature_path = output_root / f"{config.slice_name}_labeled_features_v1.csv"
    manifest_path = output_root / f"{config.slice_name}_labeling_v1_manifest.json"

    labeled_features.to_csv(labeled_feature_path, index=False)
    manifest = {
        "slice_name": config.slice_name,
        "inputs": {
            "signed_flow_feature_path": str(inputs.signed_flow_feature_path),
            "quote_feature_path": str(inputs.quote_feature_path),
        },
        "outputs": {
            "labeled_feature_path": str(labeled_feature_path),
            "manifest_path": str(manifest_path),
        },
        "labeling_status": {
            "labeling_implemented": "v1",
            "current_quote_policy": diagnostics.current_quote_policy,
            "future_quote_policy": diagnostics.future_quote_policy,
            "session_boundary_policy": diagnostics.session_boundary_policy,
            "label_usage_policy": diagnostics.label_usage_policy,
            "signals_implemented": diagnostics.signals_implemented,
            "walk_forward_implemented": diagnostics.walk_forward_implemented,
            "backtest_implemented": diagnostics.backtest_implemented,
            "research_grade_strategy_sample": diagnostics.research_grade_strategy_sample,
        },
        "labeling_scope_note": LABELING_SCOPE_NOTE,
        "diagnostics": asdict(diagnostics),
    }
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    return MidquoteLabelOutputPaths(
        labeled_feature_path=labeled_feature_path,
        manifest_path=manifest_path,
    )
