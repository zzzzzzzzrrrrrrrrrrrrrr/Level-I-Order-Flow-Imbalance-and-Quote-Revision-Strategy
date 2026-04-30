"""File-based workflow for quote-only feature outputs."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import json
from pathlib import Path

import pandas as pd

from ..schema import EVENT_TIME
from ..utils import DataSliceConfig
from .quotes import (
    QUOTE_FEATURE_SCOPE_NOTE,
    QuoteFeatureDiagnostics,
    build_quote_features_v1,
)


class QuoteFeatureWorkflowError(ValueError):
    """Raised when quote-feature workflow inputs cannot be resolved."""


@dataclass(frozen=True)
class QuoteFeatureInputPaths:
    """Clean quote input path used for quote-only features."""

    cleaned_quote_path: Path


@dataclass(frozen=True)
class QuoteFeatureOutputPaths:
    """Output paths written by the quote-feature workflow."""

    quote_feature_path: Path
    manifest_path: Path


@dataclass(frozen=True)
class QuoteFeatureBuildResult:
    """Quote features, paths, and diagnostics produced by the workflow."""

    quote_features: pd.DataFrame
    paths: QuoteFeatureOutputPaths
    diagnostics: QuoteFeatureDiagnostics


def find_cleaned_quote_feature_input(
    config: DataSliceConfig,
    *,
    processed_dir: str | Path | None = None,
) -> QuoteFeatureInputPaths:
    """Find cleaned quote input for a configured slice."""

    root = Path(processed_dir or config.storage["processed_dir"]) / config.slice_name
    cleaned_quote_path = root / f"{config.slice_name}_quotes_clean.csv"
    if not cleaned_quote_path.exists():
        raise QuoteFeatureWorkflowError(
            "Cleaned quote input file is missing: "
            f"{cleaned_quote_path}. Run scripts/build_dataset.py first or pass --processed-dir."
        )
    return QuoteFeatureInputPaths(cleaned_quote_path=cleaned_quote_path)


def build_quote_feature_dataset(
    config: DataSliceConfig,
    *,
    processed_dir: str | Path | None = None,
    output_dir: str | Path | None = None,
) -> QuoteFeatureBuildResult:
    """Build quote-only feature dataset from cleaned quotes."""

    inputs = find_cleaned_quote_feature_input(config, processed_dir=processed_dir)
    quotes = _read_market_data_csv(inputs.cleaned_quote_path)
    feature_result = build_quote_features_v1(
        quotes,
        market_timezone=config.time_range.timezone,
    )
    paths = _write_quote_feature_outputs(
        config,
        inputs=inputs,
        quote_features=feature_result.quote_features,
        diagnostics=feature_result.diagnostics,
        output_dir=output_dir or processed_dir,
    )
    return QuoteFeatureBuildResult(
        quote_features=feature_result.quote_features,
        paths=paths,
        diagnostics=feature_result.diagnostics,
    )


def _read_market_data_csv(path: Path) -> pd.DataFrame:
    frame = pd.read_csv(path)
    frame[EVENT_TIME] = pd.to_datetime(frame[EVENT_TIME], format="mixed")
    return frame


def _write_quote_feature_outputs(
    config: DataSliceConfig,
    *,
    inputs: QuoteFeatureInputPaths,
    quote_features: pd.DataFrame,
    diagnostics: QuoteFeatureDiagnostics,
    output_dir: str | Path | None,
) -> QuoteFeatureOutputPaths:
    output_root = Path(output_dir or config.storage["processed_dir"]) / config.slice_name
    output_root.mkdir(parents=True, exist_ok=True)

    quote_feature_path = output_root / f"{config.slice_name}_quote_features_v1.csv"
    manifest_path = output_root / f"{config.slice_name}_quote_features_v1_manifest.json"

    quote_features.to_csv(quote_feature_path, index=False)
    manifest = {
        "slice_name": config.slice_name,
        "inputs": {
            "cleaned_quote_path": str(inputs.cleaned_quote_path),
        },
        "outputs": {
            "quote_feature_path": str(quote_feature_path),
            "manifest_path": str(manifest_path),
        },
        "feature_status": {
            "quote_features_implemented": "v1",
            "trade_signing_applied": diagnostics.trade_signing_applied,
            "ofi_from_signed_trades_implemented": diagnostics.ofi_from_signed_trades_implemented,
            "labels_implemented": diagnostics.labels_implemented,
            "backtest_implemented": diagnostics.backtest_implemented,
            "research_grade_strategy_sample": False,
        },
        "feature_scope_note": QUOTE_FEATURE_SCOPE_NOTE,
        "diagnostics": asdict(diagnostics),
    }
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    return QuoteFeatureOutputPaths(
        quote_feature_path=quote_feature_path,
        manifest_path=manifest_path,
    )
