"""File-based workflow for trade signing v1 outputs."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import json
from pathlib import Path

import pandas as pd

from ..alignment import MATCHED_QUOTE_EVENT_TIME
from ..schema import EVENT_TIME
from ..utils import DataSliceConfig
from .signing import (
    TRADE_SIGNING_SCOPE_NOTE,
    TradeSigningDiagnostics,
    build_trade_signs_v1,
)


class TradeSigningWorkflowError(ValueError):
    """Raised when trade-signing workflow inputs cannot be resolved."""


@dataclass(frozen=True)
class TradeSigningInputPaths:
    """Aligned trade input used for trade signing."""

    aligned_trade_path: Path


@dataclass(frozen=True)
class TradeSigningOutputPaths:
    """Output paths written by the trade-signing workflow."""

    signed_trade_path: Path
    manifest_path: Path


@dataclass(frozen=True)
class TradeSigningBuildResult:
    """Signed trade frame, paths, and diagnostics produced by the workflow."""

    signed_trades: pd.DataFrame
    paths: TradeSigningOutputPaths
    diagnostics: TradeSigningDiagnostics


def find_aligned_trade_signing_input(
    config: DataSliceConfig,
    *,
    processed_dir: str | Path | None = None,
) -> TradeSigningInputPaths:
    """Find aligned trade input for a configured slice."""

    root = Path(processed_dir or config.storage["processed_dir"]) / config.slice_name
    aligned_trade_path = root / f"{config.slice_name}_trades_aligned_quotes.csv"
    if not aligned_trade_path.exists():
        raise TradeSigningWorkflowError(
            "Aligned trade input file is missing: "
            f"{aligned_trade_path}. Run scripts/align_trades.py first or pass --processed-dir."
        )
    return TradeSigningInputPaths(aligned_trade_path=aligned_trade_path)


def build_trade_signing_dataset(
    config: DataSliceConfig,
    *,
    processed_dir: str | Path | None = None,
    output_dir: str | Path | None = None,
) -> TradeSigningBuildResult:
    """Build trade signing v1 output from aligned trades."""

    inputs = find_aligned_trade_signing_input(config, processed_dir=processed_dir)
    aligned_trades = _read_aligned_trades_csv(inputs.aligned_trade_path)
    signing_result = build_trade_signs_v1(aligned_trades)
    paths = _write_trade_signing_outputs(
        config,
        inputs=inputs,
        signed_trades=signing_result.signed_trades,
        diagnostics=signing_result.diagnostics,
        output_dir=output_dir or processed_dir,
    )
    return TradeSigningBuildResult(
        signed_trades=signing_result.signed_trades,
        paths=paths,
        diagnostics=signing_result.diagnostics,
    )


def _read_aligned_trades_csv(path: Path) -> pd.DataFrame:
    frame = pd.read_csv(path)
    frame[EVENT_TIME] = pd.to_datetime(frame[EVENT_TIME], format="mixed")
    if MATCHED_QUOTE_EVENT_TIME in frame.columns:
        frame[MATCHED_QUOTE_EVENT_TIME] = pd.to_datetime(
            frame[MATCHED_QUOTE_EVENT_TIME],
            format="mixed",
        )
    return frame


def _write_trade_signing_outputs(
    config: DataSliceConfig,
    *,
    inputs: TradeSigningInputPaths,
    signed_trades: pd.DataFrame,
    diagnostics: TradeSigningDiagnostics,
    output_dir: str | Path | None,
) -> TradeSigningOutputPaths:
    output_root = Path(output_dir or config.storage["processed_dir"]) / config.slice_name
    output_root.mkdir(parents=True, exist_ok=True)

    signed_trade_path = output_root / f"{config.slice_name}_trades_signed_v1.csv"
    manifest_path = output_root / f"{config.slice_name}_trade_signing_v1_manifest.json"

    signed_trades.to_csv(signed_trade_path, index=False)
    manifest = {
        "slice_name": config.slice_name,
        "inputs": {
            "aligned_trade_path": str(inputs.aligned_trade_path),
        },
        "outputs": {
            "signed_trade_path": str(signed_trade_path),
            "manifest_path": str(manifest_path),
        },
        "trade_signing_status": {
            "trade_signing_implemented": "v1",
            "condition_filters_applied": diagnostics.condition_filters_applied,
            "sale_condition_filters_applied": diagnostics.sale_condition_filters_applied,
            "nbbo_quote_condition_filters_applied": (
                diagnostics.nbbo_quote_condition_filters_applied
            ),
            "ofi_features_implemented": diagnostics.ofi_features_implemented,
            "labels_implemented": diagnostics.labels_implemented,
            "backtest_implemented": diagnostics.backtest_implemented,
            "research_grade_signed_sample": diagnostics.research_grade_signed_sample,
        },
        "trade_signing_scope_note": TRADE_SIGNING_SCOPE_NOTE,
        "diagnostics": asdict(diagnostics),
    }
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    return TradeSigningOutputPaths(
        signed_trade_path=signed_trade_path,
        manifest_path=manifest_path,
    )
