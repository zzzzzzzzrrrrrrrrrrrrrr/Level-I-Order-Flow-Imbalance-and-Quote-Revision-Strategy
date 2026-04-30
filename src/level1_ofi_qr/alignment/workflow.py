"""File-based workflow for quote-trade alignment outputs."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import json
from pathlib import Path

import pandas as pd

from ..schema import EVENT_TIME
from ..utils import DataSliceConfig
from .quote_trade import QuoteTradeAlignmentDiagnostics, align_trades_to_prior_quotes

DEFAULT_TOLERANCE_SENSITIVITY: tuple[str | None, ...] = (
    None,
    "5s",
    "1s",
    "500ms",
    "100ms",
)

ALIGNMENT_SCOPE_NOTE = (
    "This alignment version only performs backward quote-trade matching within "
    "symbol and trading_date groups. It does not perform trade signing, "
    "sale-condition filtering, correction filtering, or final research-sample "
    "cleaning."
)


class AlignmentWorkflowError(ValueError):
    """Raised when alignment inputs or outputs cannot be resolved."""


@dataclass(frozen=True)
class QuoteTradeAlignmentInputPaths:
    """Cleaned quote/trade CSV paths used for alignment."""

    cleaned_quote_path: Path
    cleaned_trade_path: Path


@dataclass(frozen=True)
class QuoteTradeAlignmentOutputPaths:
    """Output paths written by the alignment workflow."""

    aligned_trade_path: Path
    manifest_path: Path


@dataclass(frozen=True)
class ToleranceSensitivityOutputPaths:
    """Output paths written by the tolerance-sensitivity workflow."""

    summary_json_path: Path
    summary_csv_path: Path


@dataclass(frozen=True)
class QuoteTradeAlignmentBuildResult:
    """Aligned frame, paths, and diagnostics produced by the workflow."""

    aligned_trades: pd.DataFrame
    paths: QuoteTradeAlignmentOutputPaths
    diagnostics: QuoteTradeAlignmentDiagnostics


@dataclass(frozen=True)
class ToleranceSensitivityResult:
    """Tolerance-sensitivity summary and output paths."""

    summary: tuple[dict[str, object], ...]
    paths: ToleranceSensitivityOutputPaths


def find_cleaned_alignment_inputs(
    config: DataSliceConfig,
    *,
    processed_dir: str | Path | None = None,
) -> QuoteTradeAlignmentInputPaths:
    """Find cleaned quote/trade inputs for a configured slice."""

    root = Path(processed_dir or config.storage["processed_dir"]) / config.slice_name
    cleaned_quote_path = root / f"{config.slice_name}_quotes_clean.csv"
    cleaned_trade_path = root / f"{config.slice_name}_trades_clean.csv"

    missing_paths = [
        path for path in (cleaned_quote_path, cleaned_trade_path) if not path.exists()
    ]
    if missing_paths:
        missing_list = ", ".join(str(path) for path in missing_paths)
        raise AlignmentWorkflowError(
            "Cleaned quote/trade input file(s) are missing: "
            f"{missing_list}. Run scripts/build_dataset.py first or pass --processed-dir."
        )

    return QuoteTradeAlignmentInputPaths(
        cleaned_quote_path=cleaned_quote_path,
        cleaned_trade_path=cleaned_trade_path,
    )


def build_quote_trade_alignment(
    config: DataSliceConfig,
    *,
    processed_dir: str | Path | None = None,
    output_dir: str | Path | None = None,
    tolerance: str | pd.Timedelta | None = None,
) -> QuoteTradeAlignmentBuildResult:
    """Align cleaned trades to the latest strictly prior cleaned quote."""

    inputs = find_cleaned_alignment_inputs(config, processed_dir=processed_dir)
    quotes = _read_market_data_csv(inputs.cleaned_quote_path)
    trades = _read_market_data_csv(inputs.cleaned_trade_path)

    alignment = align_trades_to_prior_quotes(
        trades,
        quotes,
        tolerance=tolerance,
        market_timezone=config.time_range.timezone,
    )
    paths = _write_alignment_outputs(
        config,
        inputs=inputs,
        aligned_trades=alignment.aligned_trades,
        diagnostics=alignment.diagnostics,
        output_dir=output_dir or processed_dir,
    )

    return QuoteTradeAlignmentBuildResult(
        aligned_trades=alignment.aligned_trades,
        paths=paths,
        diagnostics=alignment.diagnostics,
    )


def build_quote_trade_alignment_tolerance_sensitivity(
    config: DataSliceConfig,
    *,
    processed_dir: str | Path | None = None,
    output_dir: str | Path | None = None,
    tolerances: tuple[str | None, ...] = DEFAULT_TOLERANCE_SENSITIVITY,
) -> ToleranceSensitivityResult:
    """Run alignment diagnostics for multiple candidate quote-lag tolerances."""

    inputs = find_cleaned_alignment_inputs(config, processed_dir=processed_dir)
    quotes = _read_market_data_csv(inputs.cleaned_quote_path)
    trades = _read_market_data_csv(inputs.cleaned_trade_path)

    summary_rows: list[dict[str, object]] = []
    for tolerance in tolerances:
        alignment = align_trades_to_prior_quotes(
            trades,
            quotes,
            tolerance=tolerance,
            market_timezone=config.time_range.timezone,
        )
        diagnostics = asdict(alignment.diagnostics)
        diagnostics["candidate_tolerance"] = "None" if tolerance is None else str(tolerance)
        summary_rows.append(diagnostics)

    paths = _write_tolerance_sensitivity_outputs(
        config,
        inputs=inputs,
        summary_rows=summary_rows,
        output_dir=output_dir or processed_dir,
    )
    return ToleranceSensitivityResult(summary=tuple(summary_rows), paths=paths)


def _read_market_data_csv(path: Path) -> pd.DataFrame:
    frame = pd.read_csv(path)
    frame[EVENT_TIME] = pd.to_datetime(frame[EVENT_TIME], format="mixed")
    return frame


def _write_alignment_outputs(
    config: DataSliceConfig,
    *,
    inputs: QuoteTradeAlignmentInputPaths,
    aligned_trades: pd.DataFrame,
    diagnostics: QuoteTradeAlignmentDiagnostics,
    output_dir: str | Path | None,
) -> QuoteTradeAlignmentOutputPaths:
    output_root = Path(output_dir or config.storage["processed_dir"]) / config.slice_name
    output_root.mkdir(parents=True, exist_ok=True)

    aligned_trade_path = output_root / f"{config.slice_name}_trades_aligned_quotes.csv"
    manifest_path = output_root / f"{config.slice_name}_alignment_manifest.json"

    aligned_trades.to_csv(aligned_trade_path, index=False)
    manifest = {
        "slice_name": config.slice_name,
        "inputs": {
            "cleaned_quote_path": str(inputs.cleaned_quote_path),
            "cleaned_trade_path": str(inputs.cleaned_trade_path),
        },
        "outputs": {
            "aligned_trade_path": str(aligned_trade_path),
            "manifest_path": str(manifest_path),
        },
        "alignment_status": {
            "alignment_implemented": True,
            "alignment_rule": diagnostics.alignment_rule,
            "allow_exact_matches": diagnostics.allow_exact_matches,
            "session_boundary_policy": diagnostics.session_boundary_policy,
            "alignment_group_keys": diagnostics.alignment_group_keys,
            "condition_filters_applied": diagnostics.condition_filters_applied,
            "trade_signing_applied": diagnostics.trade_signing_applied,
            "research_grade_signed_sample": False,
        },
        "alignment_scope_note": ALIGNMENT_SCOPE_NOTE,
        "diagnostics": asdict(diagnostics),
    }
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    return QuoteTradeAlignmentOutputPaths(
        aligned_trade_path=aligned_trade_path,
        manifest_path=manifest_path,
    )


def _write_tolerance_sensitivity_outputs(
    config: DataSliceConfig,
    *,
    inputs: QuoteTradeAlignmentInputPaths,
    summary_rows: list[dict[str, object]],
    output_dir: str | Path | None,
) -> ToleranceSensitivityOutputPaths:
    output_root = Path(output_dir or config.storage["processed_dir"]) / config.slice_name
    output_root.mkdir(parents=True, exist_ok=True)

    summary_json_path = output_root / f"{config.slice_name}_alignment_tolerance_sensitivity.json"
    summary_csv_path = output_root / f"{config.slice_name}_alignment_tolerance_sensitivity.csv"
    payload = {
        "slice_name": config.slice_name,
        "inputs": {
            "cleaned_quote_path": str(inputs.cleaned_quote_path),
            "cleaned_trade_path": str(inputs.cleaned_trade_path),
        },
        "candidate_tolerances": [row["candidate_tolerance"] for row in summary_rows],
        "tolerance_decision": "not_selected",
        "alignment_scope_note": ALIGNMENT_SCOPE_NOTE,
        "summary": summary_rows,
    }
    summary_json_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    pd.DataFrame(summary_rows).to_csv(summary_csv_path, index=False)

    return ToleranceSensitivityOutputPaths(
        summary_json_path=summary_json_path,
        summary_csv_path=summary_csv_path,
    )
