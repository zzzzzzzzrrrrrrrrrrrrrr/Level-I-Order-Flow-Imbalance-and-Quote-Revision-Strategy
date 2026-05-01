"""File workflow for post-selection microstructure diagnostics."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import json
from pathlib import Path

import pandas as pd

from ..alignment import TRADING_DATE
from ..features.quotes import (
    MIDQUOTE,
    QUOTE_EVENT_INTERVAL_MS,
    QUOTE_REVISION_BPS,
    QUOTED_SPREAD,
    RELATIVE_SPREAD,
)
from ..models import (
    COST_AWARE_ESTIMATED_COST_BPS_COLUMN,
    COST_AWARE_PREDICTED_EDGE_BPS_COLUMN,
    COST_AWARE_LINEAR_SCORE_STRATEGY,
    MODEL_SCORE_COLUMN,
)
from ..schema import ASK, ASK_SIZE, BID, BID_SIZE, EVENT_TIME, SYMBOL
from ..signals.rules import (
    SIGNAL_QUOTE_IMBALANCE,
    SIGNAL_QUOTE_REVISION_BPS,
    SIGNAL_QUOTED_SPREAD,
    SIGNAL_RELATIVE_SPREAD,
)
from ..utils import DataSliceConfig
from .microstructure import (
    SIGNED_FLOW_COLUMNS,
    MicrostructureDiagnosticsConfig,
    MicrostructureDiagnosticsResult,
    build_cost_aware_microstructure_diagnostics,
)


class MicrostructureDiagnosticWorkflowError(ValueError):
    """Raised when microstructure diagnostic workflow inputs are missing."""


@dataclass(frozen=True)
class MicrostructureDiagnosticInputPaths:
    """Input files for microstructure diagnostics."""

    ledger_path: Path
    summary_path: Path
    predictions_path: Path
    signals_path: Path
    quote_features_path: Path
    trades_signed_path: Path


@dataclass(frozen=True)
class MicrostructureDiagnosticOutputPaths:
    """Output files written by microstructure diagnostics."""

    trades_csv_path: Path
    fold_summary_csv_path: Path
    breakdown_csv_path: Path
    horizon_csv_path: Path
    horizon_summary_csv_path: Path
    execution_trades_csv_path: Path
    execution_csv_path: Path
    cost_stress_csv_path: Path
    strategy_variants_csv_path: Path
    manifest_path: Path


@dataclass(frozen=True)
class MicrostructureDiagnosticBuildResult:
    """Diagnostic tables and paths."""

    diagnostics: MicrostructureDiagnosticsResult
    paths: MicrostructureDiagnosticOutputPaths


def find_microstructure_diagnostic_inputs(
    config: DataSliceConfig,
    *,
    processed_dir: str | Path | None = None,
) -> MicrostructureDiagnosticInputPaths:
    """Resolve cost-aware diagnostic inputs for a data slice."""

    root = Path(processed_dir or config.storage["processed_dir"]) / config.slice_name
    stem = config.slice_name
    paths = MicrostructureDiagnosticInputPaths(
        ledger_path=root / f"{stem}_{COST_AWARE_LINEAR_SCORE_STRATEGY}_ledger.csv",
        summary_path=root / f"{stem}_{COST_AWARE_LINEAR_SCORE_STRATEGY}_summary.csv",
        predictions_path=root / f"{stem}_{COST_AWARE_LINEAR_SCORE_STRATEGY}_predictions.csv",
        signals_path=root / f"{stem}_signals_v1.csv",
        quote_features_path=root / f"{stem}_quote_features_v1.csv",
        trades_signed_path=root / f"{stem}_trades_signed_v1.csv",
    )
    missing = [str(path) for path in asdict(paths).values() if not Path(path).exists()]
    if missing:
        raise MicrostructureDiagnosticWorkflowError(
            "Microstructure diagnostic inputs are missing: "
            f"{missing}. Run the upstream pipeline and cost-aware strategy first."
        )
    return paths


def build_cost_aware_microstructure_diagnostics_v1(
    config: DataSliceConfig,
    *,
    processed_dir: str | Path | None = None,
    output_dir: str | Path | None = None,
    diagnostics_config: MicrostructureDiagnosticsConfig = MicrostructureDiagnosticsConfig(),
) -> MicrostructureDiagnosticBuildResult:
    """Run post-selection diagnostics without changing strategy logic."""

    inputs = find_microstructure_diagnostic_inputs(config, processed_dir=processed_dir)
    ledger = _read_csv_with_time(inputs.ledger_path)
    summary = pd.read_csv(inputs.summary_path)
    entry_keys = _entry_keys_from_ledger(ledger)
    signal_rows = _read_matching_rows(
        inputs.signals_path,
        keys=entry_keys,
        usecols=_signal_usecols(),
    )
    prediction_rows = _read_matching_rows(
        inputs.predictions_path,
        keys=entry_keys,
        usecols=_prediction_usecols(),
    )
    quote_features = _read_csv_with_time(inputs.quote_features_path, usecols=_quote_usecols())
    trade_rows = _read_csv_with_time(inputs.trades_signed_path, usecols=_trade_usecols())
    result = build_cost_aware_microstructure_diagnostics(
        ledger,
        quote_features,
        trade_rows=trade_rows,
        signal_rows=signal_rows,
        prediction_rows=prediction_rows,
        config=diagnostics_config,
    )
    result = MicrostructureDiagnosticsResult(
        trades=result.trades,
        fold_summary=_augment_fold_summary(result.fold_summary, summary),
        breakdown=result.breakdown,
        horizon=result.horizon,
        horizon_summary=result.horizon_summary,
        execution_trades=result.execution_trades,
        execution=result.execution,
        cost_stress=result.cost_stress,
        strategy_variants=result.strategy_variants,
    )
    paths = _write_outputs(
        config,
        inputs=inputs,
        result=result,
        diagnostics_config=diagnostics_config,
        output_dir=output_dir or processed_dir,
    )
    return MicrostructureDiagnosticBuildResult(diagnostics=result, paths=paths)


def _augment_fold_summary(
    diagnostic_summary: pd.DataFrame,
    official_summary: pd.DataFrame,
) -> pd.DataFrame:
    if official_summary.empty:
        return diagnostic_summary
    diagnostic_by_fold = {
        str(row["fold_id"]): row
        for _, row in diagnostic_summary.iterrows()
        if "fold_id" in row
    }
    rows: list[dict[str, object]] = []
    for _, official in official_summary.iterrows():
        fold_id = str(official["fold_id"])
        diagnostic = diagnostic_by_fold.get(fold_id)
        num_round_trips = 0 if diagnostic is None else int(diagnostic["num_round_trips"])
        gross = float(official.get("gross_pnl", 0.0))
        cost = float(official.get("cost", official.get("total_cost", 0.0)))
        net = float(official.get("net_pnl", official.get("final_equity", 0.0)))
        num_order_events = int(official.get("num_trades", official.get("order_rows", 0)))
        row = {
            "fold_id": fold_id,
            "candidate_id": official.get("candidate_id"),
            "feature_set": official.get("feature_set"),
            "selected_threshold": official.get(
                "selected_threshold",
                official.get("score_threshold"),
            ),
            "selected_cost_multiplier": official.get(
                "selected_cost_multiplier",
                official.get("cost_multiplier"),
            ),
            "cooldown_seconds": official.get("cooldown_seconds"),
            "min_holding_seconds": official.get("min_holding_seconds"),
            "test_date": official.get("test_date"),
            "num_round_trips": num_round_trips,
            "num_order_events": num_order_events,
            "num_position_changes": int(official.get("num_position_changes", num_order_events)),
            "gross_pnl": gross,
            "cost": cost,
            "net_pnl": net,
            "gross_per_order_event": _safe_ratio(gross, num_order_events),
            "cost_per_order_event": _safe_ratio(cost, num_order_events),
            "net_per_order_event": _safe_ratio(net, num_order_events),
            "gross_per_round_trip": _safe_ratio(gross, num_round_trips),
            "cost_per_round_trip": _safe_ratio(cost, num_round_trips),
            "net_per_round_trip": _safe_ratio(net, num_round_trips),
        }
        if diagnostic is not None and "win_rate" in diagnostic:
            row["round_trip_win_rate"] = diagnostic["win_rate"]
        rows.append(row)
    return pd.DataFrame(rows)


def _safe_ratio(numerator: float, denominator: int) -> float | None:
    if denominator == 0:
        return None
    return numerator / denominator


def _entry_keys_from_ledger(ledger: pd.DataFrame) -> set[tuple[str, str, str]]:
    entries = ledger.loc[
        (pd.to_numeric(ledger["previous_position"], errors="coerce") == 0.0)
        & (pd.to_numeric(ledger["target_position"], errors="coerce") != 0.0),
        [EVENT_TIME, SYMBOL, TRADING_DATE],
    ]
    return {
        (str(row[EVENT_TIME]), str(row[SYMBOL]), str(row[TRADING_DATE]))
        for _, row in entries.iterrows()
    }


def _read_matching_rows(
    path: Path,
    *,
    keys: set[tuple[str, str, str]],
    usecols: tuple[str, ...],
    chunksize: int = 500_000,
) -> pd.DataFrame:
    if not keys:
        return pd.DataFrame(columns=usecols)
    frames: list[pd.DataFrame] = []
    for chunk in pd.read_csv(path, usecols=usecols, chunksize=chunksize):
        key_frame = pd.DataFrame(
            {
                EVENT_TIME: chunk[EVENT_TIME].astype(str),
                SYMBOL: chunk[SYMBOL].astype(str),
                TRADING_DATE: chunk[TRADING_DATE].astype(str),
            }
        )
        mask = [
            key in keys
            for key in zip(
                key_frame[EVENT_TIME],
                key_frame[SYMBOL],
                key_frame[TRADING_DATE],
                strict=True,
            )
        ]
        if any(mask):
            frames.append(chunk.loc[mask].copy())
    if not frames:
        return pd.DataFrame(columns=usecols)
    return _normalize_time(pd.concat(frames, ignore_index=True), EVENT_TIME)


def _read_csv_with_time(path: Path, *, usecols: tuple[str, ...] | None = None) -> pd.DataFrame:
    frame = pd.read_csv(path, usecols=usecols)
    return _normalize_time(frame, EVENT_TIME)


def _normalize_time(frame: pd.DataFrame, column: str) -> pd.DataFrame:
    if column in frame.columns:
        frame = frame.copy()
        frame[column] = pd.to_datetime(frame[column], format="mixed")
    return frame


def _signal_usecols() -> tuple[str, ...]:
    return (
        EVENT_TIME,
        SYMBOL,
        TRADING_DATE,
        SIGNAL_QUOTE_IMBALANCE,
        SIGNAL_QUOTE_REVISION_BPS,
        SIGNAL_QUOTED_SPREAD,
        SIGNAL_RELATIVE_SPREAD,
        *SIGNED_FLOW_COLUMNS,
    )


def _prediction_usecols() -> tuple[str, ...]:
    return (
        EVENT_TIME,
        SYMBOL,
        TRADING_DATE,
        MODEL_SCORE_COLUMN,
        COST_AWARE_ESTIMATED_COST_BPS_COLUMN,
        COST_AWARE_PREDICTED_EDGE_BPS_COLUMN,
    )


def _quote_usecols() -> tuple[str, ...]:
    return (
        EVENT_TIME,
        SYMBOL,
        TRADING_DATE,
        BID,
        ASK,
        BID_SIZE,
        ASK_SIZE,
        MIDQUOTE,
        QUOTED_SPREAD,
        RELATIVE_SPREAD,
        QUOTE_REVISION_BPS,
        QUOTE_EVENT_INTERVAL_MS,
    )


def _trade_usecols() -> tuple[str, ...]:
    return (
        EVENT_TIME,
        SYMBOL,
        TRADING_DATE,
        "trade_price",
        "trade_size",
        "trade_sign",
        "signed_trade_size",
    )


def _write_outputs(
    config: DataSliceConfig,
    *,
    inputs: MicrostructureDiagnosticInputPaths,
    result: MicrostructureDiagnosticsResult,
    diagnostics_config: MicrostructureDiagnosticsConfig,
    output_dir: str | Path | None,
) -> MicrostructureDiagnosticOutputPaths:
    output_root = Path(output_dir or config.storage["processed_dir"]) / config.slice_name
    output_root.mkdir(parents=True, exist_ok=True)
    stem = f"{config.slice_name}_cost_aware_microstructure_diagnostics"
    paths = MicrostructureDiagnosticOutputPaths(
        trades_csv_path=output_root / f"{stem}_trades.csv",
        fold_summary_csv_path=output_root / f"{stem}_fold_summary.csv",
        breakdown_csv_path=output_root / f"{stem}_breakdown.csv",
        horizon_csv_path=output_root / f"{stem}_horizon.csv",
        horizon_summary_csv_path=output_root / f"{stem}_horizon_summary.csv",
        execution_trades_csv_path=output_root / f"{stem}_execution_trades.csv",
        execution_csv_path=output_root / f"{stem}_execution.csv",
        cost_stress_csv_path=output_root / f"{stem}_cost_stress.csv",
        strategy_variants_csv_path=output_root / f"{stem}_strategy_variants.csv",
        manifest_path=output_root / f"{stem}_manifest.json",
    )
    result.trades.to_csv(paths.trades_csv_path, index=False)
    result.fold_summary.to_csv(paths.fold_summary_csv_path, index=False)
    result.breakdown.to_csv(paths.breakdown_csv_path, index=False)
    result.horizon.to_csv(paths.horizon_csv_path, index=False)
    result.horizon_summary.to_csv(paths.horizon_summary_csv_path, index=False)
    result.execution_trades.to_csv(paths.execution_trades_csv_path, index=False)
    result.execution.to_csv(paths.execution_csv_path, index=False)
    result.cost_stress.to_csv(paths.cost_stress_csv_path, index=False)
    result.strategy_variants.to_csv(paths.strategy_variants_csv_path, index=False)
    manifest = {
        "slice_name": config.slice_name,
        "strategy_variant": COST_AWARE_LINEAR_SCORE_STRATEGY,
        "test_used_for_selection": False,
        "selection_logic_changed": False,
        "diagnostic_only": True,
        "inputs": {key: str(value) for key, value in asdict(inputs).items()},
        "outputs": {key: str(value) for key, value in asdict(paths).items()},
        "diagnostics_config": asdict(diagnostics_config),
        "row_counts": {
            "trades": len(result.trades),
            "fold_summary": len(result.fold_summary),
            "breakdown": len(result.breakdown),
            "horizon": len(result.horizon),
            "horizon_summary": len(result.horizon_summary),
            "execution_trades": len(result.execution_trades),
            "execution": len(result.execution),
            "cost_stress": len(result.cost_stress),
            "strategy_variants": len(result.strategy_variants),
        },
    }
    paths.manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return paths
