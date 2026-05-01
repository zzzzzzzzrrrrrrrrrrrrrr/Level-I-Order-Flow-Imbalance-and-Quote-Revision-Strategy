"""File-based workflow for model training v1."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import json
from pathlib import Path

import pandas as pd

from ..alignment import TRADING_DATE
from ..schema import EVENT_TIME, SYMBOL
from ..signals.rules import SIGNAL_MIDQUOTE, SIGNAL_QUOTED_SPREAD
from ..utils import DataSliceConfig
from .training import (
    COST_AWARE_LINEAR_SCORE_STRATEGY,
    MODEL_TRAINING_POLICY_NOTE,
    CostAwareLinearScoreConfig,
    CostAwareLinearScoreDiagnostics,
    ModelTrainingV1Config,
    ModelTrainingV1Diagnostics,
    run_cost_aware_linear_score_v1,
    run_model_training_v1,
)


class ModelTrainingWorkflowError(ValueError):
    """Raised when model training workflow inputs cannot be resolved."""


@dataclass(frozen=True)
class ModelTrainingInputPaths:
    """Input paths for model training v1."""

    signal_path: Path


@dataclass(frozen=True)
class ModelTrainingOutputPaths:
    """Output paths written by model training v1."""

    predictions_csv_path: Path
    candidates_csv_path: Path
    backtest_orders_csv_path: Path
    backtest_ledger_csv_path: Path
    backtest_summary_csv_path: Path
    manifest_path: Path


@dataclass(frozen=True)
class ModelTrainingBuildResult:
    """Model training frames, paths, and diagnostics."""

    predictions: pd.DataFrame
    candidates: pd.DataFrame
    orders: pd.DataFrame
    ledger: pd.DataFrame
    summary: pd.DataFrame
    paths: ModelTrainingOutputPaths
    diagnostics: ModelTrainingV1Diagnostics


@dataclass(frozen=True)
class CostAwareLinearScoreOutputPaths:
    """Output paths written by the cost-aware linear-score workflow."""

    predictions_csv_path: Path
    candidates_csv_path: Path
    backtest_orders_csv_path: Path
    backtest_ledger_csv_path: Path
    backtest_summary_csv_path: Path
    report_csv_path: Path
    manifest_path: Path


@dataclass(frozen=True)
class CostAwareLinearScoreBuildResult:
    """Cost-aware linear-score frames, paths, and diagnostics."""

    predictions: pd.DataFrame
    candidates: pd.DataFrame
    orders: pd.DataFrame
    ledger: pd.DataFrame
    summary: pd.DataFrame
    report: pd.DataFrame
    paths: CostAwareLinearScoreOutputPaths
    diagnostics: CostAwareLinearScoreDiagnostics


def find_model_training_input(
    config: DataSliceConfig,
    *,
    processed_dir: str | Path | None = None,
) -> ModelTrainingInputPaths:
    """Find signal rows for model training v1."""

    root = Path(processed_dir or config.storage["processed_dir"]) / config.slice_name
    signal_path = root / f"{config.slice_name}_signals_v1.csv"
    if not signal_path.exists():
        raise ModelTrainingWorkflowError(
            f"Model training input file is missing: {signal_path}. "
            "Run scripts/build_signals.py first."
        )
    return ModelTrainingInputPaths(signal_path=signal_path)


def build_model_training_v1(
    config: DataSliceConfig,
    *,
    processed_dir: str | Path | None = None,
    output_dir: str | Path | None = None,
    model_config: ModelTrainingV1Config = ModelTrainingV1Config(),
) -> ModelTrainingBuildResult:
    """Run model training v1 from signal rows."""

    inputs = find_model_training_input(config, processed_dir=processed_dir)
    signal_rows = _read_signal_csv(inputs.signal_path, config=model_config)
    result = run_model_training_v1(signal_rows, config=model_config)
    paths = _write_model_training_outputs(
        config,
        inputs=inputs,
        predictions=result.predictions,
        candidates=result.candidates,
        orders=result.orders,
        ledger=result.ledger,
        summary=result.summary,
        diagnostics=result.diagnostics,
        output_dir=output_dir or processed_dir,
    )
    return ModelTrainingBuildResult(
        predictions=result.predictions,
        candidates=result.candidates,
        orders=result.orders,
        ledger=result.ledger,
        summary=result.summary,
        paths=paths,
        diagnostics=result.diagnostics,
    )


def build_cost_aware_linear_score_v1(
    config: DataSliceConfig,
    *,
    processed_dir: str | Path | None = None,
    output_dir: str | Path | None = None,
    cost_aware_config: CostAwareLinearScoreConfig = CostAwareLinearScoreConfig(),
) -> CostAwareLinearScoreBuildResult:
    """Run cost-aware linear-score variant from signal rows."""

    inputs = find_model_training_input(config, processed_dir=processed_dir)
    signal_rows = _read_signal_csv(inputs.signal_path, config=cost_aware_config)
    result = run_cost_aware_linear_score_v1(signal_rows, config=cost_aware_config)
    paths = _write_cost_aware_linear_score_outputs(
        config,
        inputs=inputs,
        predictions=result.predictions,
        candidates=result.candidates,
        orders=result.orders,
        ledger=result.ledger,
        summary=result.summary,
        report=result.report,
        diagnostics=result.diagnostics,
        output_dir=output_dir or processed_dir,
    )
    return CostAwareLinearScoreBuildResult(
        predictions=result.predictions,
        candidates=result.candidates,
        orders=result.orders,
        ledger=result.ledger,
        summary=result.summary,
        report=result.report,
        paths=paths,
        diagnostics=result.diagnostics,
    )


def _read_signal_csv(path: Path, *, config: ModelTrainingV1Config) -> pd.DataFrame:
    suffix = config.label_horizon.lower().replace(" ", "").replace(".", "p")
    usecols = [
        EVENT_TIME,
        SYMBOL,
        TRADING_DATE,
        SIGNAL_MIDQUOTE,
        SIGNAL_QUOTED_SPREAD,
        f"label_available_{suffix}",
        f"future_midquote_direction_{suffix}",
        f"future_midquote_return_bps_{suffix}",
    ]
    for feature_set in config.feature_sets:
        usecols.extend(feature_set.columns)
    frame = pd.read_csv(path, usecols=tuple(dict.fromkeys(usecols)))
    frame[EVENT_TIME] = pd.to_datetime(frame[EVENT_TIME], format="mixed")
    return frame


def _write_cost_aware_linear_score_outputs(
    config: DataSliceConfig,
    *,
    inputs: ModelTrainingInputPaths,
    predictions: pd.DataFrame,
    candidates: pd.DataFrame,
    orders: pd.DataFrame,
    ledger: pd.DataFrame,
    summary: pd.DataFrame,
    report: pd.DataFrame,
    diagnostics: CostAwareLinearScoreDiagnostics,
    output_dir: str | Path | None,
) -> CostAwareLinearScoreOutputPaths:
    output_root = Path(output_dir or config.storage["processed_dir"]) / config.slice_name
    output_root.mkdir(parents=True, exist_ok=True)

    stem = f"{config.slice_name}_{COST_AWARE_LINEAR_SCORE_STRATEGY}"
    predictions_csv_path = output_root / f"{stem}_predictions.csv"
    candidates_csv_path = output_root / f"{stem}_candidates.csv"
    backtest_orders_csv_path = output_root / f"{stem}_orders.csv"
    backtest_ledger_csv_path = output_root / f"{stem}_ledger.csv"
    backtest_summary_csv_path = output_root / f"{stem}_summary.csv"
    report_csv_path = output_root / f"{stem}_report.csv"
    manifest_path = output_root / f"{stem}_manifest.json"

    predictions.to_csv(predictions_csv_path, index=False)
    candidates.to_csv(candidates_csv_path, index=False)
    orders.to_csv(backtest_orders_csv_path, index=False)
    ledger.to_csv(backtest_ledger_csv_path, index=False)
    summary.to_csv(backtest_summary_csv_path, index=False)
    report.to_csv(report_csv_path, index=False)
    manifest = {
        "slice_name": config.slice_name,
        "inputs": {
            "signal_path": str(inputs.signal_path),
        },
        "outputs": {
            "predictions_csv_path": str(predictions_csv_path),
            "candidates_csv_path": str(candidates_csv_path),
            "backtest_orders_csv_path": str(backtest_orders_csv_path),
            "backtest_ledger_csv_path": str(backtest_ledger_csv_path),
            "backtest_summary_csv_path": str(backtest_summary_csv_path),
            "report_csv_path": str(report_csv_path),
            "manifest_path": str(manifest_path),
        },
        "strategy_variant_status": {
            "strategy_variant": diagnostics.strategy_variant,
            "base_score_column": diagnostics.base_score_column,
            "signal_column": diagnostics.signal_column,
            "objective": diagnostics.objective,
            "validation_used_for_selection": diagnostics.validation_used_for_selection,
            "test_used_for_selection": diagnostics.test_used_for_selection,
            "parameter_reselection_on_test": diagnostics.parameter_reselection_on_test,
            "reuses_linear_score_output": diagnostics.reuses_linear_score_output,
            "selection_uses_net_pnl": diagnostics.selection_uses_net_pnl,
            "research_grade_model_claim": diagnostics.research_grade_model_claim,
            "research_grade_backtest": diagnostics.research_grade_backtest,
        },
        "diagnostics": asdict(diagnostics),
        "strategy_report": report.to_dict(orient="records"),
        "backtest_summary": summary.to_dict(orient="records"),
    }
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return CostAwareLinearScoreOutputPaths(
        predictions_csv_path=predictions_csv_path,
        candidates_csv_path=candidates_csv_path,
        backtest_orders_csv_path=backtest_orders_csv_path,
        backtest_ledger_csv_path=backtest_ledger_csv_path,
        backtest_summary_csv_path=backtest_summary_csv_path,
        report_csv_path=report_csv_path,
        manifest_path=manifest_path,
    )


def _write_model_training_outputs(
    config: DataSliceConfig,
    *,
    inputs: ModelTrainingInputPaths,
    predictions: pd.DataFrame,
    candidates: pd.DataFrame,
    orders: pd.DataFrame,
    ledger: pd.DataFrame,
    summary: pd.DataFrame,
    diagnostics: ModelTrainingV1Diagnostics,
    output_dir: str | Path | None,
) -> ModelTrainingOutputPaths:
    output_root = Path(output_dir or config.storage["processed_dir"]) / config.slice_name
    output_root.mkdir(parents=True, exist_ok=True)

    predictions_csv_path = output_root / f"{config.slice_name}_model_training_v1_predictions.csv"
    candidates_csv_path = output_root / f"{config.slice_name}_model_training_v1_candidates.csv"
    backtest_orders_csv_path = output_root / f"{config.slice_name}_model_backtest_v1_orders.csv"
    backtest_ledger_csv_path = output_root / f"{config.slice_name}_model_backtest_v1_ledger.csv"
    backtest_summary_csv_path = output_root / f"{config.slice_name}_model_backtest_v1_summary.csv"
    manifest_path = output_root / f"{config.slice_name}_model_training_v1_manifest.json"

    predictions.to_csv(predictions_csv_path, index=False)
    candidates.to_csv(candidates_csv_path, index=False)
    orders.to_csv(backtest_orders_csv_path, index=False)
    ledger.to_csv(backtest_ledger_csv_path, index=False)
    summary.to_csv(backtest_summary_csv_path, index=False)
    manifest = {
        "slice_name": config.slice_name,
        "inputs": {
            "signal_path": str(inputs.signal_path),
        },
        "outputs": {
            "predictions_csv_path": str(predictions_csv_path),
            "candidates_csv_path": str(candidates_csv_path),
            "backtest_orders_csv_path": str(backtest_orders_csv_path),
            "backtest_ledger_csv_path": str(backtest_ledger_csv_path),
            "backtest_summary_csv_path": str(backtest_summary_csv_path),
            "manifest_path": str(manifest_path),
        },
        "model_training_v1_status": {
            "model_training_implemented": diagnostics.model_training_implemented,
            "validation_used_for_selection": diagnostics.validation_used_for_selection,
            "test_used_for_selection": diagnostics.test_used_for_selection,
            "parameter_reselection_on_test": diagnostics.parameter_reselection_on_test,
            "rule_based_signal_used_for_backtest": (
                diagnostics.rule_based_signal_used_for_backtest
            ),
            "research_grade_model_claim": diagnostics.research_grade_model_claim,
            "research_grade_backtest": diagnostics.research_grade_backtest,
            "split_policy": diagnostics.split_policy,
            "model_training_policy": diagnostics.model_training_policy,
            "model_selection_policy": diagnostics.model_selection_policy,
            "test_policy": diagnostics.test_policy,
            "objective": diagnostics.objective,
        },
        "model_training_v1_scope_note": MODEL_TRAINING_POLICY_NOTE,
        "diagnostics": asdict(diagnostics),
        "backtest_summary": summary.to_dict(orient="records"),
    }
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return ModelTrainingOutputPaths(
        predictions_csv_path=predictions_csv_path,
        candidates_csv_path=candidates_csv_path,
        backtest_orders_csv_path=backtest_orders_csv_path,
        backtest_ledger_csv_path=backtest_ledger_csv_path,
        backtest_summary_csv_path=backtest_summary_csv_path,
        manifest_path=manifest_path,
    )
