from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from level1_ofi_qr.models import (
    MODEL_TRAINING_POLICY_NOTE,
    ModelFeatureSet,
    ModelTrainingV1Config,
    build_model_training_v1,
    run_model_training_v1,
)
from level1_ofi_qr.utils import load_data_slice_config


ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = ROOT / "configs" / "data" / "aapl_wrds_20260313_20260410.yaml"


def signal_rows() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "event_time": pd.to_datetime(
                [
                    "2026-04-08T09:31:00-04:00",
                    "2026-04-08T09:31:01-04:00",
                    "2026-04-08T09:31:02-04:00",
                    "2026-04-09T09:31:00-04:00",
                    "2026-04-09T09:31:01-04:00",
                    "2026-04-10T09:31:00-04:00",
                    "2026-04-10T09:31:01-04:00",
                ],
                format="mixed",
            ),
            "symbol": ["AAPL"] * 7,
            "trading_date": [
                "2026-04-08",
                "2026-04-08",
                "2026-04-08",
                "2026-04-09",
                "2026-04-09",
                "2026-04-10",
                "2026-04-10",
            ],
            "signal_midquote": [100.0, 100.1, 100.2, 101.0, 101.2, 102.0, 101.8],
            "signal_quoted_spread": [0.02] * 7,
            "signal_quote_imbalance": [1.0, -1.0, 0.8, 1.0, 0.0, -1.0, 0.0],
            "signal_quote_revision_bps": [0.2, -0.2, 0.1, 0.2, 0.0, -0.2, 0.0],
            "signed_flow_imbalance_500ms": [1.0, -1.0, 0.5, 1.0, 0.0, -1.0, 0.0],
            "signed_flow_imbalance_50_trades": [1.0, -1.0, 0.5, 1.0, 0.0, -1.0, 0.0],
            "future_midquote_direction_500ms": [1, -1, 1, 1, 0, -1, 0],
            "future_midquote_return_bps_500ms": [1.0, -1.0, 0.5, 1.0, 0.0, 2.0, 0.0],
            "label_available_500ms": [True] * 7,
        }
    )


def model_config() -> ModelTrainingV1Config:
    return ModelTrainingV1Config(
        min_train_dates=1,
        feature_sets=(
            ModelFeatureSet(
                name="toy_core",
                columns=(
                    "signal_quote_imbalance",
                    "signal_quote_revision_bps",
                    "signed_flow_imbalance_500ms",
                    "signed_flow_imbalance_50_trades",
                ),
            ),
        ),
        score_threshold_grid=(0.0, 0.5),
        min_train_observations=2,
        min_validation_orders=1,
    )


def test_run_model_training_v1_selects_on_validation_and_tests_once() -> None:
    result = run_model_training_v1(signal_rows(), config=model_config())

    assert result.diagnostics.fold_count == 1
    assert result.diagnostics.output_candidate_rows == 2
    assert result.diagnostics.test_used_for_selection is False
    assert result.diagnostics.rule_based_signal_used_for_backtest is False
    assert result.diagnostics.research_grade_backtest is False
    assert len(result.summary) == 1

    selected = result.candidates.loc[result.candidates["selected_for_test"] == True].iloc[0]
    assert selected["validation_date"] == "2026-04-09"
    assert selected["test_date"] == "2026-04-10"
    assert pd.isna(
        result.candidates.loc[
            result.candidates["selected_for_test"] == False,
            "test_final_equity",
        ].iloc[0]
    )

    summary = result.summary.iloc[0]
    assert summary["test_date"] == "2026-04-10"
    assert summary["candidate_id"] == selected["candidate_id"]
    assert set(result.predictions["trading_date"]) == {"2026-04-10"}
    assert set(result.orders["trading_date"]) == {"2026-04-10"}


def test_build_model_training_v1_writes_manifest(tmp_path: Path) -> None:
    data_config = load_data_slice_config(CONFIG_PATH)
    processed_root = tmp_path / "processed"
    slice_root = processed_root / data_config.slice_name
    slice_root.mkdir(parents=True)
    signal_rows().to_csv(slice_root / f"{data_config.slice_name}_signals_v1.csv", index=False)

    result = build_model_training_v1(
        data_config,
        processed_dir=processed_root,
        model_config=model_config(),
    )

    assert result.paths.predictions_csv_path.exists()
    assert result.paths.candidates_csv_path.exists()
    assert result.paths.backtest_orders_csv_path.exists()
    assert result.paths.backtest_summary_csv_path.exists()
    assert result.paths.manifest_path.exists()
    manifest = json.loads(result.paths.manifest_path.read_text())
    assert manifest["model_training_v1_scope_note"] == MODEL_TRAINING_POLICY_NOTE
    assert manifest["model_training_v1_status"]["model_training_implemented"] is True
    assert manifest["model_training_v1_status"]["test_used_for_selection"] is False
    assert manifest["model_training_v1_status"]["research_grade_backtest"] is False
    assert len(manifest["backtest_summary"]) == 1
