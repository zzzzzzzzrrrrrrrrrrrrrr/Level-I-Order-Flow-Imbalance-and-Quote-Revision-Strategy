from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from level1_ofi_qr.evaluation import (
    THRESHOLD_SELECTION_POLICY_NOTE,
    ThresholdSelectionConfig,
    ThresholdSelectionError,
    build_threshold_selection,
    run_threshold_selection_v1,
)
from level1_ofi_qr.utils import load_data_slice_config


ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = ROOT / "configs" / "data" / "aapl_wrds_20260408_20260410.yaml"


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
                    "2026-04-09T09:31:02-04:00",
                    "2026-04-10T09:31:00-04:00",
                    "2026-04-10T09:31:01-04:00",
                ],
                format="mixed",
            ),
            "symbol": ["AAPL"] * 8,
            "trading_date": [
                "2026-04-08",
                "2026-04-08",
                "2026-04-08",
                "2026-04-09",
                "2026-04-09",
                "2026-04-09",
                "2026-04-10",
                "2026-04-10",
            ],
            "signal_quote_imbalance": [0.05, 0.30, -0.30, 0.30, -0.30, 0.05, 0.30, -0.30],
            "signed_flow_imbalance_500ms": [0.05, 0.30, -0.30, 0.30, -0.30, 0.05, 0.30, -0.30],
            "signal_quote_revision_bps": [0.05, 0.30, -0.30, 0.30, -0.30, 0.05, 0.30, -0.30],
            "label_available_1s": [True] * 8,
            "future_midquote_direction_1s": [-1, 1, -1, 1, -1, -1, -1, 1],
            "future_midquote_return_bps_1s": [-1.0, 4.0, -4.0, 4.0, -4.0, -1.0, -4.0, 4.0],
        }
    )


def test_run_threshold_selection_v1_selects_thresholds_from_train_dates() -> None:
    result = run_threshold_selection_v1(
        signal_rows(),
        config=ThresholdSelectionConfig(
            horizons=("1s",),
            min_train_dates=1,
            qi_threshold_grid=(0.0, 0.1),
            signed_flow_threshold_grid=(0.0, 0.1),
            qr_threshold_bps_grid=(0.0, 0.1),
            min_train_signals=1,
        ),
    )
    summary = result.summary

    assert summary["fold_id"].tolist() == ["fold_001", "fold_002"]
    first_fold = summary.iloc[0]
    assert first_fold["selected_qi_threshold"] == 0.1
    assert first_fold["selected_signed_flow_threshold"] == 0.1
    assert first_fold["selected_qr_threshold_bps"] == 0.1
    assert first_fold["selection_status"] == "selected_from_train_window"
    assert first_fold["train_evaluated_signal_rows"] == 2
    assert first_fold["train_mean_signal_aligned_return_bps"] == pytest.approx(4.0)
    assert first_fold["test_evaluated_signal_rows"] == 2
    assert first_fold["test_signal_accuracy"] == pytest.approx(1.0)

    second_fold = summary.iloc[1]
    assert second_fold["selected_qi_threshold"] == 0.1
    assert second_fold["test_signal_accuracy"] == pytest.approx(0.0)

    diagnostics = result.diagnostics
    assert diagnostics.fold_count == 2
    assert diagnostics.threshold_selection_policy == "train_window_grid_search_next_date_test"
    assert diagnostics.threshold_objective == "maximize_train_mean_signal_aligned_return_bps"
    assert diagnostics.signal_construction_policy == "recompute_sequential_gate_from_selected_thresholds"
    assert (
        diagnostics.label_usage_policy
        == "train_labels_for_threshold_selection_test_labels_for_evaluation"
    )
    assert diagnostics.model_training_implemented is False
    assert diagnostics.cost_model_implemented is False
    assert diagnostics.backtest_implemented is False


def test_run_threshold_selection_v1_rejects_negative_grid_value() -> None:
    with pytest.raises(ThresholdSelectionError, match="non-negative"):
        run_threshold_selection_v1(
            signal_rows(),
            config=ThresholdSelectionConfig(
                horizons=("1s",),
                qi_threshold_grid=(-0.1,),
            ),
        )


def test_build_threshold_selection_writes_manifest(tmp_path: Path) -> None:
    config = load_data_slice_config(CONFIG_PATH)
    processed_root = tmp_path / "processed"
    slice_root = processed_root / config.slice_name
    slice_root.mkdir(parents=True)
    signal_rows().to_csv(slice_root / f"{config.slice_name}_signals_v1.csv", index=False)

    result = build_threshold_selection(
        config,
        processed_dir=processed_root,
        selection_config=ThresholdSelectionConfig(
            horizons=("1s",),
            min_train_dates=1,
            qi_threshold_grid=(0.0, 0.1),
            signed_flow_threshold_grid=(0.0, 0.1),
            qr_threshold_bps_grid=(0.0, 0.1),
            min_train_signals=1,
        ),
    )

    assert result.paths.summary_csv_path.exists()
    assert result.paths.manifest_path.exists()
    manifest = json.loads(result.paths.manifest_path.read_text())
    assert manifest["threshold_selection_scope_note"] == THRESHOLD_SELECTION_POLICY_NOTE
    assert manifest["threshold_selection_status"] == {
        "threshold_selection_implemented": "v1_statistical",
        "threshold_selection_policy": "train_window_grid_search_next_date_test",
        "threshold_objective": "maximize_train_mean_signal_aligned_return_bps",
        "signal_construction_policy": "recompute_sequential_gate_from_selected_thresholds",
        "label_usage_policy": "train_labels_for_threshold_selection_test_labels_for_evaluation",
        "model_training_implemented": False,
        "cost_model_implemented": False,
        "backtest_implemented": False,
        "research_grade_strategy_result": False,
    }
    assert len(manifest["summary"]) == 2
