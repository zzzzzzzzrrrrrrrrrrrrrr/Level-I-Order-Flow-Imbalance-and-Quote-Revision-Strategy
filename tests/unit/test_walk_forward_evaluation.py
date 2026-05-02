from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from level1_ofi_qr.evaluation import (
    WALK_FORWARD_POLICY_NOTE,
    WalkForwardConfig,
    WalkForwardEvaluationError,
    build_walk_forward_evaluation,
    evaluate_signals_walk_forward_v1,
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
                    "2026-04-09T09:31:00-04:00",
                    "2026-04-09T09:31:01-04:00",
                    "2026-04-09T09:31:02-04:00",
                    "2026-04-10T09:31:00-04:00",
                    "2026-04-10T09:31:01-04:00",
                ],
                format="mixed",
            ),
            "symbol": ["AAPL"] * 7,
            "trading_date": [
                "2026-04-08",
                "2026-04-08",
                "2026-04-09",
                "2026-04-09",
                "2026-04-09",
                "2026-04-10",
                "2026-04-10",
            ],
            "sequential_gate_signal": [1, -1, 1, -1, 0, -1, 1],
            "label_available_1s": [True, True, True, True, True, True, True],
            "future_midquote_direction_1s": [1, -1, 1, 1, -1, -1, 0],
            "future_midquote_return_bps_1s": [2.0, -2.0, 3.0, 1.0, -1.0, -4.0, 0.0],
        }
    )


def test_evaluate_signals_walk_forward_v1_builds_expanding_date_folds() -> None:
    result = evaluate_signals_walk_forward_v1(
        signal_rows(),
        config=WalkForwardConfig(horizons=("1s",), min_train_dates=1),
    )
    summary = result.summary

    assert summary["fold_id"].tolist() == [
        "fold_001",
        "fold_002",
        "ALL_EVALUATION_FOLDS",
    ]
    fold_1 = summary.loc[summary["fold_id"] == "fold_001"].iloc[0]
    assert fold_1["train_start_date"] == "2026-04-08"
    assert fold_1["train_end_date"] == "2026-04-08"
    assert fold_1["test_date"] == "2026-04-09"
    assert fold_1["train_rows"] == 2
    assert fold_1["test_rows"] == 3
    assert fold_1["evaluated_signal_rows"] == 2
    assert fold_1["signal_coverage"] == pytest.approx(2 / 3)
    assert fold_1["correct_signal_rows"] == 1
    assert fold_1["signal_accuracy"] == pytest.approx(0.5)
    assert fold_1["nonflat_signal_accuracy"] == pytest.approx(0.5)
    assert fold_1["mean_signal_aligned_return_bps"] == pytest.approx(1.0)

    fold_2 = summary.loc[summary["fold_id"] == "fold_002"].iloc[0]
    assert fold_2["train_start_date"] == "2026-04-08"
    assert fold_2["train_end_date"] == "2026-04-09"
    assert fold_2["test_date"] == "2026-04-10"
    assert fold_2["evaluated_signal_rows"] == 2
    assert fold_2["signal_accuracy"] == pytest.approx(0.5)
    assert fold_2["nonflat_evaluated_signal_rows"] == 1
    assert fold_2["nonflat_signal_accuracy"] == pytest.approx(1.0)

    aggregate = summary.loc[summary["fold_id"] == "ALL_EVALUATION_FOLDS"].iloc[0]
    assert aggregate["test_rows"] == 5
    assert aggregate["evaluated_signal_rows"] == 4
    assert aggregate["correct_signal_rows"] == 2
    assert aggregate["signal_accuracy"] == pytest.approx(0.5)

    diagnostics = result.diagnostics
    assert diagnostics.fold_count == 2
    assert diagnostics.trading_dates == ("2026-04-08", "2026-04-09", "2026-04-10")
    assert diagnostics.evaluation_policy == "expanding_train_dates_next_date_test"
    assert diagnostics.signal_usage_policy == "evaluate_precomputed_signals_without_refitting"
    assert diagnostics.label_usage_policy == "labels_used_only_as_targets"
    assert diagnostics.threshold_optimization_implemented is False
    assert diagnostics.cost_model_implemented is False
    assert diagnostics.backtest_implemented is False


def test_evaluate_signals_walk_forward_v1_requires_enough_dates() -> None:
    one_date = signal_rows().loc[lambda frame: frame["trading_date"] == "2026-04-08"]

    with pytest.raises(WalkForwardEvaluationError, match="more trading dates"):
        evaluate_signals_walk_forward_v1(
            one_date,
            config=WalkForwardConfig(horizons=("1s",), min_train_dates=1),
        )


def test_build_walk_forward_evaluation_writes_manifest(tmp_path: Path) -> None:
    config = load_data_slice_config(CONFIG_PATH)
    processed_root = tmp_path / "processed"
    slice_root = processed_root / config.slice_name
    slice_root.mkdir(parents=True)
    signal_rows().to_csv(slice_root / f"{config.slice_name}_signals_v1.csv", index=False)

    result = build_walk_forward_evaluation(
        config,
        processed_dir=processed_root,
        evaluation_config=WalkForwardConfig(horizons=("1s",), min_train_dates=1),
    )

    assert result.paths.summary_csv_path.exists()
    assert result.paths.manifest_path.exists()
    manifest = json.loads(result.paths.manifest_path.read_text())
    assert manifest["evaluation_scope_note"] == WALK_FORWARD_POLICY_NOTE
    assert manifest["evaluation_status"] == {
        "walk_forward_implemented": "v1_statistical",
        "evaluation_policy": "expanding_train_dates_next_date_test",
        "signal_usage_policy": "evaluate_precomputed_signals_without_refitting",
        "label_usage_policy": "labels_used_only_as_targets",
        "threshold_optimization_implemented": False,
        "model_training_implemented": False,
        "cost_model_implemented": False,
        "backtest_implemented": False,
        "research_grade_strategy_result": False,
    }
    diagnostics = manifest["diagnostics"]
    assert diagnostics["fold_count"] == 2
    assert diagnostics["horizons"] == ["1s"]
    assert len(manifest["summary"]) == 3
