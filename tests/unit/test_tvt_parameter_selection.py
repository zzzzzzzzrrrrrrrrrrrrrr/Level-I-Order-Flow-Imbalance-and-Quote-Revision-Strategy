from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from level1_ofi_qr.evaluation import (
    TVT_SELECTION_POLICY_NOTE,
    TVTParameterSelectionConfig,
    TVTParameterSelectionError,
    build_tvt_parameter_selection,
    run_tvt_parameter_selection_v1,
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
                    "2026-04-10T09:31:00-04:00",
                    "2026-04-10T09:31:01-04:00",
                ],
                format="mixed",
            ),
            "symbol": ["AAPL"] * 6,
            "trading_date": [
                "2026-04-08",
                "2026-04-08",
                "2026-04-09",
                "2026-04-09",
                "2026-04-10",
                "2026-04-10",
            ],
            "sequential_gate_signal": [1, 0, 1, 0, -1, 0],
            "signal_midquote": [100.0, 100.1, 101.0, 101.1, 102.0, 101.8],
            "signal_quoted_spread": [0.02, 0.02, 0.02, 0.02, 0.04, 0.04],
        }
    )


def test_run_tvt_parameter_selection_v1_selects_on_validation_only() -> None:
    result = run_tvt_parameter_selection_v1(
        signal_rows(),
        config=TVTParameterSelectionConfig(
            min_train_dates=1,
            max_position_grid=(1.0,),
            cooldown_grid=("0ms",),
            max_trades_per_day_grid=(None,),
            fixed_bps_grid=(0.0, 1.0),
            slippage_ticks_grid=(0.0,),
        ),
    )

    summary = result.summary
    assert len(summary) == 2
    assert result.diagnostics.fold_count == 1
    assert result.diagnostics.candidate_count == 2
    assert result.diagnostics.test_used_for_selection is False
    assert result.diagnostics.model_training_implemented is False

    selected = summary.loc[summary["selected_for_test"] == True].iloc[0]
    assert selected["candidate_id"] == "candidate_0001"
    assert selected["train_start_date"] == "2026-04-08"
    assert selected["train_end_date"] == "2026-04-08"
    assert selected["validation_date"] == "2026-04-09"
    assert selected["test_date"] == "2026-04-10"
    assert selected["fixed_bps"] == pytest.approx(0.0)
    assert selected["selection_status"] == "selected_on_validation"
    assert not bool(selected["test_used_for_selection"])
    assert "test_final_equity" in summary.columns
    assert selected["test_final_equity"] == pytest.approx(0.16)

    unselected = summary.loc[summary["selected_for_test"] == False].iloc[0]
    assert unselected["selection_status"] == "not_selected"
    assert pd.isna(unselected["test_final_equity"])


def test_run_tvt_parameter_selection_v1_requires_enough_dates() -> None:
    with pytest.raises(TVTParameterSelectionError, match="min_train_dates"):
        run_tvt_parameter_selection_v1(
            signal_rows().loc[lambda frame: frame["trading_date"] != "2026-04-10"],
            config=TVTParameterSelectionConfig(min_train_dates=1),
        )


def test_build_tvt_parameter_selection_writes_manifest(tmp_path: Path) -> None:
    config = load_data_slice_config(CONFIG_PATH)
    processed_root = tmp_path / "processed"
    slice_root = processed_root / config.slice_name
    slice_root.mkdir(parents=True)
    signal_rows().to_csv(slice_root / f"{config.slice_name}_signals_v1.csv", index=False)

    result = build_tvt_parameter_selection(
        config,
        processed_dir=processed_root,
        selection_config=TVTParameterSelectionConfig(
            min_train_dates=1,
            max_position_grid=(1.0,),
            cooldown_grid=("0ms",),
            max_trades_per_day_grid=(None,),
            fixed_bps_grid=(0.0, 1.0),
            slippage_ticks_grid=(0.0,),
        ),
    )

    assert result.paths.summary_csv_path.exists()
    assert result.paths.manifest_path.exists()
    manifest = json.loads(result.paths.manifest_path.read_text())
    assert manifest["tvt_parameter_selection_scope_note"] == TVT_SELECTION_POLICY_NOTE
    assert manifest["tvt_parameter_selection_status"] == {
        "tvt_parameter_selection_implemented": "v1_validation_select_test_evaluate",
        "split_policy": "expanding_train_next_validation_next_test",
        "selection_policy": "select_on_validation_evaluate_once_on_test",
        "objective": "maximize_validation_final_equity",
        "tie_break_policy": "higher_validation_equity_lower_cost_lower_order_count",
        "model_training_implemented": False,
        "test_used_for_selection": False,
        "final_hyperparameter_claim": False,
        "research_grade_backtest": False,
    }
    assert len(manifest["summary"]) == 2
