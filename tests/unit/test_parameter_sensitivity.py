from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from level1_ofi_qr.evaluation import (
    PARAMETER_SENSITIVITY_POLICY_NOTE,
    ParameterSensitivityConfig,
    ParameterSensitivityError,
    build_parameter_sensitivity,
    run_parameter_sensitivity_v1,
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
                    "2026-04-08T09:31:03-04:00",
                ],
                format="mixed",
            ),
            "symbol": ["AAPL"] * 4,
            "trading_date": ["2026-04-08"] * 4,
            "sequential_gate_signal": [1, 0, -1, 0],
            "signal_midquote": [100.0, 100.2, 100.1, 100.0],
            "signal_quoted_spread": [0.02, 0.02, 0.04, 0.04],
        }
    )


def test_run_parameter_sensitivity_v1_reports_all_candidates() -> None:
    result = run_parameter_sensitivity_v1(
        signal_rows(),
        config=ParameterSensitivityConfig(
            max_position_grid=(1.0,),
            cooldown_grid=("0ms", "1s"),
            max_trades_per_day_grid=(None,),
            fixed_bps_grid=(0.0, 1.0),
            slippage_ticks_grid=(0.0,),
        ),
    )

    assert result.diagnostics.candidate_count == 4
    assert result.diagnostics.output_summary_rows == 4
    assert result.diagnostics.parameter_selection_policy == "no_parameter_selection"
    assert result.diagnostics.train_window_selection_implemented is False
    assert result.summary["candidate_id"].tolist() == [
        "candidate_0001",
        "candidate_0002",
        "candidate_0003",
        "candidate_0004",
    ]
    assert set(result.summary["fixed_bps"]) == {0.0, 1.0}
    assert set(result.summary["cooldown"]) == {"0ms", "1s"}
    assert "final_equity" in result.summary.columns


def test_run_parameter_sensitivity_v1_rejects_negative_grid_value() -> None:
    with pytest.raises(ParameterSensitivityError, match="non-negative"):
        run_parameter_sensitivity_v1(
            signal_rows(),
            config=ParameterSensitivityConfig(fixed_bps_grid=(-1.0,)),
        )


def test_build_parameter_sensitivity_writes_manifest(tmp_path: Path) -> None:
    config = load_data_slice_config(CONFIG_PATH)
    processed_root = tmp_path / "processed"
    slice_root = processed_root / config.slice_name
    slice_root.mkdir(parents=True)
    signal_rows().to_csv(slice_root / f"{config.slice_name}_signals_v1.csv", index=False)

    result = build_parameter_sensitivity(
        config,
        processed_dir=processed_root,
        sensitivity_config=ParameterSensitivityConfig(
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
    assert manifest["parameter_sensitivity_scope_note"] == PARAMETER_SENSITIVITY_POLICY_NOTE
    assert manifest["parameter_sensitivity_status"] == {
        "parameter_sensitivity_implemented": "v1_grid_report",
        "parameter_sensitivity_policy": "exhaustive_grid_report_no_selection_v1",
        "parameter_selection_policy": "no_parameter_selection",
        "train_window_selection_implemented": False,
        "model_training_implemented": False,
        "official_fee_model_implemented": False,
        "passive_execution_implemented": False,
        "research_grade_backtest": False,
    }
    assert len(manifest["summary"]) == 2
