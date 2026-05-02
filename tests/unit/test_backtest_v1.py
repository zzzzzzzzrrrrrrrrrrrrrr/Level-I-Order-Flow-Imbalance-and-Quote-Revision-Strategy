from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from level1_ofi_qr.backtesting import (
    BACKTEST_V1_POLICY_NOTE,
    BacktestV1Error,
    build_backtest_v1,
    run_backtest_v1,
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


def tvt_summary() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "fold_id": ["fold_001", "fold_001"],
            "candidate_id": ["candidate_0001", "candidate_0002"],
            "train_start_date": ["2026-04-08", "2026-04-08"],
            "train_end_date": ["2026-04-08", "2026-04-08"],
            "validation_date": ["2026-04-09", "2026-04-09"],
            "test_date": ["2026-04-10", "2026-04-10"],
            "max_position": [1.0, 1.0],
            "cooldown": ["0ms", "0ms"],
            "max_trades_per_day": [None, None],
            "fixed_bps": [0.0, 1.0],
            "slippage_ticks": [0.0, 0.0],
            "tick_size": [0.01, 0.01],
            "selected_for_test": [True, False],
            "test_used_for_selection": [False, False],
        }
    )


def test_run_backtest_v1_uses_selected_candidate_on_test_date_only() -> None:
    result = run_backtest_v1(signal_rows(), tvt_summary())

    assert result.diagnostics.selected_candidate_rows == 1
    assert result.diagnostics.evaluated_test_dates == ("2026-04-10",)
    assert result.diagnostics.candidate_ids == ("candidate_0001",)
    assert result.diagnostics.test_used_for_selection is False
    assert result.diagnostics.parameter_reselection_on_test is False
    assert result.diagnostics.research_grade_backtest is False

    assert set(result.orders["trading_date"]) == {"2026-04-10"}
    assert set(result.orders["candidate_id"]) == {"candidate_0001"}
    assert len(result.orders) == 2
    summary = result.summary.iloc[0]
    assert summary["fold_id"] == "fold_001"
    assert summary["candidate_id"] == "candidate_0001"
    assert summary["test_date"] == "2026-04-10"
    assert summary["order_rows"] == 2
    assert summary["total_cost"] == pytest.approx(0.04)
    assert summary["final_equity"] == pytest.approx(0.16)


def test_run_backtest_v1_rejects_test_selection_leakage() -> None:
    summary = tvt_summary()
    summary.loc[0, "test_used_for_selection"] = True

    with pytest.raises(BacktestV1Error, match="test data was used"):
        run_backtest_v1(signal_rows(), summary)


def test_build_backtest_v1_writes_manifest(tmp_path: Path) -> None:
    config = load_data_slice_config(CONFIG_PATH)
    processed_root = tmp_path / "processed"
    slice_root = processed_root / config.slice_name
    slice_root.mkdir(parents=True)
    signal_rows().to_csv(slice_root / f"{config.slice_name}_signals_v1.csv", index=False)
    tvt_summary().to_csv(
        slice_root / f"{config.slice_name}_tvt_parameter_selection_v1.csv",
        index=False,
    )

    result = build_backtest_v1(config, processed_dir=processed_root)

    assert result.paths.orders_csv_path.exists()
    assert result.paths.ledger_csv_path.exists()
    assert result.paths.summary_csv_path.exists()
    assert result.paths.manifest_path.exists()
    manifest = json.loads(result.paths.manifest_path.read_text())
    assert manifest["backtest_v1_scope_note"] == BACKTEST_V1_POLICY_NOTE
    assert manifest["backtest_v1_status"] == {
        "backtest_implemented": "v1_tvt_selected_test_accounting",
        "backtest_policy": "tvt_selected_candidate_test_accounting_v1",
        "parameter_source_policy": "frozen_candidate_selected_on_validation",
        "evaluation_policy": "evaluate_selected_candidate_on_test_date_only",
        "split_source_policy": "tvt_parameter_selection_v1",
        "target_position_accounting_used": True,
        "parameter_reselection_on_test": False,
        "test_used_for_selection": False,
        "model_training_implemented": False,
        "passive_fill_simulation_implemented": False,
        "order_book_fill_simulation_implemented": False,
        "broker_fee_model_implemented": False,
        "sec_finra_fee_model_implemented": False,
        "exchange_fee_rebate_model_implemented": False,
        "latency_model_implemented": False,
        "research_grade_backtest": False,
    }
    assert manifest["diagnostics"]["selected_candidate_rows"] == 1
    assert len(manifest["summary"]) == 1
