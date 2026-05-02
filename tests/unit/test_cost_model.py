from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from level1_ofi_qr.backtesting import (
    COST_MODEL_POLICY_NOTE,
    CostModelConfig,
    CostModelError,
    build_cost_model_diagnostics,
    run_cost_model_v1,
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
                    "2026-04-08T09:31:03-04:00",
                ],
                format="mixed",
            ),
            "symbol": ["AAPL"] * 4,
            "trading_date": ["2026-04-08"] * 4,
            "sequential_gate_signal": [1, -1, 0, 1],
            "signal_midquote": [100.0, 100.0, 100.0, pd.NA],
            "signal_quoted_spread": [0.02, 0.04, 0.02, pd.NA],
            "label_available_1s": [True, True, True, True],
            "future_midquote_return_bps_1s": [2.0, -1.0, 9.0, 3.0],
        }
    )


def test_run_cost_model_v1_computes_spread_and_stress_costs() -> None:
    result = run_cost_model_v1(
        signal_rows(),
        config=CostModelConfig(
            horizons=("1s",),
            fixed_bps_grid=(0.0, 1.0),
            slippage_ticks_grid=(0.0, 1.0),
            tick_size=0.01,
        ),
    )

    diagnostics = result.diagnostics
    assert diagnostics.input_signal_rows == 4
    assert diagnostics.active_signal_rows == 3
    assert diagnostics.skipped_no_signal_rows == 1
    assert diagnostics.costable_signal_rows == 2
    assert diagnostics.skipped_missing_cost_rows == 1
    assert diagnostics.output_summary_rows == 4
    assert diagnostics.position_accounting_implemented is False
    assert diagnostics.backtest_implemented is False

    base = result.summary.loc[
        (result.summary["fixed_bps"] == 0.0)
        & (result.summary["slippage_ticks"] == 0.0)
    ].iloc[0]
    assert base["evaluated_signal_rows"] == 2
    assert base["long_signal_rows"] == 1
    assert base["short_signal_rows"] == 1
    assert base["mean_signed_future_return_bps"] == pytest.approx(1.5)
    assert base["mean_half_spread_cost_bps"] == pytest.approx(1.5)
    assert base["mean_full_spread_round_trip_cost_bps"] == pytest.approx(3.0)
    assert base["mean_after_one_way_cost_bps"] == pytest.approx(0.0)
    assert base["mean_after_round_trip_cost_bps"] == pytest.approx(-1.5)
    assert base["share_beating_one_way_cost"] == pytest.approx(0.5)
    assert base["share_beating_round_trip_cost"] == pytest.approx(0.0)

    stressed = result.summary.loc[
        (result.summary["fixed_bps"] == 1.0)
        & (result.summary["slippage_ticks"] == 1.0)
    ].iloc[0]
    assert stressed["mean_one_way_total_cost_bps"] == pytest.approx(3.5)
    assert stressed["mean_round_trip_total_cost_bps"] == pytest.approx(7.0)


def test_run_cost_model_v1_rejects_negative_cost_grid() -> None:
    with pytest.raises(CostModelError, match="non-negative"):
        run_cost_model_v1(
            signal_rows(),
            config=CostModelConfig(
                horizons=("1s",),
                fixed_bps_grid=(-0.5,),
            ),
        )


def test_build_cost_model_diagnostics_writes_manifest(tmp_path: Path) -> None:
    config = load_data_slice_config(CONFIG_PATH)
    processed_root = tmp_path / "processed"
    slice_root = processed_root / config.slice_name
    slice_root.mkdir(parents=True)
    signal_rows().to_csv(slice_root / f"{config.slice_name}_signals_v1.csv", index=False)

    result = build_cost_model_diagnostics(
        config,
        processed_dir=processed_root,
        cost_config=CostModelConfig(
            horizons=("1s",),
            fixed_bps_grid=(0.0,),
            slippage_ticks_grid=(0.0,),
        ),
    )

    assert result.paths.summary_csv_path.exists()
    assert result.paths.manifest_path.exists()
    manifest = json.loads(result.paths.manifest_path.read_text())
    assert manifest["cost_model_scope_note"] == COST_MODEL_POLICY_NOTE
    assert manifest["cost_model_status"] == {
        "cost_model_implemented": "v1_diagnostic",
        "cost_model_policy": "spread_and_stress_cost_diagnostics_v1",
        "execution_cost_policy": "aggressive_one_way_half_spread_proxy",
        "round_trip_cost_policy": "aggressive_entry_exit_full_spread_proxy",
        "broker_fee_model_implemented": False,
        "sec_finra_fee_model_implemented": False,
        "position_accounting_implemented": False,
        "passive_fill_simulation_implemented": False,
        "backtest_implemented": False,
        "research_grade_pnl": False,
    }
    assert len(manifest["summary"]) == 1

