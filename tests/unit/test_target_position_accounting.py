from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from level1_ofi_qr.execution import (
    TARGET_POSITION_POLICY_NOTE,
    TargetPositionAccountingConfig,
    TargetPositionAccountingError,
    build_target_position_accounting,
    run_target_position_accounting_v1,
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
                    "2026-04-08T09:31:04-04:00",
                    "2026-04-08T09:31:05-04:00",
                ],
                format="mixed",
            ),
            "symbol": ["AAPL"] * 6,
            "trading_date": ["2026-04-08"] * 6,
            "sequential_gate_signal": [0, 1, 1, 0, -1, 0],
            "signal_midquote": [100.00, 100.00, 100.02, 100.05, 100.10, 100.00],
            "signal_quoted_spread": [0.02, 0.02, 0.02, 0.02, 0.04, 0.04],
        }
    )


def test_run_target_position_accounting_v1_tracks_position_cash_and_equity() -> None:
    result = run_target_position_accounting_v1(
        signal_rows(),
        config=TargetPositionAccountingConfig(),
    )

    diagnostics = result.diagnostics
    assert diagnostics.input_signal_rows == 6
    assert diagnostics.active_signal_rows == 3
    assert diagnostics.target_change_candidate_rows == 4
    assert diagnostics.output_order_rows == 4
    assert diagnostics.risk_controls_implemented is True
    assert diagnostics.research_grade_backtest is False

    orders = result.orders.reset_index(drop=True)
    assert orders["order_quantity"].tolist() == [1.0, -1.0, -1.0, 1.0]
    assert orders["order_reason"].tolist() == [
        "signal_target_change",
        "signal_target_change",
        "signal_target_change",
        "signal_target_change",
    ]

    summary = result.summary.iloc[0]
    assert summary["order_rows"] == 4
    assert summary["total_cost"] == pytest.approx(0.06)
    assert summary["final_position"] == pytest.approx(0.0)
    assert summary["final_equity"] == pytest.approx(0.09)
    assert summary["max_abs_position"] == pytest.approx(1.0)

    final = result.ledger.iloc[-1]
    assert final["position_after"] == pytest.approx(0.0)
    assert final["equity_after"] == pytest.approx(0.09)


def test_run_target_position_accounting_v1_applies_cooldown_and_eod_flat() -> None:
    result = run_target_position_accounting_v1(
        signal_rows(),
        config=TargetPositionAccountingConfig(cooldown="10s"),
    )

    assert result.diagnostics.skipped_cooldown_orders == 3
    assert result.orders["order_reason"].tolist() == ["signal_target_change", "eod_flat"]
    assert result.orders["order_quantity"].tolist() == [1.0, -1.0]
    assert result.summary.iloc[0]["final_position"] == pytest.approx(0.0)


def test_run_target_position_accounting_v1_applies_max_trades_per_day() -> None:
    result = run_target_position_accounting_v1(
        signal_rows(),
        config=TargetPositionAccountingConfig(max_trades_per_day=2),
    )

    assert result.diagnostics.skipped_max_trades_orders == 1
    assert result.orders["order_quantity"].tolist() == [1.0, -1.0]
    assert result.summary.iloc[0]["final_position"] == pytest.approx(0.0)


def test_run_target_position_accounting_v1_rejects_negative_max_position() -> None:
    with pytest.raises(TargetPositionAccountingError, match="max_position must be positive"):
        run_target_position_accounting_v1(
            signal_rows(),
            config=TargetPositionAccountingConfig(max_position=-1.0),
        )


def test_build_target_position_accounting_writes_manifest(tmp_path: Path) -> None:
    config = load_data_slice_config(CONFIG_PATH)
    processed_root = tmp_path / "processed"
    slice_root = processed_root / config.slice_name
    slice_root.mkdir(parents=True)
    signal_rows().to_csv(slice_root / f"{config.slice_name}_signals_v1.csv", index=False)

    result = build_target_position_accounting(
        config,
        processed_dir=processed_root,
        accounting_config=TargetPositionAccountingConfig(),
    )

    assert result.paths.orders_csv_path.exists()
    assert result.paths.ledger_csv_path.exists()
    assert result.paths.summary_csv_path.exists()
    assert result.paths.manifest_path.exists()
    manifest = json.loads(result.paths.manifest_path.read_text())
    assert manifest["target_position_accounting_scope_note"] == TARGET_POSITION_POLICY_NOTE
    assert manifest["target_position_accounting_status"] == {
        "target_position_accounting_implemented": "v1_scaffold",
        "target_position_policy": "signal_to_bounded_target_position_v1",
        "target_mapping_policy": "long_short_flat_signal_to_target_position",
        "order_execution_policy": "fill_at_signal_midquote_with_cost_deduction",
        "position_limit_policy": "clip_target_to_max_abs_position",
        "no_signal_policy": "flat_on_no_signal",
        "eod_policy": "force_flat_at_last_valid_row_per_symbol_date",
        "position_limit_implemented": True,
        "cooldown_implemented": True,
        "max_trades_per_day_implemented": True,
        "eod_flat_implemented": True,
        "risk_controls_implemented": True,
        "passive_fill_simulation_implemented": False,
        "order_book_fill_simulation_implemented": False,
        "broker_fee_model_implemented": False,
        "sec_finra_fee_model_implemented": False,
        "parameter_optimization_implemented": False,
        "research_grade_backtest": False,
    }
    assert len(manifest["summary"]) == 1
