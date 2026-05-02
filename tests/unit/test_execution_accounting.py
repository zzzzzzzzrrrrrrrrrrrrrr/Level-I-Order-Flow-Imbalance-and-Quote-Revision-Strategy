from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from level1_ofi_qr.execution import (
    EXECUTION_ACCOUNTING_POLICY_NOTE,
    ExecutionAccountingConfig,
    ExecutionAccountingError,
    build_execution_accounting,
    run_execution_accounting_v1,
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
            "future_midquote_1s": [100.03, 99.98, 100.50, 100.10],
            "future_midquote_event_time_1s": pd.to_datetime(
                [
                    "2026-04-08T09:31:01-04:00",
                    "2026-04-08T09:31:02-04:00",
                    "2026-04-08T09:31:03-04:00",
                    "2026-04-08T09:31:04-04:00",
                ],
                format="mixed",
            ),
        }
    )


def test_run_execution_accounting_v1_builds_round_trips_and_ledger() -> None:
    result = run_execution_accounting_v1(
        signal_rows(),
        config=ExecutionAccountingConfig(horizons=("1s",)),
    )

    diagnostics = result.diagnostics
    assert diagnostics.input_signal_rows == 4
    assert diagnostics.active_signal_rows == 3
    assert diagnostics.skipped_no_signal_rows == 1
    assert diagnostics.costable_signal_rows == 2
    assert diagnostics.skipped_missing_cost_rows == 1
    assert diagnostics.output_trade_rows == 2
    assert diagnostics.output_ledger_rows == 4
    assert diagnostics.research_grade_backtest is False
    assert diagnostics.passive_fill_simulation_implemented is False

    trades = result.trades.sort_values("signal_row_index").reset_index(drop=True)
    assert trades.loc[0, "side"] == 1
    assert trades.loc[0, "gross_pnl"] == pytest.approx(0.03)
    assert trades.loc[0, "total_cost"] == pytest.approx(0.02)
    assert trades.loc[0, "net_pnl"] == pytest.approx(0.01)
    assert trades.loc[1, "side"] == -1
    assert trades.loc[1, "gross_pnl"] == pytest.approx(0.02)
    assert trades.loc[1, "total_cost"] == pytest.approx(0.04)
    assert trades.loc[1, "net_pnl"] == pytest.approx(-0.02)

    summary = result.summary.iloc[0]
    assert summary["accounted_round_trips"] == 2
    assert summary["ledger_rows"] == 4
    assert summary["total_gross_pnl"] == pytest.approx(0.05)
    assert summary["total_cost"] == pytest.approx(0.06)
    assert summary["total_net_pnl"] == pytest.approx(-0.01)
    assert summary["final_position"] == pytest.approx(0.0)
    assert summary["final_equity"] == pytest.approx(summary["total_net_pnl"])

    final_ledger = result.ledger.iloc[-1]
    assert final_ledger["position_after"] == pytest.approx(0.0)
    assert final_ledger["equity_after"] == pytest.approx(summary["total_net_pnl"])


def test_run_execution_accounting_v1_applies_fixed_and_slippage_costs() -> None:
    result = run_execution_accounting_v1(
        signal_rows(),
        config=ExecutionAccountingConfig(
            horizons=("1s",),
            fixed_bps=1.0,
            slippage_ticks=1.0,
            tick_size=0.01,
        ),
    )

    trades = result.trades.sort_values("signal_row_index").reset_index(drop=True)
    assert trades.loc[0, "entry_cost"] == pytest.approx(0.03)
    assert trades.loc[0, "exit_cost"] == pytest.approx(0.030003)
    assert trades.loc[0, "net_pnl"] == pytest.approx(0.03 - 0.060003)


def test_run_execution_accounting_v1_rejects_negative_quantity() -> None:
    with pytest.raises(ExecutionAccountingError, match="quantity must be positive"):
        run_execution_accounting_v1(
            signal_rows(),
            config=ExecutionAccountingConfig(horizons=("1s",), quantity=-1.0),
        )


def test_build_execution_accounting_writes_manifest(tmp_path: Path) -> None:
    config = load_data_slice_config(CONFIG_PATH)
    processed_root = tmp_path / "processed"
    slice_root = processed_root / config.slice_name
    slice_root.mkdir(parents=True)
    signal_rows().to_csv(slice_root / f"{config.slice_name}_signals_v1.csv", index=False)

    result = build_execution_accounting(
        config,
        processed_dir=processed_root,
        accounting_config=ExecutionAccountingConfig(horizons=("1s",)),
    )

    assert result.paths.trades_csv_path.exists()
    assert result.paths.ledger_csv_path.exists()
    assert result.paths.summary_csv_path.exists()
    assert result.paths.manifest_path.exists()
    manifest = json.loads(result.paths.manifest_path.read_text())
    assert manifest["execution_accounting_scope_note"] == EXECUTION_ACCOUNTING_POLICY_NOTE
    assert manifest["execution_accounting_status"] == {
        "execution_accounting_implemented": "v1_scaffold",
        "accounting_policy": "independent_fixed_horizon_round_trip_accounting_v1",
        "entry_execution_policy": "entry_at_signal_midquote_with_half_spread_cost",
        "exit_execution_policy": "exit_at_future_midquote_with_entry_spread_proxy",
        "position_policy": "independent_unit_round_trips_no_position_limit",
        "position_accounting_implemented": True,
        "cash_accounting_implemented": True,
        "inventory_accounting_implemented": True,
        "pnl_attribution_implemented": True,
        "passive_fill_simulation_implemented": False,
        "order_book_fill_simulation_implemented": False,
        "broker_fee_model_implemented": False,
        "sec_finra_fee_model_implemented": False,
        "risk_controls_implemented": False,
        "parameter_optimization_implemented": False,
        "research_grade_backtest": False,
    }
    assert len(manifest["summary"]) == 1
