from __future__ import annotations

from pathlib import Path

import pandas as pd

from level1_ofi_qr.reporting import StrategyLedgerSpec, build_pnl_comparison, write_pnl_comparison


def make_ledger(
    path: Path,
    equities: list[float],
    costs: list[float],
    *,
    segments: list[str] | None = None,
) -> None:
    rows = []
    for index, (equity, cost) in enumerate(zip(equities, costs, strict=True)):
        row = {
            "event_time": f"2026-04-10T09:30:0{index}-04:00",
            "equity_after": equity,
            "event_cost": cost,
            "position_after": 0.0,
        }
        if segments is not None:
            row["backtest_id"] = segments[index]
        rows.append(row)
    pd.DataFrame(rows).to_csv(path, index=False)


def test_build_pnl_comparison_summarizes_ledgers(tmp_path: Path) -> None:
    gate_path = tmp_path / "gate.csv"
    model_path = tmp_path / "model.csv"
    make_ledger(gate_path, [-1.0, -2.0, -1.5], [0.1, 0.1, 0.1])
    make_ledger(model_path, [-0.5, -0.25, 0.1], [0.05, 0.05, 0.05])

    result = build_pnl_comparison(
        (
            StrategyLedgerSpec("gate", gate_path),
            StrategyLedgerSpec("model", model_path),
        )
    )

    assert set(result.summary["strategy"]) == {"gate", "model"}
    gate = result.summary.loc[result.summary["strategy"] == "gate"].iloc[0]
    model = result.summary.loc[result.summary["strategy"] == "model"].iloc[0]
    assert gate["final_equity"] == -1.5
    assert model["final_equity"] == 0.1
    assert "<svg" in result.svg
    assert "Cumulative net PnL after event_cost deductions" in result.svg


def test_build_pnl_comparison_accumulates_segmented_fold_equity(tmp_path: Path) -> None:
    gate_path = tmp_path / "gate.csv"
    make_ledger(
        gate_path,
        [-1.0, -2.0, -0.5, -1.25],
        [0.1, 0.1, 0.1, 0.1],
        segments=["fold_001", "fold_001", "fold_002", "fold_002"],
    )

    result = build_pnl_comparison((StrategyLedgerSpec("gate", gate_path),))

    gate = result.summary.loc[result.summary["strategy"] == "gate"].iloc[0]
    assert gate["final_equity"] == -3.25
    assert result.curve["equity_after"].tolist() == [0.0, -1.0, -2.0, -2.5, -3.25]


def test_write_pnl_comparison_writes_outputs(tmp_path: Path) -> None:
    gate_path = tmp_path / "gate.csv"
    model_path = tmp_path / "model.csv"
    make_ledger(gate_path, [-1.0, -2.0], [0.1, 0.1])
    make_ledger(model_path, [-0.5, 0.5], [0.05, 0.05])

    result = write_pnl_comparison(
        (
            StrategyLedgerSpec("gate", gate_path),
            StrategyLedgerSpec("model", model_path),
        ),
        curve_csv_path=tmp_path / "curve.csv",
        summary_csv_path=tmp_path / "summary.csv",
        svg_path=tmp_path / "plot.svg",
    )

    assert (tmp_path / "curve.csv").exists()
    assert (tmp_path / "summary.csv").exists()
    assert (tmp_path / "plot.svg").exists()
    assert len(result.curve) == 6
