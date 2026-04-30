"""Command-line entry point for PnL/equity-curve reporting."""

from __future__ import annotations

import argparse
from pathlib import Path

from level1_ofi_qr.reporting import StrategyLedgerSpec, write_pnl_comparison
from level1_ofi_qr.utils import load_data_slice_config


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Plot rule-gate and model-score backtest equity curves."
    )
    parser.add_argument("config", help="Path to a data-slice YAML config.")
    parser.add_argument("--processed-dir")
    parser.add_argument("--figures-dir", default="outputs/figures")
    parser.add_argument("--tables-dir", default="outputs/tables")
    args = parser.parse_args()

    config = load_data_slice_config(args.config)
    processed_root = Path(args.processed_dir or config.storage["processed_dir"]) / config.slice_name
    figures_root = Path(args.figures_dir)
    tables_root = Path(args.tables_dir)

    specs = (
        StrategyLedgerSpec(
            strategy_name="sequential_gate",
            ledger_path=processed_root / f"{config.slice_name}_backtest_v1_ledger.csv",
            summary_path=processed_root / f"{config.slice_name}_backtest_v1_summary.csv",
        ),
        StrategyLedgerSpec(
            strategy_name="linear_score",
            ledger_path=processed_root / f"{config.slice_name}_model_backtest_v1_ledger.csv",
            summary_path=processed_root / f"{config.slice_name}_model_backtest_v1_summary.csv",
        ),
    )
    curve_csv_path = tables_root / f"{config.slice_name}_pnl_comparison_curve.csv"
    summary_csv_path = tables_root / f"{config.slice_name}_pnl_comparison_summary.csv"
    svg_path = figures_root / f"{config.slice_name}_pnl_comparison.svg"
    result = write_pnl_comparison(
        specs,
        curve_csv_path=curve_csv_path,
        summary_csv_path=summary_csv_path,
        svg_path=svg_path,
        title=f"{config.slice_name} PnL / Equity Curve Comparison",
    )

    print(f"curve_csv_path={curve_csv_path}")
    print(f"summary_csv_path={summary_csv_path}")
    print(f"svg_path={svg_path}")
    for row in result.summary.to_dict(orient="records"):
        print(
            f"{row['strategy']} orders={row['order_count']} "
            f"final_equity={row['final_equity']} "
            f"total_cost={row['total_cost']} "
            f"max_drawdown={row['max_drawdown']}"
        )


if __name__ == "__main__":
    main()
