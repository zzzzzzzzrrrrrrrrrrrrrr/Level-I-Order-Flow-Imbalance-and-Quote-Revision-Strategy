"""Command-line entry point for backtest v1."""

from __future__ import annotations

import argparse

from level1_ofi_qr.backtesting import BacktestV1Config, build_backtest_v1
from level1_ofi_qr.signals import SEQUENTIAL_GATE_SIGNAL
from level1_ofi_qr.utils import load_data_slice_config


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Run backtest v1 by evaluating TVT-selected parameters on held-out "
            "test dates."
        )
    )
    parser.add_argument("config", help="Path to a data-slice YAML config.")
    parser.add_argument("--processed-dir")
    parser.add_argument("--output-dir")
    parser.add_argument("--tvt-summary-path")
    parser.add_argument("--fold-id")
    parser.add_argument("--signal-column", default=SEQUENTIAL_GATE_SIGNAL)
    args = parser.parse_args()

    config = load_data_slice_config(args.config)
    result = build_backtest_v1(
        config,
        processed_dir=args.processed_dir,
        output_dir=args.output_dir,
        tvt_summary_path=args.tvt_summary_path,
        backtest_config=BacktestV1Config(
            signal_column=args.signal_column,
            fold_id=args.fold_id,
        ),
    )

    diagnostics = result.diagnostics
    print(f"input_signal_rows={diagnostics.input_signal_rows}")
    print(f"input_tvt_summary_rows={diagnostics.input_tvt_summary_rows}")
    print(f"selected_candidate_rows={diagnostics.selected_candidate_rows}")
    print(f"output_order_rows={diagnostics.output_order_rows}")
    print(f"output_ledger_rows={diagnostics.output_ledger_rows}")
    print(f"output_summary_rows={diagnostics.output_summary_rows}")
    print(f"evaluated_test_dates={list(diagnostics.evaluated_test_dates)}")
    print(f"fold_ids={list(diagnostics.fold_ids)}")
    print(f"candidate_ids={list(diagnostics.candidate_ids)}")
    print(f"test_used_for_selection={diagnostics.test_used_for_selection}")
    print(f"parameter_reselection_on_test={diagnostics.parameter_reselection_on_test}")
    print(f"research_grade_backtest={diagnostics.research_grade_backtest}")
    print(f"orders_csv_path={result.paths.orders_csv_path}")
    print(f"ledger_csv_path={result.paths.ledger_csv_path}")
    print(f"summary_csv_path={result.paths.summary_csv_path}")
    print(f"manifest_path={result.paths.manifest_path}")

    for row in result.summary.to_dict(orient="records"):
        print(
            f"{row['fold_id']} selected={row['candidate_id']} "
            f"test={row['test_date']} orders={row['order_rows']} "
            f"total_cost={row['total_cost']} final_equity={row['final_equity']}"
        )


if __name__ == "__main__":
    main()
