"""Command-line entry point for execution accounting v1."""

from __future__ import annotations

import argparse

from level1_ofi_qr.execution import (
    DEFAULT_FIXED_BPS,
    DEFAULT_QUANTITY,
    DEFAULT_SLIPPAGE_TICKS,
    DEFAULT_TICK_SIZE,
    ExecutionAccountingConfig,
    build_execution_accounting,
)
from level1_ofi_qr.labeling import DEFAULT_LABEL_HORIZONS
from level1_ofi_qr.signals import SEQUENTIAL_GATE_SIGNAL
from level1_ofi_qr.utils import load_data_slice_config


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run execution accounting scaffold for active signal rows."
    )
    parser.add_argument("config", help="Path to a data-slice YAML config.")
    parser.add_argument("--processed-dir")
    parser.add_argument("--output-dir")
    parser.add_argument("--horizon", action="append", dest="horizons")
    parser.add_argument("--signal-column", default=SEQUENTIAL_GATE_SIGNAL)
    parser.add_argument("--quantity", type=float, default=DEFAULT_QUANTITY)
    parser.add_argument("--fixed-bps", type=float, default=DEFAULT_FIXED_BPS)
    parser.add_argument("--slippage-ticks", type=float, default=DEFAULT_SLIPPAGE_TICKS)
    parser.add_argument("--tick-size", type=float, default=DEFAULT_TICK_SIZE)
    args = parser.parse_args()

    config = load_data_slice_config(args.config)
    accounting_config = ExecutionAccountingConfig(
        horizons=tuple(args.horizons) if args.horizons else DEFAULT_LABEL_HORIZONS,
        signal_column=args.signal_column,
        quantity=args.quantity,
        fixed_bps=args.fixed_bps,
        slippage_ticks=args.slippage_ticks,
        tick_size=args.tick_size,
    )
    result = build_execution_accounting(
        config,
        processed_dir=args.processed_dir,
        output_dir=args.output_dir,
        accounting_config=accounting_config,
    )

    diagnostics = result.diagnostics
    print(f"input_signal_rows={diagnostics.input_signal_rows}")
    print(f"active_signal_rows={diagnostics.active_signal_rows}")
    print(f"costable_signal_rows={diagnostics.costable_signal_rows}")
    print(f"skipped_missing_cost_rows={diagnostics.skipped_missing_cost_rows}")
    print(f"output_trade_rows={diagnostics.output_trade_rows}")
    print(f"output_ledger_rows={diagnostics.output_ledger_rows}")
    print(f"output_summary_rows={diagnostics.output_summary_rows}")
    print(f"horizons={list(diagnostics.horizons)}")
    print(f"quantity={diagnostics.quantity}")
    print(f"fixed_bps={diagnostics.fixed_bps}")
    print(f"slippage_ticks={diagnostics.slippage_ticks}")
    print(f"summary_csv_path={result.paths.summary_csv_path}")
    print(f"ledger_csv_path={result.paths.ledger_csv_path}")
    print(f"trades_csv_path={result.paths.trades_csv_path}")
    print(f"manifest_path={result.paths.manifest_path}")

    for row in result.summary.to_dict(orient="records"):
        print(
            f"horizon={row['horizon']} "
            f"round_trips={row['accounted_round_trips']} "
            f"total_gross_pnl={row['total_gross_pnl']} "
            f"total_cost={row['total_cost']} "
            f"total_net_pnl={row['total_net_pnl']} "
            f"final_position={row['final_position']} "
            f"max_abs_position={row['max_abs_position']}"
        )


if __name__ == "__main__":
    main()
