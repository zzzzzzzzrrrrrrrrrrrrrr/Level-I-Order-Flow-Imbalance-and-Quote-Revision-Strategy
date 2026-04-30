"""Command-line entry point for target-position accounting v1."""

from __future__ import annotations

import argparse

from level1_ofi_qr.execution import (
    DEFAULT_FIXED_BPS,
    DEFAULT_SLIPPAGE_TICKS,
    DEFAULT_TICK_SIZE,
    TargetPositionAccountingConfig,
    build_target_position_accounting,
)
from level1_ofi_qr.signals import SEQUENTIAL_GATE_SIGNAL
from level1_ofi_qr.utils import load_data_slice_config


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run target-position accounting scaffold for signal rows."
    )
    parser.add_argument("config", help="Path to a data-slice YAML config.")
    parser.add_argument("--processed-dir")
    parser.add_argument("--output-dir")
    parser.add_argument("--signal-column", default=SEQUENTIAL_GATE_SIGNAL)
    parser.add_argument("--max-position", type=float, default=1.0)
    parser.add_argument("--fixed-bps", type=float, default=DEFAULT_FIXED_BPS)
    parser.add_argument("--slippage-ticks", type=float, default=DEFAULT_SLIPPAGE_TICKS)
    parser.add_argument("--tick-size", type=float, default=DEFAULT_TICK_SIZE)
    parser.add_argument("--cooldown", default="0ms")
    parser.add_argument("--max-trades-per-day", type=int)
    parser.add_argument(
        "--hold-on-no-signal",
        action="store_true",
        help="Keep the current target when the signal is 0 instead of flattening.",
    )
    parser.add_argument(
        "--no-eod-flat",
        action="store_true",
        help="Do not force positions flat at the last valid row of each symbol/date.",
    )
    args = parser.parse_args()

    config = load_data_slice_config(args.config)
    accounting_config = TargetPositionAccountingConfig(
        signal_column=args.signal_column,
        max_position=args.max_position,
        fixed_bps=args.fixed_bps,
        slippage_ticks=args.slippage_ticks,
        tick_size=args.tick_size,
        cooldown=args.cooldown,
        flat_on_no_signal=not args.hold_on_no_signal,
        eod_flat=not args.no_eod_flat,
        max_trades_per_day=args.max_trades_per_day,
    )
    result = build_target_position_accounting(
        config,
        processed_dir=args.processed_dir,
        output_dir=args.output_dir,
        accounting_config=accounting_config,
    )

    diagnostics = result.diagnostics
    print(f"input_signal_rows={diagnostics.input_signal_rows}")
    print(f"active_signal_rows={diagnostics.active_signal_rows}")
    print(f"target_change_candidate_rows={diagnostics.target_change_candidate_rows}")
    print(f"output_order_rows={diagnostics.output_order_rows}")
    print(f"output_ledger_rows={diagnostics.output_ledger_rows}")
    print(f"max_position={diagnostics.max_position}")
    print(f"cooldown={diagnostics.cooldown}")
    print(f"max_trades_per_day={diagnostics.max_trades_per_day}")
    print(f"skipped_missing_price_rows={diagnostics.skipped_missing_price_rows}")
    print(f"skipped_cooldown_orders={diagnostics.skipped_cooldown_orders}")
    print(f"skipped_max_trades_orders={diagnostics.skipped_max_trades_orders}")
    print(f"summary_csv_path={result.paths.summary_csv_path}")
    print(f"ledger_csv_path={result.paths.ledger_csv_path}")
    print(f"orders_csv_path={result.paths.orders_csv_path}")
    print(f"manifest_path={result.paths.manifest_path}")
    for row in result.summary.to_dict(orient="records"):
        print(
            f"orders={row['order_rows']} "
            f"total_cost={row['total_cost']} "
            f"final_equity={row['final_equity']} "
            f"final_position={row['final_position']} "
            f"max_abs_position={row['max_abs_position']} "
            f"turnover={row['total_turnover']}"
        )


if __name__ == "__main__":
    main()
