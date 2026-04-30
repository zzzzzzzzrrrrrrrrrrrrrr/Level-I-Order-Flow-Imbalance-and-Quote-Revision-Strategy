"""Command-line entry point for cost model v1 diagnostics."""

from __future__ import annotations

import argparse

from level1_ofi_qr.backtesting import (
    DEFAULT_FIXED_BPS_GRID,
    DEFAULT_SLIPPAGE_TICKS_GRID,
    DEFAULT_TICK_SIZE,
    CostModelConfig,
    build_cost_model_diagnostics,
)
from level1_ofi_qr.labeling import DEFAULT_LABEL_HORIZONS
from level1_ofi_qr.signals import SEQUENTIAL_GATE_SIGNAL
from level1_ofi_qr.utils import load_data_slice_config


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run spread and stress cost diagnostics for active signal rows."
    )
    parser.add_argument("config", help="Path to a data-slice YAML config.")
    parser.add_argument("--processed-dir")
    parser.add_argument("--output-dir")
    parser.add_argument("--horizon", action="append", dest="horizons")
    parser.add_argument("--signal-column", default=SEQUENTIAL_GATE_SIGNAL)
    parser.add_argument("--fixed-bps-grid", default=_format_grid(DEFAULT_FIXED_BPS_GRID))
    parser.add_argument(
        "--slippage-ticks-grid",
        default=_format_grid(DEFAULT_SLIPPAGE_TICKS_GRID),
    )
    parser.add_argument("--tick-size", type=float, default=DEFAULT_TICK_SIZE)
    args = parser.parse_args()

    config = load_data_slice_config(args.config)
    cost_config = CostModelConfig(
        horizons=tuple(args.horizons) if args.horizons else DEFAULT_LABEL_HORIZONS,
        signal_column=args.signal_column,
        fixed_bps_grid=_parse_float_grid(args.fixed_bps_grid),
        slippage_ticks_grid=_parse_float_grid(args.slippage_ticks_grid),
        tick_size=args.tick_size,
    )
    result = build_cost_model_diagnostics(
        config,
        processed_dir=args.processed_dir,
        output_dir=args.output_dir,
        cost_config=cost_config,
    )

    diagnostics = result.diagnostics
    print(f"input_signal_rows={diagnostics.input_signal_rows}")
    print(f"active_signal_rows={diagnostics.active_signal_rows}")
    print(f"costable_signal_rows={diagnostics.costable_signal_rows}")
    print(f"skipped_missing_cost_rows={diagnostics.skipped_missing_cost_rows}")
    print(f"output_summary_rows={diagnostics.output_summary_rows}")
    print(f"horizons={list(diagnostics.horizons)}")
    print(f"fixed_bps_grid={list(diagnostics.fixed_bps_grid)}")
    print(f"slippage_ticks_grid={list(diagnostics.slippage_ticks_grid)}")
    print(f"summary_csv_path={result.paths.summary_csv_path}")
    print(f"manifest_path={result.paths.manifest_path}")

    base_rows = result.summary.loc[
        (result.summary["fixed_bps"] == 0.0) & (result.summary["slippage_ticks"] == 0.0)
    ]
    for row in base_rows.to_dict(orient="records"):
        print(
            f"horizon={row['horizon']} "
            f"evaluated={row['evaluated_signal_rows']} "
            f"mean_signed_return_bps={row['mean_signed_future_return_bps']} "
            f"mean_half_spread_bps={row['mean_half_spread_cost_bps']} "
            f"mean_after_one_way_bps={row['mean_after_one_way_cost_bps']} "
            f"mean_after_round_trip_bps={row['mean_after_round_trip_cost_bps']}"
        )


def _parse_float_grid(raw: str) -> tuple[float, ...]:
    values = tuple(float(part.strip()) for part in raw.split(",") if part.strip())
    if not values:
        raise ValueError("Grid must not be empty.")
    return values


def _format_grid(values: tuple[float, ...]) -> str:
    return ",".join(str(value) for value in values)


if __name__ == "__main__":
    main()

