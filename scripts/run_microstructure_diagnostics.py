"""Command-line entry point for cost-aware microstructure diagnostics."""

from __future__ import annotations

import argparse

from level1_ofi_qr.diagnostics import (
    DEFAULT_COST_STRESS_MULTIPLIERS,
    DEFAULT_DIAGNOSTIC_HORIZONS,
    MicrostructureDiagnosticsConfig,
    build_cost_aware_microstructure_diagnostics_v1,
    write_microstructure_figures,
)
from level1_ofi_qr.utils import load_data_slice_config


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Build diagnostic tables for selected cost-aware linear-score trades "
            "without changing model logic or selection logic."
        )
    )
    parser.add_argument("config", help="Path to a data-slice YAML config.")
    parser.add_argument("--processed-dir")
    parser.add_argument("--output-dir")
    parser.add_argument("--figures-dir", default="outputs/figures")
    parser.add_argument("--horizons", default=",".join(DEFAULT_DIAGNOSTIC_HORIZONS))
    parser.add_argument(
        "--cost-stress-multipliers",
        default=",".join(str(value) for value in DEFAULT_COST_STRESS_MULTIPLIERS),
    )
    parser.add_argument("--trailing-window", default="1s")
    parser.add_argument("--passive-entry-timeout", default="5s")
    args = parser.parse_args()

    config = load_data_slice_config(args.config)
    result = build_cost_aware_microstructure_diagnostics_v1(
        config,
        processed_dir=args.processed_dir,
        output_dir=args.output_dir,
        diagnostics_config=MicrostructureDiagnosticsConfig(
            horizons=_parse_str_grid(args.horizons),
            cost_stress_multipliers=_parse_float_grid(args.cost_stress_multipliers),
            trailing_window=args.trailing_window,
            passive_entry_timeout=args.passive_entry_timeout,
        ),
    )
    figure_paths = write_microstructure_figures(
        result.diagnostics,
        slice_name=config.slice_name,
        figures_dir=args.figures_dir,
    )

    diagnostics = result.diagnostics
    print(f"trades_csv_path={result.paths.trades_csv_path}")
    print(f"fold_summary_csv_path={result.paths.fold_summary_csv_path}")
    print(f"breakdown_csv_path={result.paths.breakdown_csv_path}")
    print(f"horizon_csv_path={result.paths.horizon_csv_path}")
    print(f"horizon_summary_csv_path={result.paths.horizon_summary_csv_path}")
    print(f"execution_csv_path={result.paths.execution_csv_path}")
    print(f"execution_trades_csv_path={result.paths.execution_trades_csv_path}")
    print(f"cost_stress_csv_path={result.paths.cost_stress_csv_path}")
    print(f"strategy_variants_csv_path={result.paths.strategy_variants_csv_path}")
    print(f"manifest_path={result.paths.manifest_path}")
    print(f"strategy_variants_svg_path={figure_paths.strategy_variants_svg_path}")
    print(f"horizon_svg_path={figure_paths.horizon_svg_path}")
    print(f"execution_svg_path={figure_paths.execution_svg_path}")
    print(f"spread_breakdown_svg_path={figure_paths.spread_breakdown_svg_path}")
    print(f"trade_round_trips={len(diagnostics.trades)}")
    print(f"fold_rows={len(diagnostics.fold_summary)}")
    if not diagnostics.strategy_variants.empty:
        for row in diagnostics.strategy_variants.to_dict(orient="records"):
            print(
                f"variant={row['variant']} net_pnl={row['net_pnl']} "
                f"attempted={row['attempted_round_trips']} "
                f"fill_rate={row['fill_rate']}"
            )
    if not diagnostics.cost_stress.empty:
        all_stress = diagnostics.cost_stress.loc[diagnostics.cost_stress["fold_id"] == "ALL"]
        for row in all_stress.to_dict(orient="records"):
            print(
                f"stress multiplier={row['cost_multiplier']} "
                f"net_pnl={row['net_pnl']} "
                f"net_per_round_trip={row['net_per_round_trip']}"
            )


def _parse_str_grid(raw: str) -> tuple[str, ...]:
    values = tuple(part.strip() for part in raw.split(",") if part.strip())
    if not values:
        raise ValueError("Grid must not be empty.")
    return values


def _parse_float_grid(raw: str) -> tuple[float, ...]:
    values = tuple(float(part.strip()) for part in raw.split(",") if part.strip())
    if not values:
        raise ValueError("Grid must not be empty.")
    return values


if __name__ == "__main__":
    main()
