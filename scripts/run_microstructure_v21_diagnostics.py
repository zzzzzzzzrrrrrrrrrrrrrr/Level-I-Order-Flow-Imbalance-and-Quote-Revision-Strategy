"""Command-line entry point for microstructure v2.1 diagnostics."""

from __future__ import annotations

import argparse

from level1_ofi_qr.diagnostics.microstructure_v21 import (
    DEFAULT_CANDIDATE_POOLS,
    DEFAULT_EDGE_THRESHOLDS,
    DEFAULT_EXECUTION_VARIANTS,
    DEFAULT_MICROPRICE_USAGES,
    DEFAULT_QUEUE_HAIRCUTS,
    DEFAULT_TTLS,
    MicrostructureV21Config,
    build_microstructure_v21_diagnostics,
)
from level1_ofi_qr.utils import load_data_slice_config


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Build independent microstructure v2.1 passive/hybrid execution "
            "diagnostics without modifying v1 or v2.0 outputs."
        )
    )
    parser.add_argument("config", help="Path to a data-slice YAML config.")
    parser.add_argument("--processed-dir")
    parser.add_argument("--output-dir")
    parser.add_argument("--candidate-pools", default=",".join(DEFAULT_CANDIDATE_POOLS))
    parser.add_argument("--edge-thresholds", default=",".join(DEFAULT_EDGE_THRESHOLDS))
    parser.add_argument("--microprice-usages", default=",".join(DEFAULT_MICROPRICE_USAGES))
    parser.add_argument("--ttls", default=",".join(DEFAULT_TTLS))
    parser.add_argument("--queue-haircuts", default=",".join(DEFAULT_QUEUE_HAIRCUTS))
    parser.add_argument("--execution-variants", default=",".join(DEFAULT_EXECUTION_VARIANTS))
    parser.add_argument("--tick-size", type=float, default=0.01)
    parser.add_argument("--min-depth", type=float, default=200.0)
    parser.add_argument("--market-safety-margin-bps", type=float, default=0.0)
    parser.add_argument("--adverse-selection-buffer-bps", type=float, default=0.5)
    parser.add_argument("--validation-min-dates", type=int, default=2)
    parser.add_argument(
        "--validation-objective",
        default="net_pnl_per_submitted_order",
        help="Metric column used for prior-fold variant selection.",
    )
    args = parser.parse_args()

    config = load_data_slice_config(args.config)
    result = build_microstructure_v21_diagnostics(
        config,
        processed_dir=args.processed_dir,
        output_dir=args.output_dir,
        diagnostics_config=MicrostructureV21Config(
            candidate_pools=_parse_str_grid(args.candidate_pools),
            edge_thresholds=_parse_str_grid(args.edge_thresholds),
            microprice_usages=_parse_str_grid(args.microprice_usages),
            ttl_values=_parse_str_grid(args.ttls),
            queue_haircuts=_parse_str_grid(args.queue_haircuts),
            execution_variants=_parse_str_grid(args.execution_variants),
            tick_size=args.tick_size,
            min_depth=args.min_depth,
            market_safety_margin_bps=args.market_safety_margin_bps,
            adverse_selection_buffer_bps=args.adverse_selection_buffer_bps,
            validation_min_dates=args.validation_min_dates,
            validation_objective=args.validation_objective,
        ),
    )

    print(f"candidate_events_csv_path={result.paths.candidate_events_csv_path}")
    print(f"orders_csv_path={result.paths.orders_csv_path}")
    print(f"variant_daily_metrics_csv_path={result.paths.variant_daily_metrics_csv_path}")
    print(f"variant_summary_csv_path={result.paths.variant_summary_csv_path}")
    print(f"validation_selection_csv_path={result.paths.validation_selection_csv_path}")
    print(f"selected_test_metrics_csv_path={result.paths.selected_test_metrics_csv_path}")
    print(f"manifest_path={result.paths.manifest_path}")
    print(f"candidate_events={len(result.candidate_events)}")
    print(f"orders={len(result.orders)}")
    print(f"variant_daily_rows={len(result.variant_daily_metrics)}")
    print(f"variant_summary_rows={len(result.variant_summary)}")
    print(f"validation_selection_rows={len(result.validation_selection)}")
    print(f"selected_test_metric_rows={len(result.selected_test_metrics)}")
    if not result.selected_test_metrics.empty:
        selected_net = result.selected_test_metrics["net_pnl"].sum()
        selected_submitted = result.selected_test_metrics["submitted_orders"].sum()
        selected_filled = result.selected_test_metrics["filled_orders"].sum()
        print(f"selected_test_net_pnl={selected_net}")
        print(f"selected_test_submitted_orders={selected_submitted}")
        print(f"selected_test_filled_orders={selected_filled}")


def _parse_str_grid(raw: str) -> tuple[str, ...]:
    values = tuple(part.strip() for part in raw.split(",") if part.strip())
    if not values:
        raise ValueError("Grid must not be empty.")
    return values


if __name__ == "__main__":
    main()
