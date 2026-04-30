"""Command-line entry point for quote-trade alignment workflows."""

from __future__ import annotations

import argparse

import pandas as pd

from level1_ofi_qr.alignment import (
    build_quote_trade_alignment,
    build_quote_trade_alignment_tolerance_sensitivity,
)
from level1_ofi_qr.utils import load_data_slice_config


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Align cleaned trades to the latest strictly prior cleaned quote."
    )
    parser.add_argument("config", help="Path to a data-slice YAML config.")
    parser.add_argument(
        "--processed-dir",
        help=(
            "Root directory containing <slice_name> cleaned quote/trade files. "
            "Defaults to the processed_dir in the config."
        ),
    )
    parser.add_argument(
        "--output-dir",
        help=(
            "Root directory for aligned outputs. Defaults to --processed-dir when "
            "provided, otherwise the processed_dir in the config."
        ),
    )
    parser.add_argument(
        "--tolerance-ms",
        type=float,
        help="Optional maximum quote lag in milliseconds. Omit for no maximum lag.",
    )
    parser.add_argument(
        "--tolerance-sensitivity",
        action="store_true",
        help="Run comparison diagnostics for None, 5s, 1s, 500ms, and 100ms tolerances.",
    )
    args = parser.parse_args()

    if args.tolerance_sensitivity and args.tolerance_ms is not None:
        parser.error("--tolerance-sensitivity cannot be combined with --tolerance-ms.")

    config = load_data_slice_config(args.config)
    if args.tolerance_sensitivity:
        result = build_quote_trade_alignment_tolerance_sensitivity(
            config,
            processed_dir=args.processed_dir,
            output_dir=args.output_dir,
        )
        print(f"summary_json_path={result.paths.summary_json_path}")
        print(f"summary_csv_path={result.paths.summary_csv_path}")
        for row in result.summary:
            print(
                "candidate_tolerance="
                f"{row['candidate_tolerance']} "
                f"matched_trade_rows={row['matched_trade_rows']} "
                f"unmatched_trade_rows={row['unmatched_trade_rows']} "
                f"matched_ratio={row['matched_ratio']:.6f} "
                f"p99_quote_lag_ms={row['p99_quote_lag_ms']}"
            )
        return

    tolerance = (
        pd.Timedelta(milliseconds=args.tolerance_ms)
        if args.tolerance_ms is not None
        else None
    )
    result = build_quote_trade_alignment(
        config,
        processed_dir=args.processed_dir,
        output_dir=args.output_dir,
        tolerance=tolerance,
    )

    print(f"input_quote_rows={result.diagnostics.input_quote_rows}")
    print(f"input_trade_rows={result.diagnostics.input_trade_rows}")
    print(f"aligned_trade_rows={result.diagnostics.aligned_trade_rows}")
    print(f"matched_trade_rows={result.diagnostics.matched_trade_rows}")
    print(f"unmatched_trade_rows={result.diagnostics.unmatched_trade_rows}")
    print(f"matched_ratio={result.diagnostics.matched_ratio:.6f}")
    print(f"allow_exact_matches={result.diagnostics.allow_exact_matches}")
    print(f"session_boundary_policy={result.diagnostics.session_boundary_policy}")
    print(f"alignment_group_keys={list(result.diagnostics.alignment_group_keys)}")
    print(f"cross_session_match_count={result.diagnostics.cross_session_match_count}")
    print(f"tolerance={result.diagnostics.tolerance}")
    print(f"tolerance_policy={result.diagnostics.tolerance_policy}")
    print(f"min_quote_lag_ms={result.diagnostics.min_quote_lag_ms}")
    print(f"median_quote_lag_ms={result.diagnostics.median_quote_lag_ms}")
    print(f"p95_quote_lag_ms={result.diagnostics.p95_quote_lag_ms}")
    print(f"p99_quote_lag_ms={result.diagnostics.p99_quote_lag_ms}")
    print(f"max_quote_lag_ms={result.diagnostics.max_quote_lag_ms}")
    print(f"matched_locked_quote_count={result.diagnostics.matched_locked_quote_count}")
    print(f"matched_locked_quote_ratio={result.diagnostics.matched_locked_quote_ratio:.6f}")
    print(f"aligned_trade_path={result.paths.aligned_trade_path}")
    print(f"manifest_path={result.paths.manifest_path}")


if __name__ == "__main__":
    main()
