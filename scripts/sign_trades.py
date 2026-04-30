"""Command-line entry point for trade signing v1."""

from __future__ import annotations

import argparse

from level1_ofi_qr.trade_signing import build_trade_signing_dataset
from level1_ofi_qr.utils import load_data_slice_config


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build trade signing v1 output from aligned trade rows."
    )
    parser.add_argument("config", help="Path to a data-slice YAML config.")
    parser.add_argument(
        "--processed-dir",
        help=(
            "Root directory containing <slice_name> aligned trade files. "
            "Defaults to the processed_dir in the config."
        ),
    )
    parser.add_argument(
        "--output-dir",
        help=(
            "Root directory for signed-trade outputs. Defaults to --processed-dir "
            "when provided, otherwise the processed_dir in the config."
        ),
    )
    args = parser.parse_args()

    config = load_data_slice_config(args.config)
    result = build_trade_signing_dataset(
        config,
        processed_dir=args.processed_dir,
        output_dir=args.output_dir,
    )

    print(f"input_aligned_trade_rows={result.diagnostics.input_aligned_trade_rows}")
    print(f"output_signed_trade_rows={result.diagnostics.output_signed_trade_rows}")
    print(f"row_preserving={result.diagnostics.row_preserving}")
    print(f"trade_signing_method={result.diagnostics.trade_signing_method}")
    print(f"quote_matched_rows={result.diagnostics.quote_matched_rows}")
    print(f"quote_unmatched_rows={result.diagnostics.quote_unmatched_rows}")
    print(f"quote_rule_signed_rows={result.diagnostics.quote_rule_signed_rows}")
    print(f"tick_rule_signed_rows={result.diagnostics.tick_rule_signed_rows}")
    print(f"unknown_sign_rows={result.diagnostics.unknown_sign_rows}")
    print(f"unknown_sign_ratio={result.diagnostics.unknown_sign_ratio:.6f}")
    print(f"buy_sign_rows={result.diagnostics.buy_sign_rows}")
    print(f"sell_sign_rows={result.diagnostics.sell_sign_rows}")
    print(f"quote_midpoint_tie_rows={result.diagnostics.quote_midpoint_tie_rows}")
    print(f"quote_tick_conflict_rows={result.diagnostics.quote_tick_conflict_rows}")
    print(f"signed_trade_path={result.paths.signed_trade_path}")
    print(f"manifest_path={result.paths.manifest_path}")


if __name__ == "__main__":
    main()
