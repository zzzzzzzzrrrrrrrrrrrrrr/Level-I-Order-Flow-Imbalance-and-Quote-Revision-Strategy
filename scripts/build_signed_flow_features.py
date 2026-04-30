"""Command-line entry point for signed-flow feature generation."""

from __future__ import annotations

import argparse

from level1_ofi_qr.features import build_signed_flow_feature_dataset
from level1_ofi_qr.utils import load_data_slice_config


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build signed-flow v1 features from trade signing v1 output."
    )
    parser.add_argument("config", help="Path to a data-slice YAML config.")
    parser.add_argument(
        "--processed-dir",
        help=(
            "Root directory containing <slice_name> signed trade files. "
            "Defaults to the processed_dir in the config."
        ),
    )
    parser.add_argument(
        "--output-dir",
        help=(
            "Root directory for signed-flow feature outputs. Defaults to "
            "--processed-dir when provided, otherwise the processed_dir in the config."
        ),
    )
    args = parser.parse_args()

    config = load_data_slice_config(args.config)
    result = build_signed_flow_feature_dataset(
        config,
        processed_dir=args.processed_dir,
        output_dir=args.output_dir,
    )

    print(f"input_signed_trade_rows={result.diagnostics.input_signed_trade_rows}")
    print(f"output_feature_rows={result.diagnostics.output_feature_rows}")
    print(f"row_preserving={result.diagnostics.row_preserving}")
    print(f"feature_group_keys={list(result.diagnostics.feature_group_keys)}")
    print(f"trade_count_windows={list(result.diagnostics.trade_count_windows)}")
    print(f"time_windows={list(result.diagnostics.time_windows)}")
    print(f"signed_trade_rows={result.diagnostics.signed_trade_rows}")
    print(f"unknown_sign_rows={result.diagnostics.unknown_sign_rows}")
    print(f"buy_sign_rows={result.diagnostics.buy_sign_rows}")
    print(f"sell_sign_rows={result.diagnostics.sell_sign_rows}")
    print(f"zero_volume_window_rows={result.diagnostics.zero_volume_window_rows}")
    print(
        "signed_flow_imbalance_null_rows="
        f"{result.diagnostics.signed_flow_imbalance_null_rows}"
    )
    print(f"signed_flow_feature_path={result.paths.signed_flow_feature_path}")
    print(f"manifest_path={result.paths.manifest_path}")


if __name__ == "__main__":
    main()
