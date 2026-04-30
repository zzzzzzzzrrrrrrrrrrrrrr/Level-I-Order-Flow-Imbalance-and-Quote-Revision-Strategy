"""Command-line entry point for quote-only feature generation."""

from __future__ import annotations

import argparse

from level1_ofi_qr.features import build_quote_feature_dataset
from level1_ofi_qr.utils import load_data_slice_config


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build quote-only Level-I features from cleaned quote data."
    )
    parser.add_argument("config", help="Path to a data-slice YAML config.")
    parser.add_argument(
        "--processed-dir",
        help=(
            "Root directory containing <slice_name> cleaned quote files. "
            "Defaults to the processed_dir in the config."
        ),
    )
    parser.add_argument(
        "--output-dir",
        help=(
            "Root directory for quote-feature outputs. Defaults to --processed-dir "
            "when provided, otherwise the processed_dir in the config."
        ),
    )
    args = parser.parse_args()

    config = load_data_slice_config(args.config)
    result = build_quote_feature_dataset(
        config,
        processed_dir=args.processed_dir,
        output_dir=args.output_dir,
    )

    print(f"input_quote_rows={result.diagnostics.input_quote_rows}")
    print(f"output_feature_rows={result.diagnostics.output_feature_rows}")
    print(f"feature_columns={list(result.diagnostics.feature_columns)}")
    print(f"feature_group_keys={list(result.diagnostics.feature_group_keys)}")
    print(f"trading_date_count={result.diagnostics.trading_date_count}")
    print(f"quote_imbalance_null_rows={result.diagnostics.quote_imbalance_null_rows}")
    print(f"quote_revision_null_rows={result.diagnostics.quote_revision_null_rows}")
    print(f"quote_revision_bps_null_rows={result.diagnostics.quote_revision_bps_null_rows}")
    print(f"max_abs_quote_revision_bps={result.diagnostics.max_abs_quote_revision_bps}")
    print(f"quote_feature_path={result.paths.quote_feature_path}")
    print(f"manifest_path={result.paths.manifest_path}")


if __name__ == "__main__":
    main()
