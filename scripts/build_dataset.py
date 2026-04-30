"""Command-line entry point for dataset build workflows."""

from __future__ import annotations

import argparse

from level1_ofi_qr.datasets import build_dataset_from_wrds_raw
from level1_ofi_qr.utils import load_data_slice_config


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build normalized and cleaned datasets from raw WRDS CSV outputs."
    )
    parser.add_argument("config", help="Path to a data-slice YAML config.")
    parser.add_argument(
        "--raw-dir",
        help=(
            "Directory containing <slice_name>_quotes_raw.csv and "
            "<slice_name>_trades_raw.csv. Defaults to data/raw/<slice_name> "
            "when present, otherwise data/raw."
        ),
    )
    parser.add_argument("--interim-dir", help="Root directory for normalized interim outputs.")
    parser.add_argument("--processed-dir", help="Root directory for cleaned processed outputs.")
    args = parser.parse_args()

    config = load_data_slice_config(args.config)
    result = build_dataset_from_wrds_raw(
        config,
        raw_dir=args.raw_dir,
        interim_dir=args.interim_dir,
        processed_dir=args.processed_dir,
    )

    print(f"raw_quote_rows={result.diagnostics.raw_quote_rows}")
    print(f"raw_trade_rows={result.diagnostics.raw_trade_rows}")
    print(f"normalized_quote_rows={result.diagnostics.normalized_quote_rows}")
    print(f"normalized_trade_rows={result.diagnostics.normalized_trade_rows}")
    print(f"scoped_quote_rows={result.diagnostics.scoped_quote_rows}")
    print(f"scoped_trade_rows={result.diagnostics.scoped_trade_rows}")
    print(f"cleaned_quote_rows={result.diagnostics.cleaned_quote_rows}")
    print(f"cleaned_trade_rows={result.diagnostics.cleaned_trade_rows}")
    print(f"rejected_quote_rows={result.diagnostics.rejected_quote_rows}")
    print(f"rejected_trade_rows={result.diagnostics.rejected_trade_rows}")
    print(f"normalized_quote_path={result.paths.normalized_quote_path}")
    print(f"normalized_trade_path={result.paths.normalized_trade_path}")
    print(f"cleaned_quote_path={result.paths.cleaned_quote_path}")
    print(f"cleaned_trade_path={result.paths.cleaned_trade_path}")
    print(f"rejected_quote_path={result.paths.rejected_quote_path}")
    print(f"rejected_trade_path={result.paths.rejected_trade_path}")
    print(f"manifest_path={result.paths.manifest_path}")


if __name__ == "__main__":
    main()
