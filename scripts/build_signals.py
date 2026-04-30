"""Command-line entry point for signal v1 generation."""

from __future__ import annotations

import argparse

from level1_ofi_qr.signals import (
    DEFAULT_SIGNED_FLOW_COLUMN,
    SignalRuleConfig,
    build_signal_dataset,
)
from level1_ofi_qr.utils import load_data_slice_config


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build interpretable sequential-gate signal v1 rows."
    )
    parser.add_argument("config", help="Path to a data-slice YAML config.")
    parser.add_argument(
        "--processed-dir",
        help=(
            "Root directory containing <slice_name> labeled feature files. "
            "Defaults to the processed_dir in the config."
        ),
    )
    parser.add_argument(
        "--output-dir",
        help=(
            "Root directory for signal outputs. Defaults to --processed-dir "
            "when provided, otherwise the processed_dir in the config."
        ),
    )
    parser.add_argument(
        "--signed-flow-column",
        default=DEFAULT_SIGNED_FLOW_COLUMN,
        help="Signed-flow feature column used by the sequential gate.",
    )
    parser.add_argument("--qi-threshold", type=float, default=0.0)
    parser.add_argument("--signed-flow-threshold", type=float, default=0.0)
    parser.add_argument("--qr-threshold-bps", type=float, default=0.0)
    args = parser.parse_args()

    config = load_data_slice_config(args.config)
    signal_config = SignalRuleConfig(
        signed_flow_column=args.signed_flow_column,
        qi_threshold=args.qi_threshold,
        signed_flow_threshold=args.signed_flow_threshold,
        qr_threshold_bps=args.qr_threshold_bps,
    )
    result = build_signal_dataset(
        config,
        processed_dir=args.processed_dir,
        output_dir=args.output_dir,
        signal_config=signal_config,
    )

    print(f"input_feature_rows={result.diagnostics.input_feature_rows}")
    print(f"input_quote_rows={result.diagnostics.input_quote_rows}")
    print(f"output_signal_rows={result.diagnostics.output_signal_rows}")
    print(f"row_preserving={result.diagnostics.row_preserving}")
    print(f"signal_rule={result.diagnostics.signal_rule}")
    print(f"signed_flow_column={result.diagnostics.signed_flow_column}")
    print(f"qi_threshold={result.diagnostics.qi_threshold}")
    print(f"signed_flow_threshold={result.diagnostics.signed_flow_threshold}")
    print(f"qr_threshold_bps={result.diagnostics.qr_threshold_bps}")
    print(f"threshold_selection_policy={result.diagnostics.threshold_selection_policy}")
    print(f"label_usage_policy={result.diagnostics.label_usage_policy}")
    print(f"labels_used_for_signal={result.diagnostics.labels_used_for_signal}")
    print(f"signal_input_available_rows={result.diagnostics.signal_input_available_rows}")
    print(f"signal_input_missing_rows={result.diagnostics.signal_input_missing_rows}")
    print(f"long_signal_rows={result.diagnostics.long_signal_rows}")
    print(f"short_signal_rows={result.diagnostics.short_signal_rows}")
    print(f"no_trade_rows={result.diagnostics.no_trade_rows}")
    print(f"signal_path={result.paths.signal_path}")
    print(f"manifest_path={result.paths.manifest_path}")


if __name__ == "__main__":
    main()
