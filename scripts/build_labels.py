"""Command-line entry point for future midquote label generation."""

from __future__ import annotations

import argparse

from level1_ofi_qr.labeling import DEFAULT_LABEL_HORIZONS, build_midquote_label_dataset
from level1_ofi_qr.utils import load_data_slice_config


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build future midquote labels for signed-flow feature rows."
    )
    parser.add_argument("config", help="Path to a data-slice YAML config.")
    parser.add_argument(
        "--processed-dir",
        help=(
            "Root directory containing <slice_name> feature files. "
            "Defaults to the processed_dir in the config."
        ),
    )
    parser.add_argument(
        "--output-dir",
        help=(
            "Root directory for labeled outputs. Defaults to --processed-dir "
            "when provided, otherwise the processed_dir in the config."
        ),
    )
    parser.add_argument(
        "--horizon",
        action="append",
        dest="horizons",
        help=(
            "Label horizon such as 100ms, 500ms, 1s, or 5s. "
            "Can be provided multiple times. Defaults to 100ms, 500ms, 1s, 5s."
        ),
    )
    parser.add_argument(
        "--dead-zone-bps",
        type=float,
        default=0.0,
        help="Return threshold in basis points for assigning flat direction labels.",
    )
    args = parser.parse_args()

    config = load_data_slice_config(args.config)
    horizons = tuple(args.horizons) if args.horizons else DEFAULT_LABEL_HORIZONS
    result = build_midquote_label_dataset(
        config,
        processed_dir=args.processed_dir,
        output_dir=args.output_dir,
        horizons=horizons,
        dead_zone_bps=args.dead_zone_bps,
    )

    print(f"input_feature_rows={result.diagnostics.input_feature_rows}")
    print(f"input_quote_rows={result.diagnostics.input_quote_rows}")
    print(f"output_labeled_rows={result.diagnostics.output_labeled_rows}")
    print(f"row_preserving={result.diagnostics.row_preserving}")
    print(f"horizons={list(result.diagnostics.horizons)}")
    print(f"dead_zone_bps={result.diagnostics.dead_zone_bps}")
    print(f"label_group_keys={list(result.diagnostics.label_group_keys)}")
    print(f"current_quote_policy={result.diagnostics.current_quote_policy}")
    print(f"future_quote_policy={result.diagnostics.future_quote_policy}")
    print(f"session_boundary_policy={result.diagnostics.session_boundary_policy}")
    print(f"label_usage_policy={result.diagnostics.label_usage_policy}")
    print(f"current_midquote_missing_rows={result.diagnostics.current_midquote_missing_rows}")
    print(f"label_available_rows={result.diagnostics.label_available_rows}")
    print(f"label_missing_rows={result.diagnostics.label_missing_rows}")
    print(f"positive_direction_rows={result.diagnostics.positive_direction_rows}")
    print(f"flat_direction_rows={result.diagnostics.flat_direction_rows}")
    print(f"negative_direction_rows={result.diagnostics.negative_direction_rows}")
    print(f"labeled_feature_path={result.paths.labeled_feature_path}")
    print(f"manifest_path={result.paths.manifest_path}")


if __name__ == "__main__":
    main()
