"""Command-line entry point for walk-forward statistical evaluation."""

from __future__ import annotations

import argparse

from level1_ofi_qr.evaluation import WalkForwardConfig, build_walk_forward_evaluation
from level1_ofi_qr.labeling import DEFAULT_LABEL_HORIZONS
from level1_ofi_qr.signals import SEQUENTIAL_GATE_SIGNAL
from level1_ofi_qr.utils import load_data_slice_config

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run walk-forward statistical evaluation for signal v1 rows."
    )
    parser.add_argument("config", help="Path to a data-slice YAML config.")
    parser.add_argument(
        "--processed-dir",
        help=(
            "Root directory containing <slice_name> signal files. "
            "Defaults to the processed_dir in the config."
        ),
    )
    parser.add_argument(
        "--output-dir",
        help=(
            "Root directory for evaluation outputs. Defaults to --processed-dir "
            "when provided, otherwise the processed_dir in the config."
        ),
    )
    parser.add_argument(
        "--horizon",
        action="append",
        dest="horizons",
        help=(
            "Evaluation horizon such as 100ms, 500ms, 1s, or 5s. "
            "Can be provided multiple times. Defaults to labeling v1 horizons."
        ),
    )
    parser.add_argument("--min-train-dates", type=int, default=1)
    parser.add_argument("--signal-column", default=SEQUENTIAL_GATE_SIGNAL)
    args = parser.parse_args()

    config = load_data_slice_config(args.config)
    evaluation_config = WalkForwardConfig(
        horizons=tuple(args.horizons) if args.horizons else DEFAULT_LABEL_HORIZONS,
        min_train_dates=args.min_train_dates,
        signal_column=args.signal_column,
    )
    result = build_walk_forward_evaluation(
        config,
        processed_dir=args.processed_dir,
        output_dir=args.output_dir,
        evaluation_config=evaluation_config,
    )

    print(f"input_signal_rows={result.diagnostics.input_signal_rows}")
    print(f"output_summary_rows={result.diagnostics.output_summary_rows}")
    print(f"horizons={list(result.diagnostics.horizons)}")
    print(f"trading_dates={list(result.diagnostics.trading_dates)}")
    print(f"fold_count={result.diagnostics.fold_count}")
    print(f"min_train_dates={result.diagnostics.min_train_dates}")
    print(f"signal_column={result.diagnostics.signal_column}")
    print(f"evaluation_policy={result.diagnostics.evaluation_policy}")
    print(f"signal_usage_policy={result.diagnostics.signal_usage_policy}")
    print(f"label_usage_policy={result.diagnostics.label_usage_policy}")
    print(f"summary_csv_path={result.paths.summary_csv_path}")
    print(f"manifest_path={result.paths.manifest_path}")
    aggregate = result.summary.loc[result.summary["fold_id"] == "ALL_EVALUATION_FOLDS"]
    for row in aggregate.to_dict(orient="records"):
        print(
            "aggregate "
            f"horizon={row['horizon']} "
            f"evaluated_signal_rows={row['evaluated_signal_rows']} "
            f"signal_accuracy={row['signal_accuracy']} "
            f"mean_signal_aligned_return_bps={row['mean_signal_aligned_return_bps']}"
        )


if __name__ == "__main__":
    main()
