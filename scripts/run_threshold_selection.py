"""Command-line entry point for threshold selection v1."""

from __future__ import annotations

import argparse

from level1_ofi_qr.evaluation import ThresholdSelectionConfig, build_threshold_selection
from level1_ofi_qr.labeling import DEFAULT_LABEL_HORIZONS
from level1_ofi_qr.signals import DEFAULT_SIGNED_FLOW_COLUMN
from level1_ofi_qr.utils import load_data_slice_config


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run train-window threshold selection for sequential-gate signals."
    )
    parser.add_argument("config", help="Path to a data-slice YAML config.")
    parser.add_argument("--processed-dir")
    parser.add_argument("--output-dir")
    parser.add_argument("--horizon", action="append", dest="horizons")
    parser.add_argument("--min-train-dates", type=int, default=1)
    parser.add_argument("--min-train-signals", type=int, default=100)
    parser.add_argument("--signed-flow-column", default=DEFAULT_SIGNED_FLOW_COLUMN)
    parser.add_argument("--qi-grid", default="0,0.1,0.25")
    parser.add_argument("--signed-flow-grid", default="0,0.1,0.25")
    parser.add_argument("--qr-grid", default="0,0.1,0.25")
    args = parser.parse_args()

    config = load_data_slice_config(args.config)
    selection_config = ThresholdSelectionConfig(
        horizons=tuple(args.horizons) if args.horizons else DEFAULT_LABEL_HORIZONS,
        min_train_dates=args.min_train_dates,
        signed_flow_column=args.signed_flow_column,
        qi_threshold_grid=_parse_float_grid(args.qi_grid),
        signed_flow_threshold_grid=_parse_float_grid(args.signed_flow_grid),
        qr_threshold_bps_grid=_parse_float_grid(args.qr_grid),
        min_train_signals=args.min_train_signals,
    )
    result = build_threshold_selection(
        config,
        processed_dir=args.processed_dir,
        output_dir=args.output_dir,
        selection_config=selection_config,
    )

    print(f"input_signal_rows={result.diagnostics.input_signal_rows}")
    print(f"output_summary_rows={result.diagnostics.output_summary_rows}")
    print(f"horizons={list(result.diagnostics.horizons)}")
    print(f"trading_dates={list(result.diagnostics.trading_dates)}")
    print(f"fold_count={result.diagnostics.fold_count}")
    print(f"threshold_selection_policy={result.diagnostics.threshold_selection_policy}")
    print(f"threshold_objective={result.diagnostics.threshold_objective}")
    print(f"min_train_signals={result.diagnostics.min_train_signals}")
    print(f"summary_csv_path={result.paths.summary_csv_path}")
    print(f"manifest_path={result.paths.manifest_path}")
    for row in result.summary.to_dict(orient="records"):
        print(
            f"{row['fold_id']} horizon={row['horizon']} "
            f"selected_qi={row['selected_qi_threshold']} "
            f"selected_signed_flow={row['selected_signed_flow_threshold']} "
            f"selected_qr={row['selected_qr_threshold_bps']} "
            f"train_mean_aligned_bps={row['train_mean_signal_aligned_return_bps']} "
            f"test_mean_aligned_bps={row['test_mean_signal_aligned_return_bps']} "
            f"test_accuracy={row['test_signal_accuracy']}"
        )


def _parse_float_grid(raw: str) -> tuple[float, ...]:
    values = tuple(float(part.strip()) for part in raw.split(",") if part.strip())
    if not values:
        raise ValueError("Threshold grid must not be empty.")
    return values


if __name__ == "__main__":
    main()
