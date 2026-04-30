"""Command-line entry point for model training v1."""

from __future__ import annotations

import argparse

from level1_ofi_qr.models import (
    DEFAULT_LABEL_HORIZON,
    DEFAULT_SCORE_THRESHOLDS,
    ModelTrainingV1Config,
    build_model_training_v1,
)
from level1_ofi_qr.utils import load_data_slice_config


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Train AAPL prototype model candidates on train dates, select on "
            "validation, and run held-out test accounting."
        )
    )
    parser.add_argument("config", help="Path to a data-slice YAML config.")
    parser.add_argument("--processed-dir")
    parser.add_argument("--output-dir")
    parser.add_argument("--label-horizon", default=DEFAULT_LABEL_HORIZON)
    parser.add_argument("--score-threshold-grid", default=_format_grid(DEFAULT_SCORE_THRESHOLDS))
    parser.add_argument("--min-train-dates", type=int, default=1)
    parser.add_argument("--max-position", type=float, default=1.0)
    parser.add_argument("--cooldown", default="0ms")
    parser.add_argument("--max-trades-per-day", type=int)
    parser.add_argument("--fixed-bps", type=float, default=0.0)
    parser.add_argument("--slippage-ticks", type=float, default=0.0)
    parser.add_argument("--tick-size", type=float, default=0.01)
    parser.add_argument("--min-validation-orders", type=int, default=1000)
    args = parser.parse_args()

    config = load_data_slice_config(args.config)
    result = build_model_training_v1(
        config,
        processed_dir=args.processed_dir,
        output_dir=args.output_dir,
        model_config=ModelTrainingV1Config(
            min_train_dates=args.min_train_dates,
            label_horizon=args.label_horizon,
            score_threshold_grid=_parse_float_grid(args.score_threshold_grid),
            max_position=args.max_position,
            cooldown=args.cooldown,
            max_trades_per_day=args.max_trades_per_day,
            fixed_bps=args.fixed_bps,
            slippage_ticks=args.slippage_ticks,
            tick_size=args.tick_size,
            min_validation_orders=args.min_validation_orders,
        ),
    )

    diagnostics = result.diagnostics
    print(f"input_signal_rows={diagnostics.input_signal_rows}")
    print(f"trading_dates={list(diagnostics.trading_dates)}")
    print(f"fold_count={diagnostics.fold_count}")
    print(f"candidate_count_per_fold={diagnostics.candidate_count_per_fold}")
    print(f"output_candidate_rows={diagnostics.output_candidate_rows}")
    print(f"output_prediction_rows={diagnostics.output_prediction_rows}")
    print(f"output_order_rows={diagnostics.output_order_rows}")
    print(f"output_summary_rows={diagnostics.output_summary_rows}")
    print(f"label_horizon={diagnostics.label_horizon}")
    print(f"test_used_for_selection={diagnostics.test_used_for_selection}")
    print(f"research_grade_backtest={diagnostics.research_grade_backtest}")
    print(f"predictions_csv_path={result.paths.predictions_csv_path}")
    print(f"candidates_csv_path={result.paths.candidates_csv_path}")
    print(f"backtest_orders_csv_path={result.paths.backtest_orders_csv_path}")
    print(f"backtest_ledger_csv_path={result.paths.backtest_ledger_csv_path}")
    print(f"backtest_summary_csv_path={result.paths.backtest_summary_csv_path}")
    print(f"manifest_path={result.paths.manifest_path}")

    for row in result.summary.to_dict(orient="records"):
        print(
            f"{row['fold_id']} selected={row['candidate_id']} "
            f"feature_set={row['feature_set']} "
            f"threshold={row['score_threshold']} "
            f"test={row['test_date']} orders={row['order_rows']} "
            f"total_cost={row['total_cost']} final_equity={row['final_equity']}"
        )


def _parse_float_grid(raw: str) -> tuple[float, ...]:
    values = tuple(float(part.strip()) for part in raw.split(",") if part.strip())
    if not values:
        raise ValueError("Grid must not be empty.")
    return values


def _format_grid(values: tuple[float, ...]) -> str:
    return ",".join(str(value) for value in values)


if __name__ == "__main__":
    main()
