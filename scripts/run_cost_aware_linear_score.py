"""Command-line entry point for the cost-aware linear-score variant."""

from __future__ import annotations

import argparse

from level1_ofi_qr.models import (
    DEFAULT_COST_AWARE_COOLDOWN_SECONDS_GRID,
    DEFAULT_COST_AWARE_QUANTILE_TOP_FRACTIONS,
    DEFAULT_COST_AWARE_SCORE_THRESHOLDS,
    DEFAULT_COST_MULTIPLIER_GRID,
    DEFAULT_LABEL_HORIZON,
    DEFAULT_MIN_HOLDING_SECONDS_GRID,
    CostAwareLinearScoreConfig,
    build_cost_aware_linear_score_v1,
)
from level1_ofi_qr.utils import load_data_slice_config


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Run the cost-aware linear-score strategy variant using the existing "
            "linear model score as the base score."
        )
    )
    parser.add_argument("config", help="Path to a data-slice YAML config.")
    parser.add_argument("--processed-dir")
    parser.add_argument("--output-dir")
    parser.add_argument("--label-horizon", default=DEFAULT_LABEL_HORIZON)
    parser.add_argument(
        "--score-threshold-grid",
        default=_format_grid(DEFAULT_COST_AWARE_SCORE_THRESHOLDS),
    )
    parser.add_argument(
        "--quantile-top-fractions",
        default=_format_grid(DEFAULT_COST_AWARE_QUANTILE_TOP_FRACTIONS),
    )
    parser.add_argument("--disable-quantile-thresholds", action="store_true")
    parser.add_argument(
        "--cost-multiplier-grid",
        default=_format_grid(DEFAULT_COST_MULTIPLIER_GRID),
    )
    parser.add_argument(
        "--cooldown-seconds-grid",
        default=_format_grid(DEFAULT_COST_AWARE_COOLDOWN_SECONDS_GRID),
    )
    parser.add_argument(
        "--min-holding-seconds-grid",
        default=_format_grid(DEFAULT_MIN_HOLDING_SECONDS_GRID),
    )
    parser.add_argument("--min-train-dates", type=int, default=1)
    parser.add_argument("--max-position", type=float, default=1.0)
    parser.add_argument("--max-trades-per-day", type=int)
    parser.add_argument("--fixed-bps", type=float, default=0.0)
    parser.add_argument("--slippage-ticks", type=float, default=0.0)
    parser.add_argument("--tick-size", type=float, default=0.01)
    parser.add_argument("--min-validation-trades", type=int, default=1)
    args = parser.parse_args()

    config = load_data_slice_config(args.config)
    result = build_cost_aware_linear_score_v1(
        config,
        processed_dir=args.processed_dir,
        output_dir=args.output_dir,
        cost_aware_config=CostAwareLinearScoreConfig(
            min_train_dates=args.min_train_dates,
            label_horizon=args.label_horizon,
            score_threshold_grid=_parse_float_grid(args.score_threshold_grid),
            include_quantile_thresholds=not args.disable_quantile_thresholds,
            quantile_top_fractions=_parse_float_grid(args.quantile_top_fractions),
            cost_multiplier_grid=_parse_float_grid(args.cost_multiplier_grid),
            cooldown_seconds_grid=_parse_int_grid(args.cooldown_seconds_grid),
            min_holding_seconds_grid=_parse_int_grid(args.min_holding_seconds_grid),
            max_position=args.max_position,
            max_trades_per_day=args.max_trades_per_day,
            fixed_bps=args.fixed_bps,
            slippage_ticks=args.slippage_ticks,
            tick_size=args.tick_size,
            min_validation_trades=args.min_validation_trades,
        ),
    )

    diagnostics = result.diagnostics
    print(f"strategy_variant={diagnostics.strategy_variant}")
    print(f"base_score_column={diagnostics.base_score_column}")
    print(f"fold_count={diagnostics.fold_count}")
    print(f"candidate_count_per_fold={diagnostics.candidate_count_per_fold}")
    print(f"output_candidate_rows={diagnostics.output_candidate_rows}")
    print(f"output_prediction_rows={diagnostics.output_prediction_rows}")
    print(f"output_order_rows={diagnostics.output_order_rows}")
    print(f"output_summary_rows={diagnostics.output_summary_rows}")
    print(f"selection_uses_net_pnl={diagnostics.selection_uses_net_pnl}")
    print(f"predictions_csv_path={result.paths.predictions_csv_path}")
    print(f"candidates_csv_path={result.paths.candidates_csv_path}")
    print(f"backtest_ledger_csv_path={result.paths.backtest_ledger_csv_path}")
    print(f"backtest_summary_csv_path={result.paths.backtest_summary_csv_path}")
    print(f"report_csv_path={result.paths.report_csv_path}")
    print(f"manifest_path={result.paths.manifest_path}")

    for row in result.report.to_dict(orient="records"):
        print(
            f"{row['strategy']} net_pnl={row['net_pnl']} cost={row['cost']} "
            f"num_trades={row['num_trades']} thresholds={row['selected_threshold_by_fold']} "
            f"cost_multipliers={row['selected_cost_multiplier_by_fold']}"
        )


def _parse_float_grid(raw: str) -> tuple[float, ...]:
    values = tuple(float(part.strip()) for part in raw.split(",") if part.strip())
    if not values:
        raise ValueError("Grid must not be empty.")
    return values


def _parse_int_grid(raw: str) -> tuple[int, ...]:
    values = tuple(int(part.strip()) for part in raw.split(",") if part.strip())
    if not values:
        raise ValueError("Grid must not be empty.")
    return values


def _format_grid(values: tuple[float, ...] | tuple[int, ...]) -> str:
    return ",".join(str(value) for value in values)


if __name__ == "__main__":
    main()
