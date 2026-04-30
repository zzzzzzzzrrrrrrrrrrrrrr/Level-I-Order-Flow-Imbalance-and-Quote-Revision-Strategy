"""Command-line entry point for train-validation-test parameter selection v1."""

from __future__ import annotations

import argparse

from level1_ofi_qr.evaluation import (
    DEFAULT_COOLDOWN_GRID,
    DEFAULT_MAX_POSITION_GRID,
    DEFAULT_MAX_TRADES_PER_DAY_GRID,
    DEFAULT_SENSITIVITY_FIXED_BPS_GRID,
    DEFAULT_SLIPPAGE_TICKS_GRID,
    TVTParameterSelectionConfig,
    build_tvt_parameter_selection,
)
from level1_ofi_qr.signals import SEQUENTIAL_GATE_SIGNAL
from level1_ofi_qr.utils import load_data_slice_config


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run train-validation-test target-position parameter selection."
    )
    parser.add_argument("config", help="Path to a data-slice YAML config.")
    parser.add_argument("--processed-dir")
    parser.add_argument("--output-dir")
    parser.add_argument("--signal-column", default=SEQUENTIAL_GATE_SIGNAL)
    parser.add_argument("--min-train-dates", type=int, default=1)
    parser.add_argument("--max-position-grid", default=_format_grid(DEFAULT_MAX_POSITION_GRID))
    parser.add_argument("--cooldown-grid", default=",".join(DEFAULT_COOLDOWN_GRID))
    parser.add_argument(
        "--max-trades-per-day-grid",
        default=_format_optional_int_grid(DEFAULT_MAX_TRADES_PER_DAY_GRID),
    )
    parser.add_argument(
        "--fixed-bps-grid",
        default=_format_grid(DEFAULT_SENSITIVITY_FIXED_BPS_GRID),
    )
    parser.add_argument(
        "--slippage-ticks-grid",
        default=_format_grid(DEFAULT_SLIPPAGE_TICKS_GRID),
    )
    parser.add_argument("--tick-size", type=float, default=0.01)
    parser.add_argument("--hold-on-no-signal", action="store_true")
    parser.add_argument("--no-eod-flat", action="store_true")
    args = parser.parse_args()

    config = load_data_slice_config(args.config)
    selection_config = TVTParameterSelectionConfig(
        signal_column=args.signal_column,
        min_train_dates=args.min_train_dates,
        max_position_grid=_parse_float_grid(args.max_position_grid),
        cooldown_grid=_parse_string_grid(args.cooldown_grid),
        max_trades_per_day_grid=_parse_optional_int_grid(args.max_trades_per_day_grid),
        fixed_bps_grid=_parse_float_grid(args.fixed_bps_grid),
        slippage_ticks_grid=_parse_float_grid(args.slippage_ticks_grid),
        tick_size=args.tick_size,
        flat_on_no_signal=not args.hold_on_no_signal,
        eod_flat=not args.no_eod_flat,
    )
    result = build_tvt_parameter_selection(
        config,
        processed_dir=args.processed_dir,
        output_dir=args.output_dir,
        selection_config=selection_config,
    )

    diagnostics = result.diagnostics
    print(f"input_signal_rows={diagnostics.input_signal_rows}")
    print(f"trading_dates={list(diagnostics.trading_dates)}")
    print(f"fold_count={diagnostics.fold_count}")
    print(f"candidate_count={diagnostics.candidate_count}")
    print(f"output_summary_rows={diagnostics.output_summary_rows}")
    print(f"split_policy={diagnostics.split_policy}")
    print(f"selection_policy={diagnostics.selection_policy}")
    print(f"objective={diagnostics.objective}")
    print(f"test_used_for_selection={diagnostics.test_used_for_selection}")
    print(f"summary_csv_path={result.paths.summary_csv_path}")
    print(f"manifest_path={result.paths.manifest_path}")
    selected = result.summary.loc[result.summary["selected_for_test"] == True]
    for row in selected.to_dict(orient="records"):
        print(
            f"{row['fold_id']} selected={row['candidate_id']} "
            f"train={row['train_start_date']}..{row['train_end_date']} "
            f"validation={row['validation_date']} test={row['test_date']} "
            f"validation_final_equity={row['validation_final_equity']} "
            f"test_final_equity={row.get('test_final_equity')}"
        )


def _parse_float_grid(raw: str) -> tuple[float, ...]:
    values = tuple(float(part.strip()) for part in raw.split(",") if part.strip())
    if not values:
        raise ValueError("Float grid must not be empty.")
    return values


def _parse_string_grid(raw: str) -> tuple[str, ...]:
    values = tuple(part.strip() for part in raw.split(",") if part.strip())
    if not values:
        raise ValueError("String grid must not be empty.")
    return values


def _parse_optional_int_grid(raw: str) -> tuple[int | None, ...]:
    values: list[int | None] = []
    for part in raw.split(","):
        token = part.strip().lower()
        if not token:
            continue
        if token in {"none", "null"}:
            values.append(None)
        else:
            values.append(int(token))
    if not values:
        raise ValueError("Optional int grid must not be empty.")
    return tuple(values)


def _format_grid(values: tuple[float, ...]) -> str:
    return ",".join(str(value) for value in values)


def _format_optional_int_grid(values: tuple[int | None, ...]) -> str:
    return ",".join("none" if value is None else str(value) for value in values)


if __name__ == "__main__":
    main()
