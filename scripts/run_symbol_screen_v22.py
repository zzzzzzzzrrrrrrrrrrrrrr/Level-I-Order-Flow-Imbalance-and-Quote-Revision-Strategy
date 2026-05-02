"""Command-line entry point for v2.2 symbol screening diagnostics."""

from __future__ import annotations

import argparse
from pathlib import Path
import shutil

import yaml

from level1_ofi_qr.diagnostics.symbol_screening_v22 import (
    SymbolScreenV22Config,
    build_symbol_screen_v22,
    build_symbol_screen_v22_for_data_configs,
)
from level1_ofi_qr.utils import load_data_slice_config


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run validation-only v2.2 cross-symbol cost-aware screening diagnostics."
    )
    parser.add_argument("config", help="Path to a data-slice YAML config or v2.2 screen config.")
    parser.add_argument("--processed-dir")
    parser.add_argument("--experiment-dir")
    parser.add_argument("--tables-dir")
    parser.add_argument("--figures-dir")
    parser.add_argument("--universe-name")
    parser.add_argument("--horizons")
    parser.add_argument("--decile-horizons")
    parser.add_argument("--validation-min-dates", type=int)
    parser.add_argument(
        "--candidate-source",
        choices=("auto", "v21", "predictions"),
        default="auto",
    )
    args = parser.parse_args()

    raw_config = _load_raw_yaml(args.config)
    if "data_slices" in raw_config:
        screening = raw_config.get("screening", {})
        screen_name = str(raw_config.get("screen_name", "v22_symbol_screen"))
        experiment_root = _experiment_root(args.experiment_dir, screen_name=screen_name)
        tables_dir = args.tables_dir or str(experiment_root / "tables")
        figures_dir = args.figures_dir or str(experiment_root / "figures")
        universe_name = args.universe_name or raw_config.get("universe", {}).get(
            "name",
            "configured_universe",
        )
        data_configs = tuple(
            load_data_slice_config(item["config"]) for item in raw_config["data_slices"]
        )
        result = build_symbol_screen_v22_for_data_configs(
            data_configs,
            processed_dir=args.processed_dir,
            tables_dir=tables_dir,
            figures_dir=figures_dir,
            screening_config=SymbolScreenV22Config(
                universe_name=universe_name,
                horizons=_grid_from_args_or_config(
                    args.horizons,
                    screening,
                    "horizons",
                    ("1s", "5s", "10s", "30s", "60s"),
                ),
                decile_horizons=_grid_from_args_or_config(
                    args.decile_horizons,
                    screening,
                    "decile_horizons",
                    ("1s", "5s"),
                ),
                validation_min_dates=args.validation_min_dates
                if args.validation_min_dates is not None
                else int(screening.get("validation_min_dates", 2)),
                candidate_source=args.candidate_source,
            ),
        )
        _write_experiment_metadata(
            experiment_root,
            config_path=Path(args.config),
            raw_config=raw_config,
        )
    else:
        config = load_data_slice_config(args.config)
        screen_name = f"{config.slice_name}_v22_symbol_screen"
        experiment_root = _experiment_root(args.experiment_dir, screen_name=screen_name)
        tables_dir = args.tables_dir or (
            str(experiment_root / "tables") if args.experiment_dir else "outputs/tables"
        )
        figures_dir = args.figures_dir or (
            str(experiment_root / "figures") if args.experiment_dir else "outputs/figures"
        )
        result = build_symbol_screen_v22(
            config,
            processed_dir=args.processed_dir,
            tables_dir=tables_dir,
            figures_dir=figures_dir,
            screening_config=SymbolScreenV22Config(
                universe_name=args.universe_name or "configured_universe",
                horizons=_parse_grid(args.horizons or "1s,5s,10s,30s,60s"),
                decile_horizons=_parse_grid(args.decile_horizons or "1s,5s"),
                validation_min_dates=args.validation_min_dates or 2,
                candidate_source=args.candidate_source,
            ),
        )
        if args.experiment_dir:
            _write_experiment_metadata(
                experiment_root,
                config_path=Path(args.config),
                raw_config=raw_config,
            )

    print(f"summary_csv_path={result.paths.summary_csv_path}")
    print(f"deciles_csv_path={result.paths.deciles_csv_path}")
    print(f"horizon_sweep_csv_path={result.paths.horizon_sweep_csv_path}")
    print(f"manifest_path={result.paths.manifest_path}")
    print(f"move_over_cost_svg_path={result.paths.move_over_cost_svg_path}")
    print(f"decile_markout_svg_path={result.paths.decile_markout_svg_path}")
    print(f"summary_rows={len(result.tables.summary)}")
    print(f"decile_rows={len(result.tables.deciles)}")
    print(f"horizon_sweep_rows={len(result.tables.horizon_sweep)}")
    if not result.tables.summary.empty:
        for row in result.tables.summary.to_dict(orient="records"):
            print(
                f"symbol={row['symbol']} "
                f"top1_move_over_cost={row['top_1pct_move_over_cost']} "
                f"validation_pass={row['validation_pass_flag']} "
                f"adverse_selection={row['adverse_selection_flag']}"
            )


def _parse_grid(raw: str) -> tuple[str, ...]:
    values = tuple(part.strip() for part in raw.split(",") if part.strip())
    if not values:
        raise ValueError("Grid must not be empty.")
    return values


def _grid_from_args_or_config(
    raw: str | None,
    config: object,
    key: str,
    default: tuple[str, ...],
) -> tuple[str, ...]:
    if raw:
        return _parse_grid(raw)
    if isinstance(config, dict) and key in config:
        values = config[key]
        if isinstance(values, str):
            return _parse_grid(values)
        return tuple(str(value) for value in values)
    return default


def _load_raw_yaml(path: str) -> dict[str, object]:
    with Path(path).open("r", encoding="utf-8") as handle:
        loaded = yaml.safe_load(handle)
    if not isinstance(loaded, dict):
        raise ValueError(f"Config must be a YAML mapping: {path}")
    return loaded


def _experiment_root(raw: str | None, *, screen_name: str) -> Path:
    if raw:
        return Path(raw)
    return Path("outputs/experiments") / screen_name


def _write_experiment_metadata(
    root: Path,
    *,
    config_path: Path,
    raw_config: dict[str, object],
) -> None:
    root.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(config_path, root / "config.yaml")
    notes = [
        "# V2.2 Symbol Screen Experiment",
        "",
        f"Source config: `{config_path}`",
        "",
        "This directory contains one experiment-scoped v2.2 symbol screening run.",
        "Symbol-level pipeline artifacts remain under `data/processed/<slice_name>/`.",
        "Cross-symbol comparison artifacts live here under `tables/` and `figures/`.",
        "",
    ]
    universe = raw_config.get("universe")
    if isinstance(universe, dict):
        notes.append(f"Universe name: `{universe.get('name', 'configured_universe')}`")
        symbols = universe.get("symbols", [])
        if symbols:
            notes.append("")
            notes.append("Configured symbols:")
            notes.extend(f"- `{symbol}`" for symbol in symbols)
    data_slices = raw_config.get("data_slices")
    if data_slices:
        notes.append("")
        notes.append("Available processed data slices in this run:")
        for item in data_slices:
            if isinstance(item, dict):
                notes.append(f"- `{item.get('symbol')}`: `{item.get('config')}`")
    (root / "notes.md").write_text("\n".join(notes) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
