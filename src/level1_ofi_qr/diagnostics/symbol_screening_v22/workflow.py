"""Workflow and table builders for v2.2 symbol screening diagnostics."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import json
from pathlib import Path

import numpy as np
import pandas as pd

from ...alignment import TRADING_DATE
from ...features.quotes import MIDQUOTE, QUOTED_SPREAD
from ...models import COST_AWARE_LINEAR_SCORE_STRATEGY
from ...schema import EVENT_TIME, SYMBOL
from ...utils import DataSliceConfig
from ..figures import _bar_svg
from ..microstructure_v21.workflow import (
    _finalize_candidate_events,
    _read_prediction_candidate_events,
    _read_quote_state,
)
from ..microstructure_v21.candidate_pool import attach_quote_state
from .config import SymbolScreenV22Config


class SymbolScreenV22Error(ValueError):
    """Raised when v2.2 symbol screening cannot be built."""


@dataclass(frozen=True)
class SymbolScreenV22Tables:
    """In-memory v2.2 screening tables."""

    summary: pd.DataFrame
    deciles: pd.DataFrame
    horizon_sweep: pd.DataFrame


@dataclass(frozen=True)
class SymbolScreenV22OutputPaths:
    """Output paths for v2.2 screening artifacts."""

    summary_csv_path: Path
    deciles_csv_path: Path
    horizon_sweep_csv_path: Path
    manifest_path: Path
    move_over_cost_svg_path: Path
    decile_markout_svg_path: Path


@dataclass(frozen=True)
class SymbolScreenV22BuildResult:
    """Build result for v2.2 symbol screening."""

    tables: SymbolScreenV22Tables
    paths: SymbolScreenV22OutputPaths


def build_symbol_screen_v22(
    config: DataSliceConfig,
    *,
    processed_dir: str | Path | None = None,
    tables_dir: str | Path = "outputs/tables",
    figures_dir: str | Path = "outputs/figures",
    screening_config: SymbolScreenV22Config = SymbolScreenV22Config(),
) -> SymbolScreenV22BuildResult:
    """Build and write v2.2 cross-symbol screening diagnostics."""

    _validate_config(screening_config)
    root = Path(processed_dir or config.storage["processed_dir"]) / config.slice_name
    quote_path = root / f"{config.slice_name}_quote_features_v1.csv"
    if not quote_path.exists():
        raise SymbolScreenV22Error(f"Required quote features are missing: {quote_path}")

    candidates = _load_candidate_events(
        root,
        slice_name=config.slice_name,
        symbols=config.symbols,
        screening_config=screening_config,
    )
    quotes = _load_quotes(quote_path, symbols=config.symbols)
    orders = _load_optional_orders(root, slice_name=config.slice_name, symbols=config.symbols)
    tables = build_symbol_screening_tables(
        candidates,
        quotes,
        orders=orders,
        screening_config=screening_config,
        configured_symbols=config.symbols,
    )
    paths = _write_outputs(
        tables,
        slice_name=config.slice_name,
        screening_config=screening_config,
        input_paths={
            "quote_path": quote_path,
            "candidate_source": _candidate_source_path(root, config.slice_name, screening_config),
            "orders_path": root / f"{config.slice_name}_microstructure_v21_orders.csv",
        },
        tables_dir=tables_dir,
        figures_dir=figures_dir,
    )
    return SymbolScreenV22BuildResult(tables=tables, paths=paths)


def build_symbol_screen_v22_for_data_configs(
    configs: tuple[DataSliceConfig, ...],
    *,
    processed_dir: str | Path | None = None,
    tables_dir: str | Path = "outputs/tables",
    figures_dir: str | Path = "outputs/figures",
    screening_config: SymbolScreenV22Config = SymbolScreenV22Config(),
) -> SymbolScreenV22BuildResult:
    """Build one v2.2 screen from multiple independent data-slice configs."""

    if not configs:
        raise SymbolScreenV22Error("At least one data-slice config is required.")
    _validate_config(screening_config)
    summaries = []
    deciles = []
    horizon_sweeps = []
    input_paths: dict[str, Path] = {}
    for config in configs:
        root = Path(processed_dir or config.storage["processed_dir"]) / config.slice_name
        quote_path = root / f"{config.slice_name}_quote_features_v1.csv"
        if not quote_path.exists():
            raise SymbolScreenV22Error(f"Required quote features are missing: {quote_path}")
        candidates = _load_candidate_events(
            root,
            slice_name=config.slice_name,
            symbols=config.symbols,
            screening_config=screening_config,
        )
        quotes = _load_quotes(quote_path, symbols=config.symbols)
        orders = _load_optional_orders(root, slice_name=config.slice_name, symbols=config.symbols)
        tables = build_symbol_screening_tables(
            candidates,
            quotes,
            orders=orders,
            screening_config=screening_config,
            configured_symbols=config.symbols,
        )
        summaries.append(tables.summary)
        deciles.append(tables.deciles)
        horizon_sweeps.append(tables.horizon_sweep)
        input_paths[f"{config.slice_name}_quote_path"] = quote_path
        input_paths[f"{config.slice_name}_candidate_source"] = _candidate_source_path(
            root,
            config.slice_name,
            screening_config,
        )
        input_paths[f"{config.slice_name}_orders_path"] = (
            root / f"{config.slice_name}_microstructure_v21_orders.csv"
        )

    combined = SymbolScreenV22Tables(
        summary=_concat_tables(summaries),
        deciles=_concat_tables(deciles),
        horizon_sweep=_concat_tables(horizon_sweeps),
    )
    paths = _write_outputs(
        combined,
        slice_name=screening_config.universe_name,
        screening_config=screening_config,
        input_paths=input_paths,
        tables_dir=tables_dir,
        figures_dir=figures_dir,
    )
    return SymbolScreenV22BuildResult(tables=combined, paths=paths)


def build_symbol_screening_tables(
    candidates: pd.DataFrame,
    quotes: pd.DataFrame,
    *,
    orders: pd.DataFrame | None = None,
    screening_config: SymbolScreenV22Config = SymbolScreenV22Config(),
    configured_symbols: tuple[str, ...] = (),
) -> SymbolScreenV22Tables:
    """Build v2.2 summary, decile, and horizon-sweep tables from long-form data."""

    _validate_config(screening_config)
    candidate_rows = _prepare_candidates(candidates)
    quote_rows = _prepare_quotes(quotes)
    if configured_symbols:
        symbols = tuple(str(symbol) for symbol in configured_symbols)
        candidate_rows = candidate_rows.loc[candidate_rows[SYMBOL].astype(str).isin(symbols)].copy()
        quote_rows = quote_rows.loc[quote_rows[SYMBOL].astype(str).isin(symbols)].copy()
    if candidate_rows.empty:
        return SymbolScreenV22Tables(
            summary=pd.DataFrame(),
            deciles=pd.DataFrame(),
            horizon_sweep=pd.DataFrame(),
        )

    candidate_rows = _assign_splits(candidate_rows, screening_config=screening_config)
    candidate_rows = _add_future_moves(candidate_rows, quote_rows, screening_config.horizons)
    deciles = _build_deciles(candidate_rows, screening_config=screening_config)
    horizon_sweep = _build_horizon_sweep(candidate_rows, screening_config=screening_config)
    adverse_selection = _build_adverse_selection(
        orders,
        quote_rows,
        configured_symbols=configured_symbols,
    )
    spread_stats = _build_quote_spread_stats(quote_rows)
    summary = _build_summary(
        candidates=candidate_rows,
        horizon_sweep=horizon_sweep,
        spread_stats=spread_stats,
        adverse_selection=adverse_selection,
        screening_config=screening_config,
    )
    return SymbolScreenV22Tables(summary=summary, deciles=deciles, horizon_sweep=horizon_sweep)


def _concat_tables(tables: list[pd.DataFrame]) -> pd.DataFrame:
    non_empty = [table for table in tables if not table.empty]
    if not non_empty:
        return pd.DataFrame()
    return pd.concat(non_empty, ignore_index=True)


def _load_candidate_events(
    root: Path,
    *,
    slice_name: str,
    symbols: tuple[str, ...],
    screening_config: SymbolScreenV22Config,
) -> pd.DataFrame:
    source_path = _candidate_source_path(root, slice_name, screening_config)
    if source_path.name.endswith("_microstructure_v21_candidate_events.csv"):
        if not source_path.exists():
            raise SymbolScreenV22Error(f"Required v2.1 candidate file is missing: {source_path}")
        usecols = [
            "signal_id",
            EVENT_TIME,
            SYMBOL,
            TRADING_DATE,
            "side",
            "predicted_edge_bps",
            "expected_cost_bps",
            "tradable_edge_bps",
            MIDQUOTE,
            QUOTED_SPREAD,
            "displayed_depth",
            "microprice_gap_bps",
        ]
        rows = pd.read_csv(source_path, usecols=usecols)
        return rows.loc[rows[SYMBOL].astype(str).isin(symbols)].copy()

    prediction_path = root / f"{slice_name}_{COST_AWARE_LINEAR_SCORE_STRATEGY}_predictions.csv"
    quote_path = root / f"{slice_name}_quote_features_v1.csv"
    if not prediction_path.exists():
        raise SymbolScreenV22Error(f"Required prediction file is missing: {prediction_path}")
    quote_state = _read_quote_state(quote_path)
    quote_state = quote_state.loc[quote_state[SYMBOL].astype(str).isin(symbols)].copy()
    candidates = _read_prediction_candidate_events(prediction_path)
    candidates = candidates.loc[candidates[SYMBOL].astype(str).isin(symbols)].copy()
    candidates = attach_quote_state(candidates, quote_state)
    return _finalize_candidate_events(candidates, tick_size=0.01)


def _candidate_source_path(
    root: Path,
    slice_name: str,
    screening_config: SymbolScreenV22Config,
) -> Path:
    v21_path = root / f"{slice_name}_microstructure_v21_candidate_events.csv"
    if screening_config.candidate_source == "v21":
        return v21_path
    if screening_config.candidate_source == "predictions":
        return root / f"{slice_name}_{COST_AWARE_LINEAR_SCORE_STRATEGY}_predictions.csv"
    if screening_config.candidate_source == "auto":
        return v21_path if v21_path.exists() else root / f"{slice_name}_{COST_AWARE_LINEAR_SCORE_STRATEGY}_predictions.csv"
    raise SymbolScreenV22Error(f"Unknown candidate_source: {screening_config.candidate_source}")


def _load_quotes(path: Path, *, symbols: tuple[str, ...]) -> pd.DataFrame:
    rows = pd.read_csv(
        path,
        usecols=[EVENT_TIME, SYMBOL, TRADING_DATE, MIDQUOTE, QUOTED_SPREAD],
    )
    rows = rows.loc[rows[SYMBOL].astype(str).isin(symbols)].copy()
    return rows


def _load_optional_orders(
    root: Path,
    *,
    slice_name: str,
    symbols: tuple[str, ...],
) -> pd.DataFrame | None:
    path = root / f"{slice_name}_microstructure_v21_orders.csv"
    if not path.exists():
        return None
    usecols = [EVENT_TIME, SYMBOL, TRADING_DATE, "side", "filled", "entry_midquote"]
    rows = pd.read_csv(path, usecols=usecols)
    rows = rows.loc[rows[SYMBOL].astype(str).isin(symbols)].copy()
    return rows


def _prepare_candidates(rows: pd.DataFrame) -> pd.DataFrame:
    result = rows.copy()
    result[EVENT_TIME] = pd.to_datetime(result[EVENT_TIME], utc=True, format="mixed")
    result["event_time_ns"] = result[EVENT_TIME].astype("int64")
    for column in (
        "side",
        "predicted_edge_bps",
        "expected_cost_bps",
        "tradable_edge_bps",
        MIDQUOTE,
        QUOTED_SPREAD,
    ):
        result[column] = pd.to_numeric(result[column], errors="coerce")
    result["spread_bps"] = result[QUOTED_SPREAD] / result[MIDQUOTE] * 10000.0
    result = result.dropna(
        subset=[SYMBOL, TRADING_DATE, EVENT_TIME, "side", "predicted_edge_bps", MIDQUOTE],
    )
    return result.sort_values([SYMBOL, TRADING_DATE, EVENT_TIME], kind="mergesort").reset_index(
        drop=True
    )


def _prepare_quotes(rows: pd.DataFrame) -> pd.DataFrame:
    result = rows.copy()
    result[EVENT_TIME] = pd.to_datetime(result[EVENT_TIME], utc=True, format="mixed")
    result["event_time_ns"] = result[EVENT_TIME].astype("int64")
    result[MIDQUOTE] = pd.to_numeric(result[MIDQUOTE], errors="coerce")
    result[QUOTED_SPREAD] = pd.to_numeric(result[QUOTED_SPREAD], errors="coerce")
    result["spread_bps"] = result[QUOTED_SPREAD] / result[MIDQUOTE] * 10000.0
    result = result.dropna(subset=[SYMBOL, TRADING_DATE, EVENT_TIME, MIDQUOTE])
    return result.sort_values([SYMBOL, TRADING_DATE, EVENT_TIME], kind="mergesort").reset_index(
        drop=True
    )


def _assign_splits(
    rows: pd.DataFrame,
    *,
    screening_config: SymbolScreenV22Config,
) -> pd.DataFrame:
    result = rows.copy()
    result["split"] = "test"
    for symbol, group in result.groupby(SYMBOL, sort=False):
        dates = sorted(str(value) for value in group[TRADING_DATE].dropna().unique())
        validation_dates = set(dates[: screening_config.validation_min_dates])
        symbol_mask = result[SYMBOL].astype(str).eq(str(symbol))
        result.loc[
            symbol_mask & result[TRADING_DATE].astype(str).isin(validation_dates),
            "split",
        ] = "validation"
    return result


def _add_future_moves(
    rows: pd.DataFrame,
    quotes: pd.DataFrame,
    horizons: tuple[str, ...],
) -> pd.DataFrame:
    result = rows.copy()
    quote_groups = {
        (str(key[0]), str(key[1])): (
            group["event_time_ns"].to_numpy(dtype=np.int64),
            group[MIDQUOTE].to_numpy(dtype=np.float64),
        )
        for key, group in quotes.groupby([SYMBOL, TRADING_DATE], sort=False)
    }
    for horizon in horizons:
        result[f"move_{horizon}_bps"] = np.nan
    for key, index in result.groupby([SYMBOL, TRADING_DATE], sort=False).groups.items():
        quote_key = (str(key[0]), str(key[1]))
        if quote_key not in quote_groups:
            continue
        q_times, q_mids = quote_groups[quote_key]
        loc = np.asarray(index, dtype=np.int64)
        base_times = result.loc[index, "event_time_ns"].to_numpy(dtype=np.int64)
        base_mid = result.loc[index, MIDQUOTE].to_numpy(dtype=np.float64)
        side = result.loc[index, "side"].to_numpy(dtype=np.float64)
        valid_base = np.isfinite(base_mid) & (base_mid != 0)
        for horizon in horizons:
            target = base_times + int(pd.Timedelta(horizon).value)
            positions = np.searchsorted(q_times, target, side="left")
            valid = (positions < len(q_times)) & valid_base
            values = np.full(len(loc), np.nan, dtype=np.float64)
            values[valid] = side[valid] * (q_mids[positions[valid]] - base_mid[valid]) / base_mid[
                valid
            ] * 10000.0
            result.loc[index, f"move_{horizon}_bps"] = values
    return result


def _build_deciles(
    candidates: pd.DataFrame,
    *,
    screening_config: SymbolScreenV22Config,
) -> pd.DataFrame:
    rows = []
    for (symbol, split), group in candidates.groupby([SYMBOL, "split"], sort=False):
        if group["predicted_edge_bps"].nunique(dropna=True) < 2:
            continue
        working = group.copy()
        working["signal_decile"] = (
            pd.qcut(working["predicted_edge_bps"], 10, labels=False, duplicates="drop") + 1
        )
        for horizon in screening_config.decile_horizons:
            move_column = f"move_{horizon}_bps"
            valid = working.dropna(subset=[move_column, "expected_cost_bps", "signal_decile"])
            for decile, decile_rows in valid.groupby("signal_decile", sort=True):
                mean_cost = float(decile_rows["expected_cost_bps"].mean())
                mean_move = float(decile_rows[move_column].mean())
                rows.append(
                    {
                        "universe_name": screening_config.universe_name,
                        "symbol": symbol,
                        "split": split,
                        "horizon": horizon,
                        "signal_decile": int(decile),
                        "count": len(decile_rows),
                        "mean_future_move_bps": mean_move,
                        "hit_rate": float((decile_rows[move_column] > 0).mean()),
                        "mean_cost_bps": mean_cost,
                        "move_over_cost": _safe_ratio(mean_move, mean_cost),
                        "test_used_for_selection": False,
                    }
                )
    return pd.DataFrame(rows)


def _build_horizon_sweep(
    candidates: pd.DataFrame,
    *,
    screening_config: SymbolScreenV22Config,
) -> pd.DataFrame:
    rows = []
    for (symbol, split), group in candidates.groupby([SYMBOL, "split"], sort=False):
        bucket_masks = _bucket_masks(group, screening_config=screening_config)
        for horizon in screening_config.horizons:
            move_column = f"move_{horizon}_bps"
            for bucket, mask in bucket_masks.items():
                bucket_rows = group.loc[mask].dropna(subset=[move_column, "expected_cost_bps"])
                if bucket_rows.empty:
                    continue
                mean_move = float(bucket_rows[move_column].mean())
                mean_cost = float(bucket_rows["expected_cost_bps"].mean())
                rows.append(
                    {
                        "universe_name": screening_config.universe_name,
                        "symbol": symbol,
                        "split": split,
                        "signal_bucket": bucket,
                        "horizon": horizon,
                        "count": len(bucket_rows),
                        "mean_move_bps": mean_move,
                        "mean_cost_bps": mean_cost,
                        "move_over_cost": _safe_ratio(mean_move, mean_cost),
                        "hit_rate": float((bucket_rows[move_column] > 0).mean()),
                        "tradable_rate_move_gt_cost": float(
                            (bucket_rows[move_column] > bucket_rows["expected_cost_bps"]).mean()
                        ),
                        "test_used_for_selection": False,
                    }
                )
    return pd.DataFrame(rows)


def _bucket_masks(
    group: pd.DataFrame,
    *,
    screening_config: SymbolScreenV22Config,
) -> dict[str, pd.Series]:
    score = group["predicted_edge_bps"]
    masks: dict[str, pd.Series] = {}
    for bucket in screening_config.signal_buckets:
        if bucket == "all":
            masks[bucket] = pd.Series(True, index=group.index)
        elif bucket == "top10pct_score":
            masks[bucket] = score >= score.quantile(0.90)
        elif bucket == "top5pct_score":
            masks[bucket] = score >= score.quantile(0.95)
        elif bucket == "top1pct_score":
            masks[bucket] = score >= score.quantile(0.99)
        else:
            raise SymbolScreenV22Error(f"Unknown signal_bucket: {bucket}")
    return masks


def _build_adverse_selection(
    orders: pd.DataFrame | None,
    quotes: pd.DataFrame,
    *,
    configured_symbols: tuple[str, ...],
) -> pd.DataFrame:
    if orders is None or orders.empty:
        return pd.DataFrame()
    order_rows = orders.copy()
    if configured_symbols:
        order_rows = order_rows.loc[order_rows[SYMBOL].astype(str).isin(configured_symbols)].copy()
    if order_rows.empty:
        return pd.DataFrame()
    order_rows[EVENT_TIME] = pd.to_datetime(order_rows[EVENT_TIME], utc=True, format="mixed")
    order_rows["event_time_ns"] = order_rows[EVENT_TIME].astype("int64")
    order_rows["filled"] = order_rows["filled"].astype(str).str.lower().eq("true")
    order_rows["entry_midquote"] = pd.to_numeric(order_rows["entry_midquote"], errors="coerce")
    order_rows["side"] = pd.to_numeric(order_rows["side"], errors="coerce")
    working = order_rows.rename(columns={"entry_midquote": MIDQUOTE})
    working = _add_future_moves(working, quotes, ("1s",))
    rows = []
    for (symbol, filled), group in working.dropna(subset=["move_1s_bps"]).groupby(
        [SYMBOL, "filled"],
        sort=False,
    ):
        rows.append(
            {
                "symbol": symbol,
                "filled": bool(filled),
                "count": len(group),
                "markout_1s_bps": float(group["move_1s_bps"].mean()),
            }
        )
    return pd.DataFrame(rows)


def _build_quote_spread_stats(quotes: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for symbol, group in quotes.groupby(SYMBOL, sort=False):
        spread = pd.to_numeric(group["spread_bps"], errors="coerce").dropna()
        if spread.empty:
            continue
        rows.append(
            {
                "symbol": symbol,
                "mean_spread_bps": float(spread.mean()),
                "median_spread_bps": float(spread.median()),
                "p90_spread_bps": float(spread.quantile(0.90)),
            }
        )
    return pd.DataFrame(rows)


def _build_summary(
    *,
    candidates: pd.DataFrame,
    horizon_sweep: pd.DataFrame,
    spread_stats: pd.DataFrame,
    adverse_selection: pd.DataFrame,
    screening_config: SymbolScreenV22Config,
) -> pd.DataFrame:
    rows = []
    for symbol, symbol_candidates in candidates.groupby(SYMBOL, sort=True):
        validation_sweep = horizon_sweep.loc[
            horizon_sweep["symbol"].astype(str).eq(str(symbol))
            & horizon_sweep["split"].eq("validation")
        ]
        top1 = _best_bucket(validation_sweep, "top1pct_score")
        top5 = _best_bucket(validation_sweep, "top5pct_score")
        spread = _row_for_symbol(spread_stats, symbol)
        filled_markout = _adverse_markout(adverse_selection, symbol, filled=True)
        unfilled_markout = _adverse_markout(adverse_selection, symbol, filled=False)
        adverse_flag = (
            None
            if pd.isna(filled_markout) or pd.isna(unfilled_markout)
            else bool(filled_markout < unfilled_markout)
        )
        top1_ratio = top1.get("move_over_cost", np.nan)
        validation_pass = bool(
            pd.notna(top1_ratio)
            and top1_ratio > screening_config.pass_move_over_cost
            and adverse_flag is not True
        )
        rows.append(
            {
                "universe_name": screening_config.universe_name,
                "symbol": symbol,
                "trading_start": str(symbol_candidates[TRADING_DATE].min()),
                "trading_end": str(symbol_candidates[TRADING_DATE].max()),
                "candidate_events": len(symbol_candidates),
                "mean_spread_bps": spread.get("mean_spread_bps"),
                "median_spread_bps": spread.get("median_spread_bps"),
                "p90_spread_bps": spread.get("p90_spread_bps"),
                "mean_cost_bps": float(symbol_candidates["expected_cost_bps"].mean()),
                "top_1pct_best_horizon": top1.get("horizon"),
                "top_1pct_mean_move_bps": top1.get("mean_move_bps"),
                "top_1pct_mean_cost_bps": top1.get("mean_cost_bps"),
                "top_1pct_move_over_cost": top1_ratio,
                "top_5pct_best_horizon": top5.get("horizon"),
                "top_5pct_move_over_cost": top5.get("move_over_cost"),
                "filled_1s_markout_bps": filled_markout,
                "unfilled_1s_markout_bps": unfilled_markout,
                "adverse_selection_flag": adverse_flag,
                "validation_pass_flag": validation_pass,
                "validation_strong_pass_flag": bool(
                    pd.notna(top1_ratio)
                    and top1_ratio > screening_config.strong_pass_move_over_cost
                    and adverse_flag is not True
                ),
                "validation_fail_flag": bool(
                    (pd.notna(top1_ratio) and top1_ratio < screening_config.fail_move_over_cost)
                    or adverse_flag is True
                ),
                "test_used_for_selection": False,
            }
        )
    return pd.DataFrame(rows)


def _best_bucket(sweep: pd.DataFrame, bucket: str) -> dict[str, object]:
    rows = sweep.loc[sweep["signal_bucket"].eq(bucket)].dropna(subset=["move_over_cost"])
    if rows.empty:
        return {}
    return rows.sort_values("move_over_cost", ascending=False).iloc[0].to_dict()


def _row_for_symbol(frame: pd.DataFrame, symbol: object) -> dict[str, object]:
    if frame.empty:
        return {}
    rows = frame.loc[frame["symbol"].astype(str).eq(str(symbol))]
    if rows.empty:
        return {}
    return rows.iloc[0].to_dict()


def _adverse_markout(frame: pd.DataFrame, symbol: object, *, filled: bool) -> float | None:
    if frame.empty:
        return None
    rows = frame.loc[
        frame["symbol"].astype(str).eq(str(symbol)) & frame["filled"].eq(filled),
        "markout_1s_bps",
    ]
    if rows.empty:
        return None
    return float(rows.iloc[0])


def _write_outputs(
    tables: SymbolScreenV22Tables,
    *,
    slice_name: str,
    screening_config: SymbolScreenV22Config,
    input_paths: dict[str, Path],
    tables_dir: str | Path,
    figures_dir: str | Path,
) -> SymbolScreenV22OutputPaths:
    tables_root = Path(tables_dir)
    figures_root = Path(figures_dir)
    tables_root.mkdir(parents=True, exist_ok=True)
    figures_root.mkdir(parents=True, exist_ok=True)
    paths = SymbolScreenV22OutputPaths(
        summary_csv_path=tables_root / "v22_symbol_screen_summary.csv",
        deciles_csv_path=tables_root / "v22_symbol_screen_deciles.csv",
        horizon_sweep_csv_path=tables_root / "v22_symbol_screen_horizon_sweep.csv",
        manifest_path=tables_root / "v22_symbol_screen_manifest.json",
        move_over_cost_svg_path=figures_root / "v22_symbol_screen_move_over_cost.svg",
        decile_markout_svg_path=figures_root / "v22_symbol_screen_decile_markout.svg",
    )
    tables.summary.to_csv(paths.summary_csv_path, index=False)
    tables.deciles.to_csv(paths.deciles_csv_path, index=False)
    tables.horizon_sweep.to_csv(paths.horizon_sweep_csv_path, index=False)
    _write_figures(tables, paths=paths, slice_name=slice_name, screening_config=screening_config)
    manifest = {
        "slice_name": slice_name,
        "diagnostic_version": "symbol_screening_v22",
        "diagnostic_only": True,
        "core_schema_modified": False,
        "test_used_for_selection": False,
        "inputs": {key: str(value) for key, value in input_paths.items()},
        "outputs": {key: str(value) for key, value in asdict(paths).items()},
        "config": asdict(screening_config),
        "row_counts": {
            "summary": len(tables.summary),
            "deciles": len(tables.deciles),
            "horizon_sweep": len(tables.horizon_sweep),
        },
    }
    paths.manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return paths


def _write_figures(
    tables: SymbolScreenV22Tables,
    *,
    paths: SymbolScreenV22OutputPaths,
    slice_name: str,
    screening_config: SymbolScreenV22Config,
) -> None:
    if not tables.summary.empty:
        figure_rows = tables.summary.copy()
        figure_rows["label"] = figure_rows["symbol"]
        paths.move_over_cost_svg_path.write_text(
            _bar_svg(
                figure_rows,
                label_column="label",
                value_column="top_1pct_move_over_cost",
                title=f"{slice_name} V2.2 Symbol Screen",
                subtitle="Validation-only top 1% signal move / cost. Diagnostic only.",
                y_label="Move / cost",
                color="#2563eb",
            ),
            encoding="utf-8",
        )
    else:
        paths.move_over_cost_svg_path.write_text("", encoding="utf-8")

    decile_rows = tables.deciles.loc[
        tables.deciles["split"].eq("validation")
        & tables.deciles["horizon"].eq(screening_config.decile_horizons[0])
    ].copy()
    if not decile_rows.empty:
        decile_rows["label"] = (
            decile_rows["symbol"].astype(str)
            + " D"
            + decile_rows["signal_decile"].astype(str)
        )
        paths.decile_markout_svg_path.write_text(
            _bar_svg(
                decile_rows,
                label_column="label",
                value_column="mean_future_move_bps",
                title=f"{slice_name} V2.2 Decile Markout",
                subtitle=(
                    "Validation split mean future move by signal decile at "
                    f"{screening_config.decile_horizons[0]}."
                ),
                y_label="Mean future move bps",
                color="#059669",
            ),
            encoding="utf-8",
        )
    else:
        paths.decile_markout_svg_path.write_text("", encoding="utf-8")


def _safe_ratio(numerator: float, denominator: float) -> float | None:
    if denominator == 0 or pd.isna(denominator):
        return None
    return float(numerator) / float(denominator)


def _validate_config(config: SymbolScreenV22Config) -> None:
    if config.validation_min_dates < 1:
        raise SymbolScreenV22Error("validation_min_dates must be positive.")
    for horizon in (*config.horizons, *config.decile_horizons):
        pd.Timedelta(horizon)
    if config.candidate_source not in {"auto", "v21", "predictions"}:
        raise SymbolScreenV22Error(f"Unknown candidate_source: {config.candidate_source}")
