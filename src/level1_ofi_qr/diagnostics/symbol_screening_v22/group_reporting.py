"""Group-aware reporting helpers for v2.2 symbol screening diagnostics."""

from __future__ import annotations

from pathlib import Path
from typing import Any
import json

import pandas as pd

from ...alignment import TRADING_DATE
from ...schema import SYMBOL
from ...utils import DataSliceConfig
from ..figures import _bar_svg
from .config import SymbolScreenV22Config


def build_data_slice_audit(
    config: DataSliceConfig,
    *,
    root: Path,
    quotes: pd.DataFrame,
    candidates: pd.DataFrame,
) -> dict[str, Any]:
    configured_dates = [value.isoformat() for value in config.time_range.trading_dates]
    quote_dates = sorted(str(value) for value in quotes[TRADING_DATE].dropna().unique())
    candidate_dates = sorted(str(value) for value in candidates[TRADING_DATE].dropna().unique())
    raw_counts = _read_wrds_raw_counts(config)
    return {
        "slice_name": config.slice_name,
        "symbols": list(config.symbols),
        "configured_trading_dates": configured_dates,
        "configured_start_date": configured_dates[0] if configured_dates else None,
        "configured_end_date": configured_dates[-1] if configured_dates else None,
        "session": config.time_range.session,
        "market_open": config.time_range.market_open.isoformat(),
        "market_close": config.time_range.market_close.isoformat(),
        "timezone": config.time_range.timezone,
        "processed_root": str(root),
        "raw_row_count": raw_counts.get("raw_row_count"),
        "raw_quote_count": raw_counts.get("raw_quote_count"),
        "raw_trade_count": raw_counts.get("raw_trade_count"),
        "processed_row_count": int(len(quotes)),
        "processed_quote_count": int(len(quotes)),
        "candidate_event_count": int(len(candidates)),
        "actual_quote_trading_dates": quote_dates,
        "actual_candidate_trading_dates": candidate_dates,
        "missing_trading_dates": [date for date in configured_dates if date not in quote_dates],
    }


def attach_data_slice_audit_to_tables(
    tables: Any,
    data_slice_audit: tuple[dict[str, Any], ...],
) -> Any:
    if tables.summary.empty or not data_slice_audit:
        return tables
    summary = tables.summary.copy()
    for index, row in summary.iterrows():
        audit = _audit_for_symbol(str(row[SYMBOL]), data_slice_audit)
        if not audit:
            continue
        summary.loc[index, "raw_row_count"] = audit.get("raw_row_count")
        summary.loc[index, "raw_quote_count"] = audit.get("raw_quote_count")
        summary.loc[index, "raw_trade_count"] = audit.get("raw_trade_count")
        summary.loc[index, "trade_count"] = audit.get("raw_trade_count")
        summary.loc[index, "processed_row_count"] = audit.get("processed_row_count")
        summary.loc[index, "processed_quote_count"] = audit.get("processed_quote_count")
        summary.loc[index, "missing_trading_dates"] = ";".join(
            str(value) for value in audit.get("missing_trading_dates", ())
        )
    return tables.__class__(
        summary=summary,
        deciles=tables.deciles,
        horizon_sweep=tables.horizon_sweep,
    )


def write_group_aware_outputs(
    tables: Any,
    *,
    tables_root: Path,
    figures_root: Path,
    slice_name: str,
    screening_config: SymbolScreenV22Config,
) -> dict[str, Path]:
    if not screening_config.symbol_metadata:
        return {}

    extra: dict[str, Path] = {}
    experiment_root = tables_root.parent if tables_root.name == "tables" else tables_root
    groups_root = experiment_root / "groups"

    all_summary = tables_root / "all_symbols_summary.csv"
    all_deciles = tables_root / "all_symbols_decile_markout.csv"
    all_horizon = tables_root / "all_symbols_horizon_sweep.csv"
    tables.summary.to_csv(all_summary, index=False)
    tables.deciles.to_csv(all_deciles, index=False)
    tables.horizon_sweep.to_csv(all_horizon, index=False)
    extra["all_symbols_summary_csv_path"] = all_summary
    extra["all_symbols_decile_markout_csv_path"] = all_deciles
    extra["all_symbols_horizon_sweep_csv_path"] = all_horizon

    group_summary = _build_group_level_summary(
        tables.summary,
        screening_config=screening_config,
    )
    group_summary_path = tables_root / "group_level_summary.csv"
    group_ranking_path = tables_root / "group_level_ranking.csv"
    group_summary.to_csv(group_summary_path, index=False)
    group_summary.sort_values(
        ["mean_move_over_cost", "candidate_events"],
        ascending=[False, False],
        na_position="last",
    ).to_csv(group_ranking_path, index=False)
    extra["group_level_summary_csv_path"] = group_summary_path
    extra["group_level_ranking_csv_path"] = group_ranking_path

    all_move_svg = figures_root / "all_symbols_move_over_cost.svg"
    all_net_svg = figures_root / "all_symbols_net_per_trip.svg"
    group_move_svg = figures_root / "group_move_over_cost.svg"
    group_decile_svg = figures_root / "group_decile_markout.svg"
    figures_root.mkdir(parents=True, exist_ok=True)
    _write_bar(
        tables.summary,
        path=all_move_svg,
        label_column=SYMBOL,
        value_column="move_over_cost",
        title=f"{slice_name} Move / Cost By Symbol",
        subtitle="Validation-only top 1% signal bucket. Diagnostic only.",
        y_label="Move / cost",
        color="#2563eb",
    )
    _write_bar(
        tables.summary,
        path=all_net_svg,
        label_column=SYMBOL,
        value_column="net_per_trip",
        title=f"{slice_name} Net Per Trip By Symbol",
        subtitle="Top 1% validation mean move minus mean cost, in bps. Diagnostic only.",
        y_label="Net bps per trip",
        color="#0f766e",
    )
    _write_bar(
        group_summary,
        path=group_move_svg,
        label_column="group_label",
        value_column="mean_move_over_cost",
        title=f"{slice_name} Move / Cost By Liquidity Regime",
        subtitle="Group means use only available processed symbols in this run.",
        y_label="Mean move / cost",
        color="#7c3aed",
    )
    group_deciles = _build_group_decile_markout(tables.deciles, screening_config=screening_config)
    _write_bar(
        group_deciles,
        path=group_decile_svg,
        label_column="label",
        value_column="mean_future_move_bps",
        title=f"{slice_name} Decile Markout By Liquidity Regime",
        subtitle="Validation split mean future move by signal decile.",
        y_label="Mean future move bps",
        color="#059669",
    )
    extra["all_symbols_move_over_cost_svg_path"] = all_move_svg
    extra["all_symbols_net_per_trip_svg_path"] = all_net_svg
    extra["group_move_over_cost_svg_path"] = group_move_svg
    extra["group_decile_markout_svg_path"] = group_decile_svg

    for group_id, group in _configured_groups(screening_config).items():
        group_dir = groups_root / group_id
        group_tables_dir = group_dir / "tables"
        group_figures_dir = group_dir / "figures"
        group_tables_dir.mkdir(parents=True, exist_ok=True)
        group_figures_dir.mkdir(parents=True, exist_ok=True)
        prefix = _group_file_prefix(group)
        summary = _filter_group(tables.summary, group_id)
        deciles = _filter_group(tables.deciles, group_id)
        horizon = _filter_group(tables.horizon_sweep, group_id)
        summary_path = group_tables_dir / f"{prefix}_summary.csv"
        deciles_path = group_tables_dir / f"{prefix}_decile_markout.csv"
        horizon_path = group_tables_dir / f"{prefix}_horizon_sweep.csv"
        summary.to_csv(summary_path, index=False)
        deciles.to_csv(deciles_path, index=False)
        horizon.to_csv(horizon_path, index=False)
        group_move_path = group_figures_dir / f"{prefix}_move_over_cost.svg"
        group_decile_path = group_figures_dir / f"{prefix}_decile_markout.svg"
        _write_bar(
            summary,
            path=group_move_path,
            label_column=SYMBOL,
            value_column="move_over_cost",
            title=f"{group.get('group_label', group_id)} Move / Cost",
            subtitle="Validation-only top 1% signal bucket. Diagnostic only.",
            y_label="Move / cost",
            color="#2563eb",
        )
        decile_figure = deciles.loc[
            deciles.get("split", pd.Series(dtype=object)).eq("validation")
            & deciles.get("horizon", pd.Series(dtype=object)).eq(
                screening_config.decile_horizons[0]
            )
        ].copy()
        if not decile_figure.empty:
            decile_figure["label"] = (
                decile_figure[SYMBOL].astype(str)
                + " D"
                + decile_figure["signal_decile"].astype(str)
            )
        _write_bar(
            decile_figure,
            path=group_decile_path,
            label_column="label",
            value_column="mean_future_move_bps",
            title=f"{group.get('group_label', group_id)} Decile Markout",
            subtitle="Validation split mean future move by signal decile.",
            y_label="Mean future move bps",
            color="#059669",
        )
        notes_path = group_dir / "notes.md"
        notes_path.write_text(
            _group_notes(group_id, group=group, summary=summary),
            encoding="utf-8",
        )
        extra[f"{group_id}_summary_csv_path"] = summary_path
        extra[f"{group_id}_decile_markout_csv_path"] = deciles_path
        extra[f"{group_id}_horizon_sweep_csv_path"] = horizon_path
        extra[f"{group_id}_move_over_cost_svg_path"] = group_move_path
        extra[f"{group_id}_decile_markout_svg_path"] = group_decile_path
        extra[f"{group_id}_notes_path"] = notes_path

    return extra


def build_date_window_audit(
    summary: pd.DataFrame,
    *,
    screening_config: SymbolScreenV22Config,
    data_slice_audit: tuple[dict[str, Any], ...],
) -> dict[str, Any]:
    expected_dates = list(screening_config.expected_trading_dates)
    if not expected_dates and data_slice_audit:
        expected_dates = list(data_slice_audit[0].get("configured_trading_dates", ()))

    per_symbol = []
    for _, row in summary.iterrows():
        symbol = str(row.get(SYMBOL))
        actual_dates = _split_dates(
            row.get("quote_trading_dates") or row.get("candidate_trading_dates")
        )
        audit = _audit_for_symbol(symbol, data_slice_audit)
        missing_dates = [date for date in expected_dates if date not in actual_dates]
        per_symbol.append(
            {
                "symbol": symbol,
                "same_start_date": _same_value(
                    actual_dates[0] if actual_dates else None,
                    screening_config.date_window_start,
                ),
                "same_end_date": _same_value(
                    actual_dates[-1] if actual_dates else None,
                    screening_config.date_window_end,
                ),
                "same_trading_dates": actual_dates == expected_dates
                if expected_dates
                else None,
                "same_session_filter": _same_value(
                    audit.get("session"),
                    screening_config.session_filter,
                )
                if screening_config.session_filter
                else None,
                "raw_row_count": audit.get("raw_row_count"),
                "processed_row_count": audit.get("processed_row_count"),
                "missing_trading_dates": missing_dates,
                "actual_trading_dates": actual_dates,
            }
        )

    return {
        "date_window_name": screening_config.date_window_name,
        "expected_start_date": screening_config.date_window_start,
        "expected_end_date": screening_config.date_window_end,
        "expected_trading_dates": expected_dates,
        "expected_session_filter": screening_config.session_filter,
        "same_start_date": _all_true_or_none(per_symbol, "same_start_date"),
        "same_end_date": _all_true_or_none(per_symbol, "same_end_date"),
        "same_trading_dates": _all_true_or_none(per_symbol, "same_trading_dates"),
        "same_session_filter": _all_true_or_none(per_symbol, "same_session_filter"),
        "per_symbol": per_symbol,
    }


def _read_wrds_raw_counts(config: DataSliceConfig) -> dict[str, int | None]:
    manifest_path = (
        Path(config.storage["raw_dir"])
        / config.slice_name
        / f"{config.slice_name}_wrds_manifest.json"
    )
    if not manifest_path.exists():
        return {
            "raw_row_count": None,
            "raw_quote_count": None,
            "raw_trade_count": None,
        }
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {
            "raw_row_count": None,
            "raw_quote_count": None,
            "raw_trade_count": None,
        }
    diagnostics = manifest.get("diagnostics", {})
    quote_rows = diagnostics.get("quote_rows")
    trade_rows = diagnostics.get("trade_rows")
    total = None
    if quote_rows is not None and trade_rows is not None:
        total = int(quote_rows) + int(trade_rows)
    return {
        "raw_row_count": total,
        "raw_quote_count": int(quote_rows) if quote_rows is not None else None,
        "raw_trade_count": int(trade_rows) if trade_rows is not None else None,
    }


def _build_group_level_summary(
    summary: pd.DataFrame,
    *,
    screening_config: SymbolScreenV22Config,
) -> pd.DataFrame:
    groups = _configured_groups(screening_config)
    rows = []
    for group_id, group in groups.items():
        group_rows = _filter_group(summary, group_id)
        symbols_available = (
            sorted(str(value) for value in group_rows[SYMBOL].dropna().unique())
            if not group_rows.empty and SYMBOL in group_rows
            else []
        )
        rows.append(
            {
                "universe_name": screening_config.universe_name,
                "date_window": screening_config.date_window_name,
                "group_id": group_id,
                "group_label": group.get("group_label"),
                "research_role": group.get("research_role"),
                "liquidity_hypothesis": group.get("liquidity_hypothesis"),
                "configured_symbols": ";".join(group.get("symbols", ())),
                "available_symbols": ";".join(symbols_available),
                "configured_symbol_count": len(group.get("symbols", ())),
                "available_symbol_count": len(symbols_available),
                "candidate_events": _sum_numeric(group_rows, "candidate_events"),
                "mean_move_over_cost": _mean_numeric(group_rows, "move_over_cost"),
                "median_move_over_cost": _median_numeric(group_rows, "move_over_cost"),
                "mean_net_per_trip": _mean_numeric(group_rows, "net_per_trip"),
                "mean_median_spread_bps": _mean_numeric(group_rows, "median_spread_bps"),
                "validation_pass_count": _sum_bool(group_rows, "validation_pass_flag"),
                "validation_fail_count": _sum_bool(group_rows, "validation_fail_flag"),
                "test_used_for_selection": False,
                "diagnostic_only": True,
            }
        )
    return pd.DataFrame(rows)


def _build_group_decile_markout(
    deciles: pd.DataFrame,
    *,
    screening_config: SymbolScreenV22Config,
) -> pd.DataFrame:
    if deciles.empty or "group_id" not in deciles.columns:
        return pd.DataFrame()
    rows = deciles.loc[
        deciles["split"].eq("validation")
        & deciles["horizon"].eq(screening_config.decile_horizons[0])
        & deciles["group_id"].notna()
    ].copy()
    if rows.empty:
        return pd.DataFrame()
    grouped = (
        rows.groupby(["group_id", "group_label", "signal_decile"], dropna=False, sort=True)
        .agg(
            count=("count", "sum"),
            mean_future_move_bps=("mean_future_move_bps", "mean"),
            mean_cost_bps=("mean_cost_bps", "mean"),
            move_over_cost=("move_over_cost", "mean"),
        )
        .reset_index()
    )
    grouped["label"] = grouped["group_label"].astype(str) + " D" + grouped[
        "signal_decile"
    ].astype(str)
    return grouped


def _configured_groups(screening_config: SymbolScreenV22Config) -> dict[str, dict[str, Any]]:
    groups: dict[str, dict[str, Any]] = {}
    for symbol, metadata in screening_config.symbol_metadata.items():
        group_id = metadata.get("group_id")
        if not group_id:
            continue
        group = groups.setdefault(
            str(group_id),
            {
                "group_label": metadata.get("group_label"),
                "research_role": metadata.get("research_role"),
                "liquidity_hypothesis": metadata.get("hypothesis")
                or metadata.get("liquidity_hypothesis"),
                "symbols": [],
            },
        )
        group["symbols"].append(str(symbol))
    return groups


def _group_file_prefix(group: dict[str, Any]) -> str:
    label = str(group.get("group_label") or "group")
    prefix = "_".join(label.split())
    return "group_" + prefix.removeprefix("Group_")


def _filter_group(frame: pd.DataFrame, group_id: str) -> pd.DataFrame:
    if frame.empty or "group_id" not in frame.columns:
        return frame.iloc[0:0].copy()
    return frame.loc[frame["group_id"].astype(str).eq(str(group_id))].copy()


def _write_bar(
    frame: pd.DataFrame,
    *,
    path: Path,
    label_column: str,
    value_column: str,
    title: str,
    subtitle: str,
    y_label: str,
    color: str,
) -> None:
    if label_column not in frame.columns or value_column not in frame.columns:
        frame = pd.DataFrame(columns=[label_column, value_column])
    path.write_text(
        _bar_svg(
            frame,
            label_column=label_column,
            value_column=value_column,
            title=title,
            subtitle=subtitle,
            y_label=y_label,
            color=color,
        ),
        encoding="utf-8",
    )


def _group_notes(group_id: str, *, group: dict[str, Any], summary: pd.DataFrame) -> str:
    available_symbols = (
        sorted(str(value) for value in summary[SYMBOL].dropna().unique())
        if not summary.empty and SYMBOL in summary
        else []
    )
    return "\n".join(
        [
            f"# {group.get('group_label', group_id)} V2.2 Symbol Screen",
            "",
            f"Group ID: `{group_id}`",
            f"Research role: `{group.get('research_role')}`",
            "",
            "Hypothesis:",
            "",
            str(group.get("liquidity_hypothesis") or ""),
            "",
            "Configured symbols:",
            *[f"- `{symbol}`" for symbol in group.get("symbols", ())],
            "",
            "Available processed symbols in this run:",
            *([f"- `{symbol}`" for symbol in available_symbols] or ["- None"]),
            "",
            "This is a diagnostic-only grouping. The group ID is used for reporting and aggregation only; it does not alter signals, labels, thresholds, horizons, or cost accounting.",
            "",
        ]
    )


def _audit_for_symbol(symbol: str, data_slice_audit: tuple[dict[str, Any], ...]) -> dict[str, Any]:
    for audit in data_slice_audit:
        if symbol in {str(value) for value in audit.get("symbols", ())}:
            return audit
    return {}


def _split_dates(value: object) -> list[str]:
    if value is None or pd.isna(value):
        return []
    return [part for part in str(value).split(";") if part]


def _same_value(actual: object, expected: object) -> bool | None:
    if expected is None:
        return None
    return str(actual) == str(expected)


def _all_true_or_none(rows: list[dict[str, Any]], key: str) -> bool | None:
    values = [row.get(key) for row in rows if row.get(key) is not None]
    if not values:
        return None
    return all(bool(value) for value in values)


def _sum_numeric(frame: pd.DataFrame, column: str) -> float | None:
    if frame.empty or column not in frame.columns:
        return None
    return float(pd.to_numeric(frame[column], errors="coerce").fillna(0.0).sum())


def _mean_numeric(frame: pd.DataFrame, column: str) -> float | None:
    if frame.empty or column not in frame.columns:
        return None
    values = pd.to_numeric(frame[column], errors="coerce").dropna()
    if values.empty:
        return None
    return float(values.mean())


def _median_numeric(frame: pd.DataFrame, column: str) -> float | None:
    if frame.empty or column not in frame.columns:
        return None
    values = pd.to_numeric(frame[column], errors="coerce").dropna()
    if values.empty:
        return None
    return float(values.median())


def _sum_bool(frame: pd.DataFrame, column: str) -> int:
    if frame.empty or column not in frame.columns:
        return 0
    return int(frame[column].fillna(False).astype(bool).sum())
