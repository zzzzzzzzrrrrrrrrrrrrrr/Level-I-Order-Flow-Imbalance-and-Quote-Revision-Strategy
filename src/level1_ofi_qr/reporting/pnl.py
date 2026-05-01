"""PnL and equity-curve reporting utilities."""

from __future__ import annotations

from dataclasses import dataclass
from html import escape
from pathlib import Path
from typing import Final

import pandas as pd


PNL_REPORTING_POLICY_NOTE: Final[str] = (
    "PnL reporting v1 visualizes already-generated accounting ledgers. It does "
    "not recompute signals, train models, select parameters, or change backtest "
    "results."
)

TIME_COLUMNS: Final[tuple[str, ...]] = ("event_time", "fill_time")
SEGMENT_COLUMNS: Final[tuple[str, ...]] = (
    "backtest_id",
    "model_backtest_id",
    "fold_id",
    "simulation_id",
)


class PnLReportingError(ValueError):
    """Raised when PnL reporting inputs cannot be processed."""


@dataclass(frozen=True)
class StrategyLedgerSpec:
    """Ledger input for one strategy curve."""

    strategy_name: str
    ledger_path: Path
    summary_path: Path | None = None


@dataclass(frozen=True)
class PnLComparisonResult:
    """PnL comparison outputs."""

    curve: pd.DataFrame
    summary: pd.DataFrame
    svg: str


def build_pnl_comparison(
    specs: tuple[StrategyLedgerSpec, ...],
    *,
    title: str = "AAPL Prototype Equity Curve Comparison",
) -> PnLComparisonResult:
    """Build equity curves, summary metrics, and SVG plot text."""

    if not specs:
        raise PnLReportingError("At least one strategy ledger is required.")

    curve_frames = []
    summary_rows = []
    for spec in specs:
        ledger = _read_ledger(spec)
        curve = _ledger_to_curve(ledger, strategy_name=spec.strategy_name)
        curve_frames.append(curve)
        summary_rows.append(_summarize_curve(curve, spec=spec, ledger=ledger))

    combined_curve = pd.concat(curve_frames, ignore_index=True)
    summary = pd.DataFrame(summary_rows)
    svg = render_equity_svg(combined_curve, summary=summary, title=title)
    return PnLComparisonResult(curve=combined_curve, summary=summary, svg=svg)


def write_pnl_comparison(
    specs: tuple[StrategyLedgerSpec, ...],
    *,
    curve_csv_path: str | Path,
    summary_csv_path: str | Path,
    svg_path: str | Path,
    title: str = "AAPL Prototype Equity Curve Comparison",
) -> PnLComparisonResult:
    """Build and write curve CSV, summary CSV, and SVG plot."""

    result = build_pnl_comparison(specs, title=title)
    curve_csv = Path(curve_csv_path)
    summary_csv = Path(summary_csv_path)
    svg = Path(svg_path)
    curve_csv.parent.mkdir(parents=True, exist_ok=True)
    summary_csv.parent.mkdir(parents=True, exist_ok=True)
    svg.parent.mkdir(parents=True, exist_ok=True)
    result.curve.to_csv(curve_csv, index=False)
    result.summary.to_csv(summary_csv, index=False)
    svg.write_text(result.svg, encoding="utf-8")
    return result


def render_equity_svg(
    curve: pd.DataFrame,
    *,
    summary: pd.DataFrame,
    title: str,
    width: int = 1200,
    height: int = 720,
) -> str:
    """Render a dependency-free SVG equity-curve chart."""

    _validate_curve(curve)
    margin_left = 92
    margin_right = 36
    margin_top = 72
    margin_bottom = 82
    plot_width = width - margin_left - margin_right
    plot_height = height - margin_top - margin_bottom

    times = pd.to_datetime(curve["event_time"], format="mixed")
    x_min = int(times.min().value)
    x_max = int(times.max().value)
    if x_min == x_max:
        x_max = x_min + 1

    y_values = pd.to_numeric(curve["equity_after"], errors="coerce")
    y_min = min(0.0, float(y_values.min()))
    y_max = max(0.0, float(y_values.max()))
    if y_min == y_max:
        y_min -= 1.0
        y_max += 1.0
    y_padding = max((y_max - y_min) * 0.08, 1.0)
    y_min -= y_padding
    y_max += y_padding

    def x_pos(value: pd.Timestamp) -> float:
        return margin_left + (int(value.value) - x_min) / (x_max - x_min) * plot_width

    def y_pos(value: float) -> float:
        return margin_top + (y_max - value) / (y_max - y_min) * plot_height

    colors = ["#1f77b4", "#d62728", "#2ca02c", "#9467bd", "#ff7f0e"]
    paths = []
    legend = []
    for strategy_index, (strategy, group) in enumerate(curve.groupby("strategy", sort=False)):
        color = colors[strategy_index % len(colors)]
        points = _downsample_curve(group)
        path_points = []
        for row in points.itertuples(index=False):
            path_points.append(
                f"{x_pos(pd.Timestamp(row.event_time)):.2f},{y_pos(float(row.equity_after)):.2f}"
            )
        paths.append(
            f'<polyline fill="none" stroke="{color}" stroke-width="2.4" '
            f'points="{" ".join(path_points)}" />'
        )
        legend_y = margin_top + 22 * strategy_index
        final_equity = _summary_value(summary, strategy, "final_equity")
        legend.append(
            f'<rect x="{width - 360}" y="{legend_y - 11}" width="13" height="13" '
            f'fill="{color}" />'
            f'<text x="{width - 340}" y="{legend_y}" class="legend">'
            f'{escape(str(strategy))}: final {final_equity:.2f}</text>'
        )

    grid_lines = []
    y_ticks = _linear_ticks(y_min, y_max, tick_count=6)
    for tick in y_ticks:
        y = y_pos(tick)
        grid_lines.append(
            f'<line x1="{margin_left}" y1="{y:.2f}" x2="{width - margin_right}" '
            f'y2="{y:.2f}" class="grid" />'
            f'<text x="{margin_left - 12}" y="{y + 4:.2f}" class="axis-label" '
            f'text-anchor="end">{tick:.0f}</text>'
        )

    zero_y = y_pos(0.0)
    start_label = times.min().strftime("%H:%M:%S")
    end_label = times.max().strftime("%H:%M:%S")
    return "\n".join(
        [
            f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
            f'viewBox="0 0 {width} {height}" role="img">',
            "<style>",
            "text { font-family: Arial, sans-serif; fill: #1f2937; }",
            ".title { font-size: 22px; font-weight: 700; }",
            ".subtitle { font-size: 13px; fill: #4b5563; }",
            ".axis-label { font-size: 12px; fill: #4b5563; }",
            ".legend { font-size: 13px; fill: #111827; }",
            ".grid { stroke: #e5e7eb; stroke-width: 1; }",
            ".axis { stroke: #374151; stroke-width: 1.2; }",
            ".zero { stroke: #111827; stroke-width: 1.4; stroke-dasharray: 5 5; }",
            "</style>",
            f'<rect x="0" y="0" width="{width}" height="{height}" fill="#ffffff" />',
            f'<text x="{margin_left}" y="36" class="title">{escape(title)}</text>',
            f'<text x="{margin_left}" y="58" class="subtitle">'
            "Cumulative net PnL after event_cost deductions; values are dollars "
            "for the one-share target-position prototype.</text>",
            *grid_lines,
            f'<line x1="{margin_left}" y1="{margin_top}" x2="{margin_left}" '
            f'y2="{height - margin_bottom}" class="axis" />',
            f'<line x1="{margin_left}" y1="{height - margin_bottom}" '
            f'x2="{width - margin_right}" y2="{height - margin_bottom}" class="axis" />',
            f'<line x1="{margin_left}" y1="{zero_y:.2f}" '
            f'x2="{width - margin_right}" y2="{zero_y:.2f}" class="zero" />',
            *paths,
            *legend,
            f'<text x="{margin_left}" y="{height - 45}" class="axis-label">{start_label}</text>',
            f'<text x="{width - margin_right}" y="{height - 45}" class="axis-label" '
            f'text-anchor="end">{end_label}</text>',
            f'<text x="30" y="{margin_top + plot_height / 2:.2f}" class="axis-label" '
            'transform="rotate(-90 30 '
            f'{margin_top + plot_height / 2:.2f})">Cumulative net PnL after costs</text>',
            "</svg>",
        ]
    )


def _read_ledger(spec: StrategyLedgerSpec) -> pd.DataFrame:
    if not spec.ledger_path.exists():
        raise PnLReportingError(f"Ledger path does not exist: {spec.ledger_path}")
    ledger = pd.read_csv(spec.ledger_path)
    time_column = _time_column(ledger)
    required = [time_column, "equity_after", "event_cost", "position_after"]
    missing = [column for column in required if column not in ledger.columns]
    if missing:
        raise PnLReportingError(f"{spec.strategy_name} ledger is missing columns: {missing}")
    ledger = ledger.copy()
    ledger["event_time"] = pd.to_datetime(ledger[time_column], format="mixed")
    return ledger.sort_values("event_time", kind="mergesort").reset_index(drop=True)


def _ledger_to_curve(ledger: pd.DataFrame, *, strategy_name: str) -> pd.DataFrame:
    if ledger.empty:
        raise PnLReportingError(f"{strategy_name} ledger is empty.")
    equity = _cumulative_net_equity(ledger)
    curve = pd.DataFrame(
        {
            "strategy": strategy_name,
            "event_time": ledger["event_time"],
            "event_number": range(1, len(ledger) + 1),
            "equity_after": equity,
            "local_equity_after": pd.to_numeric(ledger["equity_after"], errors="coerce"),
            "pnl_measure": "cumulative_net_pnl_after_costs",
        }
    )
    start_row = pd.DataFrame(
        {
            "strategy": [strategy_name],
            "event_time": [curve["event_time"].iloc[0]],
            "event_number": [0],
            "equity_after": [0.0],
            "local_equity_after": [0.0],
            "pnl_measure": ["cumulative_net_pnl_after_costs"],
        }
    )
    curve = pd.concat([start_row, curve], ignore_index=True)
    curve["running_peak"] = curve["equity_after"].cummax()
    curve["drawdown"] = curve["running_peak"] - curve["equity_after"]
    return curve


def _summarize_curve(
    curve: pd.DataFrame,
    *,
    spec: StrategyLedgerSpec,
    ledger: pd.DataFrame,
) -> dict[str, object]:
    equity = pd.to_numeric(curve["equity_after"], errors="coerce")
    costs = pd.to_numeric(ledger["event_cost"], errors="coerce")
    positions = pd.to_numeric(ledger["position_after"], errors="coerce")
    final_equity = float(equity.iloc[-1])
    summary = _read_optional_summary(spec)
    order_count = len(ledger)
    cost = _summary_sum(summary, ("cost", "total_cost"), fallback=float(costs.sum()))
    net_pnl = _summary_sum(summary, ("net_pnl", "final_equity"), fallback=final_equity)
    gross_pnl = _summary_sum(summary, ("gross_pnl",), fallback=net_pnl + cost)
    num_trades = int(
        _summary_sum(summary, ("num_trades", "order_rows"), fallback=float(order_count))
    )
    num_position_changes = int(
        _summary_sum(
            summary,
            ("num_position_changes", "order_rows"),
            fallback=float(order_count),
        )
    )
    return {
        "strategy": spec.strategy_name,
        "ledger_path": str(spec.ledger_path),
        "summary_path": "" if spec.summary_path is None else str(spec.summary_path),
        "pnl_measure": "cumulative_net_pnl_after_costs",
        "order_count": order_count,
        "final_equity": final_equity,
        "gross_pnl": gross_pnl,
        "cost": cost,
        "net_pnl": net_pnl,
        "num_trades": num_trades,
        "num_position_changes": num_position_changes,
        "gross_per_trade": _safe_ratio(gross_pnl, num_trades),
        "cost_per_trade": _safe_ratio(cost, num_trades),
        "net_per_trade": _safe_ratio(net_pnl, num_trades),
        "selected_threshold_by_fold": _format_selected_by_fold(
            summary,
            ("selected_threshold", "score_threshold", "threshold_value"),
        ),
        "selected_cost_multiplier_by_fold": _format_selected_by_fold(
            summary,
            ("selected_cost_multiplier", "cost_multiplier"),
        ),
        "min_equity": float(equity.min()),
        "max_equity": float(equity.max()),
        "max_drawdown": float(curve["drawdown"].max()),
        "total_cost": cost,
        "final_position": float(positions.iloc[-1]),
        "mean_equity_per_event": float(equity.mean()),
        "final_equity_per_order": None if order_count == 0 else final_equity / order_count,
    }


def _read_optional_summary(spec: StrategyLedgerSpec) -> pd.DataFrame | None:
    if spec.summary_path is None or not spec.summary_path.exists():
        return None
    return pd.read_csv(spec.summary_path)


def _summary_sum(
    summary: pd.DataFrame | None,
    columns: tuple[str, ...],
    *,
    fallback: float,
) -> float:
    if summary is None or summary.empty:
        return fallback
    column = _first_existing_column(summary, columns)
    if column is None:
        return fallback
    return float(pd.to_numeric(summary[column], errors="coerce").fillna(0.0).sum())


def _format_selected_by_fold(
    summary: pd.DataFrame | None,
    columns: tuple[str, ...],
) -> str:
    if summary is None or summary.empty:
        return ""
    value_column = _first_existing_column(summary, columns)
    if value_column is None:
        return ""
    fold_column = "fold_id" if "fold_id" in summary.columns else None
    parts = []
    for index, row in summary.iterrows():
        value = row[value_column]
        if pd.isna(value):
            continue
        fold = str(row[fold_column]) if fold_column is not None else f"row_{index + 1:03d}"
        parts.append(f"{fold}={value}")
    return ";".join(parts)


def _first_existing_column(frame: pd.DataFrame, columns: tuple[str, ...]) -> str | None:
    for column in columns:
        if column in frame.columns:
            return column
    return None


def _safe_ratio(numerator: float, denominator: int) -> float | None:
    if denominator == 0:
        return None
    return numerator / denominator


def _cumulative_net_equity(ledger: pd.DataFrame) -> pd.Series:
    local_equity = pd.to_numeric(ledger["equity_after"], errors="coerce")
    segment_column = _segment_column(ledger)
    if segment_column is None:
        return local_equity.reset_index(drop=True)

    result = pd.Series(index=ledger.index, dtype="float64")
    offset = 0.0
    for _, group in ledger.groupby(segment_column, sort=False):
        group_equity = local_equity.loc[group.index]
        result.loc[group.index] = group_equity + offset
        if not group_equity.empty and not pd.isna(group_equity.iloc[-1]):
            offset += float(group_equity.iloc[-1])
    return result.reset_index(drop=True)


def _segment_column(frame: pd.DataFrame) -> str | None:
    for column in SEGMENT_COLUMNS:
        if column in frame.columns and frame[column].nunique(dropna=False) > 1:
            return column
    return None


def _time_column(frame: pd.DataFrame) -> str:
    for column in TIME_COLUMNS:
        if column in frame.columns:
            return column
    raise PnLReportingError(f"Ledger must include one of {TIME_COLUMNS}.")


def _validate_curve(curve: pd.DataFrame) -> None:
    missing = [
        column
        for column in ("strategy", "event_time", "equity_after")
        if column not in curve.columns
    ]
    if missing:
        raise PnLReportingError(f"Curve is missing columns: {missing}")
    if curve.empty:
        raise PnLReportingError("Curve is empty.")


def _downsample_curve(group: pd.DataFrame, *, max_points: int = 3000) -> pd.DataFrame:
    if len(group) <= max_points:
        return group
    step = len(group) / max_points
    indexes = sorted({min(len(group) - 1, int(i * step)) for i in range(max_points)})
    if len(group) - 1 not in indexes:
        indexes.append(len(group) - 1)
    return group.iloc[indexes]


def _linear_ticks(y_min: float, y_max: float, *, tick_count: int) -> list[float]:
    if tick_count < 2:
        return [y_min, y_max]
    step = (y_max - y_min) / (tick_count - 1)
    return [y_min + step * index for index in range(tick_count)]


def _summary_value(summary: pd.DataFrame, strategy: str, column: str) -> float:
    rows = summary.loc[summary["strategy"] == strategy]
    if rows.empty:
        return float("nan")
    return float(rows[column].iloc[0])
