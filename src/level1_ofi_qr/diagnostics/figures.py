"""SVG figures for microstructure strategy diagnostics."""

from __future__ import annotations

from dataclasses import dataclass
from html import escape
from pathlib import Path

import pandas as pd

from .microstructure import MicrostructureDiagnosticsResult


@dataclass(frozen=True)
class MicrostructureFigurePaths:
    """Figure paths written for microstructure diagnostics."""

    strategy_variants_svg_path: Path
    horizon_svg_path: Path
    execution_svg_path: Path
    spread_breakdown_svg_path: Path


def write_microstructure_figures(
    result: MicrostructureDiagnosticsResult,
    *,
    slice_name: str,
    figures_dir: str | Path,
) -> MicrostructureFigurePaths:
    """Write SVG figures under outputs/figures."""

    root = Path(figures_dir)
    root.mkdir(parents=True, exist_ok=True)
    paths = MicrostructureFigurePaths(
        strategy_variants_svg_path=root
        / f"{slice_name}_cost_aware_microstructure_strategy_v2_variants.svg",
        horizon_svg_path=root / f"{slice_name}_cost_aware_microstructure_horizon.svg",
        execution_svg_path=root / f"{slice_name}_cost_aware_microstructure_execution.svg",
        spread_breakdown_svg_path=root
        / f"{slice_name}_cost_aware_microstructure_spread_breakdown.svg",
    )
    paths.strategy_variants_svg_path.write_text(
        _bar_svg(
            result.strategy_variants,
            label_column="variant",
            value_column="net_pnl",
            title=f"{slice_name} Microstructure Strategy V2 Diagnostics",
            subtitle="Diagnostic only: state gates and passive execution are not production selection.",
            y_label="Net PnL",
            color="#2563eb",
        ),
        encoding="utf-8",
    )
    paths.horizon_svg_path.write_text(
        _bar_svg(
            result.horizon_summary,
            label_column="horizon",
            value_column="net_per_round_trip",
            title=f"{slice_name} Horizon / Exit Diagnostic",
            subtitle="Average round-trip net PnL by fixed markout horizon.",
            y_label="Net PnL per round trip",
            color="#059669",
        ),
        encoding="utf-8",
    )
    paths.execution_svg_path.write_text(
        _bar_svg(
            result.execution,
            label_column="execution_scenario",
            value_column="net_per_attempt",
            title=f"{slice_name} Execution Diagnostic",
            subtitle="Conservative passive fills are research assumptions, not live-fill claims.",
            y_label="Net PnL per attempted round trip",
            color="#7c3aed",
        ),
        encoding="utf-8",
    )
    spread = result.breakdown.loc[result.breakdown["breakdown_type"] == "spread_bucket"]
    paths.spread_breakdown_svg_path.write_text(
        _bar_svg(
            spread,
            label_column="bucket",
            value_column="net_pnl",
            title=f"{slice_name} Spread-State Diagnostic",
            subtitle="Net PnL by selected-trade spread bucket.",
            y_label="Net PnL",
            color="#dc2626",
        ),
        encoding="utf-8",
    )
    return paths


def _bar_svg(
    frame: pd.DataFrame,
    *,
    label_column: str,
    value_column: str,
    title: str,
    subtitle: str,
    y_label: str,
    color: str,
    width: int = 1280,
    height: int = 720,
) -> str:
    if frame.empty:
        return _empty_svg(title=title, subtitle="No rows available.", width=width, height=height)

    rows = frame.loc[:, [label_column, value_column]].copy()
    rows[value_column] = pd.to_numeric(rows[value_column], errors="coerce").fillna(0.0)
    labels = [str(value) for value in rows[label_column]]
    values = [float(value) for value in rows[value_column]]
    margin_left = 96
    margin_right = 36
    margin_top = 82
    margin_bottom = 180
    plot_width = width - margin_left - margin_right
    plot_height = height - margin_top - margin_bottom
    y_min = min(0.0, min(values))
    y_max = max(0.0, max(values))
    if y_min == y_max:
        y_min -= 1.0
        y_max += 1.0
    padding = max((y_max - y_min) * 0.12, 0.001)
    y_min -= padding
    y_max += padding

    def y_pos(value: float) -> float:
        return margin_top + (y_max - value) / (y_max - y_min) * plot_height

    zero_y = y_pos(0.0)
    gap = 14
    bar_width = max((plot_width - gap * (len(values) + 1)) / max(len(values), 1), 10)
    bars = []
    labels_svg = []
    for index, (label, value) in enumerate(zip(labels, values, strict=True)):
        x = margin_left + gap + index * (bar_width + gap)
        y = min(y_pos(value), zero_y)
        bar_height = abs(zero_y - y_pos(value))
        fill = color if value >= 0 else "#ef4444"
        bars.append(
            f'<rect x="{x:.2f}" y="{y:.2f}" width="{bar_width:.2f}" '
            f'height="{bar_height:.2f}" fill="{fill}" />'
        )
        labels_svg.append(
            f'<text x="{x + bar_width / 2:.2f}" y="{height - 54}" '
            f'class="x-label" text-anchor="end" '
            f'transform="rotate(-35 {x + bar_width / 2:.2f} {height - 54})">'
            f'{escape(_short_label(label))}</text>'
        )
        labels_svg.append(
            f'<text x="{x + bar_width / 2:.2f}" y="{y - 8 if value >= 0 else y + bar_height + 16:.2f}" '
            f'class="value" text-anchor="middle">{value:.4f}</text>'
        )

    grid = []
    for tick in _ticks(y_min, y_max, 6):
        y = y_pos(tick)
        grid.append(
            f'<line x1="{margin_left}" y1="{y:.2f}" x2="{width - margin_right}" '
            f'y2="{y:.2f}" class="grid" />'
            f'<text x="{margin_left - 12}" y="{y + 4:.2f}" class="axis-label" '
            f'text-anchor="end">{tick:.3f}</text>'
        )

    return "\n".join(
        [
            f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
            f'viewBox="0 0 {width} {height}" role="img">',
            "<style>",
            "text { font-family: Arial, sans-serif; fill: #111827; }",
            ".title { font-size: 22px; font-weight: 700; }",
            ".subtitle { font-size: 13px; fill: #4b5563; }",
            ".axis-label, .x-label { font-size: 12px; fill: #4b5563; }",
            ".value { font-size: 11px; fill: #111827; }",
            ".grid { stroke: #e5e7eb; stroke-width: 1; }",
            ".axis { stroke: #374151; stroke-width: 1.2; }",
            ".zero { stroke: #111827; stroke-width: 1.4; stroke-dasharray: 5 5; }",
            "</style>",
            '<rect x="0" y="0" width="100%" height="100%" fill="#ffffff" />',
            f'<text x="{margin_left}" y="38" class="title">{escape(title)}</text>',
            f'<text x="{margin_left}" y="61" class="subtitle">{escape(subtitle)}</text>',
            *grid,
            f'<line x1="{margin_left}" y1="{margin_top}" x2="{margin_left}" '
            f'y2="{height - margin_bottom}" class="axis" />',
            f'<line x1="{margin_left}" y1="{zero_y:.2f}" '
            f'x2="{width - margin_right}" y2="{zero_y:.2f}" class="zero" />',
            *bars,
            *labels_svg,
            f'<text x="30" y="{margin_top + plot_height / 2:.2f}" class="axis-label" '
            f'transform="rotate(-90 30 {margin_top + plot_height / 2:.2f})">'
            f'{escape(y_label)}</text>',
            "</svg>",
        ]
    )


def _empty_svg(*, title: str, subtitle: str, width: int, height: int) -> str:
    return "\n".join(
        [
            f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
            f'viewBox="0 0 {width} {height}" role="img">',
            '<rect x="0" y="0" width="100%" height="100%" fill="#ffffff" />',
            f'<text x="64" y="52" font-family="Arial" font-size="22" '
            f'font-weight="700">{escape(title)}</text>',
            f'<text x="64" y="82" font-family="Arial" font-size="13" '
            f'fill="#4b5563">{escape(subtitle)}</text>',
            "</svg>",
        ]
    )


def _ticks(min_value: float, max_value: float, count: int) -> list[float]:
    if count <= 1:
        return [min_value]
    step = (max_value - min_value) / (count - 1)
    return [min_value + step * index for index in range(count)]


def _short_label(value: str, max_length: int = 34) -> str:
    if len(value) <= max_length:
        return value
    return value[: max_length - 1] + "..."
