"""File-based workflow for future midquote label outputs."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import json
from pathlib import Path
from typing import Iterator

import pandas as pd

from ..alignment import TRADING_DATE
from ..features.quotes import MIDQUOTE
from ..schema import EVENT_TIME, SYMBOL
from ..utils import DataSliceConfig
from .midquote import (
    DEFAULT_DEAD_ZONE_BPS,
    DEFAULT_LABEL_HORIZONS,
    LABELING_SCOPE_NOTE,
    MidquoteLabelDiagnostics,
    build_midquote_labels_v1,
)

CSV_CHUNKSIZE: int = 500_000
DOWNSTREAM_SIGNED_FLOW_COLUMNS: tuple[str, ...] = (
    "signed_flow_imbalance_10_trades",
    "signed_flow_imbalance_50_trades",
    "signed_flow_imbalance_100_trades",
    "signed_flow_imbalance_100ms",
    "signed_flow_imbalance_500ms",
    "signed_flow_imbalance_1s",
)


class LabelingWorkflowError(ValueError):
    """Raised when labeling workflow inputs cannot be resolved."""


@dataclass(frozen=True)
class MidquoteLabelInputPaths:
    """Feature and quote input paths used for labeling."""

    signed_flow_feature_path: Path
    quote_feature_path: Path


@dataclass(frozen=True)
class MidquoteLabelOutputPaths:
    """Output paths written by the labeling workflow."""

    labeled_feature_path: Path
    manifest_path: Path


@dataclass(frozen=True)
class MidquoteLabelBuildResult:
    """Labeled features, paths, and diagnostics produced by the workflow."""

    labeled_features: pd.DataFrame
    paths: MidquoteLabelOutputPaths
    diagnostics: MidquoteLabelDiagnostics


def find_midquote_label_inputs(
    config: DataSliceConfig,
    *,
    processed_dir: str | Path | None = None,
) -> MidquoteLabelInputPaths:
    """Find signed-flow feature and quote-feature inputs for labeling."""

    root = Path(processed_dir or config.storage["processed_dir"]) / config.slice_name
    signed_flow_feature_path = root / f"{config.slice_name}_signed_flow_features_v1.csv"
    quote_feature_path = root / f"{config.slice_name}_quote_features_v1.csv"
    missing_paths = [
        path for path in (signed_flow_feature_path, quote_feature_path) if not path.exists()
    ]
    if missing_paths:
        missing_list = ", ".join(str(path) for path in missing_paths)
        raise LabelingWorkflowError(
            "Labeling input file(s) are missing: "
            f"{missing_list}. Run quote and signed-flow feature scripts first."
        )
    return MidquoteLabelInputPaths(
        signed_flow_feature_path=signed_flow_feature_path,
        quote_feature_path=quote_feature_path,
    )


def build_midquote_label_dataset(
    config: DataSliceConfig,
    *,
    processed_dir: str | Path | None = None,
    output_dir: str | Path | None = None,
    horizons: tuple[str, ...] = DEFAULT_LABEL_HORIZONS,
    dead_zone_bps: float = DEFAULT_DEAD_ZONE_BPS,
) -> MidquoteLabelBuildResult:
    """Build future midquote labels for signed-flow feature rows."""

    inputs = find_midquote_label_inputs(config, processed_dir=processed_dir)
    output_paths = _midquote_label_output_paths(
        config,
        output_dir=output_dir or processed_dir,
    )
    output_paths.labeled_feature_path.parent.mkdir(parents=True, exist_ok=True)
    if output_paths.labeled_feature_path.exists():
        output_paths.labeled_feature_path.unlink()

    diagnostics: list[MidquoteLabelDiagnostics] = []
    output_columns: tuple[str, ...] = ()
    wrote_header = False
    quote_iter = _iter_market_data_csv_by_trading_date(
        inputs.quote_feature_path,
        usecols=(EVENT_TIME, SYMBOL, TRADING_DATE, MIDQUOTE),
    )
    quote_state = _next_date_frame(quote_iter)

    for trading_date, feature_rows in _iter_market_data_csv_by_trading_date(
        inputs.signed_flow_feature_path,
        usecols=_label_feature_usecols(inputs.signed_flow_feature_path),
    ):
        while quote_state is not None and quote_state[0] < trading_date:
            quote_state = _next_date_frame(quote_iter)
        quote_features = (
            quote_state[1]
            if quote_state is not None and quote_state[0] == trading_date
            else _empty_quote_features()
        )
        label_result = build_midquote_labels_v1(
            feature_rows,
            quote_features,
            horizons=horizons,
            dead_zone_bps=dead_zone_bps,
        )
        if not output_columns:
            output_columns = tuple(label_result.labeled_features.columns)
        label_result.labeled_features.to_csv(
            output_paths.labeled_feature_path,
            mode="a",
            header=not wrote_header,
            index=False,
        )
        wrote_header = True
        diagnostics.append(label_result.diagnostics)
        if quote_state is not None and quote_state[0] == trading_date:
            quote_state = _next_date_frame(quote_iter)

    if not diagnostics:
        raise LabelingWorkflowError("No signed-flow feature rows were available for labeling.")

    combined_diagnostics = _combine_midquote_label_diagnostics(
        diagnostics,
        horizons=horizons,
        dead_zone_bps=dead_zone_bps,
    )
    paths = _write_midquote_label_outputs(
        config,
        inputs=inputs,
        paths=output_paths,
        labeled_features=pd.DataFrame(columns=output_columns),
        diagnostics=combined_diagnostics,
        output_dir=output_dir or processed_dir,
        data_already_written=True,
    )
    return MidquoteLabelBuildResult(
        labeled_features=pd.DataFrame(columns=output_columns),
        paths=paths,
        diagnostics=combined_diagnostics,
    )


def _label_feature_usecols(path: Path) -> tuple[str, ...]:
    columns = tuple(pd.read_csv(path, nrows=0).columns)
    wanted = (EVENT_TIME, SYMBOL, TRADING_DATE, *DOWNSTREAM_SIGNED_FLOW_COLUMNS)
    return tuple(column for column in wanted if column in columns)


def _read_market_data_csv(path: Path, *, usecols: tuple[str, ...] | None = None) -> pd.DataFrame:
    frame = pd.read_csv(path, usecols=usecols)
    frame[EVENT_TIME] = pd.to_datetime(frame[EVENT_TIME], format="mixed")
    return frame


def _iter_market_data_csv_by_trading_date(
    path: Path,
    *,
    usecols: tuple[str, ...],
    chunksize: int = CSV_CHUNKSIZE,
) -> Iterator[tuple[str, pd.DataFrame]]:
    current_date: str | None = None
    current_parts: list[pd.DataFrame] = []
    completed_dates: set[str] = set()

    for chunk in pd.read_csv(path, usecols=usecols, chunksize=chunksize):
        chunk[TRADING_DATE] = chunk[TRADING_DATE].astype(str)
        for trading_date, group in chunk.groupby(TRADING_DATE, sort=False):
            trading_date = str(trading_date)
            if current_date is None:
                current_date = trading_date
            if trading_date != current_date:
                completed_dates.add(current_date)
                yield current_date, _finalize_date_parts(current_parts)
                current_parts = []
                if trading_date in completed_dates:
                    raise LabelingWorkflowError(
                        f"Input file is not grouped by trading_date: {path}"
                    )
                current_date = trading_date
            current_parts.append(group.copy())

    if current_date is not None:
        yield current_date, _finalize_date_parts(current_parts)


def _finalize_date_parts(parts: list[pd.DataFrame]) -> pd.DataFrame:
    frame = pd.concat(parts, ignore_index=True)
    frame[EVENT_TIME] = pd.to_datetime(frame[EVENT_TIME], format="mixed")
    return frame


def _next_date_frame(
    iterator: Iterator[tuple[str, pd.DataFrame]],
) -> tuple[str, pd.DataFrame] | None:
    try:
        return next(iterator)
    except StopIteration:
        return None


def _empty_quote_features() -> pd.DataFrame:
    return pd.DataFrame(columns=[EVENT_TIME, SYMBOL, TRADING_DATE, MIDQUOTE]).assign(
        **{EVENT_TIME: pd.to_datetime(pd.Series(dtype="object"))}
    )


def _combine_midquote_label_diagnostics(
    diagnostics: list[MidquoteLabelDiagnostics],
    *,
    horizons: tuple[str, ...],
    dead_zone_bps: float,
) -> MidquoteLabelDiagnostics:
    first = diagnostics[0]
    return MidquoteLabelDiagnostics(
        input_feature_rows=sum(item.input_feature_rows for item in diagnostics),
        input_quote_rows=sum(item.input_quote_rows for item in diagnostics),
        output_labeled_rows=sum(item.output_labeled_rows for item in diagnostics),
        row_preserving=all(item.row_preserving for item in diagnostics),
        horizons=horizons,
        dead_zone_bps=dead_zone_bps,
        label_columns=first.label_columns,
        label_group_keys=first.label_group_keys,
        current_quote_policy=first.current_quote_policy,
        future_quote_policy=first.future_quote_policy,
        session_boundary_policy=first.session_boundary_policy,
        label_usage_policy=first.label_usage_policy,
        current_midquote_missing_rows=sum(
            item.current_midquote_missing_rows for item in diagnostics
        ),
        current_midquote_lag_missing_rows=sum(
            item.current_midquote_lag_missing_rows for item in diagnostics
        ),
        label_available_rows=_sum_dicts(item.label_available_rows for item in diagnostics),
        label_missing_rows=_sum_dicts(item.label_missing_rows for item in diagnostics),
        positive_direction_rows=_sum_dicts(
            item.positive_direction_rows for item in diagnostics
        ),
        flat_direction_rows=_sum_dicts(item.flat_direction_rows for item in diagnostics),
        negative_direction_rows=_sum_dicts(
            item.negative_direction_rows for item in diagnostics
        ),
        min_return_bps=_min_dicts(item.min_return_bps for item in diagnostics),
        median_return_bps=_median_of_partition_medians(
            item.median_return_bps for item in diagnostics
        ),
        max_return_bps=_max_dicts(item.max_return_bps for item in diagnostics),
        cross_session_label_count=sum(item.cross_session_label_count for item in diagnostics),
        signals_implemented=first.signals_implemented,
        walk_forward_implemented=first.walk_forward_implemented,
        backtest_implemented=first.backtest_implemented,
        research_grade_strategy_sample=first.research_grade_strategy_sample,
    )


def _sum_dicts(items: Iterator[dict[str, int]]) -> dict[str, int]:
    totals: dict[str, int] = {}
    for item in items:
        for key, value in item.items():
            totals[key] = totals.get(key, 0) + int(value)
    return totals


def _min_dicts(items: Iterator[dict[str, float | None]]) -> dict[str, float | None]:
    values: dict[str, list[float]] = {}
    for item in items:
        for key, value in item.items():
            if value is not None:
                values.setdefault(key, []).append(float(value))
    return {key: min(current) if current else None for key, current in values.items()}


def _max_dicts(items: Iterator[dict[str, float | None]]) -> dict[str, float | None]:
    values: dict[str, list[float]] = {}
    for item in items:
        for key, value in item.items():
            if value is not None:
                values.setdefault(key, []).append(float(value))
    return {key: max(current) if current else None for key, current in values.items()}


def _median_of_partition_medians(
    items: Iterator[dict[str, float | None]],
) -> dict[str, float | None]:
    values: dict[str, list[float]] = {}
    for item in items:
        for key, value in item.items():
            if value is not None:
                values.setdefault(key, []).append(float(value))
    return {
        key: float(pd.Series(current).median()) if current else None
        for key, current in values.items()
    }


def _midquote_label_output_paths(
    config: DataSliceConfig,
    *,
    output_dir: str | Path | None,
) -> MidquoteLabelOutputPaths:
    output_root = Path(output_dir or config.storage["processed_dir"]) / config.slice_name
    return MidquoteLabelOutputPaths(
        labeled_feature_path=output_root / f"{config.slice_name}_labeled_features_v1.csv",
        manifest_path=output_root / f"{config.slice_name}_labeling_v1_manifest.json",
    )


def _write_midquote_label_outputs(
    config: DataSliceConfig,
    *,
    inputs: MidquoteLabelInputPaths,
    paths: MidquoteLabelOutputPaths | None = None,
    labeled_features: pd.DataFrame,
    diagnostics: MidquoteLabelDiagnostics,
    output_dir: str | Path | None,
    data_already_written: bool = False,
) -> MidquoteLabelOutputPaths:
    paths = paths or _midquote_label_output_paths(config, output_dir=output_dir)
    paths.labeled_feature_path.parent.mkdir(parents=True, exist_ok=True)

    if not data_already_written:
        labeled_features.to_csv(paths.labeled_feature_path, index=False)
    manifest = {
        "slice_name": config.slice_name,
        "inputs": {
            "signed_flow_feature_path": str(inputs.signed_flow_feature_path),
            "quote_feature_path": str(inputs.quote_feature_path),
        },
        "outputs": {
            "labeled_feature_path": str(paths.labeled_feature_path),
            "manifest_path": str(paths.manifest_path),
        },
        "labeling_status": {
            "labeling_implemented": "v1",
            "current_quote_policy": diagnostics.current_quote_policy,
            "future_quote_policy": diagnostics.future_quote_policy,
            "session_boundary_policy": diagnostics.session_boundary_policy,
            "label_usage_policy": diagnostics.label_usage_policy,
            "signals_implemented": diagnostics.signals_implemented,
            "walk_forward_implemented": diagnostics.walk_forward_implemented,
            "backtest_implemented": diagnostics.backtest_implemented,
            "research_grade_strategy_sample": diagnostics.research_grade_strategy_sample,
        },
        "labeling_scope_note": LABELING_SCOPE_NOTE,
        "diagnostics": asdict(diagnostics),
    }
    paths.manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    return paths
