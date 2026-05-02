"""File-based workflow for signal v1 outputs."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import json
from pathlib import Path
from typing import Iterator

import pandas as pd

from ..alignment import TRADING_DATE
from ..features.quotes import (
    MIDQUOTE,
    QUOTE_IMBALANCE,
    QUOTE_REVISION_BPS,
    QUOTED_SPREAD,
    RELATIVE_SPREAD,
)
from ..schema import EVENT_TIME, SYMBOL
from ..utils import DataSliceConfig
from .rules import (
    SIGNAL_POLICY_NOTE,
    SignalDiagnostics,
    SignalRuleConfig,
    build_sequential_gate_signals_v1,
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


class SignalWorkflowError(ValueError):
    """Raised when signal workflow inputs cannot be resolved."""


@dataclass(frozen=True)
class SignalInputPaths:
    """Input paths used for signal generation."""

    labeled_feature_path: Path
    quote_feature_path: Path


@dataclass(frozen=True)
class SignalOutputPaths:
    """Output paths written by the signal workflow."""

    signal_path: Path
    manifest_path: Path


@dataclass(frozen=True)
class SignalBuildResult:
    """Signal frame, paths, and diagnostics produced by the workflow."""

    signals: pd.DataFrame
    paths: SignalOutputPaths
    diagnostics: SignalDiagnostics


def find_signal_inputs(
    config: DataSliceConfig,
    *,
    processed_dir: str | Path | None = None,
) -> SignalInputPaths:
    """Find labeled feature and quote feature inputs for signal v1."""

    root = Path(processed_dir or config.storage["processed_dir"]) / config.slice_name
    labeled_feature_path = root / f"{config.slice_name}_labeled_features_v1.csv"
    quote_feature_path = root / f"{config.slice_name}_quote_features_v1.csv"
    missing_paths = [
        path for path in (labeled_feature_path, quote_feature_path) if not path.exists()
    ]
    if missing_paths:
        missing_list = ", ".join(str(path) for path in missing_paths)
        raise SignalWorkflowError(
            "Signal input file(s) are missing: "
            f"{missing_list}. Run scripts/build_labels.py and quote features first."
        )
    return SignalInputPaths(
        labeled_feature_path=labeled_feature_path,
        quote_feature_path=quote_feature_path,
    )


def build_signal_dataset(
    config: DataSliceConfig,
    *,
    processed_dir: str | Path | None = None,
    output_dir: str | Path | None = None,
    signal_config: SignalRuleConfig = SignalRuleConfig(),
) -> SignalBuildResult:
    """Build signal v1 output from labeled features and quote features."""

    inputs = find_signal_inputs(config, processed_dir=processed_dir)
    labeled_usecols = _signal_feature_usecols(inputs.labeled_feature_path, signal_config)
    output_paths = _signal_output_paths(config, output_dir=output_dir or processed_dir)
    output_paths.signal_path.parent.mkdir(parents=True, exist_ok=True)
    if output_paths.signal_path.exists():
        output_paths.signal_path.unlink()

    diagnostics: list[SignalDiagnostics] = []
    output_columns: tuple[str, ...] = ()
    wrote_header = False
    quote_iter = _iter_market_data_csv_by_trading_date(
        inputs.quote_feature_path,
        usecols=_quote_feature_usecols(),
    )
    quote_state = _next_date_frame(quote_iter)

    for trading_date, feature_rows in _iter_market_data_csv_by_trading_date(
        inputs.labeled_feature_path,
        usecols=labeled_usecols,
    ):
        while quote_state is not None and quote_state[0] < trading_date:
            quote_state = _next_date_frame(quote_iter)
        quote_features = (
            quote_state[1]
            if quote_state is not None and quote_state[0] == trading_date
            else _empty_quote_features()
        )
        signal_result = build_sequential_gate_signals_v1(
            feature_rows,
            quote_features,
            config=signal_config,
        )
        if not output_columns:
            output_columns = tuple(signal_result.signals.columns)
        signal_result.signals.to_csv(
            output_paths.signal_path,
            mode="a",
            header=not wrote_header,
            index=False,
        )
        wrote_header = True
        diagnostics.append(signal_result.diagnostics)
        if quote_state is not None and quote_state[0] == trading_date:
            quote_state = _next_date_frame(quote_iter)

    if not diagnostics:
        raise SignalWorkflowError("No labeled feature rows were available for signals.")

    combined_diagnostics = _combine_signal_diagnostics(diagnostics, config=signal_config)
    paths = _write_signal_outputs(
        config,
        inputs=inputs,
        paths=output_paths,
        signals=pd.DataFrame(columns=output_columns),
        diagnostics=combined_diagnostics,
        output_dir=output_dir or processed_dir,
        data_already_written=True,
    )
    return SignalBuildResult(
        signals=pd.DataFrame(columns=output_columns),
        paths=paths,
        diagnostics=combined_diagnostics,
    )


def _signal_feature_usecols(path: Path, config: SignalRuleConfig) -> tuple[str, ...]:
    columns = tuple(pd.read_csv(path, nrows=0).columns)
    wanted = [EVENT_TIME, SYMBOL, TRADING_DATE, *DOWNSTREAM_SIGNED_FLOW_COLUMNS]
    wanted.extend(
        column
        for column in columns
        if column.startswith("label_available_")
        or column.startswith("future_midquote_")
    )
    if config.signed_flow_column not in wanted:
        wanted.append(config.signed_flow_column)
    return tuple(column for column in dict.fromkeys(wanted) if column in columns)


def _quote_feature_usecols() -> tuple[str, ...]:
    return (
        EVENT_TIME,
        SYMBOL,
        TRADING_DATE,
        MIDQUOTE,
        QUOTE_IMBALANCE,
        QUOTE_REVISION_BPS,
        QUOTED_SPREAD,
        RELATIVE_SPREAD,
    )


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
                    raise SignalWorkflowError(
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
    frame = pd.DataFrame(columns=_quote_feature_usecols())
    frame[EVENT_TIME] = pd.to_datetime(pd.Series(dtype="object"))
    return frame


def _combine_signal_diagnostics(
    diagnostics: list[SignalDiagnostics],
    *,
    config: SignalRuleConfig,
) -> SignalDiagnostics:
    first = diagnostics[0]
    return SignalDiagnostics(
        input_feature_rows=sum(item.input_feature_rows for item in diagnostics),
        input_quote_rows=sum(item.input_quote_rows for item in diagnostics),
        output_signal_rows=sum(item.output_signal_rows for item in diagnostics),
        row_preserving=all(item.row_preserving for item in diagnostics),
        signal_rule=first.signal_rule,
        signal_columns=first.signal_columns,
        signal_group_keys=first.signal_group_keys,
        signed_flow_column=config.signed_flow_column,
        qi_threshold=config.qi_threshold,
        signed_flow_threshold=config.signed_flow_threshold,
        qr_threshold_bps=config.qr_threshold_bps,
        threshold_selection_policy=first.threshold_selection_policy,
        label_usage_policy=first.label_usage_policy,
        signal_session_policy=first.signal_session_policy,
        signal_input_available_rows=sum(
            item.signal_input_available_rows for item in diagnostics
        ),
        signal_input_missing_rows=sum(item.signal_input_missing_rows for item in diagnostics),
        long_signal_rows=sum(item.long_signal_rows for item in diagnostics),
        short_signal_rows=sum(item.short_signal_rows for item in diagnostics),
        no_trade_rows=sum(item.no_trade_rows for item in diagnostics),
        qi_long_rows=sum(item.qi_long_rows for item in diagnostics),
        qi_short_rows=sum(item.qi_short_rows for item in diagnostics),
        signed_flow_long_rows=sum(item.signed_flow_long_rows for item in diagnostics),
        signed_flow_short_rows=sum(item.signed_flow_short_rows for item in diagnostics),
        qr_long_rows=sum(item.qr_long_rows for item in diagnostics),
        qr_short_rows=sum(item.qr_short_rows for item in diagnostics),
        labels_retained=any(item.labels_retained for item in diagnostics),
        labels_used_for_signal=first.labels_used_for_signal,
        walk_forward_implemented=first.walk_forward_implemented,
        backtest_implemented=first.backtest_implemented,
        threshold_optimization_implemented=first.threshold_optimization_implemented,
        research_grade_strategy_sample=first.research_grade_strategy_sample,
    )


def _signal_output_paths(
    config: DataSliceConfig,
    *,
    output_dir: str | Path | None,
) -> SignalOutputPaths:
    output_root = Path(output_dir or config.storage["processed_dir"]) / config.slice_name
    return SignalOutputPaths(
        signal_path=output_root / f"{config.slice_name}_signals_v1.csv",
        manifest_path=output_root / f"{config.slice_name}_signals_v1_manifest.json",
    )


def _write_signal_outputs(
    config: DataSliceConfig,
    *,
    inputs: SignalInputPaths,
    paths: SignalOutputPaths | None = None,
    signals: pd.DataFrame,
    diagnostics: SignalDiagnostics,
    output_dir: str | Path | None,
    data_already_written: bool = False,
) -> SignalOutputPaths:
    paths = paths or _signal_output_paths(config, output_dir=output_dir)
    paths.signal_path.parent.mkdir(parents=True, exist_ok=True)

    if not data_already_written:
        signals.to_csv(paths.signal_path, index=False)
    manifest = {
        "slice_name": config.slice_name,
        "inputs": {
            "labeled_feature_path": str(inputs.labeled_feature_path),
            "quote_feature_path": str(inputs.quote_feature_path),
        },
        "outputs": {
            "signal_path": str(paths.signal_path),
            "manifest_path": str(paths.manifest_path),
        },
        "signal_status": {
            "signals_implemented": "v1",
            "signal_rule": diagnostics.signal_rule,
            "threshold_selection_policy": diagnostics.threshold_selection_policy,
            "label_usage_policy": diagnostics.label_usage_policy,
            "labels_used_for_signal": diagnostics.labels_used_for_signal,
            "walk_forward_implemented": diagnostics.walk_forward_implemented,
            "backtest_implemented": diagnostics.backtest_implemented,
            "threshold_optimization_implemented": (
                diagnostics.threshold_optimization_implemented
            ),
            "research_grade_strategy_sample": diagnostics.research_grade_strategy_sample,
        },
        "signal_scope_note": SIGNAL_POLICY_NOTE,
        "diagnostics": asdict(diagnostics),
    }
    paths.manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    return paths
