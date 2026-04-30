"""Build normalized and cleaned datasets from raw WRDS extracts."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import json
from pathlib import Path

import pandas as pd

from ..adapters import normalize_wrds_quotes, normalize_wrds_trades
from ..cleaning import (
    CleaningRuleDiagnostics,
    QuoteQualityWarnings,
    ScopeFilterDiagnostics,
    TradeQualityWarnings,
    clean_quotes_v2,
    clean_trades_v2,
    filter_frame_to_scope,
    summarize_quote_quality_warnings,
    summarize_trade_quality_warnings,
)
from ..utils import DataSliceConfig

CLEANING_STATUS: dict[str, bool | str] = {
    "quote_cleaning": "partial_v2",
    "trade_cleaning": "partial_v2",
    "condition_filters_finalized": False,
    "alignment_implemented": False,
    "trade_signing_implemented": False,
    "research_grade_sample": False,
}

CLEANING_POLICY: dict[str, str] = {
    "current_design": "drop_only_one_pass_cleaning_after_scope_filter",
    "row_conservation": "cleaned_rows_plus_rejected_rows_equals_scoped_rows_for_current_design",
    "crossed_market_policy": "quotes_with_ask_less_than_bid_are_dropped_by_Q003_crossed_market",
    "locked_market_policy": "quotes_with_ask_equal_to_bid_are_retained_and_counted_in_quote_quality_warnings",
    "condition_code_policy": "nbbo_quote_condition_and_sale_condition_are_diagnostic_only_until_eligibility_is_finalized",
}

UNRESOLVED_DATA_ASSUMPTIONS: tuple[dict[str, str], ...] = (
    {
        "id": "UA001_quote_size_unit",
        "field": "best_bidsiz/best_asksiz",
        "issue": "WRDS NBBOM quote-size unit has not been independently verified.",
        "impact": "QI magnitude may be misinterpreted if the unit is wrong.",
        "blocking_for": "absolute QI threshold interpretation and depth-impact analysis",
        "not_blocking_for": "raw extraction, schema validation, relative QI prototypes",
    },
    {
        "id": "UA002_nbbo_condition_eligibility",
        "field": "nbbo_quote_condition",
        "issue": "Eligible NBBO condition set is not finalized.",
        "impact": "Some quote states may be unsuitable for final research samples.",
        "blocking_for": "research-grade quote sample and final backtests",
        "not_blocking_for": "diagnostic distributions and feature pipeline prototypes",
    },
    {
        "id": "UA003_sale_condition_eligibility",
        "field": "sale_condition",
        "issue": "Sale-condition eligibility set for trade signing is not finalized.",
        "impact": "Auction, cross, odd-lot, or special-condition trades may affect signing.",
        "blocking_for": "research-grade trade sample, trade signing, OFI involving trades",
        "not_blocking_for": "raw extraction and quote-only feature prototypes",
    },
    {
        "id": "UA004_quote_trade_alignment",
        "field": "quote_trade_alignment",
        "issue": "Quote-trade lag and trade-signing policy are not part of cleaning.",
        "impact": "Trade-based features must apply a separate as-of alignment contract.",
        "blocking_for": "trade signing and OFI features using trades",
        "not_blocking_for": "clean quote/trade table generation",
    },
)


class DatasetBuildError(ValueError):
    """Raised when a dataset build request cannot be completed."""


@dataclass(frozen=True)
class WrdsRawInputPaths:
    """Raw WRDS quote and trade CSV paths used by the dataset builder."""

    quote_path: Path
    trade_path: Path


@dataclass(frozen=True)
class DatasetBuildOutputPaths:
    """Output paths written by a dataset build."""

    normalized_quote_path: Path
    normalized_trade_path: Path
    cleaned_quote_path: Path
    cleaned_trade_path: Path
    rejected_quote_path: Path
    rejected_trade_path: Path
    manifest_path: Path


@dataclass(frozen=True)
class DatasetBuildDiagnostics:
    """Diagnostics emitted while building normalized and cleaned datasets."""

    raw_quote_rows: int
    raw_trade_rows: int
    normalized_quote_rows: int
    normalized_trade_rows: int
    scoped_quote_rows: int
    scoped_trade_rows: int
    cleaned_quote_rows: int
    cleaned_trade_rows: int
    rejected_quote_rows: int
    rejected_trade_rows: int
    quote_scope: ScopeFilterDiagnostics
    trade_scope: ScopeFilterDiagnostics
    quote_filters: tuple[CleaningRuleDiagnostics, ...]
    trade_filters: tuple[CleaningRuleDiagnostics, ...]
    quote_quality_warnings: QuoteQualityWarnings
    trade_quality_warnings: TradeQualityWarnings
    nbbo_quote_condition_distribution: dict[str, int]
    sale_condition_distribution: dict[str, int]
    unresolved_data_assumptions: tuple[dict[str, str], ...]


@dataclass(frozen=True)
class DatasetBuildResult:
    """Frames, paths, and diagnostics produced by a dataset build."""

    normalized_quotes: pd.DataFrame
    normalized_trades: pd.DataFrame
    cleaned_quotes: pd.DataFrame
    cleaned_trades: pd.DataFrame
    rejected_quotes: pd.DataFrame
    rejected_trades: pd.DataFrame
    paths: DatasetBuildOutputPaths
    diagnostics: DatasetBuildDiagnostics


def default_wrds_raw_input_dir(config: DataSliceConfig) -> Path:
    """Return the preferred raw WRDS input directory for a configured slice."""

    raw_root = Path(config.storage["raw_dir"])
    slice_dir = raw_root / config.slice_name
    if slice_dir.exists():
        return slice_dir
    return raw_root


def find_wrds_raw_input_paths(
    config: DataSliceConfig,
    *,
    raw_dir: str | Path | None = None,
) -> WrdsRawInputPaths:
    """Find raw WRDS quote and trade CSV files for a configured slice."""

    root = Path(raw_dir) if raw_dir is not None else default_wrds_raw_input_dir(config)
    quote_path = root / f"{config.slice_name}_quotes_raw.csv"
    trade_path = root / f"{config.slice_name}_trades_raw.csv"

    missing_paths = [path for path in (quote_path, trade_path) if not path.exists()]
    if missing_paths:
        missing_list = ", ".join(str(path) for path in missing_paths)
        raise DatasetBuildError(
            "Raw WRDS input file(s) are missing: "
            f"{missing_list}. Run scripts/extract_wrds.py first or pass --raw-dir."
        )

    return WrdsRawInputPaths(quote_path=quote_path, trade_path=trade_path)


def build_dataset_from_wrds_raw(
    config: DataSliceConfig,
    *,
    raw_dir: str | Path | None = None,
    interim_dir: str | Path | None = None,
    processed_dir: str | Path | None = None,
) -> DatasetBuildResult:
    """Normalize, scope-filter, clean, and write WRDS quote/trade datasets."""

    raw_paths = find_wrds_raw_input_paths(config, raw_dir=raw_dir)
    raw_quotes = pd.read_csv(raw_paths.quote_path)
    raw_trades = pd.read_csv(raw_paths.trade_path)

    normalized_quotes = normalize_wrds_quotes(raw_quotes, config=config)
    normalized_trades = normalize_wrds_trades(raw_trades, config=config)
    normalized_quotes.insert(0, "raw_row_index", raw_quotes.index)
    normalized_trades.insert(0, "raw_row_index", raw_trades.index)

    scoped_quotes, quote_scope = filter_frame_to_scope(
        normalized_quotes,
        symbols=config.symbols,
        trading_dates=config.time_range.trading_dates,
        market_open=config.time_range.market_open,
        market_close=config.time_range.market_close,
        market_timezone=config.time_range.timezone,
    )
    scoped_trades, trade_scope = filter_frame_to_scope(
        normalized_trades,
        symbols=config.symbols,
        trading_dates=config.time_range.trading_dates,
        market_open=config.time_range.market_open,
        market_close=config.time_range.market_close,
        market_timezone=config.time_range.timezone,
    )

    quote_quality_warnings = summarize_quote_quality_warnings(scoped_quotes)
    trade_quality_warnings = summarize_trade_quality_warnings(scoped_trades)
    nbbo_quote_condition_distribution = condition_distribution(
        scoped_quotes,
        "nbbo_quote_condition",
    )
    sale_condition_distribution = condition_distribution(
        scoped_trades,
        "sale_condition",
    )

    quote_cleaning = clean_quotes_v2(scoped_quotes)
    trade_cleaning = clean_trades_v2(scoped_trades)

    paths = _write_dataset_outputs(
        config,
        normalized_quotes=normalized_quotes,
        normalized_trades=normalized_trades,
        cleaned_quotes=quote_cleaning.cleaned,
        cleaned_trades=trade_cleaning.cleaned,
        rejected_quotes=quote_cleaning.rejected,
        rejected_trades=trade_cleaning.rejected,
        interim_dir=interim_dir,
        processed_dir=processed_dir,
    )

    diagnostics = DatasetBuildDiagnostics(
        raw_quote_rows=len(raw_quotes),
        raw_trade_rows=len(raw_trades),
        normalized_quote_rows=len(normalized_quotes),
        normalized_trade_rows=len(normalized_trades),
        scoped_quote_rows=len(scoped_quotes),
        scoped_trade_rows=len(scoped_trades),
        cleaned_quote_rows=len(quote_cleaning.cleaned),
        cleaned_trade_rows=len(trade_cleaning.cleaned),
        rejected_quote_rows=len(quote_cleaning.rejected),
        rejected_trade_rows=len(trade_cleaning.rejected),
        quote_scope=quote_scope,
        trade_scope=trade_scope,
        quote_filters=quote_cleaning.diagnostics,
        trade_filters=trade_cleaning.diagnostics,
        quote_quality_warnings=quote_quality_warnings,
        trade_quality_warnings=trade_quality_warnings,
        nbbo_quote_condition_distribution=nbbo_quote_condition_distribution,
        sale_condition_distribution=sale_condition_distribution,
        unresolved_data_assumptions=UNRESOLVED_DATA_ASSUMPTIONS,
    )
    _write_manifest(config, raw_paths=raw_paths, paths=paths, diagnostics=diagnostics)

    return DatasetBuildResult(
        normalized_quotes=normalized_quotes,
        normalized_trades=normalized_trades,
        cleaned_quotes=quote_cleaning.cleaned,
        cleaned_trades=trade_cleaning.cleaned,
        rejected_quotes=quote_cleaning.rejected,
        rejected_trades=trade_cleaning.rejected,
        paths=paths,
        diagnostics=diagnostics,
    )


def _write_dataset_outputs(
    config: DataSliceConfig,
    *,
    normalized_quotes: pd.DataFrame,
    normalized_trades: pd.DataFrame,
    cleaned_quotes: pd.DataFrame,
    cleaned_trades: pd.DataFrame,
    rejected_quotes: pd.DataFrame,
    rejected_trades: pd.DataFrame,
    interim_dir: str | Path | None,
    processed_dir: str | Path | None,
) -> DatasetBuildOutputPaths:
    interim_root = Path(interim_dir or config.storage["interim_dir"]) / config.slice_name
    processed_root = Path(processed_dir or config.storage["processed_dir"]) / config.slice_name
    interim_root.mkdir(parents=True, exist_ok=True)
    processed_root.mkdir(parents=True, exist_ok=True)

    normalized_quote_path = interim_root / f"{config.slice_name}_quotes_normalized.csv"
    normalized_trade_path = interim_root / f"{config.slice_name}_trades_normalized.csv"
    cleaned_quote_path = processed_root / f"{config.slice_name}_quotes_clean.csv"
    cleaned_trade_path = processed_root / f"{config.slice_name}_trades_clean.csv"
    rejected_quote_path = processed_root / f"{config.slice_name}_quotes_rejected.csv"
    rejected_trade_path = processed_root / f"{config.slice_name}_trades_rejected.csv"
    manifest_path = processed_root / f"{config.slice_name}_dataset_manifest.json"

    normalized_quotes.to_csv(normalized_quote_path, index=False)
    normalized_trades.to_csv(normalized_trade_path, index=False)
    cleaned_quotes.to_csv(cleaned_quote_path, index=False)
    cleaned_trades.to_csv(cleaned_trade_path, index=False)
    rejected_quotes.to_csv(rejected_quote_path, index=False)
    rejected_trades.to_csv(rejected_trade_path, index=False)

    return DatasetBuildOutputPaths(
        normalized_quote_path=normalized_quote_path,
        normalized_trade_path=normalized_trade_path,
        cleaned_quote_path=cleaned_quote_path,
        cleaned_trade_path=cleaned_trade_path,
        rejected_quote_path=rejected_quote_path,
        rejected_trade_path=rejected_trade_path,
        manifest_path=manifest_path,
    )


def _write_manifest(
    config: DataSliceConfig,
    *,
    raw_paths: WrdsRawInputPaths,
    paths: DatasetBuildOutputPaths,
    diagnostics: DatasetBuildDiagnostics,
) -> None:
    manifest = {
        "slice_name": config.slice_name,
        "raw_inputs": {
            "quote_path": str(raw_paths.quote_path),
            "trade_path": str(raw_paths.trade_path),
        },
        "outputs": {
            "normalized_quote_path": str(paths.normalized_quote_path),
            "normalized_trade_path": str(paths.normalized_trade_path),
            "cleaned_quote_path": str(paths.cleaned_quote_path),
            "cleaned_trade_path": str(paths.cleaned_trade_path),
            "rejected_quote_path": str(paths.rejected_quote_path),
            "rejected_trade_path": str(paths.rejected_trade_path),
            "manifest_path": str(paths.manifest_path),
        },
        "cleaning_status": CLEANING_STATUS,
        "cleaning_policy": CLEANING_POLICY,
        "diagnostics": asdict(diagnostics),
    }
    paths.manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")


def condition_distribution(frame: pd.DataFrame, column: str) -> dict[str, int]:
    """Return a JSON-stable distribution for a condition-code column."""

    if column not in frame.columns:
        return {}

    values = frame[column].astype("string").fillna("<NA>").str.strip()
    values = values.where(values != "", "<EMPTY>")
    counts = values.value_counts(dropna=False).sort_index()
    return {str(key): int(value) for key, value in counts.items()}
