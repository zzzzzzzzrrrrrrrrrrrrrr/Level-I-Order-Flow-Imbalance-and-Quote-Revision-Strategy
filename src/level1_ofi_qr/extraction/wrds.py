"""WRDS TAQ extraction helpers."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, time
import json
import os
from pathlib import Path
import re
from typing import Any, Literal, Mapping, Protocol, Sequence

import pandas as pd

from ..adapters.wrds_common import (
    WRDS_EVENT_TIME_EXPRESSION,
    WRDS_SYMBOL_EXPRESSION,
    WrdsMappingRule,
)
from ..schema import QUOTE_COLUMNS, SOURCE, TRADE_COLUMNS
from ..utils import DataSliceConfig

WRDS_TAQ_LIBRARY = "taqmsec"
WRDS_QUOTE_TABLE_PREFIX = "nbbom"
WRDS_TRADE_TABLE_PREFIX = "ctm"

WrdsTableKind = Literal["quotes", "trades"]

_SQL_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_SQL_TABLE_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*$")
_WRDS_SYMBOL_COMPONENT_RE = re.compile(r"^[A-Z0-9]+$")


class WrdsExtractionError(ValueError):
    """Raised when a WRDS extraction request cannot be built or executed."""


class WrdsRawSqlConnection(Protocol):
    """Minimal protocol needed from wrds.Connection."""

    def raw_sql(self, sql: str) -> pd.DataFrame:
        """Execute a SQL query and return a data frame."""


class WrdsMetadataConnection(Protocol):
    """Minimal protocol needed for WRDS metadata inspection."""

    def list_tables(self, library: str) -> list[str]:
        """List tables in a WRDS library."""


@dataclass(frozen=True)
class WrdsConnectionOptions:
    """Connection options for WRDS without storing credentials in project files."""

    username: str | None = None
    password: str | None = None
    username_env_var: str = "WRDS_USERNAME"
    password_env_var: str = "WRDS_PASSWORD"
    verbose: bool = False
    sslmode: str = "require"
    gssencmode: str = "disable"
    connect_timeout: int = 10


@dataclass(frozen=True)
class WrdsQuerySpec:
    """One SQL query against one WRDS TAQ daily table."""

    kind: WrdsTableKind
    trading_date: date
    table_identifier: str
    columns: tuple[str, ...]
    sql: str


@dataclass(frozen=True)
class WrdsQueryDiagnostics:
    """Row-count diagnostics for one WRDS query."""

    kind: WrdsTableKind
    trading_date: str
    table_identifier: str
    rows: int
    sql: str


@dataclass(frozen=True)
class WrdsExtractionDiagnostics:
    """Row-count diagnostics for a WRDS extraction run."""

    quote_rows: int
    trade_rows: int
    queries: tuple[WrdsQueryDiagnostics, ...]


@dataclass(frozen=True)
class WrdsRawExtractionResult:
    """Raw WRDS quote and trade frames plus extraction diagnostics."""

    quotes: pd.DataFrame
    trades: pd.DataFrame
    diagnostics: WrdsExtractionDiagnostics


@dataclass(frozen=True)
class WrdsRawOutputPaths:
    """Paths written by a raw WRDS extraction run."""

    quote_path: Path | None
    trade_path: Path | None
    manifest_path: Path


def connect_to_wrds(options: WrdsConnectionOptions | None = None) -> Any:
    """Create a WRDS connection using environment-backed credentials when supplied."""

    options = options or WrdsConnectionOptions()
    try:
        import wrds
    except ImportError as exc:  # pragma: no cover - exercised only in missing dependency environments
        raise WrdsExtractionError("The 'wrds' package is required for live WRDS extraction.") from exc

    username = options.username or os.getenv(options.username_env_var)
    password = options.password or os.getenv(options.password_env_var)
    connection_kwargs: dict[str, Any] = {
        "verbose": options.verbose,
        "wrds_connect_args": {
            "sslmode": options.sslmode,
            "gssencmode": options.gssencmode,
            "connect_timeout": options.connect_timeout,
        },
    }
    if username:
        connection_kwargs["wrds_username"] = username
    if password:
        connection_kwargs["wrds_password"] = password

    return wrds.Connection(**connection_kwargs)


def build_wrds_table_identifier(
    table_prefix: str,
    trading_date: date,
    *,
    library: str = WRDS_TAQ_LIBRARY,
) -> str:
    """Build a fully-qualified WRDS TAQ daily table identifier."""

    _validate_sql_identifier(library, "WRDS library")
    _validate_sql_identifier(table_prefix, "WRDS table prefix")
    return f"{library}.{table_prefix}_{trading_date:%Y%m%d}"


def wrds_table_prefix_for_kind(config: DataSliceConfig, kind: WrdsTableKind) -> str:
    """Return the configured WRDS daily table prefix for quote or trade extraction."""

    if kind == "quotes":
        source_key = "quote_dataset"
        default_prefix = WRDS_QUOTE_TABLE_PREFIX
    else:
        source_key = "trade_dataset"
        default_prefix = WRDS_TRADE_TABLE_PREFIX

    table_prefix = str(config.source.get(source_key, default_prefix)).strip().lower()
    _validate_sql_identifier(table_prefix, f"WRDS {kind} table prefix")
    return table_prefix


def wrds_raw_columns_for_mapping(
    mapping: Mapping[str, WrdsMappingRule],
    output_columns: Sequence[str],
) -> tuple[str, ...]:
    """Return raw WRDS columns needed to materialize normalized output columns."""

    raw_columns: list[str] = []
    for target_column in output_columns:
        if target_column not in mapping:
            raise WrdsExtractionError(
                f"Config mapping is missing a rule for normalized column '{target_column}'."
            )

        raw_mapping_value = mapping[target_column]
        if raw_mapping_value is None:
            continue

        if isinstance(raw_mapping_value, dict):
            _append_raw_columns_for_structured_mapping(raw_columns, target_column, raw_mapping_value)
            continue

        mapping_value = raw_mapping_value.strip()
        if target_column == SOURCE:
            continue

        if mapping_value == WRDS_EVENT_TIME_EXPRESSION:
            _append_unique(raw_columns, "date")
            _append_unique(raw_columns, "time_m")
        elif mapping_value == WRDS_SYMBOL_EXPRESSION:
            _append_unique(raw_columns, "sym_root")
            _append_unique(raw_columns, "sym_suffix")
        else:
            _validate_sql_identifier(mapping_value, f"source column for {target_column}")
            _append_unique(raw_columns, mapping_value)

    return tuple(raw_columns)


def _append_raw_columns_for_structured_mapping(
    raw_columns: list[str],
    target_column: str,
    mapping_rule: Mapping[str, Any],
) -> None:
    if "literal" in mapping_rule:
        return

    expression = str(mapping_rule.get("expr", "")).strip()
    if expression == "combine_date_time":
        date_column = str(mapping_rule.get("date_col", "date"))
        time_column = str(mapping_rule.get("time_col", "time_m"))
        _validate_sql_identifier(date_column, f"date column for {target_column}")
        _validate_sql_identifier(time_column, f"time column for {target_column}")
        _append_unique(raw_columns, date_column)
        _append_unique(raw_columns, time_column)
        return

    if expression == "concat_symbol":
        root_column = str(mapping_rule.get("root_col", "sym_root"))
        suffix_column = str(mapping_rule.get("suffix_col", "sym_suffix"))
        _validate_sql_identifier(root_column, f"symbol root column for {target_column}")
        _validate_sql_identifier(suffix_column, f"symbol suffix column for {target_column}")
        _append_unique(raw_columns, root_column)
        _append_unique(raw_columns, suffix_column)
        return

    raise WrdsExtractionError(
        f"Unsupported WRDS mapping rule for normalized column '{target_column}': {mapping_rule!r}."
    )


def build_wrds_query_specs(
    config: DataSliceConfig,
    *,
    include_quotes: bool = True,
    include_trades: bool = True,
    limit_per_query: int | None = None,
) -> tuple[WrdsQuerySpec, ...]:
    """Build SQL query specs for the configured WRDS data slice."""

    if limit_per_query is not None and limit_per_query <= 0:
        raise WrdsExtractionError("limit_per_query must be positive when provided.")

    query_specs: list[WrdsQuerySpec] = []
    quote_columns = wrds_raw_columns_for_mapping(
        config.data_contract.quote_mapping,
        QUOTE_COLUMNS,
    )
    trade_columns = wrds_raw_columns_for_mapping(
        config.data_contract.trade_mapping,
        TRADE_COLUMNS,
    )
    quote_table_prefix = wrds_table_prefix_for_kind(config, "quotes")
    trade_table_prefix = wrds_table_prefix_for_kind(config, "trades")

    for trading_date in config.time_range.trading_dates:
        if include_quotes:
            table_identifier = build_wrds_table_identifier(
                quote_table_prefix,
                trading_date,
            )
            query_specs.append(
                WrdsQuerySpec(
                    kind="quotes",
                    trading_date=trading_date,
                    table_identifier=table_identifier,
                    columns=quote_columns,
                    sql=build_wrds_sql(
                        table_identifier=table_identifier,
                        columns=quote_columns,
                        symbols=config.symbols,
                        market_open=config.time_range.market_open,
                        market_close=config.time_range.market_close,
                        limit=limit_per_query,
                    ),
                )
            )

        if include_trades:
            table_identifier = build_wrds_table_identifier(
                trade_table_prefix,
                trading_date,
            )
            query_specs.append(
                WrdsQuerySpec(
                    kind="trades",
                    trading_date=trading_date,
                    table_identifier=table_identifier,
                    columns=trade_columns,
                    sql=build_wrds_sql(
                        table_identifier=table_identifier,
                        columns=trade_columns,
                        symbols=config.symbols,
                        market_open=config.time_range.market_open,
                        market_close=config.time_range.market_close,
                        limit=limit_per_query,
                    ),
                )
            )

    return tuple(query_specs)


def list_available_wrds_daily_tables(
    connection: WrdsMetadataConnection,
    *,
    library: str = WRDS_TAQ_LIBRARY,
    table_prefixes: Sequence[str] = (WRDS_QUOTE_TABLE_PREFIX, WRDS_TRADE_TABLE_PREFIX),
) -> tuple[str, ...]:
    """List available WRDS daily TAQ tables for selected prefixes."""

    for table_prefix in table_prefixes:
        _validate_sql_identifier(table_prefix, "WRDS table prefix")

    prefix_set = set(table_prefixes)
    daily_table_pattern = re.compile(r"^([A-Za-z_][A-Za-z0-9_]*)_(\d{8})$")
    daily_tables: list[str] = []
    for table_name in connection.list_tables(library=library):
        match = daily_table_pattern.fullmatch(table_name)
        if match and match.group(1) in prefix_set:
            daily_tables.append(table_name)

    return tuple(sorted(daily_tables))


def find_missing_wrds_tables(
    connection: WrdsMetadataConnection,
    query_specs: Sequence[WrdsQuerySpec],
    *,
    library: str = WRDS_TAQ_LIBRARY,
) -> tuple[str, ...]:
    """Return fully-qualified query tables that are not listed by WRDS."""

    available_tables = set(connection.list_tables(library=library))
    missing_tables: list[str] = []
    for query_spec in query_specs:
        _, table_name = query_spec.table_identifier.split(".", 1)
        if table_name not in available_tables:
            missing_tables.append(query_spec.table_identifier)

    return tuple(missing_tables)


def validate_wrds_query_tables_exist(
    connection: WrdsMetadataConnection,
    query_specs: Sequence[WrdsQuerySpec],
    *,
    library: str = WRDS_TAQ_LIBRARY,
) -> None:
    """Raise a clear error before querying missing WRDS daily tables."""

    missing_tables = find_missing_wrds_tables(connection, query_specs, library=library)
    if not missing_tables:
        return

    missing_list = ", ".join(missing_tables)
    raise WrdsExtractionError(
        "WRDS daily table(s) are not available: "
        f"{missing_list}. Use --list-tables to inspect available WRDS daily dates, "
        "or update the config trading_dates to dates that exist in WRDS."
    )


def fetch_wrds_table_columns(
    connection: WrdsRawSqlConnection,
    table_identifier: str,
) -> tuple[str, ...]:
    """Fetch available columns for a WRDS table without downloading rows."""

    _validate_table_identifier(table_identifier)
    empty_frame = connection.raw_sql(f"select * from {table_identifier} limit 0")
    return tuple(str(column).lower() for column in empty_frame.columns)


def find_missing_wrds_query_columns(
    connection: WrdsRawSqlConnection,
    query_specs: Sequence[WrdsQuerySpec],
) -> dict[str, tuple[str, ...]]:
    """Return required source columns missing from each queried WRDS table."""

    available_by_table: dict[str, set[str]] = {}
    missing_by_table: dict[str, tuple[str, ...]] = {}
    for query_spec in query_specs:
        if query_spec.table_identifier not in available_by_table:
            available_by_table[query_spec.table_identifier] = set(
                fetch_wrds_table_columns(connection, query_spec.table_identifier)
            )

        missing_columns = tuple(
            column for column in query_spec.columns if column.lower() not in available_by_table[query_spec.table_identifier]
        )
        if missing_columns:
            missing_by_table[query_spec.table_identifier] = missing_columns

    return missing_by_table


def validate_wrds_query_columns_exist(
    connection: WrdsRawSqlConnection,
    query_specs: Sequence[WrdsQuerySpec],
) -> None:
    """Raise a clear error before querying columns that are absent in WRDS."""

    missing_by_table = find_missing_wrds_query_columns(connection, query_specs)
    if not missing_by_table:
        return

    details = "; ".join(
        f"{table}: {', '.join(columns)}" for table, columns in missing_by_table.items()
    )
    raise WrdsExtractionError(
        "WRDS query column(s) are not available for the configured table(s): "
        f"{details}. Update the data_contract mapping for the selected WRDS table family."
    )


def build_wrds_sql(
    *,
    table_identifier: str,
    columns: Sequence[str],
    symbols: Sequence[str],
    market_open: time | None = None,
    market_close: time | None = None,
    limit: int | None = None,
) -> str:
    """Build a WRDS SQL query for a TAQ daily table."""

    _validate_table_identifier(table_identifier)
    if not columns:
        raise WrdsExtractionError("At least one source column is required for extraction.")

    for column in columns:
        _validate_sql_identifier(column, "WRDS source column")

    if limit is not None and limit <= 0:
        raise WrdsExtractionError("limit must be positive when provided.")

    select_columns = ", ".join(columns)
    where_conditions = [build_wrds_symbol_where_clause(symbols)]
    if market_open is not None or market_close is not None:
        if market_open is None or market_close is None:
            raise WrdsExtractionError("market_open and market_close must be provided together.")
        if market_open > market_close:
            raise WrdsExtractionError("market_open must be less than or equal to market_close.")
        where_conditions.append(
            f"(time_m >= '{market_open.isoformat()}' and time_m <= '{market_close.isoformat()}')"
        )
    where_clause = " and ".join(where_conditions)
    order_columns = [column for column in ("date", "time_m", "qu_seqnum", "tr_seqnum") if column in columns]
    order_clause = f"\norder by {', '.join(order_columns)}" if order_columns else ""
    limit_clause = f"\nlimit {int(limit)}" if limit is not None else ""

    return (
        f"select {select_columns}\n"
        f"from {table_identifier}\n"
        f"where {where_clause}"
        f"{order_clause}"
        f"{limit_clause}"
    )


def build_wrds_symbol_where_clause(symbols: Sequence[str]) -> str:
    """Build a safe WRDS symbol filter using sym_root and sym_suffix."""

    if not symbols:
        raise WrdsExtractionError("At least one symbol is required for WRDS extraction.")

    conditions: list[str] = []
    for symbol in symbols:
        root, suffix = split_wrds_symbol(symbol)
        root_literal = _sql_string_literal(root)
        if suffix:
            conditions.append(
                f"(sym_root = {root_literal} and sym_suffix = {_sql_string_literal(suffix)})"
            )
        else:
            conditions.append(
                f"(sym_root = {root_literal} and (sym_suffix is null or sym_suffix = ''))"
            )

    return "(" + " or ".join(conditions) + ")"


def split_wrds_symbol(symbol: str) -> tuple[str, str]:
    """Split a normalized symbol into WRDS sym_root and sym_suffix values."""

    normalized = symbol.strip().upper()
    parts = normalized.split(".")
    if len(parts) > 2 or not parts[0]:
        raise WrdsExtractionError(f"Unsupported WRDS symbol format: {symbol!r}")

    root = parts[0]
    suffix = parts[1] if len(parts) == 2 else ""
    for component in (root, suffix):
        if component and not _WRDS_SYMBOL_COMPONENT_RE.fullmatch(component):
            raise WrdsExtractionError(f"Unsafe WRDS symbol component in {symbol!r}.")

    return root, suffix


def extract_wrds_raw_data(
    config: DataSliceConfig,
    *,
    connection: WrdsRawSqlConnection,
    include_quotes: bool = True,
    include_trades: bool = True,
    limit_per_query: int | None = None,
) -> WrdsRawExtractionResult:
    """Extract raw WRDS quote/trade rows for a configured data slice."""

    query_specs = build_wrds_query_specs(
        config,
        include_quotes=include_quotes,
        include_trades=include_trades,
        limit_per_query=limit_per_query,
    )

    quote_frames: list[pd.DataFrame] = []
    trade_frames: list[pd.DataFrame] = []
    query_diagnostics: list[WrdsQueryDiagnostics] = []

    for query_spec in query_specs:
        frame = connection.raw_sql(query_spec.sql)
        if query_spec.kind == "quotes":
            quote_frames.append(frame)
        else:
            trade_frames.append(frame)

        query_diagnostics.append(
            WrdsQueryDiagnostics(
                kind=query_spec.kind,
                trading_date=query_spec.trading_date.isoformat(),
                table_identifier=query_spec.table_identifier,
                rows=len(frame),
                sql=query_spec.sql,
            )
        )

    quotes = _concat_or_empty(quote_frames, _columns_for_kind(config, "quotes"))
    trades = _concat_or_empty(trade_frames, _columns_for_kind(config, "trades"))

    diagnostics = WrdsExtractionDiagnostics(
        quote_rows=len(quotes),
        trade_rows=len(trades),
        queries=tuple(query_diagnostics),
    )
    return WrdsRawExtractionResult(quotes=quotes, trades=trades, diagnostics=diagnostics)


def extract_wrds_data_slice(
    config: DataSliceConfig,
    *,
    connection_options: WrdsConnectionOptions | None = None,
    include_quotes: bool = True,
    include_trades: bool = True,
    limit_per_query: int | None = None,
    validate_tables: bool = True,
    validate_columns: bool = True,
) -> WrdsRawExtractionResult:
    """Open a WRDS connection and extract a configured data slice."""

    connection = connect_to_wrds(connection_options)
    try:
        query_specs = build_wrds_query_specs(
            config,
            include_quotes=include_quotes,
            include_trades=include_trades,
            limit_per_query=limit_per_query,
        )
        if validate_tables:
            validate_wrds_query_tables_exist(connection, query_specs)
        if validate_columns:
            validate_wrds_query_columns_exist(connection, query_specs)

        return extract_wrds_raw_data(
            config,
            connection=connection,
            include_quotes=include_quotes,
            include_trades=include_trades,
            limit_per_query=limit_per_query,
        )
    finally:
        close = getattr(connection, "close", None)
        if callable(close):
            close()


def write_wrds_raw_result(
    result: WrdsRawExtractionResult,
    *,
    config: DataSliceConfig,
    output_dir: str | Path | None = None,
) -> WrdsRawOutputPaths:
    """Write raw extraction outputs and a manifest to disk."""

    root = Path(output_dir or config.storage["raw_dir"])
    root.mkdir(parents=True, exist_ok=True)

    quote_path: Path | None = None
    trade_path: Path | None = None
    if not result.quotes.empty:
        quote_path = root / f"{config.slice_name}_quotes_raw.csv"
        result.quotes.to_csv(quote_path, index=False)

    if not result.trades.empty:
        trade_path = root / f"{config.slice_name}_trades_raw.csv"
        result.trades.to_csv(trade_path, index=False)

    manifest_path = root / f"{config.slice_name}_wrds_manifest.json"
    manifest = {
        "slice_name": config.slice_name,
        "quote_path": str(quote_path) if quote_path is not None else None,
        "trade_path": str(trade_path) if trade_path is not None else None,
        "diagnostics": asdict(result.diagnostics),
    }
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    return WrdsRawOutputPaths(
        quote_path=quote_path,
        trade_path=trade_path,
        manifest_path=manifest_path,
    )


def _columns_for_kind(config: DataSliceConfig, kind: WrdsTableKind) -> tuple[str, ...]:
    if kind == "quotes":
        return wrds_raw_columns_for_mapping(config.data_contract.quote_mapping, QUOTE_COLUMNS)
    return wrds_raw_columns_for_mapping(config.data_contract.trade_mapping, TRADE_COLUMNS)


def _concat_or_empty(frames: list[pd.DataFrame], columns: Sequence[str]) -> pd.DataFrame:
    if not frames:
        return pd.DataFrame(columns=columns)
    return pd.concat(frames, ignore_index=True)


def _append_unique(values: list[str], value: str) -> None:
    if value not in values:
        values.append(value)


def _validate_sql_identifier(identifier: str, label: str) -> None:
    if not _SQL_IDENTIFIER_RE.fullmatch(identifier):
        raise WrdsExtractionError(f"Unsafe {label}: {identifier!r}")


def _validate_table_identifier(table_identifier: str) -> None:
    if not _SQL_TABLE_IDENTIFIER_RE.fullmatch(table_identifier):
        raise WrdsExtractionError(f"Unsafe WRDS table identifier: {table_identifier!r}")


def _sql_string_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"
