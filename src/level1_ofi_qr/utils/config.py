"""Helpers for loading project configuration files."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, time
from pathlib import Path
from typing import Any

import yaml

MappingRule = str | dict[str, Any] | None


@dataclass(frozen=True)
class TimeRangeConfig:
    """Research-slice time constraints parsed from a YAML config."""

    trading_dates: tuple[date, ...]
    session: str
    market_open: time
    market_close: time
    timezone: str


@dataclass(frozen=True)
class DataContractConfig:
    """Normalized column definitions and source mappings for a data slice."""

    quote_level: str
    quote_scope: str
    quote_source: str
    trade_source: str
    separate_tables: bool
    shared_columns: tuple[str, ...]
    quote_columns: tuple[str, ...]
    trade_columns: tuple[str, ...]
    quote_mapping: dict[str, MappingRule]
    trade_mapping: dict[str, MappingRule]


@dataclass(frozen=True)
class DataSliceConfig:
    """Parsed data-slice configuration used by adapters and cleaning code."""

    path: Path
    slice_name: str
    purpose: str
    research_status: str
    source: dict[str, Any]
    symbols: tuple[str, ...]
    time_range: TimeRangeConfig
    data_contract: DataContractConfig
    storage: dict[str, str]


def load_data_slice_config(config_path: str | Path) -> DataSliceConfig:
    """Load a project data-slice YAML config into typed structures."""

    path = Path(config_path)
    with path.open("r", encoding="utf-8") as handle:
        raw_config = yaml.safe_load(handle)

    time_range = raw_config["time_range"]
    data_contract = raw_config["data_contract"]

    return DataSliceConfig(
        path=path,
        slice_name=raw_config["slice_name"],
        purpose=raw_config["purpose"],
        research_status=raw_config["research_status"],
        source=dict(raw_config["source"]),
        symbols=tuple(raw_config["universe"]["symbols"]),
        time_range=TimeRangeConfig(
            trading_dates=tuple(_coerce_date(value) for value in time_range["trading_dates"]),
            session=time_range["session"],
            market_open=_coerce_time(time_range["market_open"]),
            market_close=_coerce_time(time_range["market_close"]),
            timezone=time_range["timezone"],
        ),
        data_contract=DataContractConfig(
            quote_level=data_contract["quote_level"],
            quote_scope=data_contract["quote_scope"],
            quote_source=data_contract["quote_source"],
            trade_source=data_contract["trade_source"],
            separate_tables=bool(data_contract["separate_tables"]),
            shared_columns=tuple(data_contract["shared_columns"]),
            quote_columns=tuple(data_contract["quote_columns"]),
            trade_columns=tuple(data_contract["trade_columns"]),
            quote_mapping=dict(data_contract["quote_mapping"]),
            trade_mapping=dict(data_contract["trade_mapping"]),
        ),
        storage=dict(raw_config["storage"]),
    )


def _coerce_date(value: date | str) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(value)


def _coerce_time(value: time | str) -> time:
    if isinstance(value, time):
        return value
    return time.fromisoformat(value)
