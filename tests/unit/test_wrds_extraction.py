from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from level1_ofi_qr.extraction import (
    WrdsExtractionError,
    build_wrds_query_specs,
    build_wrds_sql,
    build_wrds_symbol_where_clause,
    build_wrds_table_identifier,
    extract_wrds_raw_data,
    find_missing_wrds_query_columns,
    find_missing_wrds_tables,
    list_available_wrds_daily_tables,
    validate_wrds_query_columns_exist,
    validate_wrds_query_tables_exist,
    wrds_raw_columns_for_mapping,
)
from level1_ofi_qr.schema import QUOTE_COLUMNS, TRADE_COLUMNS
from level1_ofi_qr.utils import load_data_slice_config

CONFIG_PATH = (
    Path(__file__).resolve().parents[2]
    / "configs"
    / "data"
    / "aapl_wrds_20260313_20260410.yaml"
)


class FakeWrdsConnection:
    def __init__(self) -> None:
        self.sql_calls: list[str] = []

    def raw_sql(self, sql: str) -> pd.DataFrame:
        self.sql_calls.append(sql)
        if ".nbbom_" in sql:
            return pd.DataFrame(
                {
                    "date": ["2026-04-10"],
                    "time_m": ["09:31:00.000000"],
                    "sym_root": ["AAPL"],
                    "sym_suffix": [""],
                    "best_bidex": ["Q"],
                    "best_askex": ["Q"],
                    "nbbo_qu_cond": ["R"],
                    "best_bid": [190.00],
                    "best_ask": [190.02],
                    "best_bidsiz": [500],
                    "best_asksiz": [600],
                }
            )

        if ".ctm_" in sql:
            return pd.DataFrame(
                {
                    "date": ["2026-04-10"],
                    "time_m": ["09:31:00.000000"],
                    "sym_root": ["AAPL"],
                    "sym_suffix": [""],
                    "ex": ["Q"],
                    "tr_scond": ["@"],
                    "tr_corr": [0],
                    "tr_seqnum": [1001],
                    "price": [190.01],
                    "size": [100],
                }
            )

        raise AssertionError(f"Unexpected SQL: {sql}")


class FakeWrdsMetadataConnection:
    def __init__(self, table_names: list[str]) -> None:
        self.table_names = table_names

    def list_tables(self, library: str) -> list[str]:
        assert library == "taqmsec"
        return self.table_names


def test_build_wrds_table_identifier_uses_daily_taq_name() -> None:
    assert build_wrds_table_identifier("cqm", date(2026, 4, 10)) == "taqmsec.cqm_20260410"


def test_wrds_raw_columns_for_quote_mapping_skips_constant_source() -> None:
    config = load_data_slice_config(CONFIG_PATH)

    columns = wrds_raw_columns_for_mapping(config.data_contract.quote_mapping, QUOTE_COLUMNS)

    assert columns == (
        "date",
        "time_m",
        "sym_root",
        "sym_suffix",
        "best_bidex",
        "best_askex",
        "nbbo_qu_cond",
        "best_bid",
        "best_ask",
        "best_bidsiz",
        "best_asksiz",
    )


def test_wrds_raw_columns_for_trade_mapping_skips_constant_source() -> None:
    config = load_data_slice_config(CONFIG_PATH)

    columns = wrds_raw_columns_for_mapping(config.data_contract.trade_mapping, TRADE_COLUMNS)

    assert columns == (
        "date",
        "time_m",
        "sym_root",
        "sym_suffix",
        "ex",
        "tr_scond",
        "tr_corr",
        "tr_seqnum",
        "price",
        "size",
    )


def test_build_wrds_symbol_where_clause_handles_root_and_suffix() -> None:
    where_clause = build_wrds_symbol_where_clause(["AAPL", "BRK.B"])

    assert "sym_root = 'AAPL'" in where_clause
    assert "sym_suffix is null or sym_suffix = ''" in where_clause
    assert "sym_root = 'BRK'" in where_clause
    assert "sym_suffix = 'B'" in where_clause


def test_build_wrds_sql_rejects_unsafe_table_identifier() -> None:
    with pytest.raises(WrdsExtractionError, match="Unsafe WRDS table identifier"):
        build_wrds_sql(
            table_identifier="taqmsec.cqm_20260410; drop table x",
            columns=("date", "time_m"),
            symbols=("AAPL",),
        )


def test_build_wrds_query_specs_creates_quote_and_trade_queries() -> None:
    config = load_data_slice_config(CONFIG_PATH)

    query_specs = build_wrds_query_specs(config, limit_per_query=10)

    assert len(query_specs) == 40
    assert query_specs[0].kind == "quotes"
    assert query_specs[0].table_identifier == "taqmsec.nbbom_20260313"
    assert "time_m >= '09:30:00'" in query_specs[0].sql
    assert "time_m <= '16:00:00'" in query_specs[0].sql
    assert "limit 10" in query_specs[0].sql
    assert query_specs[1].kind == "trades"
    assert query_specs[1].table_identifier == "taqmsec.ctm_20260313"


def test_extract_wrds_raw_data_uses_fake_connection_and_collects_diagnostics() -> None:
    config = load_data_slice_config(CONFIG_PATH)
    connection = FakeWrdsConnection()

    result = extract_wrds_raw_data(config, connection=connection)

    assert len(connection.sql_calls) == 40
    assert len(result.quotes) == 20
    assert len(result.trades) == 20
    assert "best_bid" in result.quotes.columns
    assert "price" in result.trades.columns
    assert result.diagnostics.quote_rows == 20
    assert result.diagnostics.trade_rows == 20
    assert len(result.diagnostics.queries) == 40


def test_list_available_wrds_daily_tables_filters_prefixes() -> None:
    connection = FakeWrdsMetadataConnection(
        [
            "cqm_20260410",
            "nbbom_20260410",
            "ctm_20260410",
            "complete_nbbo_20260410",
            "cqm_bad",
        ]
    )

    tables = list_available_wrds_daily_tables(connection)

    assert tables == ("ctm_20260410", "nbbom_20260410")


def test_validate_wrds_query_tables_exist_reports_missing_tables() -> None:
    config = load_data_slice_config(CONFIG_PATH)
    query_specs = build_wrds_query_specs(config, include_trades=False)
    connection = FakeWrdsMetadataConnection(["nbbom_20260408"])

    missing_tables = find_missing_wrds_tables(connection, query_specs)

    assert len(missing_tables) == 19
    assert "taqmsec.nbbom_20260313" in missing_tables
    assert "taqmsec.nbbom_20260408" not in missing_tables
    assert "taqmsec.nbbom_20260410" in missing_tables
    with pytest.raises(WrdsExtractionError, match="not available"):
        validate_wrds_query_tables_exist(connection, query_specs)


def test_validate_wrds_query_columns_exist_reports_missing_columns() -> None:
    query_specs = build_wrds_query_specs(load_data_slice_config(CONFIG_PATH), include_trades=False)
    connection = FakeWrdsConnection()

    assert find_missing_wrds_query_columns(connection, query_specs) == {}

    bad_query_spec = query_specs[0].__class__(
        kind=query_specs[0].kind,
        trading_date=query_specs[0].trading_date,
        table_identifier=query_specs[0].table_identifier,
        columns=(*query_specs[0].columns, "bidex"),
        sql=query_specs[0].sql,
    )

    missing_columns = find_missing_wrds_query_columns(connection, (bad_query_spec,))

    assert missing_columns == {"taqmsec.nbbom_20260313": ("bidex",)}
    with pytest.raises(WrdsExtractionError, match="column"):
        validate_wrds_query_columns_exist(connection, (bad_query_spec,))
