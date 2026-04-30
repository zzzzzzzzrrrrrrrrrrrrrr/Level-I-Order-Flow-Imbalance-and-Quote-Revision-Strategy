"""Command-line entry point for WRDS extraction workflows."""

from __future__ import annotations

import argparse
import os
from pathlib import Path

from level1_ofi_qr.extraction import (
    WrdsConnectionOptions,
    build_wrds_query_specs,
    connect_to_wrds,
    extract_wrds_data_slice,
    list_available_wrds_daily_tables,
    write_wrds_raw_result,
    wrds_table_prefix_for_kind,
)
from level1_ofi_qr.utils import load_data_slice_config


def _load_env_file_if_present(path: str | None) -> Path | None:
    if not path:
        return None

    for env_path in _env_file_candidates(path):
        if not env_path.exists():
            continue

        for raw_line in env_path.read_text(encoding="utf-8-sig").splitlines():
            line = raw_line.strip()
            if line.startswith("export "):
                line = line.removeprefix("export ").strip()
            if not line or line.startswith("#") or "=" not in line:
                continue

            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip("\"'")
            if key and key not in os.environ:
                os.environ[key] = value

        return env_path

    return None


def _env_file_candidates(path: str) -> tuple[Path, ...]:
    env_path = Path(path)
    if env_path.is_absolute():
        return (env_path,)

    project_root = Path(__file__).resolve().parents[1]
    candidates = [Path.cwd() / env_path, project_root / env_path]

    unique_candidates: list[Path] = []
    for candidate in candidates:
        if candidate not in unique_candidates:
            unique_candidates.append(candidate)
    return tuple(unique_candidates)


def _credential_source(*, argument_value: str | None, env_var: str) -> str:
    if argument_value:
        return "argument"
    if os.getenv(env_var):
        return f"env:{env_var}"
    return "wrds-default-prompt-or-pgpass"


def _close_connection(connection: object) -> None:
    close = getattr(connection, "close", None)
    if callable(close):
        close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Extract raw WRDS TAQ data for a configured slice.")
    parser.add_argument("config", help="Path to a data-slice YAML config.")
    parser.add_argument("--output-dir", help="Directory for raw CSV outputs and manifest.")
    parser.add_argument(
        "--tables",
        choices=("all", "quotes", "trades"),
        default="all",
        help="Which WRDS table family to extract.",
    )
    parser.add_argument("--wrds-username", help="WRDS username. Prefer WRDS_USERNAME env var.")
    parser.add_argument(
        "--env-file",
        default=".env",
        help="Optional local env file for WRDS_USERNAME/WRDS_PASSWORD. Existing env vars win.",
    )
    parser.add_argument("--username-env-var", default="WRDS_USERNAME")
    parser.add_argument("--password-env-var", default="WRDS_PASSWORD")
    parser.add_argument("--connect-timeout", type=int, default=10)
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--limit-per-query", type=int)
    parser.add_argument(
        "--skip-table-validation",
        action="store_true",
        help="Skip the WRDS list_tables preflight check before querying.",
    )
    parser.add_argument(
        "--skip-column-validation",
        action="store_true",
        help="Skip the WRDS zero-row column preflight check before querying.",
    )
    parser.add_argument(
        "--dry-run-sql",
        action="store_true",
        help="Print SQL queries without opening a WRDS connection.",
    )
    parser.add_argument(
        "--connection-test",
        action="store_true",
        help="Open and close a WRDS connection without running data queries.",
    )
    parser.add_argument(
        "--list-tables",
        action="store_true",
        help="List latest available WRDS daily tables for the configured quote/trade prefixes and exit.",
    )
    parser.add_argument("--table-list-limit", type=int, default=20)
    parser.add_argument(
        "--show-credential-sources",
        action="store_true",
        help="Print where WRDS username/password will be read from, without printing secrets.",
    )
    args = parser.parse_args()

    loaded_env_file = _load_env_file_if_present(args.env_file)

    config = load_data_slice_config(args.config)
    include_quotes = args.tables in {"all", "quotes"}
    include_trades = args.tables in {"all", "trades"}
    connection_options = WrdsConnectionOptions(
        username=args.wrds_username,
        username_env_var=args.username_env_var,
        password_env_var=args.password_env_var,
        verbose=args.verbose,
        connect_timeout=args.connect_timeout,
    )

    if args.show_credential_sources:
        print(f"env_file={loaded_env_file if loaded_env_file is not None else '<not found>'}")
        print(
            "username_source="
            f"{_credential_source(argument_value=args.wrds_username, env_var=args.username_env_var)}"
        )
        print(
            "password_source="
            f"{_credential_source(argument_value=None, env_var=args.password_env_var)}"
        )
        if not any((args.dry_run_sql, args.connection_test, args.list_tables)):
            return

    if args.dry_run_sql:
        for query_spec in build_wrds_query_specs(
            config,
            include_quotes=include_quotes,
            include_trades=include_trades,
            limit_per_query=args.limit_per_query,
        ):
            print(f"-- {query_spec.kind} {query_spec.trading_date.isoformat()}")
            print(query_spec.sql)
            print()
        return

    if args.connection_test:
        connection = connect_to_wrds(connection_options)
        try:
            print("wrds_connection=ok")
        finally:
            _close_connection(connection)
        return

    if args.list_tables:
        connection = connect_to_wrds(connection_options)
        try:
            prefixes: list[str] = []
            if include_quotes:
                prefixes.append(wrds_table_prefix_for_kind(config, "quotes"))
            if include_trades:
                prefixes.append(wrds_table_prefix_for_kind(config, "trades"))
            for prefix in prefixes:
                tables = list_available_wrds_daily_tables(
                    connection,
                    table_prefixes=(prefix,),
                )
                print(f"-- latest {prefix} tables")
                for table_name in tables[-args.table_list_limit :]:
                    print(f"taqmsec.{table_name}")
                print()
        finally:
            _close_connection(connection)
        return

    result = extract_wrds_data_slice(
        config,
        connection_options=connection_options,
        include_quotes=include_quotes,
        include_trades=include_trades,
        limit_per_query=args.limit_per_query,
        validate_tables=not args.skip_table_validation,
        validate_columns=not args.skip_column_validation,
    )
    output_paths = write_wrds_raw_result(
        result,
        config=config,
        output_dir=args.output_dir,
    )

    print(f"quote_rows={result.diagnostics.quote_rows}")
    print(f"trade_rows={result.diagnostics.trade_rows}")
    if output_paths.quote_path is not None:
        print(f"quote_path={output_paths.quote_path}")
    if output_paths.trade_path is not None:
        print(f"trade_path={output_paths.trade_path}")
    print(f"manifest_path={output_paths.manifest_path}")


if __name__ == "__main__":
    main()
