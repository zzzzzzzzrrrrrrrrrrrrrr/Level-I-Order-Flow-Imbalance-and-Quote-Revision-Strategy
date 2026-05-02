from __future__ import annotations

import json
from pathlib import Path
import shutil

import pandas as pd
import pytest

from level1_ofi_qr.datasets import (
    DatasetBuildError,
    build_dataset_from_wrds_raw,
    find_wrds_raw_input_paths,
)
from level1_ofi_qr.utils import load_data_slice_config

ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = ROOT / "configs" / "data" / "aapl_wrds_20260313_20260410.yaml"
FIXTURE_DIR = ROOT / "tests" / "fixtures"


def stage_raw_fixtures(raw_dir: Path) -> None:
    config = load_data_slice_config(CONFIG_PATH)
    raw_dir.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(
        FIXTURE_DIR / "wrds_raw_quotes.csv",
        raw_dir / f"{config.slice_name}_quotes_raw.csv",
    )
    shutil.copyfile(
        FIXTURE_DIR / "wrds_raw_trades.csv",
        raw_dir / f"{config.slice_name}_trades_raw.csv",
    )


def stage_audit_raw_fixtures(raw_dir: Path) -> None:
    config = load_data_slice_config(CONFIG_PATH)
    raw_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {
            "date": ["2026-04-10", "2026-04-10", "2026-04-10"],
            "time_m": ["09:31:00.000000", "09:31:01.000000", "09:31:02.000000"],
            "sym_root": ["AAPL", "AAPL", "AAPL"],
            "sym_suffix": ["", "", ""],
            "best_bidex": ["Q", "Q", "Q"],
            "best_askex": ["Q", "Q", "Q"],
            "nbbo_qu_cond": ["R", "R", "R"],
            "best_bid": [190.00, 191.00, 192.00],
            "best_ask": [190.02, 191.00, 191.99],
            "best_bidsiz": [500, 300, 200],
            "best_asksiz": [600, 300, 200],
        }
    ).to_csv(raw_dir / f"{config.slice_name}_quotes_raw.csv", index=False)
    pd.DataFrame(
        {
            "date": ["2026-04-10", "2026-04-10"],
            "time_m": ["09:31:00.000000", "09:31:01.000000"],
            "sym_root": ["AAPL", "AAPL"],
            "sym_suffix": ["", ""],
            "ex": ["Q", "Q"],
            "tr_scond": ["@", "O"],
            "tr_corr": ["00", "01"],
            "tr_seqnum": [1001, 1002],
            "price": [190.01, 190.02],
            "size": [100, 200],
        }
    ).to_csv(raw_dir / f"{config.slice_name}_trades_raw.csv", index=False)


def test_find_wrds_raw_input_paths_reports_missing_files(tmp_path: Path) -> None:
    config = load_data_slice_config(CONFIG_PATH)

    with pytest.raises(DatasetBuildError, match="Raw WRDS input"):
        find_wrds_raw_input_paths(config, raw_dir=tmp_path)


def test_build_dataset_from_wrds_raw_writes_normalized_and_cleaned_outputs(
    tmp_path: Path,
) -> None:
    config = load_data_slice_config(CONFIG_PATH)
    raw_dir = tmp_path / "raw"
    interim_dir = tmp_path / "interim"
    processed_dir = tmp_path / "processed"
    stage_raw_fixtures(raw_dir)

    result = build_dataset_from_wrds_raw(
        config,
        raw_dir=raw_dir,
        interim_dir=interim_dir,
        processed_dir=processed_dir,
    )

    assert result.diagnostics.raw_quote_rows == 2
    assert result.diagnostics.raw_trade_rows == 2
    assert result.diagnostics.cleaned_quote_rows == 2
    assert result.diagnostics.cleaned_trade_rows == 2
    assert result.paths.normalized_quote_path.exists()
    assert result.paths.normalized_trade_path.exists()
    assert result.paths.cleaned_quote_path.exists()
    assert result.paths.cleaned_trade_path.exists()
    assert result.paths.rejected_quote_path.exists()
    assert result.paths.rejected_trade_path.exists()
    assert result.paths.manifest_path.exists()

    cleaned_quotes = pd.read_csv(result.paths.cleaned_quote_path)
    cleaned_trades = pd.read_csv(result.paths.cleaned_trade_path)

    assert "nbbo_quote_condition" in cleaned_quotes.columns
    assert "trade_sequence_number" in cleaned_trades.columns


def test_dataset_manifest_records_all_rules_and_zero_drops(tmp_path: Path) -> None:
    config = load_data_slice_config(CONFIG_PATH)
    raw_dir = tmp_path / "raw"
    stage_raw_fixtures(raw_dir)

    result = build_dataset_from_wrds_raw(
        config,
        raw_dir=raw_dir,
        interim_dir=tmp_path / "interim",
        processed_dir=tmp_path / "processed",
    )
    manifest = json.loads(result.paths.manifest_path.read_text())

    quote_filters = {item["rule_id"]: item for item in manifest["diagnostics"]["quote_filters"]}
    trade_filters = {item["rule_id"]: item for item in manifest["diagnostics"]["trade_filters"]}

    assert set(quote_filters) == {
        "Q001_non_positive_prices",
        "Q002_negative_depth",
        "Q003_crossed_market",
    }
    assert set(trade_filters) == {
        "T001_non_positive_price_or_size",
        "T002_trade_correction",
    }
    assert all(item["dropped_count"] == 0 for item in quote_filters.values())
    assert all(item["dropped_count"] == 0 for item in trade_filters.values())


def test_dataset_manifest_emits_status_assumptions_and_condition_distributions(
    tmp_path: Path,
) -> None:
    config = load_data_slice_config(CONFIG_PATH)
    raw_dir = tmp_path / "raw"
    stage_raw_fixtures(raw_dir)

    result = build_dataset_from_wrds_raw(
        config,
        raw_dir=raw_dir,
        interim_dir=tmp_path / "interim",
        processed_dir=tmp_path / "processed",
    )
    manifest = json.loads(result.paths.manifest_path.read_text())

    assert manifest["cleaning_status"] == {
        "quote_cleaning": "partial_v2",
        "trade_cleaning": "partial_v2",
        "condition_filters_finalized": False,
        "alignment_implemented": False,
        "trade_signing_implemented": False,
        "research_grade_sample": False,
    }
    assert manifest["cleaning_policy"]["current_design"] == (
        "drop_only_one_pass_cleaning_after_scope_filter"
    )
    assert manifest["cleaning_policy"]["locked_market_policy"] == (
        "quotes_with_ask_equal_to_bid_are_retained_and_counted_in_quote_quality_warnings"
    )
    assert manifest["cleaning_policy"]["condition_code_policy"] == (
        "nbbo_quote_condition_and_sale_condition_are_diagnostic_only_until_eligibility_is_finalized"
    )
    assert manifest["diagnostics"]["unresolved_data_assumptions"]
    assert {"id", "field", "issue", "impact", "blocking_for", "not_blocking_for"} <= set(
        manifest["diagnostics"]["unresolved_data_assumptions"][0]
    )
    assert manifest["diagnostics"]["nbbo_quote_condition_distribution"] == {"R": 2}
    assert manifest["diagnostics"]["sale_condition_distribution"] == {"<NA>": 1, "@": 1}
    assert result.diagnostics.cleaned_trade_rows == result.diagnostics.raw_trade_rows


def test_rejected_rows_and_manifest_counts_are_consistent(tmp_path: Path) -> None:
    config = load_data_slice_config(CONFIG_PATH)
    raw_dir = tmp_path / "raw"
    stage_audit_raw_fixtures(raw_dir)

    result = build_dataset_from_wrds_raw(
        config,
        raw_dir=raw_dir,
        interim_dir=tmp_path / "interim",
        processed_dir=tmp_path / "processed",
    )
    manifest = json.loads(result.paths.manifest_path.read_text())
    rejected_quotes = pd.read_csv(result.paths.rejected_quote_path)
    rejected_trades = pd.read_csv(result.paths.rejected_trade_path)

    assert result.diagnostics.cleaned_quote_rows + result.diagnostics.rejected_quote_rows == 3
    assert result.diagnostics.cleaned_trade_rows + result.diagnostics.rejected_trade_rows == 2
    assert rejected_quotes["rule_id"].value_counts().to_dict() == {"Q003_crossed_market": 1}
    assert rejected_trades["rule_id"].value_counts().to_dict() == {"T002_trade_correction": 1}

    quote_drops = {
        item["rule_id"]: item["dropped_count"]
        for item in manifest["diagnostics"]["quote_filters"]
        if item["dropped_count"]
    }
    trade_drops = {
        item["rule_id"]: item["dropped_count"]
        for item in manifest["diagnostics"]["trade_filters"]
        if item["dropped_count"]
    }
    assert quote_drops == rejected_quotes["rule_id"].value_counts().to_dict()
    assert trade_drops == rejected_trades["rule_id"].value_counts().to_dict()
    for required_column in ("event_time", "symbol", "source", "raw_row_index", "rule_id", "reject_reason"):
        assert required_column in rejected_quotes.columns
        assert required_column in rejected_trades.columns


def test_crossed_and_locked_quotes_are_handled_separately(tmp_path: Path) -> None:
    config = load_data_slice_config(CONFIG_PATH)
    raw_dir = tmp_path / "raw"
    stage_audit_raw_fixtures(raw_dir)

    result = build_dataset_from_wrds_raw(
        config,
        raw_dir=raw_dir,
        interim_dir=tmp_path / "interim",
        processed_dir=tmp_path / "processed",
    )

    assert result.diagnostics.quote_quality_warnings.locked_quote_rows == 1
    assert result.diagnostics.rejected_quote_rows == 1
    assert (result.cleaned_quotes["ask"] == result.cleaned_quotes["bid"]).sum() == 1
    assert result.rejected_quotes.iloc[0]["rule_id"] == "Q003_crossed_market"
