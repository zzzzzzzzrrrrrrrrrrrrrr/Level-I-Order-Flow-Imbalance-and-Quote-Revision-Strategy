from __future__ import annotations

from pathlib import Path

import pandas as pd

from level1_ofi_qr.cleaning import (
    clean_quotes_v2,
    filter_quote_hard_constraints,
    summarize_quote_quality_warnings,
)

FIXTURE_PATH = Path(__file__).resolve().parents[1] / "fixtures" / "dirty_quotes.csv"


def load_fixture_frame() -> pd.DataFrame:
    return pd.read_csv(FIXTURE_PATH, parse_dates=["event_time"])


def test_filter_quote_hard_constraints_removes_invalid_rows() -> None:
    quotes = load_fixture_frame()

    cleaned, diagnostics = filter_quote_hard_constraints(quotes)

    assert len(cleaned) == 2
    assert diagnostics.input_rows == 5
    assert diagnostics.removed_nonpositive_bid_rows == 1
    assert diagnostics.removed_nonpositive_ask_rows == 1
    assert diagnostics.removed_negative_bid_size_rows == 0
    assert diagnostics.removed_negative_ask_size_rows == 0
    assert diagnostics.removed_crossed_quote_rows == 1
    assert diagnostics.output_rows == 2


def test_clean_quotes_v2_records_rule_diagnostics_and_rejected_rows() -> None:
    quotes = load_fixture_frame()

    result = clean_quotes_v2(quotes)

    assert len(result.cleaned) == 2
    assert len(result.rejected) == 3
    assert [item.rule_id for item in result.diagnostics] == [
        "Q001_non_positive_prices",
        "Q002_negative_depth",
        "Q003_crossed_market",
    ]
    assert set(result.rejected["rule_id"]) == {
        "Q001_non_positive_prices",
        "Q003_crossed_market",
    }


def test_summarize_quote_quality_warnings_flags_soft_issues() -> None:
    quotes = load_fixture_frame().iloc[:2].copy()

    warnings = summarize_quote_quality_warnings(quotes, relative_spread_warn_threshold=0.05)

    assert warnings.input_rows == 2
    assert warnings.wide_relative_spread_rows == 1
    assert warnings.zero_depth_rows == 1
    assert warnings.locked_quote_rows == 1
