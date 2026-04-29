from __future__ import annotations

from pathlib import Path

import pandas as pd

from level1_ofi_qr.cleaning import (
    filter_trade_hard_constraints,
    summarize_trade_quality_warnings,
)

FIXTURE_PATH = Path(__file__).resolve().parents[1] / "fixtures" / "dirty_trades.csv"


def load_fixture_frame() -> pd.DataFrame:
    return pd.read_csv(FIXTURE_PATH, parse_dates=["event_time"])


def test_filter_trade_hard_constraints_removes_invalid_rows() -> None:
    trades = load_fixture_frame()

    cleaned, diagnostics = filter_trade_hard_constraints(trades)

    assert len(cleaned) == 2
    assert diagnostics.input_rows == 4
    assert diagnostics.removed_nonpositive_trade_price_rows == 1
    assert diagnostics.removed_nonpositive_trade_size_rows == 1
    assert diagnostics.output_rows == 2


def test_summarize_trade_quality_warnings_flags_large_trades() -> None:
    trades = load_fixture_frame().iloc[:2].copy()

    warnings = summarize_trade_quality_warnings(trades, size_quantile=0.5)

    assert warnings.input_rows == 2
    assert warnings.large_trade_size_rows == 1
    assert warnings.size_threshold_value is not None
