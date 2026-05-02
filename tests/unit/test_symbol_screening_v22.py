from __future__ import annotations

import pandas as pd

from level1_ofi_qr.diagnostics.symbol_screening_v22 import (
    SymbolScreenV22Config,
    build_symbol_screening_tables,
)


def test_symbol_screening_markout_does_not_cross_symbols() -> None:
    candidates = pd.DataFrame(
        [
            _candidate("AAA", "2026-04-01", "2026-04-01T09:30:00-04:00", 100.0, 1.0),
            _candidate("BBB", "2026-04-01", "2026-04-01T09:30:00-04:00", 200.0, 1.0),
            _candidate("AAA", "2026-04-02", "2026-04-02T09:30:00-04:00", 100.0, 2.0),
            _candidate("BBB", "2026-04-02", "2026-04-02T09:30:00-04:00", 200.0, 2.0),
        ]
    )
    quotes = pd.DataFrame(
        [
            _quote("AAA", "2026-04-01", "2026-04-01T09:30:00-04:00", 100.0),
            _quote("AAA", "2026-04-01", "2026-04-01T09:30:01-04:00", 101.0),
            _quote("BBB", "2026-04-01", "2026-04-01T09:30:00-04:00", 200.0),
            _quote("BBB", "2026-04-01", "2026-04-01T09:30:01-04:00", 198.0),
            _quote("AAA", "2026-04-02", "2026-04-02T09:30:00-04:00", 100.0),
            _quote("AAA", "2026-04-02", "2026-04-02T09:30:01-04:00", 102.0),
            _quote("BBB", "2026-04-02", "2026-04-02T09:30:00-04:00", 200.0),
            _quote("BBB", "2026-04-02", "2026-04-02T09:30:01-04:00", 196.0),
        ]
    )

    tables = build_symbol_screening_tables(
        candidates,
        quotes,
        screening_config=SymbolScreenV22Config(
            horizons=("1s",),
            decile_horizons=("1s",),
            validation_min_dates=1,
        ),
        configured_symbols=("AAA", "BBB"),
    )

    sweep = tables.horizon_sweep
    aaa = sweep.loc[
        (sweep["symbol"] == "AAA")
        & (sweep["split"] == "validation")
        & (sweep["signal_bucket"] == "all")
    ].iloc[0]
    bbb = sweep.loc[
        (sweep["symbol"] == "BBB")
        & (sweep["split"] == "validation")
        & (sweep["signal_bucket"] == "all")
    ].iloc[0]

    assert aaa["mean_move_bps"] == 100.0
    assert bbb["mean_move_bps"] == -100.0


def test_symbol_screening_summary_uses_validation_not_test_for_pass_flag() -> None:
    candidates = pd.DataFrame(
        [
            _candidate("AAA", "2026-04-01", "2026-04-01T09:30:00-04:00", 100.0, 1.0),
            _candidate("AAA", "2026-04-02", "2026-04-02T09:30:00-04:00", 100.0, 2.0),
            _candidate("AAA", "2026-04-03", "2026-04-03T09:30:00-04:00", 100.0, 3.0),
        ]
    )
    quotes = pd.DataFrame(
        [
            _quote("AAA", "2026-04-01", "2026-04-01T09:30:00-04:00", 100.0),
            _quote("AAA", "2026-04-01", "2026-04-01T09:30:01-04:00", 100.005),
            _quote("AAA", "2026-04-02", "2026-04-02T09:30:00-04:00", 100.0),
            _quote("AAA", "2026-04-02", "2026-04-02T09:30:01-04:00", 100.005),
            _quote("AAA", "2026-04-03", "2026-04-03T09:30:00-04:00", 100.0),
            _quote("AAA", "2026-04-03", "2026-04-03T09:30:01-04:00", 105.0),
        ]
    )

    tables = build_symbol_screening_tables(
        candidates,
        quotes,
        screening_config=SymbolScreenV22Config(
            horizons=("1s",),
            decile_horizons=("1s",),
            validation_min_dates=2,
            pass_move_over_cost=1.0,
        ),
        configured_symbols=("AAA",),
    )

    summary = tables.summary.iloc[0]
    test_sweep = tables.horizon_sweep.loc[
        (tables.horizon_sweep["split"] == "test")
        & (tables.horizon_sweep["signal_bucket"] == "top1pct_score")
    ].iloc[0]

    assert summary["top_1pct_move_over_cost"] < 1.0
    assert not bool(summary["validation_pass_flag"])
    assert test_sweep["move_over_cost"] > 1.0
    assert not bool(summary["test_used_for_selection"])


def _candidate(
    symbol: str,
    trading_date: str,
    event_time: str,
    midquote: float,
    score: float,
) -> dict[str, object]:
    return {
        "signal_id": f"{symbol}_{trading_date}",
        "event_time": event_time,
        "symbol": symbol,
        "trading_date": trading_date,
        "side": 1,
        "predicted_edge_bps": score,
        "expected_cost_bps": 1.0,
        "tradable_edge_bps": score - 1.0,
        "midquote": midquote,
        "quoted_spread": 0.01,
        "displayed_depth": 1000,
        "microprice_gap_bps": 0.1,
    }


def _quote(symbol: str, trading_date: str, event_time: str, midquote: float) -> dict[str, object]:
    return {
        "event_time": event_time,
        "symbol": symbol,
        "trading_date": trading_date,
        "midquote": midquote,
        "quoted_spread": 0.01,
    }
