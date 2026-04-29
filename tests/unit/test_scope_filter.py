from __future__ import annotations

from datetime import date

import pandas as pd

from level1_ofi_qr.cleaning import filter_frame_to_scope


def test_filter_frame_to_scope_enforces_symbol_date_and_session() -> None:
    frame = pd.DataFrame(
        {
            "event_time": pd.to_datetime(
                [
                    "2026-04-24T09:31:00-04:00",
                    "2026-04-24T09:31:30-04:00",
                    "2026-04-23T09:31:00-04:00",
                    "2026-04-24T09:29:59-04:00",
                ]
            ),
            "symbol": ["AAPL", "MSFT", "AAPL", "AAPL"],
            "source": ["wrds_taq_cqm"] * 4,
        }
    )

    scoped, diagnostics = filter_frame_to_scope(
        frame,
        symbols=["AAPL"],
        trading_dates=[date(2026, 4, 24)],
    )

    assert len(scoped) == 1
    assert diagnostics.input_rows == 4
    assert diagnostics.removed_symbol_rows == 1
    assert diagnostics.removed_date_rows == 1
    assert diagnostics.removed_session_rows == 1
    assert diagnostics.output_rows == 1
    assert scoped.iloc[0]["symbol"] == "AAPL"
