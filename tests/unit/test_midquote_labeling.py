from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from level1_ofi_qr.labeling import (
    CURRENT_MIDQUOTE,
    CURRENT_MIDQUOTE_EVENT_TIME,
    CURRENT_MIDQUOTE_LAG_MS,
    LABELING_SCOPE_NOTE,
    MidquoteLabelError,
    build_midquote_label_dataset,
    build_midquote_labels_v1,
)
from level1_ofi_qr.utils import load_data_slice_config


ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = ROOT / "configs" / "data" / "aapl_wrds_20260313_20260410.yaml"


def feature_rows() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "event_time": pd.to_datetime(
                [
                    "2026-04-10T09:30:00.500000-04:00",
                    "2026-04-10T09:30:02.200000-04:00",
                    "2026-04-10T15:59:59.900000-04:00",
                    "2026-04-11T09:29:59-04:00",
                    "2026-04-10T09:30:00.500000-04:00",
                ],
                format="mixed",
            ),
            "symbol": ["AAPL", "AAPL", "AAPL", "AAPL", "MSFT"],
            "source": ["wrds_taq_ctm"] * 5,
            "trade_exchange": ["Q"] * 5,
            "sale_condition": ["@"] * 5,
            "trade_correction": ["00"] * 5,
            "trade_sequence_number": [100, 101, 102, 200, 300],
            "trade_price": [100.00, 100.10, 100.08, 101.00, 200.00],
            "trade_size": [100, 200, 100, 50, 300],
            "trading_date": [
                "2026-04-10",
                "2026-04-10",
                "2026-04-10",
                "2026-04-11",
                "2026-04-10",
            ],
            "trade_sign": [1, -1, 1, 0, 1],
            "signed_trade_size": [100, -200, 100, 0, 300],
        }
    )


def quote_features() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "event_time": pd.to_datetime(
                [
                    "2026-04-10T09:30:00-04:00",
                    "2026-04-10T09:30:02-04:00",
                    "2026-04-10T09:30:03.500000-04:00",
                    "2026-04-10T15:59:59-04:00",
                    "2026-04-11T09:30:01-04:00",
                    "2026-04-10T09:30:00-04:00",
                    "2026-04-10T09:30:02-04:00",
                ],
                format="mixed",
            ),
            "symbol": ["AAPL", "AAPL", "AAPL", "AAPL", "AAPL", "MSFT", "MSFT"],
            "trading_date": [
                "2026-04-10",
                "2026-04-10",
                "2026-04-10",
                "2026-04-10",
                "2026-04-11",
                "2026-04-10",
                "2026-04-10",
            ],
            "midquote": [100.00, 100.10, 100.08, 100.20, 101.00, 200.00, 200.20],
        }
    )


def test_build_midquote_labels_v1_matches_current_and_future_quotes_by_session() -> None:
    result = build_midquote_labels_v1(
        feature_rows(),
        quote_features(),
        horizons=("1s",),
        dead_zone_bps=5.0,
    )
    labeled = result.labeled_features

    assert len(labeled) == len(feature_rows())
    assert labeled.loc[0, CURRENT_MIDQUOTE] == 100.00
    assert labeled.loc[0, CURRENT_MIDQUOTE_EVENT_TIME] == pd.Timestamp(
        "2026-04-10T09:30:00-04:00"
    )
    assert labeled.loc[0, CURRENT_MIDQUOTE_LAG_MS] == 500.0
    assert labeled.loc[0, "future_midquote_1s"] == 100.10
    assert labeled.loc[0, "future_midquote_event_time_1s"] == pd.Timestamp(
        "2026-04-10T09:30:02-04:00"
    )
    assert labeled.loc[0, "future_midquote_return_bps_1s"] == pytest.approx(10.0)
    assert labeled.loc[0, "future_midquote_direction_1s"] == 1

    assert labeled.loc[1, CURRENT_MIDQUOTE] == 100.10
    assert labeled.loc[1, "future_midquote_1s"] == 100.08
    assert labeled.loc[1, "future_midquote_direction_1s"] == 0

    assert pd.isna(labeled.loc[2, "future_midquote_1s"])
    assert not bool(labeled.loc[2, "label_available_1s"])
    assert pd.isna(labeled.loc[2, "future_midquote_direction_1s"])

    assert pd.isna(labeled.loc[3, CURRENT_MIDQUOTE])
    assert pd.isna(labeled.loc[3, CURRENT_MIDQUOTE_EVENT_TIME])
    assert not bool(labeled.loc[3, "label_available_1s"])

    assert labeled.loc[4, CURRENT_MIDQUOTE] == 200.00
    assert labeled.loc[4, "future_midquote_1s"] == 200.20

    diagnostics = result.diagnostics
    assert diagnostics.row_preserving is True
    assert diagnostics.horizons == ("1s",)
    assert diagnostics.dead_zone_bps == 5.0
    assert diagnostics.label_group_keys == ("symbol", "trading_date")
    assert diagnostics.current_quote_policy == "latest_quote_at_or_before_decision_time"
    assert (
        diagnostics.future_quote_policy
        == "first_quote_at_or_after_decision_time_plus_horizon"
    )
    assert diagnostics.session_boundary_policy == "same_symbol_same_trading_date_only"
    assert diagnostics.label_usage_policy == "labels_are_targets_not_features"
    assert diagnostics.current_midquote_missing_rows == 1
    assert diagnostics.label_available_rows == {"1s": 3}
    assert diagnostics.label_missing_rows == {"1s": 2}
    assert diagnostics.positive_direction_rows == {"1s": 2}
    assert diagnostics.flat_direction_rows == {"1s": 1}
    assert diagnostics.negative_direction_rows == {"1s": 0}
    assert diagnostics.signals_implemented is False
    assert diagnostics.walk_forward_implemented is False
    assert diagnostics.backtest_implemented is False


def test_build_midquote_labels_v1_rejects_invalid_horizon() -> None:
    with pytest.raises(MidquoteLabelError, match="positive"):
        build_midquote_labels_v1(
            feature_rows(),
            quote_features(),
            horizons=("0ms",),
        )


def test_build_midquote_label_dataset_writes_manifest(tmp_path: Path) -> None:
    config = load_data_slice_config(CONFIG_PATH)
    processed_root = tmp_path / "processed"
    slice_root = processed_root / config.slice_name
    slice_root.mkdir(parents=True)
    feature_rows().to_csv(
        slice_root / f"{config.slice_name}_signed_flow_features_v1.csv",
        index=False,
    )
    quote_features().to_csv(
        slice_root / f"{config.slice_name}_quote_features_v1.csv",
        index=False,
    )

    result = build_midquote_label_dataset(
        config,
        processed_dir=processed_root,
        horizons=("1s",),
        dead_zone_bps=5.0,
    )

    assert result.paths.labeled_feature_path.exists()
    assert result.paths.manifest_path.exists()
    manifest = json.loads(result.paths.manifest_path.read_text())
    assert manifest["labeling_scope_note"] == LABELING_SCOPE_NOTE
    assert manifest["labeling_status"] == {
        "labeling_implemented": "v1",
        "current_quote_policy": "latest_quote_at_or_before_decision_time",
        "future_quote_policy": "first_quote_at_or_after_decision_time_plus_horizon",
        "session_boundary_policy": "same_symbol_same_trading_date_only",
        "label_usage_policy": "labels_are_targets_not_features",
        "signals_implemented": False,
        "walk_forward_implemented": False,
        "backtest_implemented": False,
        "research_grade_strategy_sample": False,
    }
    diagnostics = manifest["diagnostics"]
    assert diagnostics["row_preserving"] is True
    assert diagnostics["horizons"] == ["1s"]
    assert diagnostics["dead_zone_bps"] == 5.0
    assert diagnostics["label_available_rows"] == {"1s": 3}
