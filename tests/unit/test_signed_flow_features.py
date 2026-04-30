from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from level1_ofi_qr.features import (
    BUY_TRADE_SIZE,
    SELL_TRADE_SIZE,
    SIGNED_FLOW_FEATURE_SCOPE_NOTE,
    SIGNED_TRADE_VALUE,
    UNKNOWN_TRADE_SIZE,
    SignedFlowFeatureError,
    build_signed_flow_feature_dataset,
    build_signed_flow_features_v1,
)
from level1_ofi_qr.utils import load_data_slice_config


ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = ROOT / "configs" / "data" / "aapl_wrds_20260408_20260410.yaml"


def signed_trade_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "raw_row_index": [1, 2, 3, 4, 5, 6],
            "event_time": pd.to_datetime(
                [
                    "2026-04-10T09:31:00-04:00",
                    "2026-04-10T09:31:00.500000-04:00",
                    "2026-04-10T09:31:01.500000-04:00",
                    "2026-04-10T09:31:02-04:00",
                    "2026-04-11T09:31:00-04:00",
                    "2026-04-10T09:31:00-04:00",
                ],
                format="mixed",
            ),
            "symbol": ["AAPL", "AAPL", "AAPL", "AAPL", "AAPL", "MSFT"],
            "source": ["wrds_taq_ctm"] * 6,
            "trade_exchange": ["Q"] * 6,
            "sale_condition": ["@"] * 6,
            "trade_correction": ["00"] * 6,
            "trade_sequence_number": [100, 101, 102, 103, 200, 300],
            "trade_price": [10.00, 10.01, 10.02, 10.02, 11.00, 20.00],
            "trade_size": [100, 50, 25, 10, 200, 300],
            "trading_date": [
                "2026-04-10",
                "2026-04-10",
                "2026-04-10",
                "2026-04-10",
                "2026-04-11",
                "2026-04-10",
            ],
            "trade_sign": [1, -1, 1, 0, -1, 1],
            "trade_sign_source": [
                "quote_rule",
                "quote_rule",
                "tick_rule",
                "unknown",
                "quote_rule",
                "quote_rule",
            ],
            "signed_trade_size": [100, -50, 25, 0, -200, 300],
        }
    )


def test_build_signed_flow_features_v1_computes_grouped_trailing_windows() -> None:
    result = build_signed_flow_features_v1(
        signed_trade_frame(),
        trade_count_windows=(2,),
        time_windows=("1s",),
    )
    features = result.signed_flow_features

    assert features[BUY_TRADE_SIZE].tolist() == [100, 0, 25, 0, 0, 300]
    assert features[SELL_TRADE_SIZE].tolist() == [0, 50, 0, 0, 200, 0]
    assert features[UNKNOWN_TRADE_SIZE].tolist() == [0, 0, 0, 10, 0, 0]
    assert features[SIGNED_TRADE_VALUE].tolist() == pytest.approx(
        [1000.0, -500.5, 250.5, 0.0, -2200.0, 6000.0]
    )

    assert features["signed_flow_2_trades"].tolist() == [100, 50, -25, 25, -200, 300]
    assert features["trade_volume_2_trades"].tolist() == [100, 150, 75, 35, 200, 300]
    assert features["trade_count_2_trades"].tolist() == [1, 2, 2, 2, 1, 1]
    assert features["signed_flow_imbalance_2_trades"].tolist() == pytest.approx(
        [1.0, 1 / 3, -1 / 3, 25 / 35, -1.0, 1.0]
    )

    assert features["signed_flow_1s"].tolist() == [100, 50, -25, 25, -200, 300]
    assert features["trade_volume_1s"].tolist() == [100, 150, 75, 35, 200, 300]
    assert features["trade_count_1s"].tolist() == [1, 2, 2, 2, 1, 1]
    assert features["signed_flow_imbalance_1s"].tolist() == pytest.approx(
        [1.0, 1 / 3, -1 / 3, 25 / 35, -1.0, 1.0]
    )

    diagnostics = result.diagnostics
    assert diagnostics.row_preserving is True
    assert diagnostics.input_signed_trade_rows == 6
    assert diagnostics.output_feature_rows == 6
    assert diagnostics.feature_group_keys == ("symbol", "trading_date")
    assert diagnostics.trade_count_windows == (2,)
    assert diagnostics.time_windows == ("1s",)
    assert diagnostics.window_inclusion_policy == "trailing_windows_include_current_trade"
    assert (
        diagnostics.unknown_sign_policy
        == "unknown_sign_trades_contribute_zero_signed_flow_and_remain_in_trade_volume"
    )
    assert diagnostics.unknown_sign_rows == 1
    assert diagnostics.buy_sign_rows == 3
    assert diagnostics.sell_sign_rows == 2
    assert diagnostics.zero_volume_window_rows["signed_flow_imbalance_2_trades"] == 0
    assert diagnostics.signed_flow_imbalance_null_rows["signed_flow_imbalance_1s"] == 0
    assert diagnostics.labels_implemented is False
    assert diagnostics.backtest_implemented is False


def test_build_signed_flow_features_v1_rejects_invalid_windows() -> None:
    with pytest.raises(SignedFlowFeatureError, match="positive"):
        build_signed_flow_features_v1(
            signed_trade_frame(),
            trade_count_windows=(0,),
            time_windows=(),
        )


def test_build_signed_flow_feature_dataset_writes_outputs(tmp_path: Path) -> None:
    config = load_data_slice_config(CONFIG_PATH)
    processed_root = tmp_path / "processed"
    slice_root = processed_root / config.slice_name
    slice_root.mkdir(parents=True)
    signed_trade_frame().to_csv(
        slice_root / f"{config.slice_name}_trades_signed_v1.csv",
        index=False,
    )

    result = build_signed_flow_feature_dataset(
        config,
        processed_dir=processed_root,
        trade_count_windows=(2,),
        time_windows=("1s",),
    )

    assert result.paths.signed_flow_feature_path.exists()
    assert result.paths.manifest_path.exists()
    manifest = json.loads(result.paths.manifest_path.read_text())
    assert manifest["feature_scope_note"] == SIGNED_FLOW_FEATURE_SCOPE_NOTE
    assert manifest["feature_status"] == {
        "signed_flow_features_implemented": "v1",
        "window_inclusion_policy": "trailing_windows_include_current_trade",
        "unknown_sign_policy": (
            "unknown_sign_trades_contribute_zero_signed_flow_and_remain_in_trade_volume"
        ),
        "condition_filters_applied": False,
        "sale_condition_filters_applied": False,
        "nbbo_quote_condition_filters_applied": False,
        "labels_implemented": False,
        "backtest_implemented": False,
        "research_grade_strategy_sample": False,
    }
    diagnostics = manifest["diagnostics"]
    assert diagnostics["row_preserving"] is True
    assert diagnostics["trade_count_windows"] == [2]
    assert diagnostics["time_windows"] == ["1s"]
    assert diagnostics["feature_group_keys"] == ["symbol", "trading_date"]
    assert diagnostics["window_inclusion_policy"] == "trailing_windows_include_current_trade"
    assert (
        diagnostics["unknown_sign_policy"]
        == "unknown_sign_trades_contribute_zero_signed_flow_and_remain_in_trade_volume"
    )
    assert diagnostics["unknown_sign_rows"] == 1
