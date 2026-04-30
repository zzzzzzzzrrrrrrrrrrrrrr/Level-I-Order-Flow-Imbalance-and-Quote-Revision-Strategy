from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from level1_ofi_qr.features import (
    MIDQUOTE,
    QUOTE_FEATURE_SCOPE_NOTE,
    QUOTE_IMBALANCE,
    QUOTE_REVISION,
    QUOTE_REVISION_BPS,
    QUOTED_DEPTH,
    TRADING_DATE,
    build_quote_feature_dataset,
    build_quote_features_v1,
)
from level1_ofi_qr.utils import load_data_slice_config


ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = ROOT / "configs" / "data" / "aapl_wrds_20260408_20260410.yaml"


def quote_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "event_time": pd.to_datetime(
                [
                    "2026-04-10T09:31:01-04:00",
                    "2026-04-10T09:31:00-04:00",
                    "2026-04-11T09:31:00-04:00",
                    "2026-04-11T09:31:01-04:00",
                ],
                format="mixed",
            ),
            "symbol": ["AAPL", "AAPL", "AAPL", "AAPL"],
            "source": ["wrds_taq_nbbom"] * 4,
            "bid_exchange": ["Q"] * 4,
            "ask_exchange": ["Q"] * 4,
            "nbbo_quote_condition": ["R"] * 4,
            "bid": [190.02, 190.00, 191.00, 191.10],
            "ask": [190.04, 190.02, 191.04, 191.12],
            "bid_size": [300, 500, 0, 250],
            "ask_size": [700, 500, 0, 750],
        }
    )


def test_build_quote_features_v1_computes_qi_and_qr_by_trading_date() -> None:
    result = build_quote_features_v1(quote_frame())
    features = result.quote_features

    assert features[TRADING_DATE].tolist() == [
        "2026-04-10",
        "2026-04-10",
        "2026-04-11",
        "2026-04-11",
    ]
    assert features[MIDQUOTE].tolist() == pytest.approx([190.01, 190.03, 191.02, 191.11])
    assert features[QUOTED_DEPTH].tolist() == [1000, 1000, 0, 1000]
    assert features[QUOTE_IMBALANCE].iloc[0] == 0.0
    assert features[QUOTE_IMBALANCE].iloc[1] == pytest.approx(-0.4)
    assert pd.isna(features[QUOTE_IMBALANCE].iloc[2])
    assert pd.isna(features[QUOTE_REVISION].iloc[0])
    assert features[QUOTE_REVISION].iloc[1] == pytest.approx(0.02)
    assert pd.isna(features[QUOTE_REVISION].iloc[2])
    assert features[QUOTE_REVISION].iloc[3] == pytest.approx(0.09)
    assert features[QUOTE_REVISION_BPS].iloc[1] > 0

    assert result.diagnostics.input_quote_rows == 4
    assert result.diagnostics.output_feature_rows == 4
    assert result.diagnostics.zero_quoted_depth_rows == 1
    assert result.diagnostics.quote_imbalance_null_rows == 1
    assert result.diagnostics.quote_revision_null_rows == 2
    assert result.diagnostics.trade_signing_applied is False
    assert result.diagnostics.ofi_from_signed_trades_implemented is False


def test_build_quote_feature_dataset_writes_outputs(tmp_path: Path) -> None:
    config = load_data_slice_config(CONFIG_PATH)
    processed_root = tmp_path / "processed"
    slice_root = processed_root / config.slice_name
    slice_root.mkdir(parents=True)
    quote_frame().to_csv(slice_root / f"{config.slice_name}_quotes_clean.csv", index=False)

    result = build_quote_feature_dataset(config, processed_dir=processed_root)

    assert result.paths.quote_feature_path.exists()
    assert result.paths.manifest_path.exists()
    manifest = json.loads(result.paths.manifest_path.read_text())
    assert manifest["feature_scope_note"] == QUOTE_FEATURE_SCOPE_NOTE
    assert manifest["feature_status"] == {
        "quote_features_implemented": "v1",
        "trade_signing_applied": False,
        "ofi_from_signed_trades_implemented": False,
        "labels_implemented": False,
        "backtest_implemented": False,
        "research_grade_strategy_sample": False,
    }
    assert manifest["diagnostics"]["feature_group_keys"] == ["symbol", "trading_date"]
    assert manifest["diagnostics"]["zero_quoted_depth_rows"] == 1
    assert manifest["diagnostics"]["quote_imbalance_null_rows"] == 1
