from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from level1_ofi_qr.signals import (
    QI_SIGNAL,
    QR_SIGNAL,
    SEQUENTIAL_GATE_SIGNAL,
    SIGNAL_INPUT_AVAILABLE,
    SIGNAL_POLICY_NOTE,
    SIGNAL_QUOTE_EVENT_TIME,
    SIGNAL_REASON,
    SIGNED_FLOW_SIGNAL,
    SignalRuleConfig,
    SignalRuleError,
    build_sequential_gate_signals_v1,
    build_signal_dataset,
)
from level1_ofi_qr.utils import load_data_slice_config


ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = ROOT / "configs" / "data" / "aapl_wrds_20260408_20260410.yaml"


def labeled_feature_rows(label_value: int = -1) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "event_time": pd.to_datetime(
                [
                    "2026-04-10T09:30:00.500000-04:00",
                    "2026-04-10T09:30:02.500000-04:00",
                    "2026-04-10T09:30:04.500000-04:00",
                    "2026-04-11T09:29:59-04:00",
                    "2026-04-10T09:30:00.500000-04:00",
                ],
                format="mixed",
            ),
            "symbol": ["AAPL", "AAPL", "AAPL", "AAPL", "MSFT"],
            "trading_date": [
                "2026-04-10",
                "2026-04-10",
                "2026-04-10",
                "2026-04-11",
                "2026-04-10",
            ],
            "signed_flow_imbalance_500ms": [0.50, -0.25, -0.25, 0.50, 0.75],
            "future_midquote_direction_1s": [label_value] * 5,
        }
    )


def quote_features() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "event_time": pd.to_datetime(
                [
                    "2026-04-10T09:30:00-04:00",
                    "2026-04-10T09:30:02-04:00",
                    "2026-04-10T09:30:04-04:00",
                    "2026-04-10T15:59:59-04:00",
                    "2026-04-11T09:30:01-04:00",
                    "2026-04-10T09:30:00-04:00",
                ],
                format="mixed",
            ),
            "symbol": ["AAPL", "AAPL", "AAPL", "AAPL", "AAPL", "MSFT"],
            "trading_date": [
                "2026-04-10",
                "2026-04-10",
                "2026-04-10",
                "2026-04-10",
                "2026-04-11",
                "2026-04-10",
            ],
            "midquote": [100.00, 100.10, 100.00, 100.20, 101.00, 200.00],
            "quote_imbalance": [0.40, -0.30, 0.20, 0.50, 0.50, 0.75],
            "quote_revision_bps": [1.0, -0.5, -0.5, 1.0, 1.0, 1.0],
            "quoted_spread": [0.02, 0.02, 0.03, 0.02, 0.02, 0.05],
            "relative_spread": [0.0002, 0.0002, 0.0003, 0.0002, 0.0002, 0.00025],
        }
    )


def test_build_sequential_gate_signals_v1_uses_features_not_labels() -> None:
    result = build_sequential_gate_signals_v1(labeled_feature_rows(-1), quote_features())
    signals = result.signals

    assert len(signals) == len(labeled_feature_rows())
    assert signals.loc[0, SIGNAL_QUOTE_EVENT_TIME] == pd.Timestamp(
        "2026-04-10T09:30:00-04:00"
    )
    assert signals.loc[0, QI_SIGNAL] == 1
    assert signals.loc[0, SIGNED_FLOW_SIGNAL] == 1
    assert signals.loc[0, QR_SIGNAL] == 1
    assert signals.loc[0, SEQUENTIAL_GATE_SIGNAL] == 1
    assert signals.loc[0, SIGNAL_REASON] == "long_all_gates"

    assert signals.loc[1, SEQUENTIAL_GATE_SIGNAL] == -1
    assert signals.loc[1, SIGNAL_REASON] == "short_all_gates"
    assert signals.loc[2, SEQUENTIAL_GATE_SIGNAL] == 0
    assert signals.loc[2, SIGNAL_REASON] == "gates_not_aligned"

    assert not bool(signals.loc[3, SIGNAL_INPUT_AVAILABLE])
    assert signals.loc[3, SEQUENTIAL_GATE_SIGNAL] == 0
    assert signals.loc[3, SIGNAL_REASON] == "inputs_missing"

    assert signals.loc[4, SEQUENTIAL_GATE_SIGNAL] == 1

    changed_labels = build_sequential_gate_signals_v1(
        labeled_feature_rows(1),
        quote_features(),
    ).signals
    assert changed_labels[SEQUENTIAL_GATE_SIGNAL].tolist() == signals[
        SEQUENTIAL_GATE_SIGNAL
    ].tolist()

    diagnostics = result.diagnostics
    assert diagnostics.row_preserving is True
    assert diagnostics.signal_rule == "sequential_gate_qi_signed_flow_qr_v1"
    assert diagnostics.threshold_selection_policy == "diagnostic_defaults_not_optimized"
    assert (
        diagnostics.label_usage_policy
        == "labels_retained_for_evaluation_not_used_for_signal"
    )
    assert diagnostics.labels_retained is True
    assert diagnostics.labels_used_for_signal is False
    assert diagnostics.long_signal_rows == 2
    assert diagnostics.short_signal_rows == 1
    assert diagnostics.no_trade_rows == 2
    assert diagnostics.walk_forward_implemented is False
    assert diagnostics.backtest_implemented is False


def test_build_sequential_gate_signals_v1_rejects_negative_threshold() -> None:
    with pytest.raises(SignalRuleError, match="non-negative"):
        build_sequential_gate_signals_v1(
            labeled_feature_rows(),
            quote_features(),
            config=SignalRuleConfig(qi_threshold=-0.1),
        )


def test_build_signal_dataset_writes_manifest(tmp_path: Path) -> None:
    config = load_data_slice_config(CONFIG_PATH)
    processed_root = tmp_path / "processed"
    slice_root = processed_root / config.slice_name
    slice_root.mkdir(parents=True)
    labeled_feature_rows().to_csv(
        slice_root / f"{config.slice_name}_labeled_features_v1.csv",
        index=False,
    )
    quote_features().to_csv(
        slice_root / f"{config.slice_name}_quote_features_v1.csv",
        index=False,
    )

    result = build_signal_dataset(config, processed_dir=processed_root)

    assert result.paths.signal_path.exists()
    assert result.paths.manifest_path.exists()
    manifest = json.loads(result.paths.manifest_path.read_text())
    assert manifest["signal_scope_note"] == SIGNAL_POLICY_NOTE
    assert manifest["signal_status"] == {
        "signals_implemented": "v1",
        "signal_rule": "sequential_gate_qi_signed_flow_qr_v1",
        "threshold_selection_policy": "diagnostic_defaults_not_optimized",
        "label_usage_policy": "labels_retained_for_evaluation_not_used_for_signal",
        "labels_used_for_signal": False,
        "walk_forward_implemented": False,
        "backtest_implemented": False,
        "threshold_optimization_implemented": False,
        "research_grade_strategy_sample": False,
    }
    diagnostics = manifest["diagnostics"]
    assert diagnostics["row_preserving"] is True
    assert diagnostics["long_signal_rows"] == 2
    assert diagnostics["short_signal_rows"] == 1
    assert diagnostics["no_trade_rows"] == 2
