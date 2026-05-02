# Design Docs Index

Design docs define module boundaries and data contracts. They are not empirical results.

## Pipeline Stages

| Stage | Design Doc | Main Script |
| --- | --- | --- |
| Cleaning | `cleaning_rules_v2.md` | `scripts/build_dataset.py` |
| Quote-trade alignment | `quote_trade_alignment_v1.md` | `scripts/align_trades.py` |
| Quote features | `quote_features_v1.md` | `scripts/build_quote_features.py` |
| Trade signing | `trade_signing_v1.md` | `scripts/sign_trades.py` |
| Signed-flow features | `signed_flow_features_v1.md` | `scripts/build_signed_flow_features.py` |
| Labeling | `labeling_v1.md` | `scripts/build_labels.py` |
| Signals | `signals_v1.md` | `scripts/build_signals.py` |
| Walk-forward evaluation | `walk_forward_evaluation_v1.md` | `scripts/run_walk_forward.py` |
| Threshold selection | `threshold_selection_v1.md` | `scripts/run_threshold_selection.py` |
| Cost model | `cost_model_v1.md` | `scripts/run_cost_model.py` |
| Execution accounting | `execution_accounting_v1.md` | `scripts/run_execution_accounting.py` |
| Target-position accounting | `target_position_accounting_v1.md` | `scripts/run_target_position_accounting.py` |
| Parameter sensitivity | `parameter_sensitivity_v1.md` | `scripts/run_parameter_sensitivity.py` |
| TVT parameter selection | `tvt_parameter_selection_v1.md` | `scripts/run_tvt_parameter_selection.py` |
| Backtest v1 | `backtest_v1.md` | `scripts/run_backtest.py` |
| Model training v1 | `model_training_v1.md` | `scripts/run_model_training.py` |
| PnL reporting | `pnl_reporting_v1.md` | `scripts/plot_pnl.py` |
| Microstructure v2.1 | `microstructure_v21.md` | `scripts/run_microstructure_v21_diagnostics.py` |
| Symbol screening v2.2 | `symbol_screening_v22.md` | `scripts/run_symbol_screen_v22.py` |

## Current Architecture Boundary

V1 and cost-aware baselines remain unchanged research baselines. V2.1 and v2.2 live under `src/level1_ofi_qr/diagnostics/` as diagnostic paths.

Do not move passive-fill assumptions, symbol screening, or AAPL negative-result logic into the v1 signal-generation modules.
