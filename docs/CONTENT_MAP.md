# Documentation Content Map

This map organizes existing docs by research content. It does not move or
replace files. Use it when the directory-level split between `design`,
`decisions`, and `reports` is too broad.

## Current Research Checkpoint

- `reports/symbol_screening_v22_phase1_by_liquidity_regime_same_20d.md`
  Phase-1 12-symbol liquidity-regime screen setup; AAPL-only rerun complete on
  2026-05-05, remaining 11 symbols pending extraction.
- `reports/symbol_screening_v22_2026-05-01.md`
  AAPL v2.2 negative screening result, rerun 2026-05-05 after
  validation-only adverse-selection cleanup.
- `reports/microstructure_v21_diagnostic_2026-05-01.md`
  AAPL passive/hybrid diagnostic result, rerun 2026-05-05 after v2.1 leakage
  cleanup.
- `reports/microstructure_v2_diagnostic_2026-05-01.md`
  Earlier AAPL microstructure diagnostic result.
- `reports/project_milestones.md`
  Chronological implementation checkpoint list.

## Data, Schema, And Cleaning

- `design/cleaning_rules_v2.md`
- `design/quote_trade_alignment_v1.md`
- `design/quote_features_v1.md`
- `design/trade_signing_v1.md`
- `design/signed_flow_features_v1.md`
- `references/microstructure_method_sources.md`
- `references/wrds_taq_field_notes.md`
- `decisions/0001_pipeline_stage_boundaries.md`
- `decisions/0011_large_slice_memory_policy.md`

## Labels, Signals, And Statistical Evaluation

- `design/labeling_v1.md`
- `design/signals_v1.md`
- `design/walk_forward_evaluation_v1.md`
- `design/threshold_selection_v1.md`
- `design/parameter_sensitivity_v1.md`
- `design/tvt_parameter_selection_v1.md`
- `design/model_training_v1.md`
- `decisions/0002_threshold_selection_policy.md`
- `decisions/0007_parameter_sensitivity_scope.md`
- `decisions/0008_tvt_parameter_selection_scope.md`
- `decisions/0010_aapl_model_prototype_scope.md`

## Cost, Execution, And Backtesting

- `design/cost_model_v1.md`
- `design/execution_accounting_v1.md`
- `design/target_position_accounting_v1.md`
- `design/backtest_v1.md`
- `design/pnl_reporting_v1.md`
- `decisions/0004_cost_model_v1_scope.md`
- `decisions/0005_execution_accounting_v1_scope.md`
- `decisions/0006_target_position_accounting_scope.md`
- `decisions/0009_backtest_v1_scope.md`

## V2 Diagnostics And Symbol Screening

- `design/microstructure_v21.md`
- `design/symbol_screening_v22.md`
- `design/experiment_orchestration_v1.md`
- `decisions/0012_microstructure_v21_independent_diagnostic.md`
- `decisions/0013_symbol_screening_v22_additive_layer.md`
- `reports/microstructure_v21_diagnostic_2026-05-01.md`
- `reports/symbol_screening_v22_2026-05-01.md`
- `reports/symbol_screening_v22_phase1_by_liquidity_regime_same_20d.md`

## Experiment Orchestration And Reproducibility

- `design/experiment_orchestration_v1.md`
- `reports/project_milestones.md`
- `decisions/0003_assumption_registry_location.md`
- `../assumptions/parameter_registry.md`

## AAPL Negative Baseline Trail

Read in this order:

1. `reports/aapl_wrds_20260313_20260410_pipeline_report.md`
2. `reports/microstructure_v2_diagnostic_2026-05-01.md`
3. `reports/microstructure_v21_diagnostic_2026-05-01.md`
4. `reports/symbol_screening_v22_2026-05-01.md`
5. `reports/symbol_screening_v22_phase1_by_liquidity_regime_same_20d.md`

Interpretation: AAPL is the negative-control benchmark; phase 1 expands the
sample before changing baselines or schemas.
