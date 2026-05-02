# Decisions Index

Decision docs record why a methodology or architecture choice was accepted.

## Sequence

| Decision | Topic |
| --- | --- |
| `0001_pipeline_stage_boundaries.md` | Separate pipeline stages and responsibilities. |
| `0002_threshold_selection_policy.md` | Threshold selection policy. |
| `0003_assumption_registry_location.md` | Assumption registry location. |
| `0004_cost_model_v1_scope.md` | Cost model v1 scope. |
| `0005_execution_accounting_v1_scope.md` | Execution accounting v1 scope. |
| `0006_target_position_accounting_scope.md` | Target-position accounting scope. |
| `0007_parameter_sensitivity_scope.md` | Parameter sensitivity scope. |
| `0008_tvt_parameter_selection_scope.md` | Train-validation-test parameter selection scope. |
| `0009_backtest_v1_scope.md` | Backtest v1 scope. |
| `0010_aapl_model_prototype_scope.md` | AAPL model prototype scope. |
| `0011_large_slice_memory_policy.md` | Memory policy for 20-day and larger slices. |
| `0012_microstructure_v21_independent_diagnostic.md` | Keep v2.1 passive/hybrid diagnostics independent from baselines. |
| `0013_symbol_screening_v22_additive_layer.md` | Add v2.2 symbol screening without core schema changes. |

## Current Important Decision

The AAPL negative baseline is preserved by tag `aapl-negative-baseline-v22`. Generalized screening is configured separately through `configs/experiments/v22_symbol_screen_liquid_large_cap.yaml`; the original AAPL data config remains unchanged for reproducibility.
