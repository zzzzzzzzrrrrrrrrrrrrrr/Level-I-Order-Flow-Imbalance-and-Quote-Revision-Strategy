# Threshold Selection v1

## Purpose

Threshold selection v1 selects QI, signed-flow, and QR thresholds inside each
walk-forward training window, then evaluates the selected thresholds on the next
test date.

## Scope

This is still a statistical evaluation layer. It does not fit predictive
models, apply transaction costs, or run backtests.

## Inputs

- `*_signals_v1.csv`

The input contains signal feature columns and future midquote labels. Training
labels are used only for threshold selection:

```text
threshold_selection_policy = train_window_grid_search_next_date_test
threshold_objective = maximize_train_mean_signal_aligned_return_bps
signal_construction_policy = recompute_sequential_gate_from_selected_thresholds
label_usage_policy = train_labels_for_threshold_selection_test_labels_for_evaluation
```

## Default Grid

```text
qi_threshold_grid = 0.0, 0.1, 0.25
signed_flow_threshold_grid = 0.0, 0.1, 0.25
qr_threshold_bps_grid = 0.0, 0.1, 0.25
min_train_signals = 100
```

For each fold and horizon, v1 searches the grid on the training dates and
selects the threshold tuple with the highest mean signal-aligned return in bps,
subject to `min_train_signals`.

## Outputs

- `*_threshold_selection_v1.csv`
- `*_threshold_selection_v1_manifest.json`

## Current AAPL Slice

For `aapl_wrds_20260408_20260410`, threshold selection v1 currently produces:

- input signal rows: `1,648,869`
- output summary rows: `8`
- trading dates: `2026-04-08`, `2026-04-09`, `2026-04-10`
- fold count: `2`
- horizons: `100ms`, `500ms`, `1s`, `5s`
- minimum train signals: `100`

Selected thresholds and test results:

| fold | horizon | QI | signed-flow | QR bps | test mean aligned return bps | test accuracy |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| `fold_001` | `100ms` | `0.25` | `0.25` | `0.25` | `0.3825` | `0.5688` |
| `fold_001` | `500ms` | `0.25` | `0.10` | `0.25` | `-0.0220` | `0.5191` |
| `fold_001` | `1s` | `0.25` | `0.25` | `0.10` | `0.2103` | `0.4850` |
| `fold_001` | `5s` | `0.25` | `0.10` | `0.25` | `2.2773` | `0.4993` |
| `fold_002` | `100ms` | `0.25` | `0.25` | `0.25` | `0.3909` | `0.5322` |
| `fold_002` | `500ms` | `0.00` | `0.25` | `0.25` | `0.3974` | `0.4898` |
| `fold_002` | `1s` | `0.25` | `0.10` | `0.25` | `0.4187` | `0.5289` |
| `fold_002` | `5s` | `0.25` | `0.10` | `0.25` | `0.3637` | `0.6494` |

This output is not a backtest. Cost modeling and execution-aware accounting
remain separate stages.
