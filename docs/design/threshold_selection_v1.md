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

For `aapl_wrds_20260313_20260410`, threshold selection v1 currently produces:

- input signal rows: `12,141,453`
- output summary rows: `76`
- trading dates: 20 dates from `2026-03-13` through `2026-04-10`, excluding `2026-04-03`
- fold count: `19`
- horizons: `100ms`, `500ms`, `1s`, `5s`
- minimum train signals: `100`

Selected thresholds and per-fold test results are written to
`*_threshold_selection_v1.csv`. This table is intentionally not condensed into
a single final threshold because the stage is still diagnostic and not a final
hyperparameter selection.

This output is not a backtest. Cost modeling and execution-aware accounting
remain separate stages.
