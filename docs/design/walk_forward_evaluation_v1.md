# Walk-Forward Evaluation v1

## Purpose

Walk-forward evaluation v1 measures whether precomputed signal rows align with
future midquote labels in out-of-sample dates. It is a statistical evaluation
layer, not a backtest.

## Scope

Walk-forward evaluation v1 evaluates precomputed signal rows against future
midquote labels using expanding training-date context and next-date test folds.
It does not optimize thresholds, fit models, apply transaction costs, or run
backtests.

## Inputs

- `*_signals_v1.csv`

The input already contains signal columns and future midquote labels. Labels
are used only as targets:

```text
label_usage_policy = labels_used_only_as_targets
signal_usage_policy = evaluate_precomputed_signals_without_refitting
```

## Fold Policy

The default policy is:

```text
evaluation_policy = expanding_train_dates_next_date_test
min_train_dates = 1
```

For three trading dates, this creates two folds:

```text
fold_001: train day 1, test day 2
fold_002: train days 1-2, test day 3
```

No thresholds are selected from the training window in v1. The training dates
are recorded so later versions can add threshold selection without changing the
evaluation contract.

## Metrics

For each fold and horizon, v1 reports:

- label available and missing rows
- evaluated signal rows
- signal coverage
- long and short signal rows
- signal accuracy
- non-flat signal accuracy
- long and short accuracy
- label direction distribution
- mean and median signal-aligned future return in bps

`mean_signal_aligned_return_bps` is:

```text
mean(signal * future_midquote_return_bps)
```

It is a statistical directional-alignment measure, not PnL.

## Outputs

- `*_walk_forward_evaluation_v1.csv`
- `*_walk_forward_evaluation_v1_manifest.json`

## Current AAPL Slice

For `aapl_wrds_20260313_20260410`, walk-forward evaluation v1 currently uses:

- input signal rows: `12,141,453`
- trading dates: 20 dates from `2026-03-13` through `2026-04-10`, excluding `2026-04-03`
- fold count: `19`
- horizons: `100ms`, `500ms`, `1s`, `5s`
- signal column: `sequential_gate_signal`

Aggregate evaluation-fold results:

| horizon | evaluated signal rows | signal accuracy | mean signal-aligned return bps |
| --- | ---: | ---: | ---: |
| `100ms` | `347,610` | `0.4709` | `0.2169` |
| `500ms` | `347,362` | `0.4886` | `0.2195` |
| `1s` | `347,199` | `0.4979` | `0.2655` |
| `5s` | `346,251` | `0.5084` | `0.3840` |

These are statistical direction-alignment diagnostics only. They should not be
presented as tradable PnL or a cost-adjusted result.

This output is not a research-grade strategy result. Threshold optimization,
model training, cost modeling, and execution-aware backtesting remain separate
steps.
