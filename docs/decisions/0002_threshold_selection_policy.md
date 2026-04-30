# Decision 0002: Threshold Selection Policy

## Status

Accepted.

Recorded: 2026-04-30.

## Context

Signals v1 used diagnostic sign-agreement thresholds:

```text
qi_threshold = 0.0
signed_flow_threshold = 0.0
qr_threshold_bps = 0.0
```

Those defaults are useful for pipeline validation, but they should not be
treated as selected strategy parameters. A training-window threshold-selection
stage is needed before cost modeling or backtesting.

## Decision

Implement threshold selection v1 as walk-forward grid search:

```text
threshold_selection_policy = train_window_grid_search_next_date_test
threshold_objective = maximize_train_mean_signal_aligned_return_bps
signal_construction_policy = recompute_sequential_gate_from_selected_thresholds
```

The default grid is intentionally small:

```text
QI: 0.0, 0.1, 0.25
signed-flow: 0.0, 0.1, 0.25
QR bps: 0.0, 0.1, 0.25
```

Use a minimum number of train-window signals before a threshold candidate is
eligible. The current default is:

```text
min_train_signals = 100
```

## Rejected Alternatives

Using the same diagnostic zero thresholds for backtesting was rejected because
it would skip parameter selection.

Selecting thresholds on the full sample was rejected because it would leak test
date labels into the parameter choice.

Using a large threshold grid was rejected for the first version because the
current AAPL slice has only three trading dates; a large grid would invite
overfitting and produce false precision.

## Consequences

Threshold selection v1 is still statistical. It does not fit models, apply
transaction costs, or simulate execution. It is a necessary step before
backtesting, but not sufficient evidence of tradability.
