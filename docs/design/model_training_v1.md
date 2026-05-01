# Model Training v1

Recorded: 2026-04-30.

## Purpose

Model training v1 narrows the project to an AAPL single-slice prototype:

```text
AAPL signals
-> train model score on expanding train dates
-> select feature set and score threshold on the next validation date
-> run held-out test accounting backtest on the next test date
```

This layer is meant to run the full model-to-backtest loop for the current AAPL
slice. It is not a generalized multi-symbol research result.

## Split

For the current AAPL slice:

```text
fold count = 18
first fold = train 2026-03-13, validation 2026-03-16, test 2026-03-17
last fold = train 2026-03-13..2026-04-08, validation 2026-04-09, test 2026-04-10
```

Each test date is evaluated after validation selection. Test dates are not used
to select model candidates.

## Model

The v1 model is a standardized linear feature score. On train rows, each feature
is standardized using train-date mean and standard deviation. Feature weights
are learned from the train-date average relation between standardized features
and the future midquote direction label.

The selected score is converted to a trading signal:

```text
model_signal = +1 if model_score > threshold
model_signal = -1 if model_score < -threshold
model_signal =  0 otherwise
```

## Candidate Grid

The current default feature-set candidates are:

- `qi_qr_flow_500ms`
- `qi_qr_flow_multiwindow`

The current score-threshold grid is:

```text
0.0, 0.10, 0.25, 0.50, 0.75, 1.0, 1.25, 1.50, 2.0
```

Candidates must satisfy:

```text
min_validation_orders = 1000
```

This prevents a no-trade candidate from winning only because it avoids losses.

## Cost-Aware Variant

`cost_aware_linear_score` is a separate strategy variant. It reuses the same
trained `model_score` as the linear-score baseline and does not change
`linear_score` or `sequential_gate` signal construction.

The variant adds threshold, cost, cooldown, and holding-period controls:

```text
absolute thresholds = 1.5, 2.0, 2.5, 3.0, 3.5, 4.0
optional abs(score) quantile thresholds = top 10%, 5%, 2%, 1%
cost_multiplier = 1.0, 1.5, 2.0, 2.5
cooldown_seconds = 0, 1, 3, 5
min_holding_seconds = 0, 1, 3, 5
```

Candidate selection uses validation-fold net PnL after estimated costs, not
gross PnL. The rule blocks a candidate trade when estimated round-trip cost
after the selected multiplier exceeds `abs(model_score)`.

## Outputs

- `*_model_training_v1_predictions.csv`
- `*_model_training_v1_candidates.csv`
- `*_model_backtest_v1_orders.csv`
- `*_model_backtest_v1_ledger.csv`
- `*_model_backtest_v1_summary.csv`
- `*_model_training_v1_manifest.json`
- `*_cost_aware_linear_score_predictions.csv`
- `*_cost_aware_linear_score_candidates.csv`
- `*_cost_aware_linear_score_orders.csv`
- `*_cost_aware_linear_score_ledger.csv`
- `*_cost_aware_linear_score_summary.csv`
- `*_cost_aware_linear_score_report.csv`
- `*_cost_aware_linear_score_manifest.json`

## Non-Goals

Model training v1 does not:

- claim generalization beyond the AAPL 20-day prototype slice
- use test data for model or threshold selection
- train a complex ML model
- include official broker / SEC / FINRA / exchange fees
- simulate passive fills, queue priority, or latency
- produce a research-grade profitability claim
