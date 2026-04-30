# Model Training v1

Recorded: 2026-04-30.

## Purpose

Model training v1 narrows the project to an AAPL single-slice prototype:

```text
AAPL signals
-> train model score on train date
-> select feature set and score threshold on validation date
-> run held-out test accounting backtest
```

This layer is meant to run the full model-to-backtest loop for the current AAPL
slice. It is not a generalized multi-symbol research result.

## Split

For the current AAPL slice:

```text
train      = 2026-04-08
validation = 2026-04-09
test       = 2026-04-10
```

The test date is evaluated after validation selection. It is not used to select
the model candidate.

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

## Outputs

- `*_model_training_v1_predictions.csv`
- `*_model_training_v1_candidates.csv`
- `*_model_backtest_v1_orders.csv`
- `*_model_backtest_v1_ledger.csv`
- `*_model_backtest_v1_summary.csv`
- `*_model_training_v1_manifest.json`

## Non-Goals

Model training v1 does not:

- claim generalization beyond the AAPL three-day prototype slice
- use test data for model or threshold selection
- train a complex ML model
- include official broker / SEC / FINRA / exchange fees
- simulate passive fills, queue priority, or latency
- produce a research-grade profitability claim
