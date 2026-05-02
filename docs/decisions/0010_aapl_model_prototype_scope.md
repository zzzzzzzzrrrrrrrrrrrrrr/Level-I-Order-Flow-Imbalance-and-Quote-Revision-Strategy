# Decision 0010: AAPL Model Prototype Scope

Recorded: 2026-04-30.

## Status

Superseded for the active run by the 20-day AAPL slice
`aapl_wrds_20260313_20260410`. The original 3-day decision remains historical
context for the first model prototype.

## Decision

The original strategy loop was scoped to the then-current AAPL three-day slice.
The goal was to run a complete model-to-backtest prototype, not to claim general
market validity.

## Rationale

The project already has auditable data, feature, label, signal, accounting,
parameter selection, and backtest scaffolds. Before expanding to more symbols
or dates, the AAPL prototype should prove that a trained model can be selected
on validation data and evaluated through the existing accounting backtest.

## Consequence

The original 3-day model training v1 used:

```text
train      = 2026-04-08
validation = 2026-04-09
test       = 2026-04-10
```

The test date has already been inspected during earlier prototype backtests, so
results from this stage should be reported as AAPL prototype results, not as
untouched final test evidence.

The active 20-day run now uses expanding train, next validation, and next test
folds as recorded in `docs/design/model_training_v1.md` and
`docs/reports/aapl_wrds_20260313_20260410_pipeline_report.md`.
