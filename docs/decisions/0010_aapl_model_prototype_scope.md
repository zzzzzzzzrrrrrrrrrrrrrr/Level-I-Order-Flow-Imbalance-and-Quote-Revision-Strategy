# Decision 0010: AAPL Model Prototype Scope

Recorded: 2026-04-30.

## Decision

The next strategy loop is scoped to the current AAPL three-day slice. The goal
is to run a complete model-to-backtest prototype, not to claim general market
validity.

## Rationale

The project already has auditable data, feature, label, signal, accounting,
parameter selection, and backtest scaffolds. Before expanding to more symbols
or dates, the AAPL prototype should prove that a trained model can be selected
on validation data and evaluated through the existing accounting backtest.

## Consequence

Model training v1 uses:

```text
train      = 2026-04-08
validation = 2026-04-09
test       = 2026-04-10
```

The test date has already been inspected during earlier prototype backtests, so
results from this stage should be reported as AAPL prototype results, not as
untouched final test evidence.
