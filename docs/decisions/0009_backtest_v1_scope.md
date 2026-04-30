# Decision 0009: Backtest v1 Scope

Recorded: 2026-04-30.

## Decision

Backtest v1 evaluates only TVT-selected target-position accounting candidates
on held-out test dates.

## Rationale

The project already has separate layers for signal construction, cost
diagnostics, target-position accounting, parameter sensitivity, and
train-validation-test parameter selection. The first backtest layer should
compose these pieces without adding another hidden tuning step.

## Consequence

Backtest v1 writes orders, ledger, summary, and manifest artifacts for selected
test folds. It does not use test data for selection and does not make final
profitability claims.

Future research-grade backtesting still needs explicit latency modeling,
official fee assumptions, condition-code eligibility, broader train /
validation / test samples, and robustness across symbols and dates.
