# Decision 0005: Execution Accounting v1 Scope

## Status

Accepted.

Recorded: 2026-04-30.

## Context

Cost model v1 showed that the current signal specification does not survive a
spread-only average cost diagnostic. The next step should not be parameter
tuning. The project first needs an accounting layer that separates signal
evidence from tradability evidence.

## Decision

Implement execution accounting v1 as a scaffold that converts active signal rows
into independent fixed-horizon round trips and a cash/inventory ledger.

This stage tracks:

- entry and exit events
- cash deltas
- position deltas
- inventory value
- equity after each fill
- gross PnL
- costs
- net PnL
- turnover and exposure diagnostics

It does not implement:

- target-position optimization
- risk controls
- passive fill simulation
- order book queue modeling
- broker / regulatory fees
- parameter search
- research-grade backtesting

## Rejected Alternatives

Immediately tuning thresholds or horizons after cost diagnostics was rejected
because it would invite data-snooping before the accounting mechanics are fixed.

Calling this a complete backtest was rejected because execution assumptions,
position limits, official fees, routing, and risk controls are still unresolved.

## Consequences

The project can now test whether account arithmetic reconciles when signals are
converted into trades. Any later parameter tuning should use this accounting
layer or a stricter successor, rather than interpreting statistical signal
diagnostics as tradable PnL.
