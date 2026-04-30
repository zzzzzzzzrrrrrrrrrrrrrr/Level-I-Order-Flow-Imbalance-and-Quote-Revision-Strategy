# Decision 0004: Cost Model v1 Scope

## Status

Accepted.

Recorded: 2026-04-30.

## Context

Threshold selection v1 produces statistical signal diagnostics, but those
results are not cost-adjusted. Before building a full backtest, the project
needs a narrow layer that estimates the spread and stress costs active signals
must overcome.

The project does not yet model venue routing, passive order fills, broker
commission schedules, SEC / FINRA fees, inventory, or execution latency.

## Decision

Implement cost model v1 as a diagnostic summary over `*_signals_v1.csv`.

The module uses:

- half-spread one-way crossing cost
- full-spread round-trip crossing cost
- fixed bps stress grid
- slippage tick stress grid

The module does not implement:

- position accounting
- passive fill simulation
- broker / regulatory fee schedules
- exchange fees or rebates
- backtesting

## Rejected Alternatives

Starting with a full PnL backtest was rejected because the execution, cost, and
risk assumptions are not yet explicit enough.

Adding broker and exchange fee schedules immediately was rejected because the
current data does not include routing or order type information.

Writing a row-level cost output for every active signal was rejected for v1
because the summary diagnostics are sufficient and avoid creating another large
derived artifact.

## Consequences

Cost model v1 can show whether the current signal edge is large enough to
survive simple spread costs and stress assumptions. It still cannot be presented
as tradable PnL or a research-grade backtest.

The next stage should be named and scoped as an execution/accounting scaffold,
not a complete backtest. Its purpose is to define account mechanics: signal
event, target position, simulated order, fill assumption, cost deduction,
inventory update, cash update, and PnL attribution. It should not be used to
claim strategy profitability until execution assumptions, costs, risk limits,
and sample robustness are explicit.
