# Decision 0006: Target-Position Accounting Scope

## Status

Accepted.

Recorded: 2026-04-30.

## Context

Execution accounting v1 reconciles independent fixed-horizon round trips, but
that design can create many overlapping positions. It is useful for formula
checks, but it is not a realistic account state model.

Before tuning parameters, the project needs a target-position accounting layer
with explicit position state and basic account constraints.

## Decision

Implement target-position accounting v1 as a separate execution module.

This stage maps signal rows to bounded target positions and applies:

- max absolute position
- flat-on-no-signal behavior
- EOD flat behavior
- cooldown between accepted orders
- optional max trades per day
- midquote fill proxy with spread / fixed bps / slippage tick cost deduction

## Rejected Alternatives

Tuning thresholds immediately after cost diagnostics was rejected because it
would search for a positive result before account mechanics were constrained.

Replacing fixed-horizon accounting was rejected because that layer is still
useful for validating horizon-specific label and cost arithmetic.

Calling this a final backtest was rejected because official fees, passive
execution, queue position, latency, and train-window parameter selection are not
implemented.

## Consequences

The project now has a safer accounting layer for later sensitivity tests. The
next parameter work should select or stress-test `max_position`, `cooldown`,
cost assumptions, and signal thresholds inside explicit train/test boundaries,
not by manually choosing the best full-sample result.
