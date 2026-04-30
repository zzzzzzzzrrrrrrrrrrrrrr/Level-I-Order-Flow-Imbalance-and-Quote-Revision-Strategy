# Decision 0007: Parameter Sensitivity Scope

## Status

Accepted.

Recorded: 2026-04-30.

## Context

After target-position accounting, it is tempting to start changing thresholds,
cooldowns, position limits, and costs until a favorable result appears. That
would create data-snooping risk, especially on a three-day AAPL slice.

The project needs an intermediate layer that can run and record parameter grids
without selecting final hyperparameters.

## Decision

Implement parameter sensitivity v1 as an exhaustive grid report over
target-position accounting parameters.

The stage records:

- the full configured grid
- every candidate result
- manifest flags stating that parameter selection is not implemented
- no winner, no final hyperparameter, and no profitability claim

## Rejected Alternatives

Manual parameter tuning was rejected because it would make results impossible to
audit.

Selecting the best full-sample candidate was rejected because it leaks test
information into the parameter choice.

Combining sensitivity and train-window selection in one step was rejected
because the project first needs a transparent grid-reporting layer.

## Consequences

The project can now run controlled sensitivity checks. A later train-window
selection module should build on this output or the same candidate grid, but
must choose parameters using training dates only.
