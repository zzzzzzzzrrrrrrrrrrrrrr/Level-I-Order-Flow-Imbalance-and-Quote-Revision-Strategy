# Decision 0001: Pipeline Stage Boundaries

## Status

Accepted.

Recorded: 2026-04-30.

## Context

The project now has a working WRDS TAQ pipeline through statistical
walk-forward evaluation:

```text
WRDS extraction
-> dataset build / cleaning v2
-> quote-trade alignment v1
-> quote features v1
-> trade signing v1
-> signed-flow features v1
-> labeling v1
-> signals v1
-> walk-forward evaluation v1
```

Several steps are leakage-sensitive. In particular, cleaning, alignment, trade
signing, label construction, signal construction, and evaluation should not
silently absorb each other's assumptions.

## Decision

Keep each stage as a separate auditable contract with its own package module,
CLI entry point where applicable, design note, tests, and manifest diagnostics.

The current stage boundaries are:

- cleaning v2 does not perform alignment, trade signing, or final research
  sample certification.
- alignment v1 only attaches prior quote state; it does not sign trades or
  filter condition codes.
- trade signing v1 classifies trade direction but does not compute OFI,
  labels, signals, or backtests.
- signed-flow features v1 compute trailing signed-flow features but do not
  construct labels or evaluate signals.
- labeling v1 constructs future midquote targets only; labels are targets, not
  features.
- signals v1 builds precomputed diagnostic sequential-gate signals and does not
  use labels.
- walk-forward evaluation v1 evaluates precomputed signals statistically and
  does not optimize thresholds, fit models, apply costs, or run backtests.

## Rejected Alternatives

One monolithic script that cleans data, signs trades, builds features, labels,
signals, and evaluates results was rejected because it would obscure leakage
boundaries and make diagnostics hard to audit.

Merging trade signing into cleaning was rejected because sale-condition and
NBBO-condition eligibility policies are not finalized.

Starting with a backtest before statistical walk-forward evaluation was rejected
because the current signals use diagnostic default thresholds rather than
training-window-selected thresholds.

## Consequences

Each stage adds some file and manifest overhead, but the project can now inspect
row counts, missing rates, assumptions, and scope flags stage by stage.

Backtesting remains blocked until threshold selection, cost assumptions, and
execution accounting are explicit and tested.
