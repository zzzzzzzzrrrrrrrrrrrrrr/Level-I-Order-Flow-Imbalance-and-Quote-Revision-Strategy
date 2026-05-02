# 0012 Microstructure V2.1 Independent Diagnostic Path

Date: 2026-05-01

## Decision

Add microstructure v2.1 as an independent diagnostic path under `src/level1_ofi_qr/diagnostics/microstructure_v21/` instead of embedding it in the existing v1 strategy or cost-aware selection files.

## Reason

AGENTS.md requires separation between forecasting model, decision rule, execution rule, risk controls, accounting, and reporting. V2.1 introduces passive/hybrid execution assumptions, cancellation, queue-haircut scenarios, and submitted-order metrics. These are different responsibilities from v1 signal generation and v1 cost-aware threshold selection.

Keeping v2.1 separate prevents diagnostic execution assumptions from silently changing the established baselines.

## Rejected Alternatives

Embedding v2.1 into the existing strategy file was rejected because it would blur baseline signal logic with experimental execution simulation.

Treating passive fills as a production backtest was rejected because Level-I data cannot identify queue position, hidden liquidity, cancellation priority, or venue-specific matching priority.

Running only the full default grid before saving any focused result was rejected for now because the current implementation repeats passive-fill simulation over 186,405 candidate events and timed out on the full AAPL slice.

## Consequences

V1 and v2.0 artifacts remain comparable baselines. V2.1 outputs are clearly labeled diagnostics and write separate `*_microstructure_v21_*` artifacts.

The next engineering need is performance optimization for full-grid validation, not further final-test tuning.
