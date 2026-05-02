# 0013 Symbol Screening V2.2 As Additive Layer

Date: 2026-05-01

## Decision

Add symbol screening v2.2 as an additive diagnostics layer under `src/level1_ofi_qr/diagnostics/symbol_screening_v22/`.

Do not change the raw, cleaned, aligned, feature, label, or WRDS mapping schemas for cross-symbol screening.

## Reason

The existing data model already uses long-form rows with a `symbol` column. Cross-symbol diagnostics can be implemented by grouping on `symbol` and `trading_date`, so broad schema refactoring would add risk without improving research correctness.

The research need is to avoid expensive passive/hybrid simulation on symbols whose validation top-bucket move/cost is clearly below threshold.

## Rejected Alternatives

Changing WRDS mappings was rejected because the WRDS TAQ fields are the same across symbols.

Creating symbol-specific columns such as `aapl_bid` or `nvda_bid` was rejected because it would break the long-form event model and complicate leakage controls.

Selecting symbols on final test output was rejected because it would violate the project's test-set policy.

## Consequences

V2.2 produces separate screening outputs:

- summary
- deciles
- horizon sweep
- manifest
- screening figures

The screening layer can rank configured symbols by validation-only move/cost before any expensive full passive/hybrid execution grid is run.

The original AAPL data-slice YAML remains unchanged for reproducibility. Larger universe screening is configured separately in `configs/experiments/v22_symbol_screen_liquid_large_cap.yaml`, where `universe.symbols` records the intended universe and `data_slices` records which processed symbol slices are currently available.
