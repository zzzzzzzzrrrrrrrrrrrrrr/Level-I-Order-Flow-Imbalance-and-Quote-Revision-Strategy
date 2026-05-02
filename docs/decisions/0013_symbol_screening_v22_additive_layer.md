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

## Addendum: Phase-1 Liquidity-Regime Groups

Date: 2026-05-02

Add a new group-aware diagnostic experiment:

```text
configs/experiments/v22_symbol_screen_phase1_by_liquidity_regime_same_20d.yaml
```

The experiment treats liquidity-regime groups as first-class, ex-ante
research-design objects:

- `group_A_ultra_liquid_mega_cap_control`
- `group_B_high_turnover_tick_sensitive_candidates`
- `group_C_non_tech_large_cap_liquidity_controls`

The group names describe liquidity-regime hypotheses before the run. They do
not encode expected winners, profitability, or post-run outcomes.

Group metadata is propagated to diagnostic tables, figures, manifests, notes,
and reports. It is explicitly not allowed to alter:

- signal generation
- labeling
- threshold selection
- horizon selection
- cost accounting
- pass/fail criteria

The same-date-window assumption is audited from actual processed trading dates
and data-slice metadata. The config declares the 2026-03-13 through 2026-04-10
20-trading-day window, while manifests record per-symbol start/end/date-list
checks and missing trading dates.

This is an additive reporting and audit extension. It does not change the AAPL
negative baseline, v1 baseline logic, WRDS mappings, or core schemas.
