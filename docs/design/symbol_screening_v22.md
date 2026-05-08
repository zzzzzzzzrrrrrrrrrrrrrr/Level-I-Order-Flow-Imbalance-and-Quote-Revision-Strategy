# Symbol Screening V2.2

## Scope

Symbol screening v2.2 is an additive diagnostic layer for deciding whether a symbol deserves expensive passive/hybrid execution simulation.

It does not modify:

- raw WRDS mappings
- cleaned quote/trade schemas
- alignment schema
- quote feature schema
- label schema
- cost schema

The core data model remains long-form:

```text
event_time | symbol | trading_date | ...
```

All screening calculations group by `symbol` and `trading_date`.

## Group-Aware Experiment Design

Liquidity-regime groups are first-class experiment-design objects when an
experiment config includes `universe.groups`. They are ex-ante research
hypotheses, not result labels or post-run chart folders.

The phase-1 group-aware experiment is:

```text
configs/experiments/v22_symbol_screen_phase1_by_liquidity_regime_same_20d.yaml
```

Its screen name and output root are both:

```text
v22_symbol_screen_phase1_by_liquidity_regime_same_20d
```

The experiment defines:

- `group_A_ultra_liquid_mega_cap_control`
- `group_B_high_turnover_tick_sensitive_candidates`
- `group_C_non_tech_large_cap_liquidity_controls`

Group metadata propagates into summary, decile, horizon-sweep, group-level,
manifest, notes, and figure artifacts through:

- `group_id`
- `group_label`
- `research_role`
- `liquidity_hypothesis`
- `date_window`

Boundary rule:

```text
group metadata affects reporting, aggregation, diagnostics, and artifact names only.
It must not affect signal generation, labeling, threshold selection, horizon
selection, cost accounting, or pass/fail criteria.
```

## Phase-1 Research Sequence

Phase 1 follows the AAPL diagnostic path:

```text
v1 sequential-gate baseline
-> cost-aware linear-score baseline
-> v2.1 passive/hybrid microstructure diagnostics
-> v2.2 cross-symbol liquidity-regime screen
```

The AAPL-only path did not support a robust positive cost-after-trading alpha
conclusion. AAPL therefore remains a negative-control benchmark. Phase 1 should
expand the sample before changing baseline logic, retuning AAPL, or adding new
core schemas.

The same-date-window claim is verified from manifests and actual processed
trading dates. The experiment config declares the expected 20 trading dates,
but the output manifest records per-symbol checks for:

- same start date
- same end date
- same trading-date list
- same session filter
- raw row count when a WRDS raw manifest exists
- processed row count
- missing trading dates

## Inputs

Required:

- `*_quote_features_v1.csv`
- candidate events from `*_microstructure_v21_candidate_events.csv` when available

Fallback:

- `*_cost_aware_linear_score_predictions.csv` can be used to rebuild candidate events when the v2.1 candidate file is absent.

Optional:

- `*_microstructure_v21_orders.csv` supplies filled-vs-unfilled passive markout diagnostics when available.

## Outputs

V2.2 writes only diagnostic result tables and figures. Experiment-config runs default to an experiment-scoped folder:

- `outputs/experiments/<screen_name>/config.yaml`
- `outputs/experiments/<screen_name>/notes.md`
- `outputs/experiments/<screen_name>/tables/v22_symbol_screen_summary.csv`
- `outputs/experiments/<screen_name>/tables/v22_symbol_screen_deciles.csv`
- `outputs/experiments/<screen_name>/tables/v22_symbol_screen_horizon_sweep.csv`
- `outputs/experiments/<screen_name>/tables/v22_symbol_screen_manifest.json`
- `outputs/experiments/<screen_name>/figures/v22_symbol_screen_move_over_cost.svg`
- `outputs/experiments/<screen_name>/figures/v22_symbol_screen_decile_markout.svg`

These outputs include explicit `universe_name`, `symbol`, `split`, `horizon`, and `signal_bucket` columns where applicable.

Group-aware experiment-config runs also write additive artifacts:

- `outputs/experiments/<screen_name>/tables/all_symbols_summary.csv`
- `outputs/experiments/<screen_name>/tables/all_symbols_decile_markout.csv`
- `outputs/experiments/<screen_name>/tables/all_symbols_horizon_sweep.csv`
- `outputs/experiments/<screen_name>/tables/group_level_summary.csv`
- `outputs/experiments/<screen_name>/tables/group_level_ranking.csv`
- `outputs/experiments/<screen_name>/figures/all_symbols_move_over_cost.svg`
- `outputs/experiments/<screen_name>/figures/all_symbols_net_per_trip.svg`
- `outputs/experiments/<screen_name>/figures/group_move_over_cost.svg`
- `outputs/experiments/<screen_name>/figures/group_decile_markout.svg`
- `outputs/experiments/<screen_name>/groups/<group_id>/tables/`
- `outputs/experiments/<screen_name>/groups/<group_id>/figures/`
- `outputs/experiments/<screen_name>/groups/<group_id>/notes.md`

The legacy `v22_symbol_screen_*.csv` and SVG outputs remain available for
backward compatibility.

## Validation Policy

For each symbol, v2.2 assigns the first `validation_min_dates` candidate dates to `validation` and later dates to `test`. Symbol pass/fail flags are computed from validation rows only. Test rows are reported for diagnostics but are not used for symbol selection.

Passive filled-vs-unfilled adverse-selection diagnostics are also computed from
validation order dates only. Test-period v2.1 orders must not influence
`adverse_selection_flag`, `validation_pass_flag`, `validation_strong_pass_flag`,
or `validation_fail_flag`.

Every output records `test_used_for_selection=False`.

## Screening Criteria

The summary table reports:

- candidate event count
- spread diagnostics
- top 1% and top 5% move/cost by validation horizon
- filled and unfilled 1s passive markout when v2.1 orders are available
- adverse-selection flag
- validation pass, strong pass, and fail flags

Default pass logic:

```text
validation_pass_flag =
    top_1pct_move_over_cost > 1.0
    and adverse_selection_flag is not True
```

Default strong pass:

```text
top_1pct_move_over_cost > 1.5
```

Default fail:

```text
top_1pct_move_over_cost < 0.5
or adverse_selection_flag is True
```

These are screening diagnostics, not profitability claims.

## Current Limitation

The AAPL data-slice config is intentionally unchanged and remains the reproducible negative benchmark. The larger intended universe is declared in `configs/experiments/v22_symbol_screen_liquid_large_cap.yaml`.

That experiment config can list many intended symbols while only including processed symbols under `data_slices`. A true cross-symbol screen requires adding processed data slices after extraction and pipeline generation. The preferred low-risk path is a generic phase-1 WRDS/data config, or a small config-expansion wrapper, that shares the common WRDS mappings, date window, storage roots, and symbol universe instead of hand-copying one large YAML per symbol.

The extraction layer already supports multiple configured symbols in
`universe.symbols`, and the core data model remains long-form with a `symbol`
column. The v2.2 screening layer still consumes processed slice artifacts listed
under `data_slices`, so every included symbol must have the required processed
artifacts before it is added to a screening run.

The phase-1 liquidity-regime experiment follows the same rule. At creation
time, it references only the existing AAPL processed slice; the remaining
symbols should be added to `data_slices` only after WRDS extraction and
pipeline artifacts exist for the same 20-trading-day window.

## Proxy And Schema Boundary

The V2 roadmap allows additive proxy diagnostics such as microprice pressure,
dynamic quote imbalance, signed-flow persistence, liquidity state,
price-impact proxies, interaction terms, intensity, and duration features.
Those additions should be implemented as additive feature or diagnostic columns
only after the current cross-symbol screen is reproducible.

For v2.2 phase 1, there is no required core schema change. The relevant schema
boundary remains:

```text
event_time | symbol | trading_date | existing quote/trade/feature columns
```

Group metadata is reporting/audit metadata only and must not create
group-dependent signal, label, threshold, horizon, or cost fields.
