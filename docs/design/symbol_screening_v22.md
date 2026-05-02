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

## Validation Policy

For each symbol, v2.2 assigns the first `validation_min_dates` candidate dates to `validation` and later dates to `test`. Symbol pass/fail flags are computed from validation rows only. Test rows are reported for diagnostics but are not used for symbol selection.

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

That experiment config can list many intended symbols while only including processed symbols under `data_slices`. A true cross-symbol screen requires adding one processed data-slice config per symbol after extraction and pipeline generation. Adding those symbols should happen through data configs and generated slice outputs, not by changing core schemas.
