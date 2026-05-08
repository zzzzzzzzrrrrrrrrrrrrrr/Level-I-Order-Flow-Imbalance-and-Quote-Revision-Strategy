# Symbol Screening V2.2 Phase-1 Liquidity-Regime Screen

Date: 2026-05-02

Latest AAPL-only rerun: 2026-05-05 after v2.1/v2.2 diagnostic cleanup

Config:

```text
configs/experiments/v22_symbol_screen_phase1_by_liquidity_regime_same_20d.yaml
```

Research status: diagnostic only.

## Purpose

This experiment is the first cross-symbol v2.2 screening design organized by
ex-ante liquidity-regime hypotheses. It is not a profitability test and does
not replace the AAPL negative benchmark.

The date window is a 12-symbol same 20-trading-day slice:

```text
2026-03-13 through 2026-04-10
regular market hours
```

The same-window claim must be verified from the run manifest, not inferred only
from start/end dates.

Latest status:

```text
AAPL processed artifacts are available and were rerun.
The remaining 11 configured symbols are still pending extraction / processed artifacts.
This is not yet a 12-symbol result.
```

## Research Setup Logic

The phase-1 screen is motivated by the AAPL diagnostic chain:

```text
v1 sequential-gate baseline
-> cost-aware linear-score baseline
-> v2.1 passive/hybrid diagnostics
```

Those AAPL diagnostics did not support a robust positive cost-after-trading
alpha conclusion. The research response is not to keep tuning AAPL or change
v1 baseline logic; it is to enlarge the controlled sample and test whether the
same Level-I proxies behave differently across pre-declared liquidity regimes.

The phase-1 screen therefore asks a narrower question:

```text
Do any symbols in the same 20-trading-day window show validation-only
top-bucket move/cost diagnostics strong enough to justify more expensive
execution simulation?
```

Any positive-looking result remains a screening result until it survives the
project's out-of-sample, cost, execution, and robustness checks.

## Groups

| Group | Group ID | Research Role | Symbols |
| --- | --- | --- | --- |
| Group A | `group_A_ultra_liquid_mega_cap_control` | ultra-liquid mega-cap control | AAPL, MSFT, NVDA, AMZN, META |
| Group B | `group_B_high_turnover_tick_sensitive_candidates` | high-turnover tick/spread-structure candidates | AMD, BAC, C, F |
| Group C | `group_C_non_tech_large_cap_liquidity_controls` | non-tech large-cap liquidity controls | XOM, JPM, WMT |

These groups are research-design objects. They are not post-run winner/loser
labels.

## Guardrails

Group metadata may affect:

- reporting tables
- group-level aggregation
- diagnostic figures
- output folder names
- notes and report sections

Group metadata must not affect:

- signal generation
- labeling
- threshold selection
- horizon selection
- cost accounting
- pass/fail criteria

Test data must not be used for symbol or parameter selection.

## Current Data Availability

At scaffold creation time, only the AAPL processed slice exists:

```text
configs/data/aapl_wrds_20260313_20260410.yaml
```

The phase-1 generic data config is:

```text
configs/data/phase1_wrds_12_symbols_20260313_20260410.yaml
```

The preferred implementation path is to use this shared config for extraction
and pipeline planning. If existing downstream output contracts require
symbol-specific `slice_name` artifacts, generate per-symbol configs or paths
from the shared config rather than hand-copying one large YAML per symbol.

The remaining 11 symbols should be added to the experiment's `data_slices` only
after WRDS extraction and required pipeline artifacts exist for the same
configured trading-date list.

## Planned Command

```powershell
& "D:\python_library_envs\VHFT_lab\python.exe" scripts\run_symbol_screen_v22.py configs\experiments\v22_symbol_screen_phase1_by_liquidity_regime_same_20d.yaml
```

2026-05-05 AAPL-only rerun command:

```powershell
& "D:\python_library_envs\VHFT_lab\python.exe" scripts\run_symbol_screen_v22.py configs\experiments\v22_symbol_screen_phase1_by_liquidity_regime_same_20d.yaml --processed-dir data\processed
```

Config-driven full-pipeline planning command:

```powershell
& "D:\python_library_envs\VHFT_lab\python.exe" scripts\run_experiment.py configs\experiments\v22_symbol_screen_phase1_by_liquidity_regime_same_20d.yaml
```

The orchestration command defaults to a dry-run plan. Add `--execute` only when
the resolved stages and WRDS credentials have been checked.

## Expected Outputs

```text
outputs/experiments/v22_symbol_screen_phase1_by_liquidity_regime_same_20d/
  config.yaml
  notes.md
  tables/
    all_symbols_summary.csv
    all_symbols_decile_markout.csv
    all_symbols_horizon_sweep.csv
    group_level_summary.csv
    group_level_ranking.csv
  figures/
    all_symbols_move_over_cost.svg
    all_symbols_net_per_trip.svg
    group_move_over_cost.svg
    group_decile_markout.svg
  groups/
    group_A_ultra_liquid_mega_cap_control/
    group_B_high_turnover_tick_sensitive_candidates/
    group_C_non_tech_large_cap_liquidity_controls/
```

Legacy v2.2 table and figure names are also written for backward compatibility.

## Manifest Audit Requirements

The run manifest should record:

- same start-date check
- same end-date check
- same trading-date-list check
- same session-filter check
- raw row counts when WRDS raw manifests exist
- processed row counts
- missing trading dates

Any group-level interpretation should be written only after these checks pass
for the symbols included in the run.

## 2026-05-05 AAPL-Only Rerun Result

Artifacts:

```text
outputs/experiments/v22_symbol_screen_phase1_by_liquidity_regime_same_20d/
```

Manifest audit for the included AAPL slice:

```text
same_start_date=True
same_end_date=True
same_trading_dates=True
same_session_filter=True
raw_row_count=33431744
processed_row_count=21287434
candidate_event_count=186405
missing_trading_dates=[]
test_used_for_selection=False
```

AAPL summary:

| metric | value |
| --- | ---: |
| group | Group A |
| candidate_events | 186,405 |
| top_1pct_best_horizon | 1s |
| top_1pct_mean_move_bps | 0.6102 |
| top_1pct_mean_cost_bps | 0.9413 |
| top_1pct_move_over_cost | 0.6483 |
| net_per_trip | -0.3311 |
| filled_1s_markout_bps | 0.1355 |
| unfilled_1s_markout_bps | 0.3170 |
| adverse_selection_flag | true |
| validation_pass_flag | false |
| validation_fail_flag | true |
| test_used_for_selection | false |

Group-level summary for currently available artifacts:

| group | configured_symbols | available_symbols | candidate_events | mean_move_over_cost | mean_net_per_trip | validation_pass_count | validation_fail_count |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Group A | 5 | 1 | 186,405 | 0.6483 | -0.3311 | 0 | 1 |
| Group B | 4 | 0 | n/a | n/a | n/a | 0 | 0 |
| Group C | 3 | 0 | n/a | n/a | n/a | 0 | 0 |

Interpretation: the AAPL negative-control result persists under the phase-1
group-aware report path. No cross-symbol or group-level conclusion should be
drawn until the other 11 symbols have matching same-window artifacts.
