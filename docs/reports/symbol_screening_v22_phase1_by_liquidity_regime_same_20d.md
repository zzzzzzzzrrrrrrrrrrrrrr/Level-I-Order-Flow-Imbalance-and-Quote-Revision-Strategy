# Symbol Screening V2.2 Phase-1 Liquidity-Regime Screen

Date: 2026-05-02

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

The remaining 11 symbols should be added as independent data-slice configs only
after WRDS extraction and per-symbol pipeline artifacts exist for the same
configured trading-date list.

## Planned Command

```powershell
& "D:\python_library_envs\VHFT_lab\python.exe" scripts\run_symbol_screen_v22.py configs\experiments\v22_symbol_screen_phase1_by_liquidity_regime_same_20d.yaml
```

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
