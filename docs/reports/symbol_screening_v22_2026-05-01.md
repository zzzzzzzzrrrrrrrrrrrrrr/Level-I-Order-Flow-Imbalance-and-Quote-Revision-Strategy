# Symbol Screening V2.2 Report

Date: 2026-05-01

Latest rerun: 2026-05-05 after validation-only adverse-selection cleanup

Slice: `aapl_wrds_20260313_20260410`

Universe name: `liquid_large_cap_v22_screen`

## Status

This run is an AAPL negative benchmark, not a true cross-symbol screen. The AAPL data-slice config remains unchanged for reproducibility. The larger intended universe is declared separately in `configs/experiments/v22_symbol_screen_liquid_large_cap.yaml`, but that experiment config currently has only the AAPL processed slice under `data_slices`.

The workflow did not change core data schemas. It read existing v2.1 candidate events, quote features, and optional v2.1 orders.

2026-05-05 method recheck:

- pass/fail flags are validation-only screening diagnostics.
- filled-vs-unfilled passive adverse-selection is now computed from validation
  order dates only.
- test-period rows remain diagnostic output and must not affect symbol
  selection.
- Level-I passive fills are not real queue-position fills.

## Command

```powershell
& "D:\python_library_envs\VHFT_lab\python.exe" scripts\run_symbol_screen_v22.py configs\experiments\v22_symbol_screen_liquid_large_cap.yaml
```

2026-05-05 rerun command:

```powershell
& "D:\python_library_envs\VHFT_lab\python.exe" scripts\run_symbol_screen_v22.py configs\experiments\v22_symbol_screen_liquid_large_cap.yaml --processed-dir data\processed
```

## Outputs

- `outputs/experiments/v22_symbol_screen_liquid_large_cap/config.yaml`
- `outputs/experiments/v22_symbol_screen_liquid_large_cap/notes.md`
- `outputs/experiments/v22_symbol_screen_liquid_large_cap/tables/v22_symbol_screen_summary.csv`
- `outputs/experiments/v22_symbol_screen_liquid_large_cap/tables/v22_symbol_screen_deciles.csv`
- `outputs/experiments/v22_symbol_screen_liquid_large_cap/tables/v22_symbol_screen_horizon_sweep.csv`
- `outputs/experiments/v22_symbol_screen_liquid_large_cap/tables/v22_symbol_screen_manifest.json`
- `outputs/experiments/v22_symbol_screen_liquid_large_cap/figures/v22_symbol_screen_move_over_cost.svg`
- `outputs/experiments/v22_symbol_screen_liquid_large_cap/figures/v22_symbol_screen_decile_markout.svg`

## AAPL Summary

| metric | value |
| --- | ---: |
| candidate_events | 186,405 |
| mean_spread_bps | 1.1248 |
| median_spread_bps | 1.1782 |
| p90_spread_bps | 1.6001 |
| mean_cost_bps | 0.7279 |
| top_1pct_best_horizon | 1s |
| top_1pct_mean_move_bps | 0.6102 |
| top_1pct_mean_cost_bps | 0.9413 |
| top_1pct_move_over_cost | 0.6483 |
| top_5pct_best_horizon | 30s |
| top_5pct_move_over_cost | 0.4837 |
| filled_1s_markout_bps | 0.1355 |
| unfilled_1s_markout_bps | 0.3170 |
| adverse_selection_flag | true |
| validation_pass_flag | false |
| validation_strong_pass_flag | false |
| validation_fail_flag | true |
| test_used_for_selection | false |

## Interpretation

AAPL fails the v2.2 screen on validation. The top 1% validation bucket does not cover cost, and validation-period passive filled markout is worse than validation-period unfilled markout. This supports keeping AAPL as a negative benchmark before spending compute on broader v2.1 passive/hybrid grids.

The 2026-05-05 rerun changed the adverse-selection markout numbers because the
calculation now filters v2.1 orders to validation dates before summary
pass/fail flags are built. The conclusion did not change:

```text
top_1pct_move_over_cost=0.6482601556238383
filled_1s_markout_bps=0.1354918622508357
unfilled_1s_markout_bps=0.3170084542740186
adverse_selection_flag=True
validation_pass_flag=False
validation_fail_flag=True
test_used_for_selection=False
```

## Validation

Unit tests:

```text
128 passed
```
