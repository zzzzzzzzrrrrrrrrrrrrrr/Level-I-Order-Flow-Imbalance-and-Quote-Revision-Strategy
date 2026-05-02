# Microstructure V2.1 Diagnostic

Date: 2026-05-01

Slice: `aapl_wrds_20260313_20260410`

Status: diagnostic only. This does not replace v1 baselines or v2.0 cost-aware diagnostics.

## Boundary

V1 remains the unchanged forecasting / decision / execution baseline path. V2.1 is an independent diagnostic path under `src/level1_ofi_qr/diagnostics/microstructure_v21/` that tests whether a wider candidate pool can survive conservative passive or hybrid execution assumptions.

V2.1 uses the existing cost-aware linear-score output as input. It does not change `sequential_gate`, `linear_score`, or the existing `cost_aware_linear_score` selection logic.

## Implementation

V2.1 separates:

- `config.py`: variant grid and diagnostic assumptions.
- `candidate_pool.py`: microprice and candidate-pool filters.
- `execution_selector.py`: `market_entry`, `passive_entry`, or `no_trade`.
- `passive_fill.py`: strict post-submission passive-fill evidence.
- `cancellation.py`: TTL, microprice, quote-revision, spread, and volatility cancellation.
- `metrics.py`: submitted-order and filled-order reporting.
- `workflow.py`: independent build workflow and validation-fold selection.

The default v2.1 adverse-selection buffer is `0.5` bps so weak positive edge does not automatically become aggressive market entry.

## Full-Slice Run

Command run:

```powershell
& "D:\python_library_envs\VHFT_lab\python.exe" scripts\run_microstructure_v21_diagnostics.py configs\data\aapl_wrds_20260313_20260410.yaml --candidate-pools one_tick_spread --edge-thresholds edge_gt_0 --microprice-usages cancellation_only --ttls 1s --queue-haircuts conservative --execution-variants passive_entry_market_exit
```

This is a full-date AAPL slice run over the available v2.1 candidate events, but it is not the full 1728-variant grid. The full default grid timed out before completion on this machine because it implies many repeated passive-fill simulations over 186,405 candidate events.

## Outputs

- Candidate events: `data/processed/aapl_wrds_20260313_20260410/aapl_wrds_20260313_20260410_microstructure_v21_candidate_events.csv`
- Orders: `data/processed/aapl_wrds_20260313_20260410/aapl_wrds_20260313_20260410_microstructure_v21_orders.csv`
- Variant summary: `data/processed/aapl_wrds_20260313_20260410/aapl_wrds_20260313_20260410_microstructure_v21_variant_summary.csv`
- Validation selection: `data/processed/aapl_wrds_20260313_20260410/aapl_wrds_20260313_20260410_microstructure_v21_validation_selection.csv`
- Selected test metrics: `data/processed/aapl_wrds_20260313_20260410/aapl_wrds_20260313_20260410_microstructure_v21_selected_test_metrics.csv`
- Manifest: `data/processed/aapl_wrds_20260313_20260410/aapl_wrds_20260313_20260410_microstructure_v21_manifest.json`
- Daily net figure: `outputs/figures/aapl_wrds_20260313_20260410_microstructure_v21_daily_net.svg`
- Order outcome figure: `outputs/figures/aapl_wrds_20260313_20260410_microstructure_v21_order_outcomes.svg`

## Key Metrics

Focused passive/hybrid diagnostic:

| metric | value |
| --- | ---: |
| candidate_events | 186,405 |
| submitted_orders | 142,563 |
| filled_orders | 43,781 |
| fill_rate | 30.71% |
| unfilled_rate | 69.29% |
| gross_pnl | 104.66 |
| cost | 446.45 |
| net_pnl | -341.79 |
| net_pnl_per_filled_order | -0.00781 |
| net_pnl_per_submitted_order | -0.00240 |
| daily_net_pnl_mean | -18.99 |
| daily_net_pnl_std | 12.97 |
| daily_sharpe | -1.46 |
| max_daily_loss | -49.64 |
| unfilled_opportunity_cost | 1308.08 |

Order-mode breakdown:

| execution_mode | filled | count |
| --- | --- | ---: |
| market_entry | True | 15,945 |
| passive_entry | True | 27,836 |
| passive_entry | False | 98,782 |

Selected-test roll-forward metrics:

| metric | value |
| --- | ---: |
| selected_test_net_pnl | -329.52 |
| selected_test_submitted_orders | 131,415 |
| selected_test_filled_orders | 41,148 |
| test_used_for_selection | False |

## Interpretation

The v2.1 frequency expansion does submit many more passive orders, but the conservative fill rule reveals a large gap between submitted and filled economics. Filled-order net PnL is still negative, and submitted-order net PnL is worse once unfilled orders are counted in the denominator.

The result does not invalidate passive/hybrid execution research, but it blocks any claim that the current v2.1 rule is economically solved. The next engineering step is performance optimization for full-grid validation, not broader strategy tuning on the final test slice.

## Validation And Limits

Unit tests passed after implementation:

```text
110 passed
```

The v2.1 workflow marks `test_used_for_selection=False`. The focused run only has one variant, so validation selection is structurally trivial; it still exercises chronological prior-date selection. Full multi-variant validation requires optimizing the repeated passive-fill simulation before it is practical on the full AAPL slice.

## Engineering Update

After the focused diagnostic, variant evaluation was updated to reuse execution simulation across variants that share the same `(microprice_usage, ttl, queue_haircut, execution_variant)` key. Candidate-pool and edge-threshold checks are now boolean inclusion masks applied after simulation.

This is an engineering optimization. It does not reduce the AAPL date range, remove grid options, or change v1 / v2.0 strategy logic. A focused full-slice rerun after the optimization reproduced the same headline counts and selected-test result:

```text
candidate_events=186405
orders=142563
selected_test_net_pnl=-329.5199999999966
selected_test_submitted_orders=131415
selected_test_filled_orders=41148
```
