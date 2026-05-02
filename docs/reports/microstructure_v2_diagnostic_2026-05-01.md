# Microstructure V2 Diagnostic - 2026-05-01

## Scope

This report preserves the current v2.0 diagnostic result before adding v2.1.
It is diagnostic evidence only. It does not change v1 baselines, cost-aware
selection, or final test selection logic.

Data slice:

```text
aapl_wrds_20260313_20260410
```

Input strategy:

```text
cost_aware_linear_score
```

## V1 Reference

The selected cost-aware v1 market-style accounting result was:

```text
round_trips = 1577
gross_pnl   = 25.4350
cost        = 28.1750
net_pnl     = -2.7400
net/trip    = -0.001737
```

Interpretation: no-cost directional pressure is positive, but market-style
execution cost consumes the edge.

## V2.0 Diagnostic Variants

| variant | attempted | filled | fill_rate | net_pnl | net/trip |
|---|---:|---:|---:|---:|---:|
| v1_market_entry_market_exit | 1577 | 1577 | 1.0000 | -2.7400 | -0.001737 |
| v2_spread_q1_edge_gate_market_exit | 395 | 395 | 1.0000 | 1.5800 | 0.004000 |
| v2_spread_q1_edge_microprice_gate_market_exit | 370 | 370 | 1.0000 | -0.0400 | -0.000108 |
| v2_spread_q1_edge_microprice_gate_5s_exit | 370 | 370 | 1.0000 | 0.0750 | 0.000203 |
| v2_spread_q1_microprice_passive_entry_market_exit | 370 | 336 | 0.9081 | 2.4400 | 0.006595 |
| v2_spread_q1_microprice_passive_entry_limit_timeout_exit | 370 | 336 | 0.9081 | 7.3800 | 0.019946 |

## Horizon Diagnostic

| horizon | net/trip | MFE mean | MAE mean |
|---|---:|---:|---:|
| 100ms | -0.002685 | 0.023887 | -0.000765 |
| 250ms | -0.002305 | 0.025934 | -0.002073 |
| 500ms | -0.001649 | 0.028693 | -0.003447 |
| 1s | -0.000989 | 0.032081 | -0.006172 |
| 2s | -0.000533 | 0.038115 | -0.010698 |
| 5s | 0.000488 | 0.049030 | -0.019645 |

Interpretation: the markout profile improves with longer horizon, but the
average edge remains thin.

## Spread State Diagnostic

| spread bucket | round trips | net_pnl | net/trip |
|---|---:|---:|---:|
| spread_q1 | 395 | 1.5800 | 0.004000 |
| spread_q2 | 394 | -0.8000 | -0.002030 |
| spread_q3 | 394 | -2.5500 | -0.006472 |
| spread_q4 | 394 | -0.9700 | -0.002462 |

Interpretation: liquidity state materially changes economics. The narrowest
spread bucket is the only positive spread bucket under market-style accounting.

## Caveats

- These diagnostics are not parameter-selected on validation folds.
- Passive entry diagnostics are not production fill claims.
- Level-I data does not identify queue position, hidden liquidity, venue
  priority, or cancel/replace priority.
- Filled-order results must be evaluated together with submitted-order metrics,
  non-fill bias, queue haircut sensitivity, and post-fill adverse selection.

## V2.1 Motivation

V2.1 should expand passive submitted-order frequency and test whether the edge
survives conservative fills, cancellation rules, adverse-selection diagnostics,
daily metrics, and validation-fold selection.

