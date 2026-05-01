# PnL Reporting v1

Recorded: 2026-04-30.

## Purpose

PnL reporting v1 visualizes already-generated accounting ledgers. It is a
reporting layer only.

It does not:

- recompute signals
- train models
- select parameters
- alter backtest results

## Inputs

For the current AAPL prototype comparison:

```text
data/processed/aapl_wrds_20260313_20260410/aapl_wrds_20260313_20260410_backtest_v1_ledger.csv
data/processed/aapl_wrds_20260313_20260410/aapl_wrds_20260313_20260410_model_backtest_v1_ledger.csv
```

The first ledger corresponds to the sequential gate strategy. The second ledger
corresponds to the trained linear score strategy.

If present, the cost-aware linear-score ledger is included as a third strategy:

```text
data/processed/aapl_wrds_20260313_20260410/aapl_wrds_20260313_20260410_cost_aware_linear_score_ledger.csv
```

## Outputs

```text
outputs/figures/aapl_wrds_20260313_20260410_pnl_comparison.svg
outputs/tables/aapl_wrds_20260313_20260410_pnl_comparison_summary.csv
outputs/tables/aapl_wrds_20260313_20260410_pnl_comparison_curve.csv
```

Generated output files remain ignored by Git.

## Interpretation

The SVG plots cumulative net PnL after costs. For single-run ledgers this is
the ledger's `equity_after`. For multi-fold backtest ledgers, each fold's local
`equity_after` is accumulated in chronological order so the plotted final value
equals total held-out net PnL across folds.

In the current one-share target-position prototype, net PnL is account equity
after spread and stress-cost deductions configured in the backtest.

The comparison summary reports:

- `gross_pnl`
- `cost`
- `net_pnl`
- `num_trades`
- `num_position_changes`
- `gross_per_trade`
- `cost_per_trade`
- `net_per_trade`
- `selected_threshold_by_fold`
- `selected_cost_multiplier_by_fold` when available

For the current 20-day AAPL run, both compared strategies are net negative
after spread costs. The sequential gate and linear score both show positive
gross-before-cost results across held-out folds, but turnover and spread costs
more than consume that edge.
