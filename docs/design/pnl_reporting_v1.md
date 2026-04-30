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
data/processed/aapl_wrds_20260408_20260410/aapl_wrds_20260408_20260410_backtest_v1_ledger.csv
data/processed/aapl_wrds_20260408_20260410/aapl_wrds_20260408_20260410_model_backtest_v1_ledger.csv
```

The first ledger corresponds to the sequential gate strategy. The second ledger
corresponds to the trained linear score strategy.

## Outputs

```text
outputs/figures/aapl_wrds_20260408_20260410_pnl_comparison.svg
outputs/tables/aapl_wrds_20260408_20260410_pnl_comparison_summary.csv
outputs/tables/aapl_wrds_20260408_20260410_pnl_comparison_curve.csv
```

Generated output files remain ignored by Git.

## Interpretation

The SVG plots `equity_after` from the accounting ledger after each event. In
the current one-share target-position prototype, equity is equivalent to
cumulative net PnL after spread and stress-cost deductions configured in the
backtest.
