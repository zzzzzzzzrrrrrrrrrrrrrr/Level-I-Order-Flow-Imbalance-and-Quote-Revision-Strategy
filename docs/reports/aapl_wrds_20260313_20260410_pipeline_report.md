# AAPL WRDS 20-Day Pipeline Report

## Scope

```text
config = configs/data/aapl_wrds_20260313_20260410.yaml
symbol = AAPL
dates = 2026-03-13 through 2026-04-10
excluded date = 2026-04-03
quote source = taqmsec.nbbom_YYYYMMDD
trade source = taqmsec.ctm_YYYYMMDD
```

This is a single-symbol prototype run. It is useful for validating the
end-to-end pipeline and studying turnover/cost pressure, but it is not a
research-grade profitability claim.

## Data And Pipeline Status

The active slice uses slice-named output roots, separate from the earlier
3-day validation slice:

```text
data/raw/aapl_wrds_20260313_20260410/
data/interim/aapl_wrds_20260313_20260410/
data/processed/aapl_wrds_20260313_20260410/
outputs/figures/aapl_wrds_20260313_20260410_pnl_comparison.svg
outputs/tables/aapl_wrds_20260313_20260410_pnl_comparison_summary.csv
outputs/tables/aapl_wrds_20260313_20260410_pnl_comparison_curve.csv
```

Completed stages:

```text
WRDS extraction -> dataset build -> alignment -> quote features -> trade signing
-> signed-flow features -> labels -> signals -> walk-forward evaluation
-> threshold selection -> cost model -> execution accounting
-> target-position accounting -> parameter sensitivity
-> TVT parameter selection -> backtest v1 -> model training v1
-> PnL reporting v1
```

## Row Counts

| Stage | Rows |
| --- | ---: |
| raw quotes | 21,290,182 |
| raw trades | 12,141,562 |
| cleaned quotes | 21,287,434 |
| cleaned trades | 12,141,453 |
| rejected quotes | 2,748 |
| rejected trades | 109 |
| aligned trades | 12,141,453 |
| matched trades | 12,141,282 |
| unmatched trades | 171 |
| quote feature rows | 21,287,434 |
| signed-flow feature rows | 12,141,453 |
| labeled rows | 12,141,453 |
| signal rows | 12,141,453 |

Alignment matched ratio: `0.9999859160`.

## Signal And Label Diagnostics

Label availability:

| Horizon | Available rows | Missing rows |
| --- | ---: | ---: |
| 100ms | 12,140,136 | 1,317 |
| 500ms | 12,136,449 | 5,004 |
| 1s | 12,130,975 | 10,478 |
| 5s | 12,107,013 | 34,440 |

Sequential-gate signal counts:

| Signal state | Rows |
| --- | ---: |
| long | 177,014 |
| short | 195,895 |
| no trade | 11,768,544 |

Walk-forward mean signal-aligned return is positive across horizons in the
current diagnostic evaluation. This is a statistical edge proxy only; it does
not include execution costs.

## PnL Result

The held-out accounting results are directionally useful but not profitable
after spread costs.

| Strategy | Held-out folds | Orders | Gross before cost | Total cost | Net final equity |
| --- | ---: | ---: | ---: | ---: | ---: |
| sequential gate | 18 | 288,451 | 348.24 | 4,186.19 | -3,837.95 |
| linear score | 18 | 61,288 | 115.26 | 757.06 | -641.80 |

Both strategies show positive gross-before-cost results on all 18 held-out
test days and negative net results on all 18 held-out test days.

The PnL comparison report writes cumulative net PnL after costs:

| Strategy | PnL curve final equity | Total cost | Max drawdown |
| --- | ---: | ---: | ---: |
| sequential gate | -3,837.95 | 4,186.19 | 3,837.95 |
| linear score | -641.80 | 757.05 | 641.80 |

The SVG and comparison table use cumulative net PnL after `event_cost`
deductions. Per-fold ledger `equity_after` values are local to each held-out
fold and are accumulated by the reporting layer for the comparison curve.

## Memory Changes

The 20-day run exposed full-table memory pressure in labeling and downstream
steps. The current code path now:

- streams labeling and signal construction by `trading_date`,
- reads only required columns from large signal and feature CSVs,
- avoids carrying the full 12M-row, 50-plus-column signal table into workflows
that need only accounting or model fields.

This should make 20-day runs reliable and improves the path toward 60-day runs,
but the next 60-day attempt should still be treated as a memory test.

## Residual Risks

- Net PnL is negative after spread costs; the current strategy is not
  profitable.
- Larger TVT turnover-control grids are still too slow on the 20-day slice.
  Exploratory grids with daily order caps and cooldowns timed out before
  producing a completed selection file, so candidate-search internals need
  optimization before broad net-PnL tuning.
- Alignment tolerance sensitivity still needs a streaming implementation on
  this slice. The main no-tolerance alignment path completed successfully.
- Condition-code eligibility filters, official fees, passive fills, queueing,
  explicit latency, and research-grade execution modeling remain unresolved.
- The pushed credential incident requires external remediation: rotate the WRDS
  password and clean remote history if the repository must be scrubbed.

## Next Optimization Targets

The next strategy iteration should optimize for net PnL directly:

- reduce turnover with wider thresholds and explicit no-trade bands,
- select on validation net equity or cost-adjusted return, not gross edge,
- add max orders per day and cooldown grids to TVT selection,
- train the linear score with a cost-aware objective,
- test longer holding horizons and liquidity/spread filters,
- implement memory-safe alignment tolerance sensitivity before 60-day runs.
