# 0011 Large-Slice Memory Policy

## Status

Accepted for 20-day and larger prototype runs.

## Context

The 20-day AAPL slice contains about 21.3M quote rows and 12.1M trade rows.
Full-table reads of wide downstream CSVs created avoidable memory pressure,
especially after signed-flow, labels, and signals expanded the row schema.

The first failure occurred while building labels from the full signed-flow
feature table. Alignment tolerance sensitivity also exceeded local memory when
it created large sorted copies for each tolerance candidate.

## Decision

Large-slice workflows must avoid full wide-table reads unless the stage truly
needs the whole table in memory. For 20-day and 60-day runs:

- read only required columns with `usecols`,
- stream naturally date-partitioned market data by `trading_date`,
- write large outputs incrementally when the stage is row-preserving,
- aggregate diagnostics from date-level partitions,
- keep output folders slice-named so 3-day, 20-day, and later 60-day artifacts
  do not overwrite each other.

## Consequences

Labeling and signal generation now return an empty schema-only frame in the
workflow result after writing the full CSV incrementally. Downstream callers
should use the returned paths and diagnostics rather than expecting all rows in
memory.

Several downstream workflows now read only their required columns from
`*_signals_v1.csv`, including walk-forward evaluation, threshold selection,
cost diagnostics, execution accounting, target-position accounting, parameter
sensitivity, TVT selection, backtest v1, and model training v1.

Two known gaps remain. The alignment tolerance-sensitivity path should be
rewritten to reuse the grouped alignment state or process one tolerance at a
time without retaining all large intermediate copies before 60-day production
runs. TVT candidate-grid evaluation also needs a faster implementation before
broad turnover-control searches; small completed baseline grids are fine, but
larger daily-order-cap / cooldown searches timed out on the 20-day slice.
