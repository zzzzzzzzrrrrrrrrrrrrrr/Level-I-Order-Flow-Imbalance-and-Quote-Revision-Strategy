# Labeling v1

## Purpose

Labeling v1 creates future midquote return and direction targets for signed-flow
feature rows. It is the target-construction layer before signal rules,
walk-forward evaluation, or backtesting.

## Scope

Labeling v1 creates future midquote return and direction targets from quote
feature data. Labels are computed strictly after `decision_time` and must not be
used as features. This step does not create trading signals, run walk-forward
evaluation, or run backtests.

## Inputs

- `*_signed_flow_features_v1.csv`
- `*_quote_features_v1.csv`

`decision_time` is the feature row timestamp. In the current pipeline, those
rows are trade-event rows after signed-flow feature construction.

## Policies

```text
current_quote_policy = latest_quote_at_or_before_decision_time
future_quote_policy = first_quote_at_or_after_decision_time_plus_horizon
session_boundary_policy = same_symbol_same_trading_date_only
label_usage_policy = labels_are_targets_not_features
```

Current and future midquotes are matched only within:

```json
["symbol", "trading_date"]
```

The first rows of a session may have no current midquote, and late-session rows
may have no future label for longer horizons. Those rows are retained with null
label fields.

## Default Horizons

```text
100ms, 500ms, 1s, 5s
```

Direction labels use a configurable dead zone:

```text
+1 if future_midquote_return_bps > dead_zone_bps
 0 if abs(future_midquote_return_bps) <= dead_zone_bps
-1 if future_midquote_return_bps < -dead_zone_bps
```

The default `dead_zone_bps` is `0.0`.

## Outputs

- `*_labeled_features_v1.csv`
- `*_labeling_v1_manifest.json`

## Current AAPL Slice

For `aapl_wrds_20260408_20260410`, labeling v1 currently produces:

- input feature rows: `1,648,869`
- input quote rows: `2,029,892`
- output labeled rows: `1,648,869`
- current midquote missing rows: `80`
- horizons: `100ms`, `500ms`, `1s`, `5s`
- dead zone: `0.0 bps`
- label available rows:
  - `100ms`: `1,648,736`
  - `500ms`: `1,648,392`
  - `1s`: `1,647,377`
  - `5s`: `1,644,743`
- label missing rows:
  - `100ms`: `133`
  - `500ms`: `477`
  - `1s`: `1,492`
  - `5s`: `4,126`

Missing labels are retained as nulls. They mainly occur when the current quote
is unavailable at the feature timestamp or when there is no same-session future
quote at or after `decision_time + horizon`.

This output is not a strategy or backtest result. Sequential gate signals,
walk-forward evaluation, cost modeling, and execution-aware backtests remain
separate steps.
