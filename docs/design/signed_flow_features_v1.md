# Signed-Flow Features v1

## Purpose

Signed-flow features v1 derives trailing signed trade-flow features from
`trade_signing_v1` output. It is the first feature layer that uses inferred
trade direction.

## Scope

Signed-flow feature v1 computes row-preserving trade-flow features from
trade-signing output. It uses event-count and clock-time trailing windows within
symbol and trading_date groups. It does not apply condition-code filters,
construct labels, or run backtests. These are trade-signed-flow features, not a
final research-grade OFI sample.

## Inputs

- `*_trades_signed_v1.csv`
- normalized trade columns
- `trading_date`
- `trade_sign`
- `signed_trade_size`

## Grouping

All rolling features are computed within:

```json
["symbol", "trading_date"]
```

No signed flow is carried across symbols or trading days.

At each signed trade row, signed-flow features are computed using trades
observable up to and including the current trade event:

```text
window_inclusion_policy = trailing_windows_include_current_trade
```

This should be interpreted as an updated pressure state after observing the
current trade print, not as a feature available before that trade occurred.

Unknown trade signs are retained:

```text
unknown_sign_policy = unknown_sign_trades_contribute_zero_signed_flow_and_remain_in_trade_volume
```

That means an unknown-sign trade contributes `0` to signed flow, but its
`trade_size` remains in the trailing total-volume denominator.

## Features

Row-level features:

- `buy_trade_size`
- `sell_trade_size`
- `unknown_trade_size`
- `signed_trade_value`

Trailing event-count windows:

```text
10 trades, 50 trades, 100 trades
```

Trailing clock-time windows:

```text
100ms, 500ms, 1s
```

For each window, v1 computes:

- `signed_flow_<window>`
- `trade_volume_<window>`
- `buy_volume_<window>`
- `sell_volume_<window>`
- `trade_count_<window>`
- `signed_flow_imbalance_<window>`

`signed_flow_imbalance` is:

```text
signed_flow / trade_volume
```

If trailing trade volume is zero, the imbalance is null instead of infinite.

## Outputs

- `*_signed_flow_features_v1.csv`
- `*_signed_flow_features_v1_manifest.json`

## Current AAPL Slice

For `aapl_wrds_20260313_20260410`, signed-flow feature v1 currently produces:

- input signed trade rows: `12,141,453`
- output feature rows: `12,141,453`
- signed trade rows: `12,141,422`
- unknown sign rows: `31`
- buy sign rows: `6,823,163`
- sell sign rows: `5,318,259`
- trade-count windows: `10`, `50`, `100`
- clock-time windows: `100ms`, `500ms`, `1s`
- window inclusion policy: `trailing_windows_include_current_trade`
- unknown sign policy: `unknown_sign_trades_contribute_zero_signed_flow_and_remain_in_trade_volume`
- zero-volume window rows: `0` for every default window
- signed-flow imbalance null rows: `0` for every default window
- max absolute signed-flow imbalance: `1.0` for every default window

This output is not a final strategy sample. Label construction, sequential
signal rules, walk-forward evaluation, and execution-aware backtests remain
separate steps.
