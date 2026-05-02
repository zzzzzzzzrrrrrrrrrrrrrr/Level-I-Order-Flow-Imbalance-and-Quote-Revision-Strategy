# Trade Signing v1

## Purpose

Trade signing v1 classifies the initiator side of each aligned trade without
changing the cleaning or alignment contracts. It sits after quote-trade
alignment and before signed order-flow or OFI features.

## Scope

Trade signing v1 classifies trade direction using quote rule with tick-rule
fallback on aligned trade rows. It does not apply sale-condition filters, NBBO
condition filters, OFI aggregation, labels, or backtest signals.

## Inputs

- `*_trades_aligned_quotes.csv`
- cleaned trade columns
- alignment columns including quote match state, matched quote bid, and matched
  quote ask

## Rules

The primary rule is quote rule:

- `matched_midquote = (quote_bid + quote_ask) / 2`
- `trade_price > matched_midquote` maps to `+1`
- `trade_price < matched_midquote` maps to `-1`
- `trade_price == matched_midquote` remains unresolved by quote rule

The fallback rule is tick rule within:

```json
["symbol", "trading_date"]
```

The tick rule uses the sign of the latest non-zero trade price change within the
same symbol and trading date. It does not borrow price direction across symbols
or trading days.

Rows that cannot be classified by either rule are retained with:

```text
trade_sign = 0
trade_sign_source = unknown
```

## Outputs

The output preserves every aligned trade row and appends:

- `matched_midquote`
- `quote_rule_sign`
- `tick_rule_sign`
- `trade_sign`
- `trade_sign_source`
- `signed_trade_size`

`signed_trade_size` is a row-level signed trade quantity. It is not an OFI
aggregation or a final strategy signal.

## Diagnostics

The manifest records row-preservation status, quote-rule counts, tick-rule
fallback counts, unknown sign counts, buy/sell counts, midpoint ties, and
quote/tick rule conflicts.

## Current AAPL Slice

For `aapl_wrds_20260313_20260410`, trade signing v1 currently produces:

- input aligned trade rows: `12,141,453`
- output signed trade rows: `12,141,453`
- quote matched rows: `12,141,282`
- quote unmatched rows: `171`
- quote-rule signed rows: `9,054,301`
- tick-rule fallback signed rows: `3,087,121`
- unknown sign rows: `31`
- buy sign rows: `6,823,163`
- sell sign rows: `5,318,259`
- quote midpoint tie rows: `3,086,981`
- quote/tick conflict rows: `1,874,402`

Quote/tick conflicts are diagnostic rows where tick rule would disagree with
quote rule. In v1, quote rule remains primary when it is available.

This output is not a final research-grade signed sample. Condition-code
eligibility, OFI features, labels, and backtests remain separate steps.
