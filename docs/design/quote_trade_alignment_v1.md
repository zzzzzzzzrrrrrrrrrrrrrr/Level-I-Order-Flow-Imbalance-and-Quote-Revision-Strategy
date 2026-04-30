# Quote-Trade Alignment v1

## Purpose

Alignment v1 attaches quote state to each cleaned trade without changing the
cleaning contract. It is a separate step after dataset build and before trade
signing or OFI features that use trades.

## Contract

This alignment version only performs backward quote-trade matching within
symbol and trading_date groups. It does not perform trade signing,
sale-condition filtering, correction filtering, or final research-sample
cleaning.

- Input quotes: cleaned WRDS TAQ NBBOM quote rows.
- Input trades: cleaned WRDS TAQ CTM trade rows.
- Alignment rule: latest quote for the same symbol strictly before the trade.
- Session boundary policy: match only within the same symbol and same
  `trading_date`.
- `trading_date` is derived from `event_time` in the configured market timezone,
  currently `America/New_York` for the AAPL WRDS slice.
- Exact timestamp matches are not allowed.
- Optional tolerance may cap maximum quote lag.
- Condition-code filters are not applied in this module.
- Trade signing is not applied in this module.

## Output Columns

The aligned output preserves the cleaned trade columns and appends:

- `is_quote_matched`
- `trading_date`
- `matched_quote_event_time`
- `matched_quote_trading_date`
- `quote_lag_ms`
- `quote_source`
- `quote_raw_row_index`
- `quote_bid_exchange`
- `quote_ask_exchange`
- `quote_nbbo_quote_condition`
- `quote_bid`
- `quote_ask`
- `quote_bid_size`
- `quote_ask_size`

Trades without a strictly prior quote are retained with null quote fields.

## Diagnostics

The alignment manifest records:

- input quote rows
- input trade rows
- aligned trade rows
- matched trade rows
- unmatched trade rows
- matched ratio
- allow exact matches flag
- tolerance
- tolerance policy
- min quote lag in milliseconds
- median quote lag in milliseconds
- p95 quote lag in milliseconds
- p99 quote lag in milliseconds
- max quote lag in milliseconds
- matched locked quote count
- matched locked quote ratio
- session boundary policy
- alignment group keys
- cross-session match count
- condition-filter and trade-signing status flags

The default alignment group keys are:

```json
["symbol", "trading_date"]
```

The expected `cross_session_match_count` is `0`.

## Tolerance Sensitivity

Alignment v1 can run a diagnostic comparison without choosing a final
tolerance:

```text
None, 5s, 1s, 500ms, 100ms
```

The comparison writes `*_alignment_tolerance_sensitivity.json` and
`*_alignment_tolerance_sensitivity.csv`. It is diagnostic-only and does not
select the final tolerance.

## Current AAPL Slice

For `aapl_wrds_20260408_20260410`, alignment v1 currently produces:

- input quote rows: `2,029,892`
- input trade rows: `1,648,869`
- aligned trade rows: `1,648,869`
- matched trade rows: `1,648,788`
- unmatched trade rows: `81`
- min quote lag: `0.001 ms`
- median quote lag: `17.971 ms`
- p95 quote lag: `612.05165 ms`
- p99 quote lag: `1348.36265 ms`
- max quote lag: `9311.365 ms`
- matched locked quote count: `6,778`
- matched locked quote ratio: `0.0041109`
- cross-session match count: `0`

This is not a research-grade signed sample yet. Trade signing and final
condition-code eligibility remain separate unresolved steps.
