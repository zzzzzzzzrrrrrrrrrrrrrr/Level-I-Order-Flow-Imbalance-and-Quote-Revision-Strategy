# Cleaning Rules V2

## Purpose

Cleaning v2 makes every sample inclusion and exclusion rule explicit, reproducible, and auditable before feature construction.
It does not certify that the research sample is final. It produces cleaned quote/trade tables, rejected-row tables, per-rule diagnostics, unresolved data assumptions, and tests for critical boundary cases.

## Raw Data Sources

- Quotes: WRDS TAQ `taqmsec.nbbom_YYYYMMDD`
- Trades: WRDS TAQ `taqmsec.ctm_YYYYMMDD`

The main strategy quote source is national BBO state, not participant quote messages.

## Layers

### Schema Validation

Schema validation checks required columns, timestamp dtype, non-empty symbol/source fields, and base non-null fields required by the normalized schema. It must not encode research assumptions.

### Cleaning Rules

Cleaning rules drop or flag rows according to explicit market-data rules. Every rule has:

- `rule_id`
- `description`
- `input_columns`
- `action`
- `severity`
- row-count diagnostics
- rejected-row output when action is `drop`

### Research Sample Policy

Research sample policy is separate from cleaning. It includes quote-trade alignment, trade signing, lag tolerance, final condition-code eligibility, and feature/label sample boundaries.

## Quote Rules

| rule_id | action | severity | input_columns | policy |
| --- | --- | --- | --- | --- |
| `Q001_non_positive_prices` | drop | fail | `bid`, `ask` | Drop quotes with `bid <= 0` or `ask <= 0`. |
| `Q002_negative_depth` | drop | fail | `bid_size`, `ask_size` | Drop quotes with negative displayed depth. Zero depth is warned, not dropped. |
| `Q003_crossed_market` | drop | fail | `bid`, `ask` | Drop quotes with `ask < bid`. Locked markets are warned, not dropped. |

Current policy preserves `nbbo_quote_condition` without filtering because the eligible condition set has not been finalized.

## Trade Rules

| rule_id | action | severity | input_columns | policy |
| --- | --- | --- | --- | --- |
| `T001_non_positive_price_or_size` | drop | fail | `trade_price`, `trade_size` | Drop trades with non-positive price or size. |
| `T002_trade_correction` | drop | fail | `trade_correction` | Keep only uncorrected trades where correction is `0`/`00`. |

Current policy preserves `sale_condition` without filtering. Auction, cross, odd-lot, average-price, derivatively priced, and out-of-sequence policies remain unresolved until trade-signing policy is implemented.

## Rejected Rows

Dataset builds write:

- `*_quotes_clean.csv`
- `*_trades_clean.csv`
- `*_quotes_rejected.csv`
- `*_trades_rejected.csv`
- `*_dataset_manifest.json`

Rejected rows include the normalized row plus:

- `rule_id`
- `reject_reason`
- `rule_action`
- `rule_severity`

## Manifest Diagnostics

The manifest records raw, normalized, scoped, cleaned, and rejected row counts. It also records per-rule diagnostics:

```json
{
  "rule_id": "Q003_crossed_market",
  "before": 2030307,
  "dropped": 415,
  "after": 2029892
}
```

Unresolved assumptions are recorded in the manifest so feature and backtest results cannot be interpreted as final without review.

## Quote-Trade Alignment

Quote-trade alignment is not part of cleaning. It must be implemented as a separate module before trade signing or OFI features.

The default candidate rule is:

```python
pd.merge_asof(
    trades.sort_values("event_time"),
    quotes.sort_values("event_time"),
    on="event_time",
    by="symbol",
    direction="backward",
    tolerance=pd.Timedelta("1s"),
)
```

The alignment manifest must include:

- matched trade count
- unmatched trade count
- median quote lag
- p95 quote lag
- p99 quote lag
- trade exactly tied with quote count

## Trade Signing

Trade direction is not directly supplied by the current normalized CTM contract. Trade signing must be implemented after alignment and should explicitly declare:

- primary method
- fallback method
- quote lag policy
- unknown classification handling
- buy/sell/unknown counts and ratios

## Required Future Fixture Coverage

Fixtures should cover:

1. quote with `bid <= 0`
2. quote with `ask < bid`
3. quote with `bid_size = 0`
4. duplicate quote at same timestamp
5. `trade_correction != 00`
6. odd-lot or special-condition trade
7. average-price trade
8. auction/cross trade
9. trade before first eligible quote
10. trade exactly at same timestamp as quote
11. trade requiring quote lag
12. locked market
13. out-of-sequence trade
14. premarket trade
15. post-close trade

## Current Unresolved Assumptions

- `best_bidsiz` and `best_asksiz` unit must be verified against WRDS/NYSE variable documentation.
- `nbbo_quote_condition` eligibility is not finalized.
- `sale_condition` eligibility is not finalized.
- quote-trade alignment and trade signing are not part of cleaning and must be implemented before OFI/trade-signing features.
