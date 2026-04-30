# Quote Features v1

## Purpose

Quote features v1 derives quote-only Level-I features from cleaned NBBOM quote
rows. It is designed to support QI and QR diagnostics before trade signing is
available.

## Scope

Quote feature v1 computes row-preserving, quote-only features from cleaned
Level-I quotes. It supports spread, depth, signed top-of-book imbalance, quote
revision, and quote event interval diagnostics. It does not compute trade
signing, signed order flow imbalance, labels, or backtest signals.

## Inputs

- `*_quotes_clean.csv`
- normalized quote schema with bid, ask, bid size, and ask size

## Grouping

Quote revision features are computed within:

```json
["symbol", "trading_date"]
```

`trading_date` is derived from `event_time` in the configured market timezone.
The first quote for each symbol/date has null revision fields.

## Features

- `midquote = (bid + ask) / 2`
- `quoted_spread = ask - bid`
- `relative_spread = quoted_spread / midquote`
- `quoted_depth = bid_size + ask_size`
- `quote_imbalance = (bid_size - ask_size) / quoted_depth`
- `previous_midquote`
- `quote_revision = midquote - previous_midquote`
- `quote_revision_bps = quote_revision / previous_midquote * 10000`
- `quote_event_interval_ms`

If `quoted_depth` is zero, `quote_imbalance` is null instead of infinite.

The manifest records both:

- `zero_quoted_depth_rows`
- `quote_imbalance_null_rows`

## Outputs

- `*_quote_features_v1.csv`
- `*_quote_features_v1_manifest.json`

This output is not a final strategy sample. OFI from signed trades, labels, and
backtests remain separate steps.
