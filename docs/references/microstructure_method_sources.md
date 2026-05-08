# Microstructure Method Source Notes

Checked: 2026-05-05.

This note records external method references used to sanity-check the current
Level-I diagnostics. It is not a literature review and does not make a final
profitability claim.

## Level-I OFI / QI

Cont, Kukanov, and Stoikov (2014) study NYSE TAQ data and motivate best-bid /
best-ask order-flow imbalance as a short-horizon price-impact proxy:

```text
https://academic.oup.com/jfec/article/12/1/47/816163
```

Project implication:

- `quote_imbalance = (bid_size - ask_size) / (bid_size + ask_size)` is a
  Level-I top-of-book pressure proxy only.
- It must not be described as full-depth order-book imbalance.
- Depth, spread, and cost checks remain necessary before any tradability claim.

## Trade Signing

Lee and Ready (1991) is the canonical reference for quote/tick-rule trade
classification, and later direct tests document classification error and bias
risk:

```text
https://ideas.repec.org/a/bla/jfinan/v46y1991i2p733-46.html
https://www.cambridge.org/core/journals/journal-of-financial-and-quantitative-analysis/article/abs/direct-test-of-methods-for-inferringtrade-direction-from-intraday-data/245925D13B14CBD388979E0FF98721DF
```

Project implication:

- quote-rule plus tick-rule fallback is a defensible baseline.
- trade-signing output is still a noisy inferred field.
- Robustness work should include alternative lag conventions and
  sale-condition-aware filters before final claims.

## Midquote, Spread, And Microprice

The project's midquote, spread, relative-spread, and top-of-book microprice
formulas match standard market-microstructure conventions. A concise public
implementation of top-of-book microprice and book imbalance is:

```text
https://databento.com/docs/examples/order-book/microprice
```

Project implication:

- `midquote = (bid + ask) / 2` is the primary label and markout reference.
- `relative_spread_bps = (ask - bid) / midquote * 10000`.
- `microprice = (bid_size * ask + ask_size * bid) / (bid_size + ask_size)`.
- Microprice should be treated as an empirical pressure proxy, not a guaranteed
  fair value.

## Bid-Ask Bounce

Roll (1984) connects transaction costs with serial dependence in transaction
price changes, which is the main reason this project prefers midquote labels
over raw trade-price movement:

```text
https://econpapers.repec.org/article/blajfinan/v_3a39_3ay_3a1984_3ai_3a4_3ap_3a1127-39.htm
```

Project implication:

- trade-price labels are vulnerable to bid-ask bounce.
- midquote labels reduce, but do not eliminate, microstructure noise.

## Passive Fill Boundary

Level-I data does not identify queue position, hidden liquidity, matching
priority, or venue-specific fill priority. V2.1 passive fills therefore remain
strict evidence-based diagnostics, not production fill or PnL claims.
