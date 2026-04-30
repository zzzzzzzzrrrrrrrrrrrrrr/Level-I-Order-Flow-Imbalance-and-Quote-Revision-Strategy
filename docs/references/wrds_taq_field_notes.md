# WRDS TAQ Field Notes

## Purpose

This note records source-field assumptions used by the WRDS TAQ pipeline. It is
not a replacement for the WRDS data dictionary or NYSE TAQ specifications.

## Current Source Tables

The current AAPL slice uses:

```text
taqmsec.nbbom_YYYYMMDD  -> normalized quote state
taqmsec.ctm_YYYYMMDD    -> normalized trade messages
```

The quote source is NBBOM because the strategy uses national best bid/offer
state for midquote, QI, and QR. CQM participant quote messages are not used as
the primary strategy quote state.

## Normalized Field Mapping

NBBOM quote fields:

| normalized field | WRDS field |
| --- | --- |
| `event_time` | `date + time_m` |
| `symbol` | `sym_root + sym_suffix` |
| `bid_exchange` | `best_bidex` |
| `ask_exchange` | `best_askex` |
| `nbbo_quote_condition` | `nbbo_qu_cond` |
| `bid` | `best_bid` |
| `ask` | `best_ask` |
| `bid_size` | `best_bidsiz` |
| `ask_size` | `best_asksiz` |
| `source` | literal `wrds_taq_nbbom` |

CTM trade fields:

| normalized field | WRDS field |
| --- | --- |
| `event_time` | `date + time_m` |
| `symbol` | `sym_root + sym_suffix` |
| `trade_price` | `price` |
| `trade_size` | `size` |
| `trade_exchange` | `ex` |
| `sale_condition` | `tr_scond` |
| `trade_correction` | `tr_corr` |
| `trade_sequence_number` | `tr_seqnum` |
| `source` | literal `wrds_taq_ctm` |

## Field-Semantic Decisions

- `cqm.ex` is a quote-message source exchange and is not used as
  `bid_exchange` or `ask_exchange`.
- `bid_exchange` and `ask_exchange` are national best-side exchange fields from
  NBBOM.
- `nbbo_quote_condition` and `sale_condition` distributions are recorded, but
  final eligibility filters are not yet applied.
- `trade_correction` filtering currently keeps uncorrected trades where the
  normalized correction value is `0` or `00`.

## Unresolved Assumptions

The dataset manifest currently records:

- quote size unit needs independent confirmation before absolute QI/depth
  interpretation.
- NBBO quote-condition eligibility is not finalized.
- sale-condition eligibility is not finalized.

## Source Notes

Authoritative references should be checked before changing mappings:

- WRDS TAQ table metadata / data dictionary available through WRDS account
  access.
- NYSE Daily TAQ client specifications for official trade, quote, and NBBO
  field semantics.

Any future mapping change should update this note, the YAML data contract,
adapter tests, and extraction/table-validation logic together.
