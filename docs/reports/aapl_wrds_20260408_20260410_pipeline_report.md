# AAPL WRDS 2026-04-08 to 2026-04-10 Pipeline Report

## Scope

Data slice:

```text
configs/data/aapl_wrds_20260408_20260410.yaml
symbol = AAPL
trading dates = 2026-04-08, 2026-04-09, 2026-04-10
quote source = WRDS TAQ NBBOM
trade source = WRDS TAQ CTM
```

This report records the local pipeline run through walk-forward statistical
evaluation. It is not a backtest report.

## Pipeline Status

Implemented stages:

- dataset build / cleaning v2
- quote-trade alignment v1
- quote features v1
- trade signing v1
- signed-flow features v1
- labeling v1
- signals v1
- walk-forward evaluation v1

Not implemented:

- condition-code final eligibility filters
- threshold optimization
- model fitting
- transaction cost model
- execution-aware backtest

## Data Quality Summary

Raw and cleaned rows:

| artifact | rows |
| --- | ---: |
| raw quotes | `2,030,307` |
| cleaned quotes | `2,029,892` |
| rejected quotes | `415` |
| raw trades | `1,648,877` |
| cleaned trades | `1,648,869` |
| rejected trades | `8` |

Rates:

| metric | rate |
| --- | ---: |
| quote reject rate | `0.020440%` |
| trade reject rate | `0.000485%` |
| alignment unmatched rate | `0.004912%` |
| trade unknown sign rate | `0.000121%` |
| signal input missing rate | `0.005580%` |

Formula audit passed for:

- `midquote = (bid + ask) / 2`
- `quoted_depth = bid_size + ask_size`
- `quote_imbalance = (bid_size - ask_size) / quoted_depth`
- `signed_trade_size = trade_sign * trade_size`
- signed-flow imbalance bounds
- current/future midquote label timing
- sequential-gate signal logic

## Label Availability

| horizon | label missing rate |
| --- | ---: |
| `100ms` | `0.008066%` |
| `500ms` | `0.028929%` |
| `1s` | `0.090486%` |
| `5s` | `0.250232%` |

Missing labels are retained as null rows.

## Signal Summary

Signals v1 uses diagnostic default thresholds:

```text
qi_threshold = 0.0
signed_flow_threshold = 0.0
qr_threshold_bps = 0.0
signed_flow_column = signed_flow_imbalance_500ms
```

Signal counts:

| signal bucket | rows |
| --- | ---: |
| long | `25,080` |
| short | `29,630` |
| no trade | `1,594,159` |

These are diagnostic signals, not optimized trading rules.

## Walk-Forward Evaluation

Policy:

```text
evaluation_policy = expanding_train_dates_next_date_test
signal_usage_policy = evaluate_precomputed_signals_without_refitting
label_usage_policy = labels_used_only_as_targets
```

Aggregate evaluation-fold results:

| horizon | evaluated signal rows | signal accuracy | mean signal-aligned return bps |
| --- | ---: | ---: | ---: |
| `100ms` | `33,674` | `0.4771` | `0.1917` |
| `500ms` | `33,651` | `0.4862` | `0.1657` |
| `1s` | `33,627` | `0.4967` | `0.2166` |
| `5s` | `33,575` | `0.5114` | `0.1204` |

These results are statistical diagnostics only. They are not cost-adjusted and
must not be presented as tradable PnL.

## Known Limitations

- NBBO quote-condition eligibility remains diagnostic-only.
- Sale-condition eligibility remains unresolved.
- Quote size unit interpretation is not independently finalized.
- Signal thresholds are diagnostic defaults, not training-window-selected.
- There is no transaction cost model.
- There is no execution-aware backtest.
