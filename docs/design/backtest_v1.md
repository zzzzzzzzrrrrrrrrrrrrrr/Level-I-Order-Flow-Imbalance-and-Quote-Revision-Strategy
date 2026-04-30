# Backtest v1

Recorded: 2026-04-30.

## Purpose

Backtest v1 evaluates the candidate selected by train-validation-test parameter
selection on the held-out test date.

It is intentionally an orchestration layer over target-position accounting v1.
It does not introduce new parameter search.

## Inputs

- `*_signals_v1.csv`
- `*_tvt_parameter_selection_v1.csv`

The TVT summary must identify rows with `selected_for_test = true`. Those rows
provide:

- `fold_id`
- `candidate_id`
- train / validation / test dates
- `max_position`
- `cooldown`
- `max_trades_per_day`
- `fixed_bps`
- `slippage_ticks`
- `tick_size`

## Policy

```text
backtest_policy = tvt_selected_candidate_test_accounting_v1
parameter_source_policy = frozen_candidate_selected_on_validation
evaluation_policy = evaluate_selected_candidate_on_test_date_only
split_source_policy = tvt_parameter_selection_v1
```

For each selected TVT row, backtest v1 filters signal rows to that row's
`test_date`, constructs a `TargetPositionAccountingConfig` from the frozen
candidate parameters, and runs target-position accounting.

## Outputs

- `*_backtest_v1_orders.csv`
- `*_backtest_v1_ledger.csv`
- `*_backtest_v1_summary.csv`
- `*_backtest_v1_manifest.json`

The orders, ledger, and summary include fold and candidate metadata so the
result can be traced back to its train / validation / test split.

## Non-Goals

Backtest v1 does not:

- use test data for parameter selection
- reselect thresholds or accounting parameters
- train a predictive model
- simulate passive fills or queue priority
- model explicit latency
- include official broker, SEC, FINRA, exchange fee, or rebate schedules
- claim research-grade profitability

## Interpretation

Backtest v1 answers a narrow question:

```text
After selecting this target-position accounting candidate on validation,
what is the account result on the held-out test date under the current
midquote-plus-cost execution proxy?
```

It should be reported as a held-out accounting result, not as a final trading
strategy validation.
