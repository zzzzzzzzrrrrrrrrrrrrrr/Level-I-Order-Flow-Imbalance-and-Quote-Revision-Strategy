# Target-Position Accounting v1

## Purpose

Target-position accounting v1 converts signal rows into bounded target
positions and records order-level account state. It reduces the unrealistic
overlap in independent fixed-horizon round trips by introducing an explicit
position state.

This is still an accounting scaffold, not a research-grade backtest.

## Scope

Implemented:

- signal-to-target-position mapping
- max absolute position cap
- flat-on-no-signal policy
- EOD flat policy per `symbol` and `trading_date`
- cooldown between accepted orders
- optional max trades per day
- midquote fill proxy with cost deduction
- order-level cash, position, inventory value, and equity
- summary diagnostics

Not implemented:

- parameter optimization
- train-window hyperparameter selection
- passive fill simulation
- queue or order-book modeling
- official broker / SEC / FINRA / exchange fee schedules
- latency model beyond user-configured stress parameters
- research-grade backtesting

The active assumptions are tracked in
`assumptions/parameter_registry.md`; this document describes the mechanics.

## Inputs

- `*_signals_v1.csv`

Required columns:

- `event_time`
- `symbol`
- `trading_date`
- `sequential_gate_signal`
- `signal_midquote`
- `signal_quoted_spread`

## Target Mapping

Default mapping:

```text
signal = +1 -> target_position = +max_position
signal = -1 -> target_position = -max_position
signal =  0 -> target_position = 0
```

The default `flat_on_no_signal = true` makes no-signal rows flatten current
exposure. If disabled, no-signal rows do not change the current position.

Target positions are bounded by:

```text
abs(target_position) <= max_position
```

## Order Logic

An order is generated only when:

```text
target_position != current_position
```

Order quantity is:

```text
order_quantity = target_position - current_position
```

The v1 fill proxy is:

```text
fill_midquote = signal_midquote
```

Cost is deducted separately:

```text
event_cost
  = abs(order_quantity) * signal_quoted_spread / 2
    + abs(order_quantity) * fill_midquote * fixed_bps / 10000
    + abs(order_quantity) * slippage_ticks * tick_size
```

Cash delta:

```text
cash_delta = -order_quantity * fill_midquote - event_cost
```

Ledger state:

```text
position_after = cumulative order_quantity
cash_after = cumulative cash_delta
inventory_value_after = position_after * fill_midquote
equity_after = cash_after + inventory_value_after
```

## Controls

Default v1 controls:

```text
max_position = 1 share
cooldown = 0ms
max_trades_per_day = none
eod_flat = true
```

These controls are engineering safeguards, not selected strategy parameters.
Parameter search must happen in a later train/test selection layer.

## Outputs

- `*_target_position_accounting_v1_orders.csv`
- `*_target_position_accounting_v1_ledger.csv`
- `*_target_position_accounting_v1_summary.csv`
- `*_target_position_accounting_v1_manifest.json`

## Boundary

Target-position accounting v1 answers:

```text
Do bounded target-position account mechanics reconcile under explicit account
constraints?
```

It does not answer:

```text
Which hyperparameters should be selected, or whether the strategy is tradable?
```
