# Execution Accounting v1

## Purpose

Execution accounting v1 builds a mechanical account ledger from active signal
rows. It exists to verify account mechanics before parameter tuning or full
backtesting.

This is not a profitability test.

## Scope

Implemented:

- active signal selection from `sequential_gate_signal`
- independent fixed-horizon round trips
- one-share default quantity
- entry cash and inventory update
- exit cash and inventory update
- spread, fixed bps, and slippage tick cost deduction
- trade-level gross PnL, cost, and net PnL
- ledger-level cash, position, inventory value, and equity path
- summary diagnostics by horizon

Not implemented:

- target-position strategy logic
- position limits
- risk controls
- passive fill simulation
- order book queue modeling
- broker / SEC / FINRA / exchange fee schedules
- parameter optimization
- research-grade backtesting

The active accounting assumptions are tracked in
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
- `label_available_<horizon>`
- `future_midquote_<horizon>`
- `future_midquote_event_time_<horizon>`

Only rows with signal `+1` or `-1` become round trips. Rows with signal `0`,
missing midquote, missing spread, or missing label horizon are counted and
skipped.

## Mechanics

For each active signal row and horizon:

```text
side = +1 for long signal, -1 for short signal
quantity = configured quantity
entry_time = signal event_time
exit_time = future_midquote_event_time_<horizon>
entry_midquote = signal_midquote
exit_midquote = future_midquote_<horizon>
```

The current v1 policy treats each signal independently:

```text
position_policy = independent_unit_round_trips_no_position_limit
```

This deliberately creates an accounting scaffold, not a realistic execution
policy. Later versions must add target-position logic, risk limits, and
execution constraints before making backtest claims.

## Cost Deduction

Entry side:

```text
entry_cost
  = quantity * signal_quoted_spread / 2
    + quantity * entry_midquote * fixed_bps / 10000
    + quantity * slippage_ticks * tick_size
```

Exit side:

```text
exit_cost
  = quantity * signal_quoted_spread / 2
    + quantity * exit_midquote * fixed_bps / 10000
    + quantity * slippage_ticks * tick_size
```

The exit spread uses the entry spread as a proxy because signal rows do not yet
carry future quoted spread. This is a documented v1 limitation.

## PnL

Trade-level:

```text
gross_pnl = side * quantity * (exit_midquote - entry_midquote)
total_cost = entry_cost + exit_cost
net_pnl = gross_pnl - total_cost
```

Ledger-level:

```text
cash_after = cumulative cash_delta
position_after = cumulative quantity_delta
inventory_value_after = position_after * fill_midquote
equity_after = cash_after + inventory_value_after
```

For a completed horizon simulation, final position should return to zero.

## Outputs

- `*_execution_accounting_v1_trades.csv`
- `*_execution_accounting_v1_ledger.csv`
- `*_execution_accounting_v1_summary.csv`
- `*_execution_accounting_v1_manifest.json`

The manifest records accounting scope flags and explicitly marks passive fills,
order book fills, official fee models, risk controls, parameter optimization,
and research-grade backtesting as not implemented.

## Boundary

Execution accounting v1 answers:

```text
If these active signal rows were converted into fixed-horizon round trips, do
the account, cash, inventory, cost, and PnL arithmetic reconcile?
```

It does not answer:

```text
Is the strategy profitable or executable under realistic market conditions?
```
