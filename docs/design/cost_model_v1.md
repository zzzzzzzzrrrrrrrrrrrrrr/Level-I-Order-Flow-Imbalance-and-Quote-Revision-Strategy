# Cost Model v1

## Purpose

Cost model v1 measures whether active signal rows have enough future midquote
movement to overcome simple execution-cost proxies.

This is a cost diagnostic layer, not a backtest.

## Scope

Implemented:

- active signal filtering using `sequential_gate_signal != 0`
- one-way aggressive half-spread cost
- round-trip aggressive full-spread cost
- fixed bps stress grid
- slippage tick stress grid
- per-horizon signed future return after cost diagnostics

Not implemented:

- broker commissions
- SEC / FINRA fees
- exchange fee / rebate schedules
- passive fill simulation
- order matching
- position accounting
- realized PnL path
- risk controls
- execution-aware backtesting

The active cost assumptions are tracked in
`assumptions/parameter_registry.md`; this design note only describes module
logic and formulas.

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
- `future_midquote_return_bps_<horizon>`

Rows with signal `0` are not evaluated as trades. Active signal rows with
missing midquote or spread are counted as missing-cost rows and excluded from
cost calculations.

## Formulas

For an active signal row:

```text
half_spread_cost_bps
  = 10000 * (signal_quoted_spread / 2) / signal_midquote

full_spread_round_trip_cost_bps
  = 10000 * signal_quoted_spread / signal_midquote

tick_cost_bps
  = 10000 * tick_size / signal_midquote

one_way_total_cost_bps
  = half_spread_cost_bps + fixed_bps + slippage_ticks * tick_cost_bps

round_trip_total_cost_bps
  = full_spread_round_trip_cost_bps
    + 2 * fixed_bps
    + 2 * slippage_ticks * tick_cost_bps

signed_future_return_bps
  = sequential_gate_signal * future_midquote_return_bps_<horizon>
```

The diagnostic after-cost edge is:

```text
after_one_way_cost_bps = signed_future_return_bps - one_way_total_cost_bps
after_round_trip_cost_bps = signed_future_return_bps - round_trip_total_cost_bps
```

## Outputs

- `*_cost_model_v1.csv`
- `*_cost_model_v1_manifest.json`

The CSV is a scenario summary by horizon, fixed bps, and slippage ticks. It does
not write a full row-level cost table by default.

The manifest records:

- input and output paths
- cost scope flags
- active signal rows
- costable signal rows
- skipped missing-cost rows
- cost grids
- policy notes
- the summary rows

## Boundary

Cost model v1 answers:

```text
How large is the signal-aligned future midquote move relative to simple
crossing-cost proxies?
```

It does not answer:

```text
Would a strategy make money after executable order routing and inventory
accounting?
```

For the current AAPL slice, cost model v1 should be treated as a rejection
diagnostic for the naive signal configuration when spread-only average
post-cost outcomes are negative. It is a filter before backtest construction,
not a profitability test.

The next implementation stage should be an execution/accounting scaffold, not
parameter tuning. That stage should track positions, trades, cash, inventory,
cost deductions, and PnL attribution while preserving the distinction between
signal evidence and tradability evidence.
