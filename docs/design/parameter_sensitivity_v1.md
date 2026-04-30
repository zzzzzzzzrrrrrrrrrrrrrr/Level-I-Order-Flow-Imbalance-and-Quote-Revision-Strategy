# Parameter Sensitivity v1

## Purpose

Parameter sensitivity v1 runs a configured grid of target-position accounting
parameters and reports every candidate. It is the controlled experiment layer
before any train-window hyperparameter selection.

This stage does not choose final parameters.

## Scope

Implemented:

- grid over target-position accounting parameters
- target-position accounting run per candidate
- candidate-level summary table
- manifest with full grid and scope flags
- all candidates retained in output

Not implemented:

- train-window parameter selection
- model fitting
- final hyperparameter choice
- passive execution
- official fee model
- research-grade backtesting

## Inputs

- `*_signals_v1.csv`

The runner reuses target-position accounting v1 and therefore needs the same
signal, midquote, spread, symbol, date, and timestamp columns.

## Candidate Parameters

The v1 grid covers:

- `max_position`
- `cooldown`
- `max_trades_per_day`
- `fixed_bps`
- `slippage_ticks`

Defaults are intentionally small so full-slice smoke checks remain practical:

```text
max_position_grid = 1
cooldown_grid = 0ms
max_trades_per_day_grid = none
fixed_bps_grid = 0, 1
slippage_ticks_grid = 0
```

Larger grids must be passed explicitly through the CLI. They are sensitivity
experiments, not optimized parameters.

## Outputs

- `*_parameter_sensitivity_v1.csv`
- `*_parameter_sensitivity_v1_manifest.json`

The CSV contains every candidate and its target-position accounting summary. It
is sorted only by the analyst or downstream report; the module itself does not
select a winner.

## Boundary

Parameter sensitivity v1 answers:

```text
How do account diagnostics change across a predeclared grid?
```

It does not answer:

```text
Which hyperparameters should be selected for future trading?
```

The next methodological step is train-window parameter selection, where
candidate choice must be made on training dates and tested out of sample.
