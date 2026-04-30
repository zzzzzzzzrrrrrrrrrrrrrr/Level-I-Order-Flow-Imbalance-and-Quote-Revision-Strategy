# Train-Validation-Test Parameter Selection v1

## Purpose

Train-validation-test parameter selection v1 chooses target-position accounting
parameters on a validation date and evaluates the frozen selected candidate on
the next test date.

This is the first parameter-selection layer that avoids full-sample selection.

## Scope

Implemented:

- expanding train dates
- next validation date
- next test date
- target-position accounting candidate grid
- validation-date parameter choice
- frozen selected-candidate test evaluation
- all validation candidates retained in output

Not implemented:

- predictive model fitting
- signal-threshold retraining
- final hyperparameter claim
- passive execution
- official fee model
- research-grade backtesting

## Split Policy

The split policy is:

```text
split_policy = expanding_train_next_validation_next_test
selection_policy = select_on_validation_evaluate_once_on_test
```

For the current three-day AAPL slice, this creates one fold:

```text
train      = 2026-04-08
validation = 2026-04-09
test       = 2026-04-10
```

For longer samples, v1 expands the train dates and advances validation/test by
one date.

## Selection Objective

The v1 objective is:

```text
objective = maximize_validation_final_equity
```

Tie-break:

```text
higher validation final equity
lower validation total cost
lower validation order count
```

The test date is not used for selection.

## Candidate Parameters

The v1 candidate grid covers:

- `max_position`
- `cooldown`
- `max_trades_per_day`
- `fixed_bps`
- `slippage_ticks`

Defaults are intentionally small:

```text
max_position_grid = 1
cooldown_grid = 0ms
max_trades_per_day_grid = none
fixed_bps_grid = 0, 1
slippage_ticks_grid = 0
```

## Outputs

- `*_tvt_parameter_selection_v1.csv`
- `*_tvt_parameter_selection_v1_manifest.json`

The CSV contains one row per fold/candidate. The selected validation candidate
has test metrics attached; unselected candidates keep test metrics null.

## Boundary

TVT parameter selection v1 answers:

```text
Which candidate is chosen by validation, and how does that frozen candidate
perform on the next test date?
```

It does not answer:

```text
What is the final strategy or final production hyperparameter set?
```
