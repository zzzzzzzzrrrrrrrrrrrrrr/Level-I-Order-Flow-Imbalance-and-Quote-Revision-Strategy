# AAPL WRDS 2026-04-08 to 2026-04-10 Pipeline Report

Recorded: 2026-04-30.

## Scope

Data slice:

```text
configs/data/aapl_wrds_20260408_20260410.yaml
symbol = AAPL
trading dates = 2026-04-08, 2026-04-09, 2026-04-10
quote source = WRDS TAQ NBBOM
trade source = WRDS TAQ CTM
```

This report records the local pipeline run through target-position accounting
v1 diagnostics. It is not a research-grade backtest report.

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
- threshold selection v1
- cost model v1 diagnostics
- execution accounting v1 scaffold
- target-position accounting v1 scaffold
- parameter sensitivity v1

Not implemented:

- condition-code final eligibility filters
- model fitting
- research-grade execution-aware backtest
- broker / SEC / FINRA / exchange fee modeling
- advanced position limits and risk controls
- train-window parameter optimization

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

## Threshold Selection

Policy:

```text
threshold_selection_policy = train_window_grid_search_next_date_test
threshold_objective = maximize_train_mean_signal_aligned_return_bps
signal_construction_policy = recompute_sequential_gate_from_selected_thresholds
label_usage_policy = train_labels_for_threshold_selection_test_labels_for_evaluation
```

Default grid:

```text
qi_threshold_grid = 0.0, 0.1, 0.25
signed_flow_threshold_grid = 0.0, 0.1, 0.25
qr_threshold_bps_grid = 0.0, 0.1, 0.25
min_train_signals = 100
```

Selected thresholds and test results:

| fold | horizon | QI | signed-flow | QR bps | test mean aligned return bps | test accuracy |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| `fold_001` | `100ms` | `0.25` | `0.25` | `0.25` | `0.3825` | `0.5688` |
| `fold_001` | `500ms` | `0.25` | `0.10` | `0.25` | `-0.0220` | `0.5191` |
| `fold_001` | `1s` | `0.25` | `0.25` | `0.10` | `0.2103` | `0.4850` |
| `fold_001` | `5s` | `0.25` | `0.10` | `0.25` | `2.2773` | `0.4993` |
| `fold_002` | `100ms` | `0.25` | `0.25` | `0.25` | `0.3909` | `0.5322` |
| `fold_002` | `500ms` | `0.00` | `0.25` | `0.25` | `0.3974` | `0.4898` |
| `fold_002` | `1s` | `0.25` | `0.10` | `0.25` | `0.4187` | `0.5289` |
| `fold_002` | `5s` | `0.25` | `0.10` | `0.25` | `0.3637` | `0.6494` |

This remains a statistical threshold-selection result. It is not a costed
trading result.

## Cost Model Diagnostics

Policy:

```text
cost_model_policy = spread_and_stress_cost_diagnostics_v1
execution_cost_policy = aggressive_one_way_half_spread_proxy
round_trip_cost_policy = aggressive_entry_exit_full_spread_proxy
```

Default stress grid:

```text
fixed_bps_grid = 0.0, 0.5, 1.0, 2.0, 5.0
slippage_ticks_grid = 0.0, 0.5, 1.0
tick_size = 0.01
```

Base scenario with `fixed_bps = 0.0` and `slippage_ticks = 0.0`:

| horizon | evaluated signals | mean signed future return bps | mean half-spread cost bps | mean after one-way cost bps | share beating one-way cost | mean after round-trip cost bps |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `100ms` | `54,707` | `0.1952` | `0.5758` | `-0.3806` | `0.2055` | `-0.9564` |
| `500ms` | `54,647` | `0.1863` | `0.5755` | `-0.3892` | `0.2502` | `-0.9647` |
| `1s` | `54,599` | `0.2149` | `0.5756` | `-0.3607` | `0.2899` | `-0.9362` |
| `5s` | `54,508` | `0.2003` | `0.5756` | `-0.3753` | `0.3942` | `-0.9509` |

Interpretation: cost diagnostics show that the current Level-I directional
signal does not generate sufficient average future midquote movement to overcome
a one-way half-spread execution assumption. Under the base spread-only scenario,
average post-cost outcomes are negative across `100ms`, `500ms`, `1s`, and `5s`
horizons.

Cost model v1 should therefore be interpreted as a rejection diagnostic for the
current naive signal configuration, not as a profitability test. The framework
now has a cost-aware filter for signal specifications before any execution or
PnL claims are made.

## Execution Accounting Scaffold

Policy:

```text
accounting_policy = independent_fixed_horizon_round_trip_accounting_v1
entry_execution_policy = entry_at_signal_midquote_with_half_spread_cost
exit_execution_policy = exit_at_future_midquote_with_entry_spread_proxy
position_policy = independent_unit_round_trips_no_position_limit
```

Default scenario:

```text
quantity = 1 share
fixed_bps = 0.0
slippage_ticks = 0.0
tick_size = 0.01
```

Accounting summary:

| horizon | round trips | gross PnL | cost | net PnL | mean net per round trip | win rate | final position | max abs position |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `100ms` | `54,707` | `276.605` | `1,631.170` | `-1,354.565` | `-0.024760` | `0.0856` | `0.0` | `84.0` |
| `500ms` | `54,647` | `263.655` | `1,628.600` | `-1,364.945` | `-0.024977` | `0.1185` | `0.0` | `84.0` |
| `1s` | `54,599` | `304.150` | `1,627.310` | `-1,323.160` | `-0.024234` | `0.1557` | `0.0` | `123.0` |
| `5s` | `54,508` | `282.540` | `1,624.650` | `-1,342.110` | `-0.024622` | `0.2716` | `0.0` | `153.0` |

The scaffold reconciles cash, inventory, and final position for fixed-horizon
round trips. The negative net PnL is consistent with the cost model diagnostic:
gross signal-aligned price movement is positive, but not large enough to cover
spread costs. The high max absolute position comes from the current independent
round-trip policy and should not be interpreted as a risk-controlled strategy
position.

## Target-Position Accounting Scaffold

Policy:

```text
target_position_policy = signal_to_bounded_target_position_v1
target_mapping_policy = long_short_flat_signal_to_target_position
order_execution_policy = fill_at_signal_midquote_with_cost_deduction
position_limit_policy = clip_target_to_max_abs_position
```

Default scenario:

```text
max_position = 1 share
flat_on_no_signal = true
eod_flat = true
cooldown = 0ms
max_trades_per_day = none
fixed_bps = 0.0
slippage_ticks = 0.0
```

Target-position result:

| metric | value |
| --- | ---: |
| input signal rows | `1,648,869` |
| active signal rows | `54,710` |
| target-change candidates | `45,288` |
| accepted orders | `45,288` |
| skipped missing price rows | `0` |
| skipped cooldown orders | `0` |
| skipped max-trades orders | `0` |
| total cost | `594.835` |
| final equity | `-546.100` |
| final position | `0.0` |
| max abs position | `1.0` |
| mean abs position | `0.500199` |
| total turnover | `11,735,709.655` |

This scaffold materially reduces the unrealistic exposure of independent
round-trip accounting by enforcing a bounded target position. The result remains
negative after spread costs. This is still not hyperparameter selection: no
threshold, cooldown, spread filter, depth filter, or cost assumption was chosen
inside a train window.

## Parameter Sensitivity

Policy:

```text
parameter_sensitivity_policy = exhaustive_grid_report_no_selection_v1
parameter_selection_policy = no_parameter_selection
```

Default smoke grid:

```text
max_position_grid = 1
cooldown_grid = 0ms
max_trades_per_day_grid = none
fixed_bps_grid = 0.0, 1.0
slippage_ticks_grid = 0.0
```

Candidate results:

| candidate | max position | cooldown | max trades/day | fixed bps | slippage ticks | orders | final equity | total cost |
| --- | ---: | --- | --- | ---: | ---: | ---: | ---: | ---: |
| `candidate_0001` | `1.0` | `0ms` | none | `0.0` | `0.0` | `45,288` | `-546.100` | `594.835` |
| `candidate_0002` | `1.0` | `0ms` | none | `1.0` | `0.0` | `45,288` | `-1,719.671` | `1,768.406` |

This sensitivity run reports all configured candidates and does not choose a
final parameter. It is a framework check showing how account outcomes change
under a predeclared cost stress, not a train-window hyperparameter selection.

## Known Limitations

- NBBO quote-condition eligibility remains diagnostic-only.
- Sale-condition eligibility remains unresolved.
- Quote size unit interpretation is not independently finalized.
- Cost model v1 is diagnostic-only and excludes official broker, SEC, FINRA,
  exchange fee, rebate, and routing assumptions.
- Execution accounting v1 is an account-mechanics scaffold only.
- Target-position accounting v1 is a bounded account-state scaffold only.
- There is no research-grade execution-aware backtest.
- Execution accounting v1 has no target-position logic, position limits,
  cooldown, or risk controls.
- Target-position accounting v1 does not implement train-window parameter
  optimization, latency modeling, passive execution, or official fee schedules.
- Parameter sensitivity v1 reports candidates but does not select final
  hyperparameters.
