# Level-I OFI / QR Strategy

Execution-aware research scaffold for studying whether Level-I quote and trade information contains stable short-horizon predictive content in liquid U.S. equities.

## Repository layout

- `AGENTS.md`: project operating rules and research constraints
- `assumptions/`: centralized registry of project assumptions and parameters
- `configs/`: data, experiment, backtest, and live-paper configs
- `data/`: local raw, interim, processed, cache, and fixture data
- `docs/`: design notes, decisions, reports, and references
- `notebooks/`: staged exploratory notebooks by workflow step
- `scripts/`: thin entry points that call package code
- `src/level1_ofi_qr/`: package modules for schema, cleaning, features, evaluation, backtesting, execution, and risk
- `tests/`: fixtures plus unit, integration, and regression tests
- `outputs/`: generated experiment, backtest, figure, table, and log artifacts

Documentation starts at `docs/README.md`. For the current AAPL negative-result checkpoint, read `docs/reports/README.md` first.

## Current status

- `AGENTS.md` remains the source of project rules.
- The existing notebook lives at `notebooks/00_data_inspection/experiment.ipynb`.
- Data, outputs, local agent state, and notebook files are ignored by default in `.gitignore`.
- Stage 1 schema, WRDS normalization, cleaning primitives, and raw WRDS extraction scaffolding are implemented with unit tests.
- The current AAPL WRDS slice uses `taqmsec.nbbom_YYYYMMDD` as the main quote source, so normalized quotes represent national BBO state: `bid_exchange`, `ask_exchange`, `bid`, `ask`, `bid_size`, and `ask_size`. Trades use `taqmsec.ctm_YYYYMMDD`.
- The active prototype slice is `aapl_wrds_20260313_20260410`: 20 regular-session AAPL dates from `2026-03-13` through `2026-04-10`, excluding the `2026-04-03` market holiday. Its data and outputs live in slice-named folders, separate from the earlier 3-day validation slice.
- Cleaning v2, quote-trade alignment v1, quote feature v1, trade signing v1, signed-flow feature v1, labeling v1, signals v1, walk-forward evaluation v1, threshold selection v1, cost model v1, target-position accounting v1, TVT parameter selection v1, backtest v1, model training v1, cost-aware linear-score selection, microstructure v2 diagnostics, microstructure v2.1 passive/hybrid diagnostics, v2.2 symbol screening diagnostics, and the assumption registry are implemented as separate auditable artifacts. Condition-code eligibility, official fee modeling, full-grid v2.1 performance optimization, multi-symbol data extraction/runs, and research-grade backtesting remain unfinished stages.

## WRDS extraction

Preview generated SQL without connecting to WRDS:

```powershell
D:\python_library_envs\VHFT_lab\python.exe scripts\extract_wrds.py configs\data\aapl_wrds_20260313_20260410.yaml --dry-run-sql --limit-per-query 5
```

Run extraction after WRDS credentials are available through WRDS configuration or `WRDS_USERNAME`:

```powershell
D:\python_library_envs\VHFT_lab\python.exe scripts\extract_wrds.py configs\data\aapl_wrds_20260313_20260410.yaml
```

Generated raw data and manifests are written under the configured raw-data directory and are ignored by Git.

For local credentials, either use WRDS' `.pgpass` flow or create a git-ignored `.env` file in the project root:

```text
WRDS_USERNAME=your_wrds_username
WRDS_PASSWORD=your_wrds_password
```

If `.pgpass` already stores the password, `WRDS_USERNAME` alone is usually enough.

Then test the login path without downloading data:

```powershell
D:\python_library_envs\VHFT_lab\python.exe scripts\extract_wrds.py configs\data\aapl_wrds_20260313_20260410.yaml --connection-test
```

Check which credentials source the script sees:

```powershell
D:\python_library_envs\VHFT_lab\python.exe scripts\extract_wrds.py configs\data\aapl_wrds_20260313_20260410.yaml --show-credential-sources --dry-run-sql
```

List the latest available daily TAQ tables before choosing config dates:

```powershell
D:\python_library_envs\VHFT_lab\python.exe scripts\extract_wrds.py configs\data\aapl_wrds_20260313_20260410.yaml --list-tables --table-list-limit 10
```

For the active 20-day AAPL slice, omit `--limit-per-query` and write raw outputs to a slice-named folder:

```powershell
D:\python_library_envs\VHFT_lab\python.exe scripts\extract_wrds.py configs\data\aapl_wrds_20260313_20260410.yaml --output-dir data\raw\aapl_wrds_20260313_20260410
```

## Dataset build

Build normalized and cleaned datasets from raw WRDS CSV outputs:

```powershell
D:\python_library_envs\VHFT_lab\python.exe scripts\build_dataset.py configs\data\aapl_wrds_20260313_20260410.yaml
```

The build writes normalized files under `data/interim/<slice_name>/`; cleaned files, rejected-row audit files, and a diagnostics manifest under `data/processed/<slice_name>/`. If you are building from the small validation extract instead of the full slice, pass `--raw-dir data\raw\validation_structured_contract`.

## Quote-Trade Alignment

Align cleaned trades to the latest cleaned quote strictly before each trade:

```powershell
D:\python_library_envs\VHFT_lab\python.exe scripts\align_trades.py configs\data\aapl_wrds_20260313_20260410.yaml
```

The alignment output is written under `data/processed/<slice_name>/` as `*_trades_aligned_quotes.csv` plus `*_alignment_manifest.json`. Matches are constrained to the same `symbol` and `trading_date`.

This alignment version only performs backward quote-trade matching within symbol and trading_date groups. It does not perform trade signing, sale-condition filtering, correction filtering, or final research-sample cleaning.

Run tolerance-sensitivity diagnostics without selecting a final tolerance:

```powershell
D:\python_library_envs\VHFT_lab\python.exe scripts\align_trades.py configs\data\aapl_wrds_20260313_20260410.yaml --tolerance-sensitivity
```

## Quote Features

Build quote-only Level-I features from cleaned quotes:

```powershell
D:\python_library_envs\VHFT_lab\python.exe scripts\build_quote_features.py configs\data\aapl_wrds_20260313_20260410.yaml
```

Quote feature v1 computes row-preserving, quote-only features from cleaned Level-I quotes. It supports spread, depth, signed top-of-book imbalance, quote revision, and quote event interval diagnostics. It does not compute trade signing, signed order flow imbalance, labels, or backtest signals.

## Trade Signing

Build trade signing v1 output from aligned trades:

```powershell
D:\python_library_envs\VHFT_lab\python.exe scripts\sign_trades.py configs\data\aapl_wrds_20260313_20260410.yaml
```

Trade signing v1 uses quote rule with tick-rule fallback on aligned trade rows. It preserves all aligned trades and writes `*_trades_signed_v1.csv` plus `*_trade_signing_v1_manifest.json`. It does not apply sale-condition filters, NBBO condition filters, OFI aggregation, labels, or backtest signals.

## Signed-Flow Features

Build signed-flow v1 features from signed trades:

```powershell
D:\python_library_envs\VHFT_lab\python.exe scripts\build_signed_flow_features.py configs\data\aapl_wrds_20260313_20260410.yaml
```

Signed-flow feature v1 computes row-preserving trade-flow features over trailing event-count and clock-time windows within `symbol` and `trading_date`. Windows include the current trade print; unknown-sign trades contribute zero signed flow while remaining in total trade volume. It does not apply condition-code filters, construct labels, or run backtests.

## Labels

Build future midquote labels for signed-flow feature rows:

```powershell
D:\python_library_envs\VHFT_lab\python.exe scripts\build_labels.py configs\data\aapl_wrds_20260313_20260410.yaml
```

Labeling v1 creates future midquote return and direction targets from quote feature data. Labels are computed strictly after `decision_time` and must not be used as features. This step does not create trading signals, run walk-forward evaluation, or run backtests.

## Signals

Build sequential-gate signal v1 rows:

```powershell
D:\python_library_envs\VHFT_lab\python.exe scripts\build_signals.py configs\data\aapl_wrds_20260313_20260410.yaml
```

Signals v1 builds an interpretable sequential gate from current quote imbalance, signed-flow imbalance, and quote revision. Default thresholds are diagnostic sign-agreement defaults, not optimized trading thresholds. Labels may be retained in the output for later evaluation, but labels are not used to compute signals. This step does not run walk-forward evaluation or backtests.

## Walk-Forward Evaluation

Run walk-forward statistical evaluation for signal v1 rows:

```powershell
D:\python_library_envs\VHFT_lab\python.exe scripts\run_walk_forward.py configs\data\aapl_wrds_20260313_20260410.yaml
```

Walk-forward evaluation v1 evaluates precomputed signals against future midquote labels using expanding training-date context and next-date test folds. It does not optimize thresholds, fit models, apply transaction costs, or run backtests.

## Threshold Selection

Run training-window threshold selection for sequential-gate signals:

```powershell
D:\python_library_envs\VHFT_lab\python.exe scripts\run_threshold_selection.py configs\data\aapl_wrds_20260313_20260410.yaml
```

Threshold selection v1 selects QI, signed-flow, and QR thresholds inside each walk-forward training window, then evaluates the selected thresholds on the next test date. It does not fit predictive models, apply transaction costs, or run backtests.

## Cost Model

Run spread and stress cost diagnostics for active signal rows:

```powershell
D:\python_library_envs\VHFT_lab\python.exe scripts\run_cost_model.py configs\data\aapl_wrds_20260313_20260410.yaml
```

Cost model v1 estimates one-way half-spread, round-trip full-spread, fixed-bps,
and slippage-tick stress costs. It writes `*_cost_model_v1.csv` and
`*_cost_model_v1_manifest.json`. Treat it as a cost-aware rejection diagnostic,
not a profitability test. It does not simulate orders, position accounting,
broker fees, exchange fees/rebates, SEC/FINRA fees, or risk controls.

## Execution Accounting

Run the account-mechanics scaffold for active signal rows:

```powershell
D:\python_library_envs\VHFT_lab\python.exe scripts\run_execution_accounting.py configs\data\aapl_wrds_20260313_20260410.yaml
```

Execution accounting v1 converts active signals into independent one-share
fixed-horizon round trips and writes trade, ledger, summary, and manifest
artifacts. It verifies cash, position, inventory, cost, and PnL arithmetic. It
is not a complete backtest and does not implement risk controls, official fees,
passive fills, order book queueing, or parameter optimization.

## Target-Position Accounting

Run bounded target-position accounting for signal rows:

```powershell
D:\python_library_envs\VHFT_lab\python.exe scripts\run_target_position_accounting.py configs\data\aapl_wrds_20260313_20260410.yaml
```

Target-position accounting v1 maps signals to bounded target positions, applies
flat-on-no-signal, EOD flat, cooldown, and optional max-trades controls, and
writes orders, ledger, summary, and manifest artifacts. It is the safer account
state scaffold for later sensitivity tests, but it is not hyperparameter
selection or a research-grade backtest.

## Parameter Sensitivity

Run a predeclared sensitivity grid over target-position accounting parameters:

```powershell
D:\python_library_envs\VHFT_lab\python.exe scripts\run_parameter_sensitivity.py configs\data\aapl_wrds_20260313_20260410.yaml
```

Parameter sensitivity v1 reports every configured candidate and writes
`*_parameter_sensitivity_v1.csv` plus `*_parameter_sensitivity_v1_manifest.json`.
It does not select final hyperparameters. Larger grids should be passed
explicitly through the CLI and treated as sensitivity, not optimization.

## TVT Parameter Selection

Run train-validation-test parameter selection for target-position accounting:

```powershell
D:\python_library_envs\VHFT_lab\python.exe scripts\run_tvt_parameter_selection.py configs\data\aapl_wrds_20260313_20260410.yaml
```

TVT parameter selection v1 uses expanding train dates, selects parameters on the
next validation date, and evaluates the frozen selected candidate on the next
test date. In the current version, no predictive model is trained and no final
hyperparameter claim is made.

## Model Training

Run the AAPL prototype model training and model-based held-out backtest:

```powershell
D:\python_library_envs\VHFT_lab\python.exe scripts\run_model_training.py configs\data\aapl_wrds_20260313_20260410.yaml
```

Model training v1 learns a standardized linear directional score on the train
date, selects feature-set and score-threshold candidates on the validation date,
and runs target-position accounting on the held-out test date. It writes
`*_model_training_v1_candidates.csv`, `*_model_training_v1_predictions.csv`,
`*_model_backtest_v1_summary.csv`, and related order / ledger / manifest files.
This is an AAPL prototype loop, not a generalized research-grade model claim.

## Microstructure V2.1 Diagnostics

Run the independent passive/hybrid execution diagnostic path:

```powershell
D:\python_library_envs\VHFT_lab\python.exe scripts\run_microstructure_v21_diagnostics.py configs\data\aapl_wrds_20260313_20260410.yaml
```

Microstructure v2.1 reads the existing cost-aware linear-score predictions and
does not modify the v1 signal-generation logic, the v1 backtest path, or the
existing v2.0 diagnostic outputs. It separates candidate-pool construction,
execution-mode selection, passive-fill simulation, cancellation rules, metrics,
and chronological validation selection under
`src/level1_ofi_qr/diagnostics/microstructure_v21/`.

The default grid supports candidate-pool, edge-threshold, microprice-usage, TTL,
queue-haircut, and execution-variant choices. On the current full AAPL slice the
full default grid is computationally heavy because it repeats passive-fill
simulation over 186,405 candidate events. For focused diagnostics, restrict the
grid explicitly, for example:

```powershell
D:\python_library_envs\VHFT_lab\python.exe scripts\run_microstructure_v21_diagnostics.py configs\data\aapl_wrds_20260313_20260410.yaml --candidate-pools one_tick_spread --edge-thresholds edge_gt_0 --microprice-usages cancellation_only --ttls 1s --queue-haircuts conservative --execution-variants passive_entry_market_exit
```

Outputs are written under `data/processed/<slice_name>/` with the
`*_microstructure_v21_*` stem. Treat all v2.1 results as diagnostics, not
confirmed profitability.

## Symbol Screening V2.2

Run the cheap cross-symbol screening layer:

```powershell
D:\python_library_envs\VHFT_lab\python.exe scripts\run_symbol_screen_v22.py configs\experiments\v22_symbol_screen_liquid_large_cap.yaml
```

Symbol screening v2.2 does not modify the raw, cleaned, aligned, feature,
label, or WRDS mapping schemas. It uses the existing long-form `symbol` column
and the existing `universe.symbols` config field. The AAPL data-slice config is
left unchanged so it remains a reproducible negative benchmark. The experiment
config declares the larger intended universe and lists the processed data slices
currently available for screening.

The screening workflow writes new result tables only:

```text
outputs/tables/v22_symbol_screen_summary.csv
outputs/tables/v22_symbol_screen_deciles.csv
outputs/tables/v22_symbol_screen_horizon_sweep.csv
outputs/figures/v22_symbol_screen_move_over_cost.svg
outputs/figures/v22_symbol_screen_decile_markout.svg
```

The current experiment config declares the intended large-cap universe but only
has an available AAPL data slice. A real cross-symbol screen requires adding one
processed data-slice config per symbol under `data_slices`.

## PnL Reporting

Generate an SVG equity/PnL curve comparing the sequential-gate strategy and the
linear-score model:

```powershell
D:\python_library_envs\VHFT_lab\python.exe scripts\plot_pnl.py configs\data\aapl_wrds_20260313_20260410.yaml
```

Outputs:

```text
outputs/figures/aapl_wrds_20260313_20260410_pnl_comparison.svg
outputs/tables/aapl_wrds_20260313_20260410_pnl_comparison_summary.csv
outputs/tables/aapl_wrds_20260313_20260410_pnl_comparison_curve.csv
```

The 20-day prototype currently shows positive gross edge before spread costs on
held-out dates, but negative net PnL after the current spread-cost accounting.
Treat this as a turnover/cost-control finding, not as a profitable result.

## Backtest

Run backtest v1 for TVT-selected target-position accounting candidates:

```powershell
D:\python_library_envs\VHFT_lab\python.exe scripts\run_backtest.py configs\data\aapl_wrds_20260313_20260410.yaml
```

Backtest v1 reads `*_signals_v1.csv` and
`*_tvt_parameter_selection_v1.csv`, evaluates only candidates marked
`selected_for_test` on their held-out test dates, and writes orders, ledger,
summary, and manifest artifacts. It does not reselect parameters on test data,
train a predictive model, simulate passive fills, model explicit latency, or
claim research-grade profitability.
