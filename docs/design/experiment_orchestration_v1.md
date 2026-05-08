# Experiment Orchestration V1

## Scope

Experiment orchestration v1 provides a thin config-driven runner over the
existing scripts. It does not move research logic out of the current pipeline
modules and does not change raw, cleaned, aligned, feature, label, signal, cost,
or diagnostic schemas.

The goal is:

```text
same Python runner + different YAML = different reproducible experiment
```

## Config Layers

The project uses separate config responsibilities:

- data config: WRDS source, symbol universe, date window, source mappings, and
  storage roots
- experiment config: research design, groups, validation rules, diagnostic
  settings, and output scope
- pipeline section: ordered stage list that tells the runner which existing
  scripts to call

The phase-1 data config is:

```text
configs/data/phase1_wrds_12_symbols_20260313_20260410.yaml
```

The phase-1 experiment config is:

```text
configs/experiments/v22_symbol_screen_phase1_by_liquidity_regime_same_20d.yaml
```

## Runner

The orchestration entry point is:

```text
scripts/run_experiment.py
```

Default mode is dry-run planning. It prints the resolved stage commands and
does not connect to WRDS or write pipeline outputs.

Execution requires an explicit flag:

```powershell
& "D:\python_library_envs\VHFT_lab\python.exe" scripts/run_experiment.py configs/experiments/v22_symbol_screen_phase1_by_liquidity_regime_same_20d.yaml --execute
```

Use stage filters to resume or test a subset:

```powershell
& "D:\python_library_envs\VHFT_lab\python.exe" scripts/run_experiment.py configs/experiments/v22_symbol_screen_phase1_by_liquidity_regime_same_20d.yaml --from-stage build_labels --to-stage run_cost_aware_linear_score
```

## Phase-1 Pipeline Intent

The phase-1 experiment config declares a multi-symbol data config as the
default input and lists the existing pipeline stages in order. This is a
minimal orchestration layer: each stage still calls the same script that was
previously run by hand.

The preferred path is to run a generic 12-symbol data slice first. If a later
stage proves that existing output contracts require per-symbol `slice_name`
artifacts, add a small config-expansion wrapper rather than hand-copying one
large YAML per symbol.

`run_microstructure_v21` is declared but disabled in the phase-1 pipeline
because the passive/hybrid grid is computationally expensive. It should be
enabled only when the required cost-aware prediction artifacts exist and the
intended grid has been selected deliberately.

## Schema Boundary

No schema change is required for the orchestration layer. Cross-symbol
processing uses the existing long-form model:

```text
event_time | symbol | trading_date | ...
```

Do not add symbol-wide columns such as `aapl_bid` or group-dependent signal
fields. Liquidity-regime group metadata remains reporting and audit metadata
only.
