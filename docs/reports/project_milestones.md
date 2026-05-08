# Project Milestones

## Purpose

This file is only a dated index of completed project milestones. It should not
duplicate module design notes, run reports, or the canonical parameter table.

Project-wide assumptions and manually selected parameters remain in
`assumptions/parameter_registry.md`.

## 2026-05-05

Completed diagnostic cleanup and AAPL reruns after the audit of leakage and
multi-symbol accounting risks.

Code-level corrections:

- v2.2 adverse-selection pass/fail input now filters v2.1 orders to validation
  dates only.
- v2.1 spread-quantile candidate pools use prior per-symbol candidate dates
  rather than full-sample quantiles.
- v2.1 cancellation volatility-spike threshold is a fixed config value instead
  of being fitted from the same future TTL window.
- v2.1 chunked candidate loading sorts across chunk boundaries before
  side-change detection.
- fixed-horizon and target-position accounting maintain per-symbol/date books
  and report portfolio net/gross exposure.
- CSV boolean parsing guards were added where diagnostics read saved artifacts.

Reran the focused AAPL v2.1 diagnostic and both AAPL v2.2 screen paths from
existing processed artifacts. AAPL remains a negative benchmark:

```text
v2.1 selected_test_net_pnl = -329.52
v2.2 top_1pct_move_over_cost = 0.6483
v2.2 filled_1s_markout_bps = 0.1355
v2.2 unfilled_1s_markout_bps = 0.3170
v2.2 validation_pass_flag = false
```

## 2026-05-04

Added the first lightweight config-driven experiment orchestration layer:

```text
scripts/run_experiment.py
docs/design/experiment_orchestration_v1.md
```

The runner reads an experiment YAML, resolves configured pipeline stages, and
prints the commands by default. Execution requires `--execute`. This keeps the
existing script/module boundaries intact and does not change v1 baseline logic
or core schemas.

Added the generic phase-1 multi-symbol data config:

```text
configs/data/phase1_wrds_12_symbols_20260313_20260410.yaml
```

This config declares the 12-symbol liquidity-regime sample on the same
20-trading-day window. It is intended to avoid hand-copying one full WRDS YAML
per symbol. Per-symbol artifacts should be generated only where existing
pipeline output contracts require symbol-specific slice names.

## 2026-05-02

Configured the additive v2.2 phase-1 liquidity-regime diagnostic scaffold:

```text
configs/experiments/v22_symbol_screen_phase1_by_liquidity_regime_same_20d.yaml
```

This experiment treats groups as ex-ante liquidity-regime hypotheses:

```text
Group A = ultra-liquid mega-cap control
Group B = high-turnover tick/spread-structure candidates
Group C = non-tech large-cap liquidity controls
```

The scaffold is reporting-only metadata around the existing v2.2 diagnostic
screen. It does not change v1 baseline signals, labels, threshold selection,
horizon selection, or cost accounting. AAPL remains the only processed
available slice at creation time and remains the known negative control until
additional WRDS slices are extracted and processed.

## 2026-05-01

Current implemented pipeline:

```text
cleaning -> alignment -> quote features -> trade signing -> signed flow
-> labels -> signals -> walk-forward evaluation -> threshold selection
-> cost diagnostics -> execution accounting -> target-position accounting
-> parameter sensitivity -> TVT parameter selection -> backtest v1
-> model training v1 -> PnL reporting v1
```

20-day AAPL prototype run completed:

```text
configs/data/aapl_wrds_20260313_20260410.yaml
data/raw/aapl_wrds_20260313_20260410/
data/interim/aapl_wrds_20260313_20260410/
data/processed/aapl_wrds_20260313_20260410/
```

This slice covers 20 regular-session dates from `2026-03-13` through
`2026-04-10`, excluding `2026-04-03`. The full pipeline now runs through PnL
reporting on this slice. Gross directional edge is positive before spread
costs, but net PnL remains negative after the current spread-cost accounting.

Large-slice memory handling was upgraded so labeling, signal generation, and
downstream analysis read only required columns and stream date groups where the
full 20-day input would otherwise exceed local memory.

Status details live in:

- `docs/reports/aapl_wrds_20260313_20260410_pipeline_report.md`
- `docs/reports/aapl_wrds_20260408_20260410_pipeline_report.md`
- `docs/design/`
- `docs/decisions/`
- `assumptions/parameter_registry.md`

Still separate and not final at this date: condition-code eligibility filters,
research-grade backtesting, official fee modeling, passive execution, latency
modeling, cost-aware signal selection, and final hyperparameter selection.

Residual technical note: alignment tolerance sensitivity still needs a more
memory-efficient implementation for the 20-day slice; the main no-tolerance
alignment path completed successfully.

## 2026-04-30

The earlier 3-day AAPL validation slice
`aapl_wrds_20260408_20260410` completed the first end-to-end scaffold through
PnL reporting v1. It remains a historical validation artifact, not the active
prototype slice.
