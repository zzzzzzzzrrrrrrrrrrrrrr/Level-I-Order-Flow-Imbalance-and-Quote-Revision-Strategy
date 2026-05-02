# Project Milestones

## Purpose

This file is only a dated index of completed project milestones. It should not
duplicate module design notes, run reports, or the canonical parameter table.

Project-wide assumptions and manually selected parameters remain in
`assumptions/parameter_registry.md`.

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
