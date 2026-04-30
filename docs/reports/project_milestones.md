# Project Milestones

## Purpose

This file is only a dated index of completed project milestones. It should not
duplicate module design notes, run reports, or the canonical parameter table.

Project-wide assumptions and manually selected parameters remain in
`assumptions/parameter_registry.md`.

## 2026-04-30

Current implemented pipeline:

```text
cleaning -> alignment -> quote features -> trade signing -> signed flow
-> labels -> signals -> walk-forward evaluation -> threshold selection
-> cost diagnostics -> execution accounting -> target-position accounting
-> parameter sensitivity -> TVT parameter selection -> backtest v1
-> model training v1 -> PnL reporting v1
```

Additional data configuration prepared:

```text
configs/data/aapl_wrds_20260313_20260410.yaml
```

This expanded AAPL prototype slice covers 20 trading dates and is intended for
the next extraction / build run before adding more complex ML models.

Status details live in:

- `docs/reports/aapl_wrds_20260408_20260410_pipeline_report.md`
- `docs/design/`
- `docs/decisions/`
- `assumptions/parameter_registry.md`

Still separate and not final at this date: condition-code eligibility filters,
research-grade backtesting, official fee modeling, passive execution, latency
modeling, model training, and final hyperparameter selection.
