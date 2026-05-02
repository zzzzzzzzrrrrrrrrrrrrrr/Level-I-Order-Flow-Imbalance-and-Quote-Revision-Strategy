# Reports Index

Reports are empirical artifacts. They record what was run, what assumptions were used, and what the result means. Negative findings should stay visible.

## Current Result Stack

| Report | Status | What It Answers |
| --- | --- | --- |
| `symbol_screening_v22_phase1_by_liquidity_regime_same_20d.md` | Configured pending data extraction | How will the phase-1 same-window liquidity-regime screen be run and audited? |
| `symbol_screening_v22_2026-05-01.md` | Current conclusion | Does AAPL pass validation-only move/cost and adverse-selection screening? No. |
| `microstructure_v21_diagnostic_2026-05-01.md` | Current execution diagnostic | Does passive/hybrid focused execution rescue AAPL? No under the tested variant. |
| `microstructure_v2_diagnostic_2026-05-01.md` | Prior diagnostic | Why did cost-aware selection reduce losses but not create stable tradable alpha? |
| `aapl_wrds_20260313_20260410_pipeline_report.md` | Pipeline baseline | What happened in the 20-day AAPL v1/v1-model pipeline before v2 execution diagnostics? |
| `aapl_wrds_20260408_20260410_pipeline_report.md` | Historical small-slice report | What happened in the earlier 3-day validation slice? |
| `project_milestones.md` | Milestone log | What was implemented chronologically? |

## AAPL Negative Baseline

Tag:

```text
aapl-negative-baseline-v22
```

Core metrics:

```text
v2.1 focused net_pnl = -341.79
v2.1 selected_test_net_pnl = -329.52
v2.2 top_1pct_move_over_cost = 0.6483
v2.2 filled_1s_markout_bps = 0.0954
v2.2 unfilled_1s_markout_bps = 0.3115
v2.2 validation_pass_flag = false
```

Current v2.2 experiment artifacts:

```text
outputs/experiments/v22_symbol_screen_liquid_large_cap/
```

Phase-1 liquidity-regime diagnostic scaffold:

```text
configs/experiments/v22_symbol_screen_phase1_by_liquidity_regime_same_20d.yaml
outputs/experiments/v22_symbol_screen_phase1_by_liquidity_regime_same_20d/
```

The phase-1 scaffold keeps AAPL as the known negative control and treats groups
as ex-ante liquidity-regime hypotheses. Group metadata is reporting-only and
must not change signals, labels, thresholds, horizons, or costs.

Interpretation:

```text
Current AAPL Level-I QI/OFI/QR setup has weak statistical predictive content,
but not enough edge to cover cost and passive adverse selection under the tested
execution assumptions.
```

## How To Add A New Report

Use a dated report name:

```text
<topic>_YYYY-MM-DD.md
```

Each report should include:

- config path
- data slice
- command
- output paths
- key metrics
- interpretation
- limits and whether test data was used for selection
