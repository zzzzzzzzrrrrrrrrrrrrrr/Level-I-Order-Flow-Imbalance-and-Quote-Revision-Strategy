# Documentation Index

This directory is the project source of truth for design, decisions, and empirical reports. `AGENTS.md` remains the operating rulebook; this file explains where to look first.

## Current Checkpoint

Current reproducible checkpoint:

```text
tag = aapl-negative-baseline-v22
commit = 4d38234 Add microstructure v2 diagnostics and symbol screening
```

Meaning:

```text
AAPL 2026-03-13 to 2026-04-10 has been tested through cost-aware selection,
microstructure v2.1 passive/hybrid diagnostics, and v2.2 symbol screening.
The current AAPL QI/OFI/QR setup has weak statistical edge but no tradable alpha
under the tested execution assumptions.
```

See the full tag note:

```powershell
git show aapl-negative-baseline-v22 --no-patch --format=fuller
```

## Read This First

For a topic-based map across all documentation, start with:

```text
CONTENT_MAP.md
```

1. `reports/symbol_screening_v22_phase1_by_liquidity_regime_same_20d.md`
   Phase-1 group-aware liquidity-regime screening design and audit requirements.

2. `reports/symbol_screening_v22_2026-05-01.md`
   Current AAPL negative benchmark conclusion.

3. `reports/microstructure_v21_diagnostic_2026-05-01.md`
   Passive/hybrid execution diagnostic and submitted-vs-filled accounting.

4. `reports/microstructure_v2_diagnostic_2026-05-01.md`
   Earlier microstructure diagnostics that showed why market-order execution was too expensive.

5. `reports/aapl_wrds_20260313_20260410_pipeline_report.md`
   End-to-end AAPL 20-day pipeline report before v2 microstructure extensions.

6. `reports/project_milestones.md`
   Chronological milestone log.

## Directory Roles

| Directory | Use |
| --- | --- |
| `design/` | Module contracts and stage boundaries. Read when changing code behavior. |
| `decisions/` | Accepted architecture and methodology decisions. Read when asking why something is structured this way. |
| `reports/` | Empirical run results, metrics, negative findings, and reproducibility notes. |
| `references/` | External data/manual/source notes. |

## Content Areas

| Content Area | Primary Docs |
| --- | --- |
| Current checkpoint and negative baseline | `CONTENT_MAP.md`, `reports/README.md` |
| Data, schema, cleaning, and WRDS fields | `design/cleaning_rules_v2.md`, `design/quote_trade_alignment_v1.md`, `references/wrds_taq_field_notes.md` |
| Feature, label, signal, and evaluation pipeline | `design/quote_features_v1.md`, `design/signed_flow_features_v1.md`, `design/labeling_v1.md`, `design/signals_v1.md`, `design/walk_forward_evaluation_v1.md` |
| Cost, execution, backtest, and PnL accounting | `design/cost_model_v1.md`, `design/execution_accounting_v1.md`, `design/backtest_v1.md`, `design/pnl_reporting_v1.md` |
| V2 diagnostics and 12-symbol phase-1 screen | `design/microstructure_v21.md`, `design/symbol_screening_v22.md`, `reports/symbol_screening_v22_phase1_by_liquidity_regime_same_20d.md` |
| Experiment orchestration | `design/experiment_orchestration_v1.md` |

## Reproduction Commands

AAPL negative benchmark through v2.2 symbol screening:

```powershell
D:\python_library_envs\VHFT_lab\python.exe scripts\run_symbol_screen_v22.py configs\experiments\v22_symbol_screen_liquid_large_cap.yaml
```

Phase-1 liquidity-regime v2.2 diagnostic scaffold:

```powershell
D:\python_library_envs\VHFT_lab\python.exe scripts\run_symbol_screen_v22.py configs\experiments\v22_symbol_screen_phase1_by_liquidity_regime_same_20d.yaml
```

AAPL v2.1 focused passive/hybrid diagnostic:

```powershell
D:\python_library_envs\VHFT_lab\python.exe scripts\run_microstructure_v21_diagnostics.py configs\data\aapl_wrds_20260313_20260410.yaml --candidate-pools one_tick_spread --edge-thresholds edge_gt_0 --microprice-usages cancellation_only --ttls 1s --queue-haircuts conservative --execution-variants passive_entry_market_exit
```

Full unit suite at the checkpoint:

```powershell
D:\python_library_envs\VHFT_lab\python.exe -m pytest tests\unit
```

Expected result at the checkpoint:

```text
112 passed
```

## Naming Convention

Reports with an explicit date such as `*_2026-05-01.md` are empirical snapshots. They should not be silently rewritten after new experiments. Add a new report for materially new results.

Design docs describe intended module behavior. Decisions describe why a direction was chosen. Reports describe what happened in a run.
