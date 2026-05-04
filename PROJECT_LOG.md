# PROJECT_LOG.md

Last updated: 2026-05-03 America/New_York

This file is a handoff log, not a policy file. `AGENTS.md` remains the source
of project rules when available.

## Current Active Worktree

- Use this worktree for current development:
  `D:\pycharm_project\vHft\Level-I_Order-Flow_Imbalance_and_Quote-Revision_Strategy_clean`
- Current safe branch:
  `clean/phase1-liquidity-regime-migration`
- Current clean commit:
  `6f9e395 Add phase1 liquidity-regime symbol screen scaffold`
- `HEAD` and `origin/master` both point to `6f9e395` at the time of this log.

## Old Local History Safety

- The old local worktree is:
  `D:\pycharm_project\vHft\Level-I_Order-Flow_Imbalance_and_Quote-Revision_Strategy`
- Its local `master` is pre-cleanup history and still diverges from
  `origin/master`.
- The old local history may contain old WRDS credential-bearing commits.
- A local-only backup pointer exists:
  `backup/local-master-before-cleanup-20260502 -> 5b75264`
- Do not push local `master`.
- Do not push `backup/local-master-before-cleanup-20260502`.
- Do not force-push, mirror-push, or push tags unless the user explicitly
  approves after a history-safety explanation.

Safe PyCharm push target:

```text
clean/phase1-liquidity-regime-migration -> origin:master
```

Unsafe PyCharm push target:

```text
master -> origin:master
```

## Credential State

- WRDS password has been rotated.
- Remote history was cleaned with `git-filter-repo`.
- `origin/master:.env.example` must contain only:

```text
WRDS_USERNAME=
WRDS_PASSWORD=
```

- Do not commit real `.env` files or real WRDS credentials in docs, configs,
  tests, logs, notebooks, or generated reports.

## Research State

- Do not modify v1 `sequential_gate` or `linear_score` baseline logic.
- AAPL v2.2 negative baseline has been saved; do not continue tuning AAPL
  parameters.
- Current phase1 experiment:
  `configs/experiments/v22_symbol_screen_phase1_by_liquidity_regime_same_20d.yaml`
- Group is an experiment-design-level object, not a post-hoc output category.
- Group metadata is reporting/audit only. It must not affect signals, labels,
  thresholds, horizons, or costs.

Current 12-symbol design:

- Group A: AAPL, MSFT, NVDA, AMZN, META
- Group B: AMD, BAC, C, F
- Group C: XOM, JPM, WMT

Data state:

- Current committed pipeline artifacts only include the AAPL slice.
- The other 11 symbols still need WRDS extraction/data configs/pipeline
  artifacts.

## Last Validation

Command:

```powershell
& "D:\python_library_envs\VHFT_lab\python.exe" -m pytest tests/unit
```

Last known result:

```text
113 passed
```

## Next Actions

1. Continue from the clean worktree, not the old local `master`.
2. Add WRDS extraction/data configs/pipeline artifacts for the remaining 11
   symbols.
3. Preserve v1 baseline behavior.
4. Keep AAPL v2.2 negative baseline fixed; do not retune AAPL.
5. Run `tests/unit` after code or config changes.
