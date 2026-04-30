# Level-I OFI / QR Strategy

Execution-aware research scaffold for studying whether Level-I quote and trade information contains stable short-horizon predictive content in liquid U.S. equities.

## Repository layout

- `AGENTS.md`: project operating rules and research constraints
- `configs/`: data, experiment, backtest, and live-paper configs
- `data/`: local raw, interim, processed, cache, and fixture data
- `docs/`: design notes, decisions, reports, and references
- `notebooks/`: staged exploratory notebooks by workflow step
- `scripts/`: thin entry points that call package code
- `src/level1_ofi_qr/`: package modules for schema, cleaning, features, evaluation, backtesting, execution, and risk
- `tests/`: fixtures plus unit, integration, and regression tests
- `outputs/`: generated experiment, backtest, figure, table, and log artifacts

## Current status

- `AGENTS.md` remains the source of project rules.
- The existing notebook lives at `notebooks/00_data_inspection/experiment.ipynb`.
- Data, outputs, local agent state, and notebook files are ignored by default in `.gitignore`.
- Stage 1 schema, WRDS normalization, cleaning primitives, and raw WRDS extraction scaffolding are implemented with unit tests.
- The current AAPL WRDS slice uses `taqmsec.nbbom_YYYYMMDD` as the main quote source, so normalized quotes represent national BBO state: `bid_exchange`, `ask_exchange`, `bid`, `ask`, `bid_size`, and `ask_size`. Trades use `taqmsec.ctm_YYYYMMDD`.
- Cleaning v2, quote-trade alignment v1, quote feature v1, trade signing v1, signed-flow feature v1, labeling v1, signals v1, walk-forward evaluation v1, and threshold selection v1 are implemented as separate auditable steps. Condition-code eligibility, cost modeling, and backtests are still separate unfinished stages.

## WRDS extraction

Preview generated SQL without connecting to WRDS:

```powershell
D:\python_library_envs\VHFT_lab\python.exe scripts\extract_wrds.py configs\data\aapl_wrds_20260408_20260410.yaml --dry-run-sql --limit-per-query 5
```

Run extraction after WRDS credentials are available through WRDS configuration or `WRDS_USERNAME`:

```powershell
D:\python_library_envs\VHFT_lab\python.exe scripts\extract_wrds.py configs\data\aapl_wrds_20260408_20260410.yaml
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
D:\python_library_envs\VHFT_lab\python.exe scripts\extract_wrds.py configs\data\aapl_wrds_20260408_20260410.yaml --connection-test
```

Check which credentials source the script sees:

```powershell
D:\python_library_envs\VHFT_lab\python.exe scripts\extract_wrds.py configs\data\aapl_wrds_20260408_20260410.yaml --show-credential-sources --dry-run-sql
```

List the latest available daily TAQ tables before choosing config dates:

```powershell
D:\python_library_envs\VHFT_lab\python.exe scripts\extract_wrds.py configs\data\aapl_wrds_20260408_20260410.yaml --list-tables --table-list-limit 10
```

For the full AAPL slice, omit `--limit-per-query` and write raw outputs to a slice-named folder:

```powershell
D:\python_library_envs\VHFT_lab\python.exe scripts\extract_wrds.py configs\data\aapl_wrds_20260408_20260410.yaml --output-dir data\raw\aapl_wrds_20260408_20260410
```

## Dataset build

Build normalized and cleaned datasets from raw WRDS CSV outputs:

```powershell
D:\python_library_envs\VHFT_lab\python.exe scripts\build_dataset.py configs\data\aapl_wrds_20260408_20260410.yaml
```

The build writes normalized files under `data/interim/<slice_name>/`; cleaned files, rejected-row audit files, and a diagnostics manifest under `data/processed/<slice_name>/`. If you are building from the small validation extract instead of the full slice, pass `--raw-dir data\raw\validation_structured_contract`.

## Quote-Trade Alignment

Align cleaned trades to the latest cleaned quote strictly before each trade:

```powershell
D:\python_library_envs\VHFT_lab\python.exe scripts\align_trades.py configs\data\aapl_wrds_20260408_20260410.yaml
```

The alignment output is written under `data/processed/<slice_name>/` as `*_trades_aligned_quotes.csv` plus `*_alignment_manifest.json`. Matches are constrained to the same `symbol` and `trading_date`.

This alignment version only performs backward quote-trade matching within symbol and trading_date groups. It does not perform trade signing, sale-condition filtering, correction filtering, or final research-sample cleaning.

Run tolerance-sensitivity diagnostics without selecting a final tolerance:

```powershell
D:\python_library_envs\VHFT_lab\python.exe scripts\align_trades.py configs\data\aapl_wrds_20260408_20260410.yaml --tolerance-sensitivity
```

## Quote Features

Build quote-only Level-I features from cleaned quotes:

```powershell
D:\python_library_envs\VHFT_lab\python.exe scripts\build_quote_features.py configs\data\aapl_wrds_20260408_20260410.yaml
```

Quote feature v1 computes row-preserving, quote-only features from cleaned Level-I quotes. It supports spread, depth, signed top-of-book imbalance, quote revision, and quote event interval diagnostics. It does not compute trade signing, signed order flow imbalance, labels, or backtest signals.

## Trade Signing

Build trade signing v1 output from aligned trades:

```powershell
D:\python_library_envs\VHFT_lab\python.exe scripts\sign_trades.py configs\data\aapl_wrds_20260408_20260410.yaml
```

Trade signing v1 uses quote rule with tick-rule fallback on aligned trade rows. It preserves all aligned trades and writes `*_trades_signed_v1.csv` plus `*_trade_signing_v1_manifest.json`. It does not apply sale-condition filters, NBBO condition filters, OFI aggregation, labels, or backtest signals.

## Signed-Flow Features

Build signed-flow v1 features from signed trades:

```powershell
D:\python_library_envs\VHFT_lab\python.exe scripts\build_signed_flow_features.py configs\data\aapl_wrds_20260408_20260410.yaml
```

Signed-flow feature v1 computes row-preserving trade-flow features over trailing event-count and clock-time windows within `symbol` and `trading_date`. Windows include the current trade print; unknown-sign trades contribute zero signed flow while remaining in total trade volume. It does not apply condition-code filters, construct labels, or run backtests.

## Labels

Build future midquote labels for signed-flow feature rows:

```powershell
D:\python_library_envs\VHFT_lab\python.exe scripts\build_labels.py configs\data\aapl_wrds_20260408_20260410.yaml
```

Labeling v1 creates future midquote return and direction targets from quote feature data. Labels are computed strictly after `decision_time` and must not be used as features. This step does not create trading signals, run walk-forward evaluation, or run backtests.

## Signals

Build sequential-gate signal v1 rows:

```powershell
D:\python_library_envs\VHFT_lab\python.exe scripts\build_signals.py configs\data\aapl_wrds_20260408_20260410.yaml
```

Signals v1 builds an interpretable sequential gate from current quote imbalance, signed-flow imbalance, and quote revision. Default thresholds are diagnostic sign-agreement defaults, not optimized trading thresholds. Labels may be retained in the output for later evaluation, but labels are not used to compute signals. This step does not run walk-forward evaluation or backtests.

## Walk-Forward Evaluation

Run walk-forward statistical evaluation for signal v1 rows:

```powershell
D:\python_library_envs\VHFT_lab\python.exe scripts\run_walk_forward.py configs\data\aapl_wrds_20260408_20260410.yaml
```

Walk-forward evaluation v1 evaluates precomputed signals against future midquote labels using expanding training-date context and next-date test folds. It does not optimize thresholds, fit models, apply transaction costs, or run backtests.

## Threshold Selection

Run training-window threshold selection for sequential-gate signals:

```powershell
D:\python_library_envs\VHFT_lab\python.exe scripts\run_threshold_selection.py configs\data\aapl_wrds_20260408_20260410.yaml
```

Threshold selection v1 selects QI, signed-flow, and QR thresholds inside each walk-forward training window, then evaluates the selected thresholds on the next test date. It does not fit predictive models, apply transaction costs, or run backtests.
