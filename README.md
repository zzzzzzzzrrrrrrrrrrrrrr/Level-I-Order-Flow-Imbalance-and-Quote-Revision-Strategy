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
