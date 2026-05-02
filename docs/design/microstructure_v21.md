# Microstructure V2.1 Passive/Hybrid Diagnostics

## Scope

Microstructure v2.1 is an independent diagnostic path. It is not a replacement for v1 baselines, and it does not alter `sequential_gate`, `linear_score`, or the existing `cost_aware_linear_score` selection logic.

The research question is whether a wider Level-I candidate pool can survive conservative passive or hybrid execution assumptions after submitted-order accounting, fill-rate sensitivity, cancellation, and chronological validation selection.

## Inputs

Required processed inputs for a slice:

- `*_cost_aware_linear_score_predictions.csv`
- `*_quote_features_v1.csv`
- `*_trades_signed_v1.csv`

The prediction file supplies existing model score, selected threshold, estimated cost, fold, symbol, trading date, and event timestamp fields. Quote features supply bid, ask, sizes, midquote, spread, quote revision, and event time. Signed trades supply trade timestamps and prices for conservative passive-fill evidence.

Non-finite model scores are treated as no-side candidate rows inside v2.1 candidate construction. This protects diagnostics from bad numeric rows without changing upstream prediction output.

## Module Responsibilities

- `config.py`: grid definitions and diagnostic assumptions.
- `candidate_pool.py`: microprice, quote-state attachment, candidate-pool masks, and edge-threshold masks.
- `execution_selector.py`: converts a candidate into `market_entry`, `passive_entry`, or `no_trade`.
- `passive_fill.py`: strict post-submission passive-fill evidence using quote/trade arrays.
- `cancellation.py`: TTL, microprice flip, quote-revision flip, spread-widening, and volatility-spike cancellation.
- `metrics.py`: submitted-order and filled-order reporting.
- `workflow.py`: input loading, candidate construction, variant evaluation, validation selection, and artifact writing.

Scripts remain thin entry points. `scripts/run_microstructure_v21_diagnostics.py` parses config and grid arguments, then calls package workflow code.

## Execution Selector

For each candidate:

```text
if predicted_edge_bps > full_market_cost_bps + adverse_selection_buffer_bps + safety_margin_bps:
    execution_mode = market_entry
elif predicted_edge_bps - expected_cost_bps > 0 and passive state passes:
    execution_mode = passive_entry
else:
    execution_mode = no_trade
```

The default adverse-selection buffer is `0.5` bps. This keeps weak positive edge from automatically becoming market entry and preserves the v2.1 purpose: frequency expansion through passive/hybrid order management, not more aggressive market crossing.

## Passive Fill Rules

Passive fills are leakage-sensitive:

- no same-timestamp fill
- fill evidence must occur strictly after order submission
- unfilled passive orders create zero realized PnL
- submitted-order and filled-order metrics are reported separately
- queue-haircut assumptions are explicit

The diagnostic cannot infer true queue position, hidden liquidity, cancellation priority, or venue-specific matching priority from Level-I data. Therefore v2.1 passive fills are research diagnostics, not production fill claims.

## Validation

Variant selection is chronological. For each test date, v2.1 selects from prior validation dates only. The final test date is not used for parameter selection, and output manifests record `test_used_for_selection=False`.

The full default grid is supported by configuration, but the current implementation repeats passive-fill simulation across many variants. On the full AAPL slice this is computationally heavy. A focused single-variant full-date run has completed; full-grid selection should be run only after optimizing repeated fill simulation.

## Engineering Optimization

Variant evaluation groups work by execution-simulation keys:

```text
(microprice_usage, ttl, queue_haircut, execution_variant)
```

Candidate-pool and edge-threshold membership are computed once as boolean masks. The workflow then simulates the fill / cancellation / exit path once for each execution-simulation key and expands the resulting order record to the matching candidate-pool and edge-threshold variants.

This preserves the configured grid and does not shrink the data slice. It reduces repeated simulation for variants that differ only by inclusion masks, but full-grid evaluation can still be large because distinct TTL, queue-haircut, microprice, and exit policies require separate execution simulation.

## Outputs

V2.1 writes:

- candidate events
- order-level diagnostics
- variant daily metrics
- variant summary metrics
- chronological validation selection
- selected-test metrics
- manifest

Generated files use the `*_microstructure_v21_*` stem under the slice's processed-data folder.
