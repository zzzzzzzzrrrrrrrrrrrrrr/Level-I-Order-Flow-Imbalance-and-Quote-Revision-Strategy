# Signals v1

## Purpose

Signals v1 builds an interpretable sequential gate from the three current core
feature families:

- QI: signed top-of-book quote imbalance
- signed-flow: recent signed aggressive trade pressure
- QR: quote-side midquote revision

This layer turns features into directional views. It does not evaluate those
views statistically or economically.

## Scope

Signals v1 builds an interpretable sequential gate from current quote imbalance,
signed-flow imbalance, and quote revision. Default thresholds are diagnostic
sign-agreement defaults, not optimized trading thresholds. Labels may be
retained in the output for later evaluation, but labels are not used to compute
signals. This step does not run walk-forward evaluation or backtests.

## Inputs

- `*_labeled_features_v1.csv`
- `*_quote_features_v1.csv`

Quote state is matched at each feature row using:

```text
signal_session_policy = same_symbol_same_trading_date_quote_state
```

Signal quote features use the latest quote feature row at or before the feature
row timestamp within the same `symbol` and `trading_date`.

## Default Rule

Default signed-flow input:

```text
signed_flow_imbalance_500ms
```

Default thresholds:

```text
qi_threshold = 0.0
signed_flow_threshold = 0.0
qr_threshold_bps = 0.0
threshold_selection_policy = diagnostic_defaults_not_optimized
```

The component rules are:

```text
qi_signal = sign(signal_quote_imbalance) subject to qi_threshold
signed_flow_signal = sign(signed_flow_imbalance_500ms) subject to signed_flow_threshold
qr_signal = sign(signal_quote_revision_bps) subject to qr_threshold_bps
```

The sequential gate emits:

```text
+1 if QI, signed-flow, and QR are all positive
-1 if QI, signed-flow, and QR are all negative
 0 otherwise
```

## Label Policy

Labels are retained only for later evaluation:

```text
label_usage_policy = labels_retained_for_evaluation_not_used_for_signal
labels_used_for_signal = false
```

## Outputs

- `*_signals_v1.csv`
- `*_signals_v1_manifest.json`

## Current AAPL Slice

For `aapl_wrds_20260408_20260410`, signals v1 currently produces:

- input feature rows: `1,648,869`
- input quote rows: `2,029,892`
- output signal rows: `1,648,869`
- signed-flow column: `signed_flow_imbalance_500ms`
- thresholds: `0.0` for QI, signed-flow, and QR bps
- signal input available rows: `1,648,777`
- signal input missing rows: `92`
- long signal rows: `25,080`
- short signal rows: `29,630`
- no-trade rows: `1,594,159`

These counts use diagnostic sign-agreement thresholds. They should not be
presented as optimized or validated trading rules.

This output is not a research-grade strategy result. Walk-forward evaluation,
threshold selection, cost modeling, and backtesting remain separate steps.
