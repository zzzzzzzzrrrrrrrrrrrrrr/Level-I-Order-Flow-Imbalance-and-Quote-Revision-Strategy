# Assumption and Parameter Registry

## Purpose

This registry tracks project assumptions, subjective research parameters,
external data rules, and cost/backtest parameters before they enter code or
reports.

Parameter types:

- `research_design`: chosen by the project and must be explained.
- `data_rule`: must be checked against WRDS / TAQ / market documentation.
- `cost_rule`: must be checked against official fee schedules or clearly marked
  as a stress-test assumption.
- `diagnostic_default`: useful for pipeline validation but not a final
  research setting.

Sensitivity status:

- `required`: must be stress-tested or varied before final claims.
- `optional`: useful robustness check but not blocking for the current stage.
- `not_applicable`: deterministic rule or source mapping.

## Current Scope

Current slice:

```text
symbol = AAPL
config = configs/data/aapl_wrds_20260313_20260410.yaml
trading_dates = 20 regular-session dates from 2026-03-13 through 2026-04-10
excluded_date = 2026-04-03
data_source = WRDS TAQ
quote_table = taqmsec.nbbom_YYYYMMDD
trade_table = taqmsec.ctm_YYYYMMDD
```

This is a 20-day single-symbol AAPL prototype slice. It is not evidence that a
U.S. equity Level-I strategy is broadly validated. The earlier
`aapl_wrds_20260408_20260410` slice remains a small validation artifact only.

Current 20-day run result:

```text
raw quotes = 21,290,182
raw trades = 12,141,562
signal rows = 12,141,453
held-out gross before spread cost = positive on 18/18 test days
held-out net PnL after spread cost = negative on 18/18 test days
```

The positive gross edge is a research lead, not a profitable strategy claim.

## Phase-1 Liquidity-Regime Screen Assumptions

The next cross-symbol diagnostic experiment is additive and does not replace
the AAPL negative benchmark:

```text
experiment = v22_symbol_screen_phase1_by_liquidity_regime_same_20d
config = configs/experiments/v22_symbol_screen_phase1_by_liquidity_regime_same_20d.yaml
research_status = diagnostic_only
date_window = 2026-03-13 through 2026-04-10, same 20 configured trading dates
session = regular_market_hours
```

Ex-ante liquidity-regime groups:

```text
Group A = AAPL, MSFT, NVDA, AMZN, META
Group B = AMD, BAC, C, F
Group C = XOM, JPM, WMT
```

Group definitions are research-design assumptions, not results. Group metadata
may be used for reporting, aggregation, diagnostics, figure names, table names,
notes, and report sections only. It must not alter signal generation, labeling,
threshold selection, horizon selection, cost accounting, or pass/fail criteria.

Same-window claims must be verified from the run manifest. The manifest should
record actual per-symbol trading dates, same start/end checks, same
trading-date-list checks, same session-filter checks, row counts when
available, and missing trading dates. Results such as one group outperforming
another belong in reports, not in this assumptions registry.

## Data Extraction Assumptions

| Parameter | Current value | Type | Source / rationale | Sensitivity required | Status |
| --- | --- | --- | --- | --- | --- |
| symbol universe | `AAPL` | research_design | liquid large-cap demonstration slice | required | provisional |
| trading dates | `2026-03-13` to `2026-04-10`, excluding `2026-04-03` | research_design | larger single-symbol prototype before adding ML complexity | required | implemented_run_complete |
| validation slice dates | `2026-04-08` to `2026-04-10` | research_design | earlier smoke / validation extract | optional | archived_validation_slice |
| quote table | `taqmsec.nbbom_YYYYMMDD` | data_rule | national BBO quote state needed for midquote/QI/QR | not_applicable | implemented |
| trade table | `taqmsec.ctm_YYYYMMDD` | data_rule | consolidated trade messages | not_applicable | implemented |
| session | regular market hours | research_design / market_rule | current pipeline uses configured regular session | required | implemented |
| timezone | `America/New_York` | data_rule | U.S. equity market timestamp convention | not_applicable | implemented |
| phase-1 liquidity-regime universe | `AAPL`, `MSFT`, `NVDA`, `AMZN`, `META`, `AMD`, `BAC`, `C`, `F`, `XOM`, `JPM`, `WMT` | research_design | ex-ante liquidity-regime diagnostic groups; AAPL remains known negative control | required | configured_pending_extraction |
| phase-1 date-window control | same 20 configured trading dates from `2026-03-13` through `2026-04-10` | research_design | controls date, volatility-regime, and news-cycle effects across symbols | required | configured_pending_manifest_verification |
| phase-1 group metadata policy | reporting-only metadata | research_design | prevents liquidity-regime design from becoming implicit group-specific tuning | not_applicable | implemented_guardrail |
| phase-1 test selection policy | no test-set symbol or parameter selection | methodology | preserves validation-only screening discipline | not_applicable | configured |

## WRDS / TAQ Field Assumptions

| Parameter | Current value | Type | Source / rationale | Sensitivity required | Status |
| --- | --- | --- | --- | --- | --- |
| NBBOM bid exchange field | `best_bidex` | data_rule | WRDS table metadata and field validation | not_applicable | implemented |
| NBBOM ask exchange field | `best_askex` | data_rule | WRDS table metadata and field validation | not_applicable | implemented |
| NBBOM bid/ask fields | `best_bid`, `best_ask` | data_rule | WRDS table metadata and field validation | not_applicable | implemented |
| NBBOM size fields | `best_bidsiz`, `best_asksiz` | data_rule | WRDS field names confirmed, unit not independently finalized | optional | unresolved_unit |
| NBBOM quote condition | `nbbo_qu_cond` | data_rule | distribution recorded, filter not finalized | required | unresolved_filter |
| CTM sale condition | `tr_scond` | data_rule | distribution recorded, filter not finalized | required | unresolved_filter |
| CTM trade correction | `tr_corr` | data_rule | keep normalized `0` / `00` only | optional | implemented_needs_source_note |

## Cleaning Assumptions

| Parameter / rule | Current value | Type | Source / rationale | Sensitivity required | Status |
| --- | --- | --- | --- | --- | --- |
| quote non-positive price policy | drop `bid <= 0` or `ask <= 0` | data_rule | invalid quote state | not_applicable | implemented |
| quote negative depth policy | drop negative bid/ask size | data_rule | invalid displayed size | not_applicable | implemented |
| crossed quote policy | drop `ask < bid` | research_design | invalid top-of-book state for v1 | required | implemented |
| locked quote policy | retain and count `ask == bid` | research_design | locked markets are not crossed; retained for v1 | required | implemented |
| quote condition filter | diagnostic only | data_rule | eligible code policy not finalized | required | unresolved |
| sale condition filter | diagnostic only | data_rule | eligible code policy not finalized | required | unresolved |
| duplicate policy | not final | research_design | sequence/timestamp duplicate handling not implemented as final filter | required | unresolved |
| abnormal spread threshold | not set | research_design | should be sensitivity-tested, not hard-coded | required | unresolved |

## Alignment Assumptions

| Parameter | Current value | Type | Source / rationale | Sensitivity required | Status |
| --- | --- | --- | --- | --- | --- |
| quote match direction | latest quote strictly before trade | research_design | avoid future quote leakage | required | implemented |
| exact timestamp matches | `allow_exact_matches = false` | research_design | conservative timestamp-tie policy | required | implemented |
| session boundary | same `symbol` and `trading_date` | research_design | prevents prior-day quote carryover | not_applicable | implemented |
| quote lag tolerance | diagnostic sensitivity only | research_design | stale quote control not finalized | required | unresolved_final_choice |
| unmatched trades | retained with null quote fields | research_design | preserve row auditability | optional | implemented |

Current tolerance sensitivity candidates:

```text
None, 5s, 1s, 500ms, 100ms
```

## Trade Signing Assumptions

| Parameter | Current value | Type | Source / rationale | Sensitivity required | Status |
| --- | --- | --- | --- | --- | --- |
| primary signing method | quote rule | research_design | use matched quote midpoint when available | required | implemented |
| fallback signing method | tick rule | research_design | fallback for midpoint ties and unmatched quote-rule cases | required | implemented |
| midpoint trade policy | tick-rule fallback | research_design | midpoint trades unresolved by quote rule | required | implemented |
| unknown sign policy | retain as `trade_sign = 0` | research_design | preserve rows; exclude from signed volume direction | required | implemented |
| Lee-Ready-style comparison | not implemented | research_design | required robustness before final claims | required | unresolved |
| sale-condition-aware signing | not implemented | data_rule | blocked by sale condition eligibility policy | required | unresolved |

## Feature Assumptions

| Parameter | Current value | Type | Source / rationale | Sensitivity required | Status |
| --- | --- | --- | --- | --- | --- |
| quote feature grouping | `symbol`, `trading_date` | research_design | avoid cross-session quote revision | not_applicable | implemented |
| quote imbalance | `(bid_size - ask_size) / (bid_size + ask_size)` | methodology | Level-I top-of-book imbalance | optional | implemented |
| quote revision | current midquote minus previous midquote | methodology | quote-side confirmation | optional | implemented |
| signed-flow event windows | `10`, `50`, `100` trades | research_design | short-horizon trade-flow pressure | required | implemented |
| signed-flow time windows | `100ms`, `500ms`, `1s` | research_design | short-horizon trade-flow pressure | required | implemented |
| signed-flow window inclusion | include current trade | research_design | pressure state after observing current print | required | implemented |
| unknown sign in signed-flow | zero signed flow, included in total volume | research_design | denominator retains observed trade volume | required | implemented |

## Label Assumptions

| Parameter | Current value | Type | Source / rationale | Sensitivity required | Status |
| --- | --- | --- | --- | --- | --- |
| label price | midquote | methodology | reduces bid-ask bounce relative to trade price | optional | implemented |
| current quote policy | latest quote at or before decision time | methodology | no future quote leakage | not_applicable | implemented |
| future quote policy | first quote at or after decision time + horizon | methodology | future target construction | optional | implemented |
| label horizons | `100ms`, `500ms`, `1s`, `5s` | research_design | short-horizon prediction targets | required | implemented |
| dead zone | `0.0 bps` | diagnostic_default | initial sign labels without no-move band | required | provisional |
| microprice label comparison | not implemented | research_design | optional alternative target | optional | unresolved |

## Signal and Threshold Assumptions

| Parameter | Current value | Type | Source / rationale | Sensitivity required | Status |
| --- | --- | --- | --- | --- | --- |
| signal form | sequential gate QI + signed-flow + QR | research_design | interpretable baseline | required | implemented |
| diagnostic signal thresholds | all `0.0` | diagnostic_default | sign-agreement pipeline check | required | superseded_by_threshold_selection |
| threshold grid QI | `0.0`, `0.1`, `0.25` | research_design | small first-pass grid | required | implemented |
| threshold grid signed-flow | `0.0`, `0.1`, `0.25` | research_design | small first-pass grid | required | implemented |
| threshold grid QR bps | `0.0`, `0.1`, `0.25` | research_design | small first-pass grid | required | implemented |
| threshold objective | maximize train mean signal-aligned return bps | research_design | directional edge proxy, not PnL | required | implemented |
| minimum train signals | `100` | research_design | avoid sparse lucky thresholds | required | implemented |
| tie-break policy | objective, accuracy, count, then larger threshold sum | research_design | prefer more selective threshold when tied | required | implemented |

## Evaluation Assumptions

| Parameter | Current value | Type | Source / rationale | Sensitivity required | Status |
| --- | --- | --- | --- | --- | --- |
| walk-forward split | expanding train dates, next-date test | methodology | avoid random split leakage | not_applicable | implemented |
| min train dates | `1` | research_design | prototype expanding-window smoke setting | required | provisional |
| metric: signal accuracy | reported | methodology | direction agreement diagnostic | optional | implemented |
| metric: signal-aligned return bps | reported | methodology | statistical edge proxy, not PnL | optional | implemented |
| cost-adjusted evaluation | cost model and accounting layers | cost_rule | current 20-day run is net negative after spread costs | required | implemented_needs_optimization |

## Cost Model Assumptions

Cost model v1 is a diagnostic cost layer, not a backtest.

| Parameter | Current value | Type | Source / rationale | Sensitivity required | Status |
| --- | --- | --- | --- | --- | --- |
| half-spread cost | data-derived | cost_rule | aggressive one-way crossing proxy | required | implemented |
| full-spread round trip | data-derived | cost_rule | aggressive entry + exit proxy | required | implemented |
| fixed bps stress grid | `0`, `0.5`, `1`, `2`, `5` bps | cost_rule / stress_test | conservative cost stress, not an official fee schedule | required | implemented |
| slippage ticks grid | `0`, `0.5`, `1` ticks | cost_rule / stress_test | latency/adverse-selection proxy, not a latency model | required | implemented |
| tick size proxy | `$0.01` | cost_rule / stress_test | U.S. equity penny tick proxy for v1 diagnostics | required | provisional |
| broker commission | TBD | cost_rule | broker official schedule if used | required | unresolved |
| SEC / FINRA fees | TBD | cost_rule | official fee schedules if used | required | unresolved |
| exchange fee / rebate | TBD | cost_rule | venue-specific, not reliable without routing model | optional | unresolved |

## Execution Accounting Assumptions

Execution accounting v1 is an account-mechanics scaffold, not a research-grade
backtest.

| Parameter | Current value | Type | Source / rationale | Sensitivity required | Status |
| --- | --- | --- | --- | --- | --- |
| accounting policy | independent fixed-horizon round trips | research_design | simple account reconciliation before strategy backtest | required | implemented_scaffold |
| signal source | `sequential_gate_signal` | research_design | current interpretable baseline signal | required | implemented |
| quantity | `1` share | research_design | unit-size accounting check | required | implemented |
| entry price proxy | signal midquote plus cost deduction | research_design | separates price move from cost deduction | required | implemented_scaffold |
| exit price proxy | future midquote at label horizon | research_design | uses existing label horizon target | required | implemented_scaffold |
| exit spread proxy | entry spread reused for exit half-spread | research_design | future spread not carried in signal rows | required | provisional |
| fixed bps | `0.0` by default | cost_rule / stress_test | default base scenario; CLI parameter for stress | required | implemented |
| slippage ticks | `0.0` by default | cost_rule / stress_test | default base scenario; CLI parameter for stress | required | implemented |
| position limit | none | research_design | scaffold allows overlapping independent round trips | required | unresolved |
| risk controls | not implemented | research_design | must be explicit before final backtest | required | unresolved |
| parameter optimization | not implemented | research_design | avoid data-snooping before accounting is fixed | required | unresolved |

## Target-Position Accounting Assumptions

Target-position accounting v1 is the first bounded account-state scaffold. It
is not hyperparameter selection.

| Parameter | Current value | Type | Source / rationale | Sensitivity required | Status |
| --- | --- | --- | --- | --- | --- |
| target mapping | `+1 -> long`, `-1 -> short`, `0 -> flat` | research_design | transparent signal-to-position baseline | required | implemented_scaffold |
| max position | `1` share | research_design | bounded unit-size account state | required | implemented |
| flat on no signal | `true` | research_design | avoid holding exposure without an active gate | required | implemented |
| EOD flat | `true` | research_design | avoid overnight inventory in intraday scaffold | required | implemented |
| cooldown | `0ms` | research_design | baseline no-delay control; CLI parameter for sensitivity | required | implemented |
| max trades per day | none | research_design | optional control, not selected yet | required | implemented_optional |
| fill proxy | signal midquote | research_design | accounting scaffold separates price state from cost deduction | required | implemented_scaffold |
| cost deduction | half spread + fixed bps + slippage ticks | cost_rule / stress_test | aligned with cost model v1 | required | implemented |
| parameter optimization | not implemented | research_design | must be train-window selected later | required | unresolved |

## Parameter Sensitivity Assumptions

Parameter sensitivity v1 reports candidate grids but does not choose final
hyperparameters.

| Parameter | Current value | Type | Source / rationale | Sensitivity required | Status |
| --- | --- | --- | --- | --- | --- |
| sensitivity policy | exhaustive grid report, no selection | research_design | avoids manual cherry-picking | not_applicable | implemented |
| default max position grid | `1` | research_design | bounded smoke-check grid | required | implemented |
| default cooldown grid | `0ms` | research_design | baseline no-delay scaffold check | required | implemented |
| default max trades per day grid | none | research_design | baseline unrestricted daily trade count | required | implemented |
| default fixed bps grid | `0`, `1` | cost_rule / stress_test | small base/stress cost comparison | required | implemented |
| default slippage ticks grid | `0` | cost_rule / stress_test | baseline no extra slippage in smoke grid | required | implemented |
| final parameter selection | not implemented | research_design | must be train-window selected later | required | unresolved |

## Train-Validation-Test Selection Assumptions

TVT parameter selection v1 chooses accounting parameters on validation dates
and evaluates frozen choices on test dates.

| Parameter | Current value | Type | Source / rationale | Sensitivity required | Status |
| --- | --- | --- | --- | --- | --- |
| split policy | expanding train, next validation, next test | methodology | separates selection and final evaluation dates | not_applicable | implemented |
| min train dates | `1` | research_design | prototype expanding-window smoke setting | required | provisional |
| validation objective | maximize validation final equity | research_design | simple account-level objective for v1 | required | implemented |
| tie-break policy | higher validation equity, lower cost, lower order count | research_design | deterministic selection among equal candidates | required | implemented |
| test leakage policy | test date not used for selection | methodology | avoids full-sample parameter choice | not_applicable | implemented |
| model training | not implemented | research_design | v1 selects accounting parameters only | required | unresolved |
| final hyperparameter claim | false | research_design | AAPL 20-day single-symbol slice is not final evidence | not_applicable | implemented |

## Model Training Assumptions

Model training v1 is scoped to the current AAPL prototype. It is not a
generalized multi-symbol model claim.

| Parameter | Current value | Type | Source / rationale | Sensitivity required | Status |
| --- | --- | --- | --- | --- | --- |
| model form | standardized linear feature score | research_design | no extra ML dependency; auditable baseline | required | implemented |
| split policy | expanding train, next validation, next test | methodology | 18 held-out model folds on current 20-day slice | not_applicable | implemented |
| label horizon | `500ms` | research_design | first model prototype target | required | implemented |
| feature-set candidates | `qi_qr_flow_500ms`, `qi_qr_flow_multiwindow` | research_design | QI / QR / signed-flow baseline feature families | required | implemented |
| score threshold grid | `0`, `0.10`, `0.25`, `0.50`, `0.75`, `1.0`, `1.25`, `1.50`, `2.0` | research_design | validation-selected model signal threshold | required | implemented |
| minimum validation orders | `1000` | research_design | prevents no-trade candidates from winning by avoiding losses | required | implemented |
| accounting engine | target-position accounting v1 | research_design | bounded test-date account state | required | implemented |
| final model claim | false | research_design | AAPL 20-day single-symbol prototype only | not_applicable | implemented |

## Backtest Assumptions

Backtest v1 evaluates frozen TVT-selected target-position accounting candidates
on held-out test dates. It is not a research-grade profitability test.

| Parameter | Current value | Type | Source / rationale | Sensitivity required | Status |
| --- | --- | --- | --- | --- | --- |
| parameter source | TVT-selected candidate | methodology | validation-selected parameters frozen before test | not_applicable | implemented |
| evaluation sample | selected fold test date only | methodology | prevents test data from selecting parameters | not_applicable | implemented |
| accounting engine | target-position accounting v1 | research_design | bounded position account scaffold | required | implemented_scaffold |
| execution price proxy | signal midquote with cost deduction | research_design | inherited from target-position accounting v1 | required | implemented_scaffold |
| explicit latency | none | research_design | latency sensitivity still required | required | unresolved |
| passive fills / queueing | not implemented | research_design | Level-I data cannot support queue simulation directly | required | unresolved |
| official fee schedules | not implemented | cost_rule | broker / SEC / FINRA / exchange fees not modeled | required | unresolved |
| research-grade backtest | false | research_design | broader samples and stronger execution model needed | not_applicable | implemented_status_flag |

## Items Requiring Official Verification

Before final research claims:

- WRDS TAQ NBBOM field definitions and size units.
- WRDS / TAQ NBBO quote condition codes and eligibility.
- WRDS / TAQ CTM sale condition codes and eligibility.
- WRDS / TAQ trade correction semantics.
- Timestamp precision and timezone conventions.
- SEC / FINRA / broker fee schedules if explicit fee modeling is used.

## Maintenance Rule

When a parameter is added to configs, code, or reports, update this registry in
the same task. If a parameter is intentionally omitted from this registry,
state why in the task summary or commit message.
