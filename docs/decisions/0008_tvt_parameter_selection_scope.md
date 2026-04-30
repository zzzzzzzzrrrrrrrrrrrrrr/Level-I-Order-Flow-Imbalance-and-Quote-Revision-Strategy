# Decision 0008: Train-Validation-Test Parameter Selection Scope

## Status

Accepted.

Recorded: 2026-04-30.

## Context

Parameter sensitivity v1 reports every candidate but does not choose
hyperparameters. The next step needs a selection layer that avoids choosing from
the full sample.

The user explicitly requested the standard training / validation / test
structure: training for model construction, validation for parameter and model
selection, and test for final frozen evaluation.

## Decision

Implement train-validation-test parameter selection v1 with:

- expanding train dates
- next-date validation selection
- next-date test evaluation
- validation-only parameter choice
- no test leakage into candidate selection

For v1, the selected parameters are target-position accounting parameters. No
predictive model is trained yet, so training dates are recorded as split
context rather than used for model fitting.

## Rejected Alternatives

Selecting the best candidate from full-sample sensitivity was rejected because
it leaks test information into the parameter choice.

Using the previous train-test threshold selection pattern for execution
parameters was rejected because it lacks a distinct validation set.

Declaring final hyperparameters from the three-day AAPL demonstration slice was
rejected because the sample is too small for final research claims.

## Consequences

The project can now separate validation choice from test evaluation for
execution/accounting parameters. A later version should extend this same split
discipline to signal thresholds, model selection, larger universes, and longer
walk-forward windows.
