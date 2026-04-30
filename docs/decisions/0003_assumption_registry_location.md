# Decision 0003: Assumption Registry Location

## Status

Accepted.

Recorded: 2026-04-30.

## Context

The project now has enough configurable assumptions that keeping them only in
chat history, code defaults, or stage-specific design notes is risky. The user
requested a centralized assumption / parameter inventory at the Level-I project
root level, parallel to directories such as `configs/`, `docs/`, `src/`, and
`tests/`.

## Decision

Create a root-level `assumptions/` directory and maintain the primary registry
at:

```text
assumptions/parameter_registry.md
```

This top-level location makes assumptions visible next to configs and source
code, rather than burying them in one stage-specific design note.

`docs/decisions/` will still record methodological decisions about the registry
and major assumption-policy changes.

## Consequences

Every meaningful new parameter should be added to the registry in the same task
that introduces it. Stage-specific docs may reference the registry, but they
should not duplicate the full parameter table.

This adds one more top-level directory, but it reduces the chance that
thresholds, horizons, cost assumptions, and unresolved data rules are hidden in
code or chat history.
