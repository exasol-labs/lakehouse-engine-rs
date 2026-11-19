# Decisions: fix-scan-empty-string-as-null

## ADR: Empty Strings Read as NULL at Their Two Producers

**ID:** empty-strings-read-as-null-at-their-two-producers
**Plan:** fix-scan-empty-string-as-null
**Status:** Accepted

### Context

Exasol has no empty string, and DataFusion keeps `''` as a value, so every expression the scan computes over one returns rows or values that native Exasol does not. An empty string enters the scan only from a string column of a data file or from a text-returning function.

### Decision

The scan translator reads an empty string as NULL at those two producers: a string column used inside an expression and the result of a text-returning function each render as `nullif(x, '')`. A comparison or null test over a bare string column in a filter keeps the bare column and adds an empty-string condition where one is needed.

### Options Considered

| Option | Verdict |
|--------|---------|
| Normalize at the two producers in the translator | ✓ Chosen — a filter over a bare string column still skips row groups by statistics |
| Normalize every string column in the scan's base select | ✗ Rejected — every string filter loses row-group pruning, because statistics pruning does not read through `nullif` |

### Consequences

- A new text-returning function is declared as one in the function table, and its empty result then reads as NULL with no further code.
- A site that passes a string column to the scan outside the translator, such as an aggregate argument, renders the column through the translator.
