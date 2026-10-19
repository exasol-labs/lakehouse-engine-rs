# Decisions: add-pushdown-capability-gaps

## ADR: Exclude FN_TO_CHAR and FN_TO_NUMBER — Format-Model Incompatibility

**ID:** exclude-fn-to-char-and-fn-to-number-format-model-incompatibility
**Plan:** `add-pushdown-capability-gaps`
**Status:** Accepted

### Context

DataFusion `to_char` uses strftime masks, not Exasol's Oracle-style format models, and rejects numeric formatting. DataFusion has no `to_number`. Capability advertisement is per function, so partial support cannot be expressed.

### Decision

The engine does not advertise `FN_TO_CHAR` or `FN_TO_NUMBER`. String-to-number conversion without a format model stays reachable through `FN_CAST`.

### Options Considered

| Option | Verdict |
|--------|---------|
| Advertise the no-format case only | Rejected: Exasol would still push format-argument variants the translator cannot render |

### Consequences

`TO_CHAR` and `TO_NUMBER` always evaluate in Exasol.

