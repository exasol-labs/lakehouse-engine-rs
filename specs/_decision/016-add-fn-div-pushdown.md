# Decisions: add-fn-div-pushdown

## ADR: Correct the FN_DIV Decline Rationale to Verified Truncation Semantics

**ID:** exclude-fn-div-no-faithful-datafusion-truncated-division
**Plan:** `add-fn-div-pushdown`
**Status:** Accepted
**Supersedes:** exclude-fn-div-no-faithful-datafusion-floor-division

### Context

Live Exasol verification shows `DIV` truncates toward zero, as DataFusion integer `/` does, so the earlier floor-division rationale is wrong. DataFusion has no `div` builtin, and a `TRUNC(m/n)` emulation diverges on DOUBLE division by zero: Exasol raises an error, DataFusion yields infinity. The expression node does not carry operand types, so the translator cannot render only the safe integer case.

### Decision

`FN_DIV` stays unadvertised. The translator declines `DIV` and Exasol evaluates it. The outcome is unchanged from the superseded ADR, and only the reason is corrected.

### Options Considered

| Option | Verdict |
|--------|---------|
| Advertise `FN_DIV` via `TRUNC(m/n)` | Rejected: DOUBLE division by zero diverges, and operand types are unavailable to restrict it to integers |
| Close issue #105 with no spec change | Rejected: the spec would keep a refuted premise and invite a future `FLOOR`-based fix that also diverges |

### Consequences

`DIV` never pushes down.
