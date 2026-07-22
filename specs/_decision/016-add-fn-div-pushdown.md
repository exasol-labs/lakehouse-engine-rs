# Decisions: add-fn-div-pushdown

## ADR: Correct the FN_DIV Decline Rationale to Verified Truncation Semantics

**ID:** exclude-fn-div-no-faithful-datafusion-truncated-division
**Plan:** `add-fn-div-pushdown`
**Status:** Accepted
**Supersedes:** exclude-fn-div-no-faithful-datafusion-floor-division

### Context

ADR `exclude-fn-div-no-faithful-datafusion-floor-division` declined `FN_DIV` on the premise that
Exasol `DIV` is floor division, diverging from DataFusion `/` truncation on negative operands. Live
Exasol verification (2026-07-22) refutes that premise: `DIV(-7, 2) = -3` and `DIV(7, -2) = -3`
(truncation toward zero, not the `-4` floor division would give), `DIV(15.7, 6.2) = 2` (`TRUNC(m/n)`
applies to decimal operands too), and `DIV(5, 0)` raises SQL state 22012. DataFusion 54 integer `/`
also truncates toward zero (docs verified), so the two already match for integer operands — the
divergence the superseded ADR cited does not exist. DataFusion 54 still has no `div` builtin, and a
`TRUNC(m/n)` emulation diverges from Exasol on DOUBLE-operand division by zero: Exasol raises
22012, DataFusion float division yields infinity. Unlike CAST, whose explicit `dataType` field lets
`render_cast_target` decline only the unsupported target subset while still advertising `FN_CAST`,
`DIV`'s operand types are not carried in the expression node — the arithmetic operator arm renders
operands via recursive calls into opaque SQL strings without inspecting their types — so the
translator cannot identify a safe integer-only case to render selectively.

### Decision

Keep `FN_DIV` unadvertised. The `crates/vs-expression` translator declines a `DIV` node in both
raising and safe modes; the adapter omits the expression and Exasol evaluates `DIV` itself. The
outcome is unchanged from the superseded ADR; only the stated reason is corrected.

### Options Considered

| Option | Verdict |
|--------|---------|
| Keep `FN_DIV` declined; correct the rationale to truncation + DOUBLE div-by-zero divergence | ✓ Chosen — the decline is still required, but for the verified reason |
| Advertise `FN_DIV` via `TRUNC(m/n)` | ✗ Rejected — DOUBLE-operand division by zero yields infinity in DataFusion vs. an Exasol error, and `DIV`'s operand types aren't available to the translator to restrict rendering to the safe integer case |
| Close issue #105 with no spec change | ✗ Rejected — the permanent spec and ADR asserted a live-refuted premise, risking a future "fix" via `FLOOR` that would itself diverge from Exasol |

### Consequences

`DIV` expressions never push down; Exasol always evaluates them — same as before. The record now
states the true reason (DataFusion has no `div` builtin and DOUBLE division-by-zero diverges),
removing the risk that a future planner ships `FN_DIV` via `FLOOR` on the false belief that
truncation vs. floor division was the blocker.
