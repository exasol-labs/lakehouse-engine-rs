# Decisions: remove-emit-exa-types

## ADR: The declared output column is the single authority for the emitted type, on every emit path

**ID:** declared-output-column-single-authority-emit-type
**Plan:** remove-emit-exa-types
**Status:** Accepted

### Context

The scan's `EMITS (...)` clause at the call site already declares the output schema. A second, unenforced copy of that type list caused four partial-aggregate type mismatches.

### Decision

The scan reads each output column's declared type from the call and coerces to it, on both the raw-row path and the partial-aggregate path. The second copy is removed. A missing or wrong-arity declared list fails the call with a named mismatch and no fallback.

### Options Considered

| Option | Verdict |
|--------|---------|
| Keep `emit_exa_types` as a sanity-check copy | Rejected: two unenforced copies is the defect |
| Keep it only for the join path | Rejected: the value is available at every call site |
| Fall back when the declared list is missing or short | Rejected: every scan call has an `EMITS` clause, so a missing list is drift |

### Consequences

- The adapter and the scan cannot drift on the output type.
- Test doubles of the UDF context must declare output columns.
