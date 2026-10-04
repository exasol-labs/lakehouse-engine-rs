# Decisions: remove-emit-exa-types

## ADR: The declared output column is the single authority for the emitted type, on every emit path

**ID:** declared-output-column-single-authority-emit-type
**Plan:** remove-emit-exa-types
**Status:** Accepted

### Context

`LAKEHOUSE_SCAN` is declared `EMITS (...)`, so the call-site clause is already the sole declaration of the output schema. `CommonScanSpec::emit_exa_types` was a second, unenforced copy of that decision, and the same duplication caused four partial-aggregate type mismatches.

### Decision

The scan reads the declared `ExaType` of each output column from `UdfContext::output_column` and coerces to it, on both the Arrow `emit_batch` path and the `Value` partial-aggregate path. `CommonScanSpec::emit_exa_types` is removed. A missing or wrong-arity declared list fails the call with a named mismatch and no fallback. The plan also fixes the four partial-aggregate mismatches by coercing each partial column to its declared type before cell conversion, reusing the raw-row path's emit-boundary coercion.

### Options Considered

| Option | Verdict |
|--------|---------|
| Keep `emit_exa_types` as a sanity-check copy | Rejected: two unenforced copies is the defect |
| Keep it only for the join path | Rejected: the value is available at every call site |
| Fall back when the declared list is missing or short | Rejected: every scan call has an `EMITS` clause, so a missing list is drift |

### Consequences

The adapter and the scan cannot drift on the output type. The string parse in `exasol_type_to_arrow` leaves the emit path, and test `UdfContext` doubles must declare output columns.
