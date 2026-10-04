# Decisions: remove-emit-exa-types

## ADR: The declared output column is the single authority for the emitted type, on every emit path

**ID:** declared-output-column-single-authority-emit-type
**Plan:** remove-emit-exa-types
**Status:** Accepted

### Context

`LAKEHOUSE_SCAN` is declared `EMITS (...)`, so the call-site clause Exasol parses is already the
sole declaration of a scan call's output schema. `CommonScanSpec::emit_exa_types` was a second copy
of that same decision: the adapter built both the clause and a JSON array from one `proj_types`
vector, and the scan trusted the array without checking it against the engine. Nothing enforced
agreement between the two, and that same duplication pattern caused four pre-existing
partial-aggregate type mismatches (see the companion ADR
`sdk-bump-row-validation-forces-partial-agg-fix`).

### Decision

The scan reads the declared `ExaType` for output column `i` from `UdfContext::output_column(i)`
and coerces the value to it, on both the Arrow `emit_batch` path and the `Value`
partial-aggregate path. No module carries a second copy of that declaration and no module
re-derives it. `CommonScanSpec::emit_exa_types` is removed. An absent or wrong-arity declared
column list fails the call, naming the mismatch, with no fallback.

### Options Considered

| Option | Verdict |
|--------|---------|
| Read the declared `ExaType` from `UdfContext::output_column` on both emit paths | ✓ Chosen — single authority, no possibility of disagreement |
| Keep `CommonScanSpec::emit_exa_types` as a sanity-check copy | ✗ Rejected — two unenforced copies is the defect being removed |
| Keep the copy only for the join path | ✗ Rejected — the value is available at every call site as a local |
| Keep a fallback when the declared list is absent or short | ✗ Rejected — every scan call has a call-site `EMITS` clause; an absent declaration is drift, not a legacy spec |

### Consequences

The adapter and the scan can no longer independently drift on the output-type decision.
`exasol_type_to_arrow`'s string parse leaves the emit path. Test `UdfContext` doubles must
now declare output columns, since the fallback that tolerated an empty list is gone.

Fix the four pre-existing partial-aggregate type mismatches in this plan, by coercing each partial-aggregate column to its declared `ExaType` (read via `UdfContext::output_column`) before the per-cell conversion, reusing the same emit-boundary coercion the raw-row path applies rather than adding a per-aggregate-kind cast.

