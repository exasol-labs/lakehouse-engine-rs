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

## ADR: The SDK bump's row validation forces a partial-aggregate fix into this plan

**ID:** sdk-bump-row-validation-forces-partial-agg-fix
**Plan:** remove-emit-exa-types
**Status:** Accepted

### Context

`exasol-udf-sdk` 0.26.1 validates every `Value` row against the declared output column before
buffering it (`column_accepts` in `exa-udf-runtime`'s `rowset.rs`), where the earlier bridge
silently coerced a mismatched cell instead. `column_accepts` rejects `Value::Int64` and
`Value::Numeric` in a `Double` column, `Value::Numeric` in an `Int32`/`Int64` column,
`Value::Double` in a `Numeric` column, and `Value::String` in any numeric column. Four
pre-existing mismatches in the partial-aggregate path reach those rules: `AvgSum`/`StatSum`/
`StatSumSq` declared `DOUBLE PRECISION` while DataFusion keeps the argument's integer or decimal
type; `SUM` over a `decimal(p,s)` with `p` at least 27 widening past the declared `DECIMAL(36,s)`
into `Value::String`; `MIN`/`MAX` over a `decimal(p,0)` with `p` at most 18 producing
`Value::Numeric` into an `Int64`-binned column; and an aggregate reached only nested inside a
scalar, declared by the hardcoded `DOUBLE PRECISION` literal `NESTED_AGGREGATE_PLAN_TYPE` while its
DataFusion expression yields `Decimal128`. Existing coverage misses all four because every `AVG`
and `STDDEV` test case uses a `double` column. Issue #399's own Scope section states the
partial-aggregate path is not in scope, a note written before the 0.26.0 release bundled row
validation into the same bump.

### Decision

Fix the four pre-existing partial-aggregate type mismatches in this plan, by coercing each
partial-aggregate column to its declared `ExaType` (read via `UdfContext::output_column`) before
the per-cell conversion, reusing the same emit-boundary coercion the raw-row path applies rather
than adding a per-aggregate-kind cast. Group-key columns are excluded from the coercion and keep
their existing `value_to_gk_string` stringification, because they are already declared
`VARCHAR(2000000)` and already emit an admitted `Value::String`.

### Options Considered

| Option | Verdict |
|--------|---------|
| Fix the four mismatches now, via the shared emit-boundary coercion | ✓ Chosen — avoids shipping a regression between the bump and a follow-up |
| Bump the SDK and defer the partial-aggregate fix | ✗ Rejected — ships a known regression window |
| Cast per aggregate kind in `partial_select_items` | ✗ Rejected — cannot fix `MIN`/`MAX` or wide-decimal `SUM` without declared types from the context |
| Coerce the whole partial batch, including group-key columns | ✗ Rejected — Arrow cast formats differently from `NaiveDate::to_string()`, changing merge identity |

### Consequences

`AVG`, `STDDEV`, `STDDEV_POP`, `VARIANCE`, and `VAR_POP` over integer or decimal columns keep
returning correct values instead of failing with a row-validation error. The adapter needs no
change; only the scan's conformance to the declared partial types is new.
