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
| Read the declared `ExaType` from `UdfContext::output_column` on both emit paths | ✓ Chosen — single authority; the engine already reports the decision, so the scan reading it removes the possibility of disagreement |
| Keep `CommonScanSpec::emit_exa_types` as a sanity-check copy | ✗ Rejected — two copies with nothing enforcing agreement is the defect being removed |
| Keep the copy only for the join path, which round-tripped `proj_types` through the spec | ✗ Rejected — the value is available at every call site as a local |
| Keep a fallback to a spec-carried or source-derived type when the declared list is absent or short | ✗ Rejected — every scan call has a call-site `EMITS` clause; an absent or wrong-arity declaration is drift, not a legacy spec, and hiding it would also hide the drift acceptance criterion 4 asks to prove |

### Consequences

The adapter and the scan can no longer independently assume one output-type decision and drift
apart. `exasol_type_to_arrow`'s string parse and its scale-0 DECIMAL precision binning leave the
emit path, because the engine already reports the bin it chose. Every one of the roughly twenty
test `UdfContext` doubles that drive the scan must now declare its output columns, since the
pre-change fallback that tolerated an empty or short list is gone.

## ADR: The SDK bump's row validation forces a partial-aggregate fix into this plan

**ID:** sdk-bump-row-validation-forces-partial-agg-fix
**Plan:** remove-emit-exa-types
**Status:** Accepted

### Context

`exasol-udf-sdk` 0.26.0 validates every `Value` row against the declared output column before
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
| Fix the four mismatches now, via the shared emit-boundary coercion | ✓ Chosen — `AVG` over an integer column works today through the old bridge's silent coercion and would start failing under 0.26.0's row validation, a regression this plan would otherwise introduce |
| Bump the SDK and defer the partial-aggregate fix to a follow-up | ✗ Rejected — ships a known regression between the bump landing and the follow-up landing |
| Cast per aggregate kind in `partial_select_items` | ✗ Rejected — cannot fix `MIN`/`MAX` over a `decimal(p,0)` or a wide-decimal `SUM`, because the scan does not know those declared types without reading the context |
| Coerce the whole partial batch uniformly, including group-key columns | ✗ Rejected — an Arrow `cast(Date32 → Utf8)` formats differently from `NaiveDate::to_string()`, so a group's key text, and therefore its merge identity across shards, could change |

### Consequences

`AVG`, `STDDEV`, `STDDEV_POP`, `VARIANCE`, and `VAR_POP` over an integer or decimal column keep
returning the values they returned before the SDK bump, instead of failing with an
`output column … is … but the value is …` error. The adapter needs no change, because the declared
partial types were already the authority; only the scan's conformance to them is new. The fix
lands under issue #399's own `Closes #399` trailer rather than a separate tracked issue, an
explicit, informed override of both #399's "Not in scope" text and this repo's usual
one-feature-one-issue convention.
