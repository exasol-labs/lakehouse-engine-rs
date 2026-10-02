# Decisions: fix-string-fn-type-coercion-udf

## ADR: exa_to_varchar converts every Arrow type, and the adapter makes no string-conversion decision

**ID:** exa-to-varchar-converts-every-arrow-type
**Plan:** fix-string-fn-type-coercion-udf
**Status:** Accepted

### Context

The scan session is the only point that knows the Arrow type of a computed string-function argument. A syntactic wrapper gives every text match the same rendered string. A wrapper that exists in the DataFusion dialect only cannot reach Exasol SQL.

### Decision

`vs-expression` renders every string-converted argument as `exa_to_varchar(<arg>)` in the DataFusion dialect only. The scan session function picks the conversion from the argument's Arrow type and covers every type the scan can produce, including DOUBLE, BOOLEAN, and TIMESTAMP. The adapter reads no column type for string conversion and rewrites no tree for it.

### Options Considered

| Option | Verdict |
|--------|---------|
| Session UDF converts every Arrow type, adapter decides nothing | ✓ Chosen: the scan session knows the computed type |
| Adapter decline check for bare DOUBLE, BOOLEAN, and TIMESTAMP columns, plus a planning error in `exa_to_varchar` | ✗ Rejected: DataFusion yields `Float64` where Exasol yields DECIMAL (`c_acctbal * 1.5`) or DOUBLE (`ROUND(c_acctbal / 3, 2)`). Both convert correctly today, and the planning error would fail them with no fallback because the adapter cannot see a computed type |
| Run `apply_type_rewrites` on the GROUP BY and aggregate-argument paths | ✗ Rejected: eight sites match plans and group keys by rendered text and would each need the same rewritten tree. Exasol rejects the rewritten `decimal_to_varchar_exasol` node's `CAST(x AS VARCHAR)` (SQL state `42000`) |

### Consequences

- No `string_conversion_declined` predicate exists, and `classify_request_shape` is unchanged.
- The string-converted argument table is private to `vs-expression`, because no adapter code reads it.
- `vs-expression` exports `EXA_TO_VARCHAR_FN` and does not implement the function, the same split as `CHECKED_FLOAT_DIV_FN`. A DataFusion-dialect consumer of the sibling-shared crate MUST register the function.
- #223 closes, because its "possible fix" section proposes exactly this conversion.
