# Decisions: fix-pushdown-tstz-literal-emits

## ADR: Route a non-emittable select-list item to the qualified single-table wrapper

**ID:** route-non-emittable-selectlist-to-qualified-wrapper
**Plan:** fix-pushdown-tstz-literal-emits
**Status:** Accepted

### Context

A select-list item the scan UDF cannot emit — declared EMITS-invalid (`TIMESTAMP WITH LOCAL TIME
ZONE`) or session-context-dependent — previously fell back to responding with the full base row.
Verified live that this is an INVALID pushdown response, not a correct-but-unaccelerated one:
Exasol validates the response positionally against the request's `selectList` and rejects a
column-count mismatch with SQL state `04000` ("Expected number of columns is N but pushdown query
has M"), so the query FAILS outright. Reproduced for the literal branch, the scalar branch, a TSTZ
`FN_CAST` over a column, and every item position.

### Decision

Route the whole request to `qualified_single_table_fallback_pushdown` — the shape the
grouped-aggregate and multi-`COUNT(DISTINCT)` declines already use — instead of responding with the
full base row.

### Options Considered

| Option | Verdict |
|--------|---------|
| Route to the qualified single-table wrapper | ✓ Chosen — the mechanism already exists, is already specified normatively for two other decline shapes, and its documented contract ("the result column count and per-column types match Exasol's positional `selectListDataTypes` validation") is exactly the requirement |
| Keep declining to the full base row | ✗ Rejected — verified `04000` hard failure at every item position, not a correct-but-slow path |
| Append the item as a flat sibling scalar expression next to `LAKEHOUSE_SCAN(...) EMITS (...)` | ✗ Rejected — an EMITS call expands to a contiguous column block, so an item between two scan columns cannot be positioned, and a bare `SELECT CURRENT_TIMESTAMP FROM t` needs exactly one output column while the scan must still emit at least one to drive the rows |
| Withdraw the session-dependent capabilities so Exasol never delegates the item | ✗ Rejected (independently, this direction shipped anyway via the unrelated `fix-vs-expression-dialect` plan) — capabilities are global, not per-clause: withdrawal would also kill `WHERE ts < CURRENT_TIMESTAMP` predicate pushdown and Iceberg timestamptz-literal file pruning, and cannot cover `FN_CAST` to TSTZ, which is not separately withdrawable |
| Carry the value as a VARCHAR bearing a UTC offset | ✗ Rejected — Exasol's VARCHAR → TSTZ conversion honors only `NLS_TIMESTAMP_FORMAT` and rejects an offset suffix (SQL state 22018) |
| Read `SESSIONTIMEZONE` in the adapter over connect-back and compensate | ✗ Rejected — connect-back opens an independent session and cannot observe the user session's zone |

### Consequences

The wrapper returns the session-local value, reproduces a TSTZ literal exactly, supports arbitrary
column interleaving, and yields column type `TIMESTAMP(3) WITH LOCAL TIME ZONE` — verified
end-to-end at the SQL level against the deployed scan UDF. The routing predicate is reason-based
over the request rather than an arity comparison, so it cannot fire on the absent, empty, or
non-array `selectList` arms where the full base row is the correct response.

Do not substitute plain `TIMESTAMP` for a declared `TIMESTAMP WITH LOCAL TIME ZONE` EMITS type.

