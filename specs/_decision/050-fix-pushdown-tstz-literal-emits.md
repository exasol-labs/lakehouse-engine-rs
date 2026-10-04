# Decisions: fix-pushdown-tstz-literal-emits

## ADR: Route a non-emittable select-list item to the qualified single-table wrapper

**ID:** route-non-emittable-selectlist-to-qualified-wrapper
**Plan:** fix-pushdown-tstz-literal-emits
**Status:** Accepted

### Context

A select-list item the scan UDF cannot emit (EMITS-invalid `TIMESTAMP WITH LOCAL TIME ZONE`, or session-context-dependent) used to produce a full-base-row response. Verified live, that response is invalid: Exasol checks it positionally against `selectList` and rejects a column-count mismatch with `04000`, so the query fails at every item position.

### Decision

The adapter routes the whole request to the qualified single-table wrapper, as the grouped-aggregate and multi-`COUNT(DISTINCT)` declines already do. It does not substitute plain `TIMESTAMP` for a declared `TIMESTAMP WITH LOCAL TIME ZONE` EMITS type.

### Options Considered

| Option | Verdict |
|--------|---------|
| Keep falling back to the full base row | Rejected: hard `04000` failure at every position |
| Append the item as a sibling scalar expression next to the scan call | Rejected: an EMITS call expands to a contiguous column block, so an item between scan columns cannot be positioned |
| Withdraw the session-dependent capabilities | Rejected: capabilities are global, so this also kills `WHERE ts < CURRENT_TIMESTAMP` pushdown and timestamptz-literal file pruning and cannot cover `FN_CAST` to TSTZ (the `fix-vs-expression-dialect` plan shipped a withdrawal independently) |
| Carry the value as a VARCHAR with a UTC offset | Rejected: Exasol's VARCHAR to TSTZ conversion honors only `NLS_TIMESTAMP_FORMAT` and rejects an offset (22018) |
| Read `SESSIONTIMEZONE` over connect-back | Rejected: connect-back opens an independent session and cannot see the user session's zone |

### Consequences

The wrapper returns the session-local value, reproduces a TSTZ literal exactly, allows any column interleaving, and yields `TIMESTAMP(3) WITH LOCAL TIME ZONE`, verified end to end. The routing predicate is reason-based, so it does not fire on absent, empty, or non-array `selectList` arms where the full base row is correct.
