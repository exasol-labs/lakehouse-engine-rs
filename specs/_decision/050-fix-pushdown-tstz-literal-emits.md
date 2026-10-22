# Decisions: fix-pushdown-tstz-literal-emits

## ADR: Route a non-emittable select-list item to the qualified single-table wrapper

**ID:** route-non-emittable-selectlist-to-qualified-wrapper
**Plan:** fix-pushdown-tstz-literal-emits
**Status:** Accepted

### Context

A select-list item the scan UDF cannot emit (`TIMESTAMP WITH LOCAL TIME ZONE`, or session-context-dependent) cannot be answered with a full base row. Exasol checks the response positionally against the select list and rejects a column-count mismatch with `04000`.

### Decision

The adapter routes the whole request to the qualified single-table wrapper, as other declined requests already do. It does not substitute plain `TIMESTAMP` for the declared type.

### Options Considered

| Option | Verdict |
|--------|---------|
| Keep falling back to the full base row | Rejected: fails with `04000` |
| Append the item as a sibling scalar expression next to the scan call | Rejected: an EMITS call expands to a contiguous column block, so an item between scan columns cannot be positioned |
| Withdraw the session-dependent capabilities | Rejected: capabilities are global, so this also kills `WHERE ts < CURRENT_TIMESTAMP` pushdown and timestamptz-literal file pruning, and it cannot cover `FN_CAST` to TSTZ |
| Carry the value as a VARCHAR with a UTC offset | Rejected: Exasol's VARCHAR to TSTZ conversion honors only `NLS_TIMESTAMP_FORMAT` and rejects an offset (22018) |
| Read `SESSIONTIMEZONE` over connect-back | Rejected: connect-back opens an independent session and cannot see the user session's zone |

### Consequences

- The wrapper returns the session-local value, reproduces a TSTZ literal exactly, and allows any column interleaving.
- The routing is reason-based, so it does not fire where the full base row is correct (absent, empty, or non-array select list).
