# Decisions: fix-225-orderby-non-projected-column

## ADR: Extend the scan's emitted columns; never widen the visible projection

**ID:** hidden-sort-key-columns-not-full-row-widening
**Plan:** fix-225-orderby-non-projected-column
**Status:** Accepted

### Context

A pushed-down `ORDER BY <col>` fails when the column is not a select-list item (issue #225, same cause as #189). The #190 fix widens the projection to the full base row, but Exasol checks the returned column count against the original select list and rejects it with `sqlCode 04000`.

### Decision

On the declined-`ORDER BY` path, the adapter appends each unprojected sort-key column after all original items, and the wrapper names only the original items. The scan emits more columns than the query shows. Top-N detection runs on the original projection, and the extension runs only after the shape is known to be declined and before the scan-spec template is built.

### Options Considered

| Option | Verdict |
|--------|---------|
| Keep full-row widening and add an outer select list | Rejected: scans and transports every base column for a narrow query |
| Decline pushdown for this shape | Rejected: a user-visible failure for a common shape |
| Drop the pushed `orderBy` and let Exasol sort | Rejected: Exasol does not re-apply a delegated `orderBy` once `ORDER_BY_COLUMN` is advertised, so rows return unordered |

### Consequences

The scan transports a few hidden columns instead of every base column.
