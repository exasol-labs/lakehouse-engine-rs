# Decisions: fix-225-orderby-non-projected-column

## ADR: Extend the scan's emitted columns; never widen the visible projection

**ID:** hidden-sort-key-columns-not-full-row-widening
**Plan:** fix-225-orderby-non-projected-column
**Status:** Accepted

### Context

A pushed-down `ORDER BY <col>` fails when `<col>` is not a bare select-list item
(issue #225, same root cause as #189). The existing fix for a related bug (#190) widens
the adapter's derived projection to the full base row so the declined-`ORDER BY` wrapper's
outer `ORDER BY` resolves, but Exasol validates a returned pushdown query's column count
positionally against the original select list, so a widened row is rejected with
`sqlCode 04000`.

### Decision

On the declined-`ORDER BY` path, append each unprojected bare sort-key column, resolved by
name from `col_types`, to `proj_cols`/`proj_types` AFTER every original item, and have the
wrapper name only the original items explicitly via `emits_ident`. The scan's
emitted-column set and the query's visible column set become two different sets instead of
being forced equal by widening.

### Options Considered

| Option | Verdict |
|--------|---------|
| Append hidden sort-key columns after the original projection, name only originals in the wrapper | ✓ Chosen — preserves every original select-list index by construction, so `emits_ident` stays aligned without a second hand-maintained rule; matches issue #189's own suggested fix |
| Keep the full-base-row widening and add an explicit outer select list | ✗ Rejected — fixes arity but still scans and transports every base column for a narrow query |
| Decline the pushdown entirely for this shape | ✗ Rejected — a hard, user-visible failure for a very common shape (`SELECT a FROM t ORDER BY b`) |
| Drop the pushed `orderBy` and let Exasol sort | ✗ Rejected — Exasol does not re-apply a delegated `orderBy` once `ORDER_BY_COLUMN` is advertised, so this silently returns unordered rows |

### Consequences

A declined `ORDER BY` on an unprojected column now returns the correct rows in the correct
order with the select list's exact arity, instead of failing with `sqlCode 04000`. The scan
transports a small number of extra hidden columns rather than every base-table column.

`detect_topn` is called on the ORIGINAL, pre-extension `proj_cols`; the hidden-sort-key extension runs only once the shape is known to be declined, and — separately — before `spec_template` so the EMITS clause and the scan-spec projection stay consistent.

