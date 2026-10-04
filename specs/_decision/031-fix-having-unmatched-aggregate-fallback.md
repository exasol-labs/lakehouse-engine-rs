# Decisions: fix-having-unmatched-aggregate-fallback

## ADR: An unrenderable HAVING is a routing outcome, not an error

**ID:** having-unrenderable-is-routing-not-error
**Plan:** fix-having-unmatched-aggregate-fallback
**Status:** Accepted

### Context

A grouped query whose HAVING references an aggregate absent from the select list failed at
`EXPLAIN VIRTUAL` time with SQL state `22002` / `F-UDF-CL-RUST-9001` (issue #195). The premise
behind that error — Exasol will not re-apply a HAVING the adapter advertised `AGGREGATE_HAVING`
for, so it must never be silently dropped — is established, not assumed: the adapter has exactly
two HAVING renderers and neither omits a HAVING, and the adapter's own code asserts the rule in
six places (`request_shape.rs` 16/70/85, `grouped_agg.rs` 3394, `file_resolution.rs` 1480,
`mod.rs` 363). Exasol's re-apply behavior is also shape-dependent: under `add-topn-pushdown`
B5/B6 (issues #225 / #189) an `orderBy` pushed together with a `limit` was fully delegated and no
backstop ran, returning wrong unsorted unbounded rows. But the premise only rules out routes that
DROP the HAVING; it does not require an error, and a route that preserves the HAVING already
exists.

### Decision

Route a grouped request whose HAVING cannot be rewritten over the partial/merge decomposition to
the existing qualified-single-table-wrapper fallback (`RequestShape::GroupByWrapper`) instead of
raising an error. The wrapper renders the HAVING as ordinary Exasol SQL over materialized rows, so
the HAVING is preserved and the advertised `AGGREGATE_HAVING` contract holds.

### Options Considered

| Option | Verdict |
|--------|---------|
| Route to the qualified single-table wrapper | ✓ Chosen — a correct native path already exists; the hard error was a false negative |
| Keep the hard error and document the unsupported shape | ✗ Rejected — the error is a false negative given a working native path |
| Stop advertising `AGGREGATE_HAVING` | ✗ Rejected — de-optimizes every HAVING query to fix one shape |
| Teach `render_having_over_merge` to synthesize a partial for the unprojected aggregate | ✗ Rejected — widens the per-shard EMITS clause and the merge decomposition for a shape the wrapper already serves correctly |

### Consequences

A HAVING referencing an unselected aggregate, a mixed AND/OR junction with one unmatched operand,
or a `DISTINCT` aggregate in a HAVING now succeeds via the wrapper instead of hard-erroring.
A non-numeric aggregate carrying a HAVING also now routes to the wrapper: Exasol's own engine
either implicitly converts (query succeeds) or raises its standard `22018` cast error naming the
offending value, rather than the adapter erroring pre-emptively.

Delete `classify_request_shape`'s non-numeric-with-HAVING `Err` block in this plan.

