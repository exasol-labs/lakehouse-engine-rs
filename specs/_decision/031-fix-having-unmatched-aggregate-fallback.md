# Decisions: fix-having-unmatched-aggregate-fallback

## ADR: An unrenderable HAVING is a routing outcome, not an error

**ID:** having-unrenderable-is-routing-not-error
**Plan:** fix-having-unmatched-aggregate-fallback
**Status:** Accepted

### Context

A grouped query whose HAVING references an aggregate absent from the select list failed with `F-UDF-CL-RUST-9001` (issue #195). Exasol does not re-apply a HAVING that the adapter advertised `AGGREGATE_HAVING` for, so the adapter must never drop it. Dropping the HAVING is ruled out, but an error is not required, because a route that preserves the HAVING already exists.

### Decision

The adapter routes a grouped request whose HAVING cannot be rewritten over the partial/merge decomposition to the qualified single-table wrapper. The wrapper renders the HAVING as Exasol SQL over materialized rows. The non-numeric-with-HAVING error in the request classifier is deleted.

### Options Considered

| Option | Verdict |
|--------|---------|
| Keep the hard error and document the unsupported shape | Rejected: the error is a false negative given a working native path |
| Stop advertising `AGGREGATE_HAVING` | Rejected: de-optimizes every HAVING query to fix one shape |
| Synthesize a partial for the unprojected aggregate | Rejected: widens the per-shard EMITS clause and merge for a shape the wrapper already serves |

### Consequences

A HAVING on an unselected aggregate, a mixed AND/OR with one unmatched operand, or a `DISTINCT` aggregate now succeeds. A non-numeric aggregate with a HAVING also routes to the wrapper, where Exasol converts implicitly or raises its own `22018` cast error.
