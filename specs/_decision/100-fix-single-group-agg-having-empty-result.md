# Decisions: fix-single-group-agg-having-empty-result

## ADR: Exasol evaluates a zero-file aggregate without GROUP BY over an empty relation

**ID:** zero-file-single-group-aggregate-evaluated-by-exasol
**Plan:** fix-single-group-agg-having-empty-result
**Status:** Accepted

### Context

When pruning leaves no file, an aggregate without GROUP BY must still return the native empty-set result: one row, or none when HAVING is false. The select list can hold constants, scalars over aggregates, aggregates the adapter does not decompose, and a HAVING.

### Decision

The adapter answers such a request with the request's own select list, HAVING, and LIMIT over one zero-row relation per table, each typed like that table's columns. Exasol computes every value and column type.

### Options Considered

| Option | Verdict |
|--------|---------|
| Render each item's empty-set value as an adapter-computed literal and evaluate HAVING over those literals | Rejected: needs an empty value and a type for every aggregate kind, scalar, and constant, and diverges from Exasol wherever one is missing |

### Consequences

- A new aggregate kind needs no empty-set rule.
