# Decisions: add-delta-reader-gating-and-type-mapping

## ADR: Scope the Delta type refusal to the column, not the table

**ID:** scope-delta-type-refusal-to-column-not-table
**Plan:** `add-delta-reader-gating-and-type-mapping`
**Status:** Accepted

### Context

The shipped behavior refused a whole table on any unmapped column. That made the vendored `stats-all-types` fixture, which mixes mappable columns with `binary`, `map`, and `struct` columns, wholly unqueryable and blocked the E2E acceptance criterion of issue #322. It also makes a real table with one struct column unreachable over a column nobody selected.

### Decision

A refused column is omitted from the logical schema and recorded on `ResolvedScan`. One adapter gate refuses a pushdown request that reads or emits a refused column, and every other request on the table plans normally. This supersedes the table-scoped refusal.

### Options Considered

| Option | Verdict |
|--------|---------|
| Keep the table-scoped refusal | Rejected: leaves the fixture and such real tables unqueryable |
| Omit refused columns from the `createVirtualSchema` declaration | Rejected: duplicates the classification across two type vocabularies and silently shrinks `SELECT *` |
| Author an all-mappable fixture and keep table scope | Rejected: adds a network-dependent seed step for less useful behavior |

### Consequences

Omitting the column from the logical schema is defense in depth: a gate miss fails with an unresolved-column error instead of emitting a silent NULL column.

