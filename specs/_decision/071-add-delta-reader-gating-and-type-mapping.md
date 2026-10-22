# Decisions: add-delta-reader-gating-and-type-mapping

## ADR: Scope the Delta type refusal to the column, not the table

**ID:** scope-delta-type-refusal-to-column-not-table
**Plan:** `add-delta-reader-gating-and-type-mapping`
**Status:** Accepted

### Context

Refusing a whole Delta table on any unmapped column makes a table with one struct or binary column unqueryable, even when no query selects that column.

### Decision

The adapter omits a refused column from the logical schema and records it. One gate refuses a pushdown request that reads or emits a refused column, and every other request on the table plans normally.

### Options Considered

| Option | Verdict |
|--------|---------|
| Keep the table-scoped refusal | Rejected: leaves such tables unqueryable |
| Omit refused columns from the `createVirtualSchema` declaration | Rejected: duplicates the classification across two type vocabularies and silently shrinks `SELECT *` |
| Author an all-mappable fixture and keep table scope | Rejected: adds a network-dependent seed step and keeps the worse behavior |

### Consequences

- Omitting the column from the logical schema is defense in depth: a gate miss fails with an unresolved-column error instead of emitting a silent NULL column.

