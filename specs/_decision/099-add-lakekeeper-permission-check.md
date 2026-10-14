# Decisions: add-lakekeeper-permission-check

## ADR: The permission check runs where a pushdown first reads table metadata

**ID:** permission-check-runs-where-a-pushdown-first-reads-table-metadata
**Plan:** add-lakekeeper-permission-check
**Status:** Accepted

### Context

Every query shape loads its tables through one point: row scan, aggregates, COUNT(DISTINCT), top-N, fallback wrappers, and joins.

### Decision

The permission check runs at the one point where a pushdown request first reads table metadata, before any table is loaded. That point refuses to load a table that the check did not cover.

### Options Considered

| Option | Verdict |
|--------|---------|
| A check in each query shape | Rejected: adds sites without adding coverage, and a new shape could miss its check |
| Refuse every shape except the row scan | Rejected: no shape bypasses the point, so the refusal removes function and no risk |
| Take the table list from `involvedTables` | Rejected: a second source of truth for which tables a query reads |

### Consequences

- A future bypass becomes a refusal instead of an unchecked read.
- A catalog kind whose resolution runs no check gets every table refused while the check is on.
