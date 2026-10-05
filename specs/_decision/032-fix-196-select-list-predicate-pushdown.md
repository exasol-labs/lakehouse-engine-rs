# Decisions: fix-196-select-list-predicate-pushdown

## ADR: Keep the pushable-node whitelist; do not invert it to "anything the translator renders"

**ID:** keep-select-list-pushable-node-whitelist
**Plan:** fix-196-select-list-predicate-pushdown
**Status:** Accepted

### Context

Select-list projection uses an explicit whitelist of node types safe to project per shard. It drifted from what the translator renders, so six advertised boolean predicate types widened the projection to the full base row (issue #196).

### Decision

The whitelist gains the six missing node types and stays an explicit list.

### Options Considered

| Option | Verdict |
|--------|---------|
| Delete the whitelist and push anything the translator renders | Rejected: the translator also renders aggregate nodes, which would be evaluated per shard and give wrong results for non-associative aggregates, and a non-decomposable aggregate does reach the row-scan path |

### Consequences

The whitelist is the single source of truth for what a row-scan projection may evaluate per shard. A new safe translator node type needs a deliberate whitelist edit.
