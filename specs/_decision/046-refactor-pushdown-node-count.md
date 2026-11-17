# Decisions: refactor-pushdown-node-count

## ADR: `adapterNotes` Admits Only Create-Time Values a Pushdown Cannot Recompute

**ID:** adapternotes-admits-only-create-time-values-a-pushdown-cannot-recompute
**Plan:** refactor-pushdown-node-count
**Status:** Accepted

### Context

A value stored in the adapter notes at virtual schema creation stays frozen until `REFRESH`. For a value a pushdown can recompute, such as the node count, that contradicts the rule that metadata is resolved per query.

### Decision

The adapter notes hold a value only when it is derived at create time and a pushdown cannot recompute it. The table map qualifies because recomputing it costs a namespace enumeration per query. The node count does not, so the adapter reads it live at each pushdown.

### Options Considered

| Option | Verdict |
|--------|---------|
| Use `adapterNotes` as a general cache for anything convenient at pushdown | Rejected: a cached value goes stale until `REFRESH` |

### Consequences

- The node count follows the cluster at each pushdown.
