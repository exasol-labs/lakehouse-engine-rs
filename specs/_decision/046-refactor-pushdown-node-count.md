# Decisions: refactor-pushdown-node-count

## ADR: `adapterNotes` Admits Only Create-Time Values a Pushdown Cannot Recompute (Supersedes ADR)

**ID:** adapternotes-admits-only-create-time-values-a-pushdown-cannot-recompute
**Plan:** refactor-pushdown-node-count
**Status:** Accepted

### Context

The node count was stored in the adapter notes at virtual schema creation and read at pushdown. This froze shard fan-out at the creation-time node count until `REFRESH` and contradicts the rule that metadata is resolved per query.

### Decision

The adapter notes hold a value only when it is derived at create time and a pushdown cannot recompute it. The table map qualifies because recomputing it costs a namespace enumeration per query. The node count does not, so the adapter reads it live at each pushdown and stops writing it. An inherited node-count entry stays unread and inert.

### Options Considered

| Option | Verdict |
|--------|---------|
| Actively remove an inherited key on every response | Rejected: needs removal code for a problem that dropping and recreating the virtual schema already solves |
| Use `adapterNotes` as a general cache for anything convenient at pushdown | Rejected: this is how the stale node count arose |

### Consequences

- The node count follows the cluster at each pushdown.
- No migration or cleanup code exists.
