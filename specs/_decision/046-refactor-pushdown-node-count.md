# Decisions: refactor-pushdown-node-count

## ADR: `adapterNotes` Admits Only Create-Time Values a Pushdown Cannot Recompute (Supersedes ADR)

**ID:** adapternotes-admits-only-create-time-values-a-pushdown-cannot-recompute
**Plan:** refactor-pushdown-node-count
**Status:** Accepted
**Supersedes:** source-cluster-node-count-from-udfcontext-node-count-not-a-connect-back-select-nproc-supersedes-adr-006

### Context

`CLUSTER_NODES` was written into `schemaMetadata.adapterNotes` at virtual schema creation and read at pushdown, so a writer and a reader agreed only through an untyped string key. This contradicts the mission rule that UDFs hold no cross-call state and resolve metadata per query, and it froze shard fan-out at the creation-time node count until `REFRESH`.

### Decision

`adapterNotes` holds a value only when it is derived at create time and a pushdown cannot recompute it. `TABLE_MAP` qualifies because recomputing it costs a namespace enumeration per query, and handshake metadata never qualifies. The adapter stops writing `CLUSTER_NODES` and adds no migration. Per architect review (PR #282), an operator upgrading past this change drops and recreates the virtual schema, so an inherited `CLUSTER_NODES` entry stays unread and inert.

### Options Considered

| Option | Verdict |
|--------|---------|
| Actively remove an inherited key on every response | Rejected: needs removal code, a follow-up issue, and a manual test gate for a problem that drop-and-recreate already solves |
| Use `adapterNotes` as a general cache for anything convenient at pushdown | Rejected: this status quo produced `CLUSTER_NODES` |

### Consequences

The node count now comes live per pushdown instead of frozen at creation, which `plan.md` § Impact states. The superseded ADR's `UdfContext::node_count()` source and `0 => 1` floor stay. No tombstone constant, removal path, or cleanup issue exists.
