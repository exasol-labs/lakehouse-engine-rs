# Decisions: refactor-pushdown-node-count

## ADR: Gate the Unverified Multi-Node Premise in Manual Testing Instead of Escalating

**ID:** gate-multinode-handshake-premise-in-manual-testing-not-escalation
**Plan:** refactor-pushdown-node-count
**Status:** Accepted

### Context

The refactor rests on `ctx.node_count()` returning the live cluster size at `pushdown` time, not only at `createVirtualSchema`. The triggering issue's author flagged this as unverified on their four-node staging cluster. `exa-udf-runtime` decodes one `UdfMeta` per handshake; for a single-call script, `single_call.rs` builds `HandshakeMeta::from(meta)` and hands it to `SingleCallContext`, whose `node_count()` returns `self.handshake.node_count`. The request type (`createVirtualSchema`, `pushdown`, `refresh`) is a JSON payload field, not part of the handshake, so the mechanism is decidable by reading the SLC runtime. What remains genuinely unverified is whether Exasol populates `numberOfNodes` as the real node count (e.g. `4`) on a live multi-node cluster.

### Decision

Plan the refactor without escalating via `OPEN QUESTIONS:`, and make a four-node staging check a mandatory pre-merge gate in `plan.md` § Manual Testing, with an explicit "MUST NOT ship" failure condition.

### Options Considered

| Option | Verdict |
|--------|---------|
| Gate the four-node check in Manual Testing with a hard fail condition | ✓ Chosen — the mechanism is decidable by code inspection; only the database-side value needs a live check, and today's `CLUSTER_NODES` write already depends on the same call, so this refactor introduces no new risk |
| Escalate as an irreducible open question and stop | ✗ Rejected — would block a plan on a condition the current code already depends on |
| Plan as a pure refactor with no extra verification | ✗ Rejected — the `0 => 1` floor makes a wrong value degrade silently rather than fail, so the check earns a hard gate |
| Keep a defensive `adapterNotes` fallback so a wrong `node_count()` cannot degrade `G` | ✗ Rejected separately — restores the coupling the refactor removes (see the adapterNotes-admission ADR) |

### Consequences

The plan ships without a live multi-node cluster available in the implementation sandbox, and `verification-report.md` records the four-node gate as not run, flagged for a human to execute before the PR leaves draft. A wrong `numberOfNodes` on a real cluster would silently collapse `G` to `parallelism_factor` rather than fail loudly, so the gate — not a test — is what catches it.

## ADR: `adapterNotes` Admits Only Create-Time Values a Pushdown Cannot Recompute (Supersedes ADR)

**ID:** adapternotes-admits-only-create-time-values-a-pushdown-cannot-recompute
**Plan:** refactor-pushdown-node-count
**Status:** Accepted
**Supersedes:** source-cluster-node-count-from-udfcontext-node-count-not-a-connect-back-select-nproc-supersedes-adr-006

### Context

`CLUSTER_NODES` was persisted into `schemaMetadata.adapterNotes` at `createVirtualSchema` and read back at `pushdown`, duplicating one decision — where the node count comes from — across a writer and a reader that agreed only through the untyped string key `"CLUSTER_NODES"`. That is back-door information leakage, and it contradicts the mission's rule that UDFs hold no cross-call state and resolve metadata per query. It also froze the shard fan-out to the node count at `CREATE VIRTUAL SCHEMA` time until an operator ran `REFRESH`.

### Decision

Adopt as a standing rule: `schemaMetadata.adapterNotes` carries a value only when the value is derived at create time and a pushdown cannot recompute it. `TABLE_MAP` qualifies (recomputing it costs a catalog namespace enumeration per query); handshake metadata never qualifies. `CLUSTER_NODES` is deleted as a write. No in-place migration mechanism is added for a note persisted by a pre-refactor adapter version — an operator upgrading past this change drops and recreates the virtual schema, per architect review (PR #282) — so an inherited `CLUSTER_NODES` entry simply survives the merge like any other foreign key, unread and inert.

### Options Considered

| Option | Verdict |
|--------|---------|
| Drop only the write and let an inherited key persist unread on pre-refactor schemas | ✓ Chosen — an operator upgrading past this change drops and recreates the virtual schema rather than upgrading in place, so no code needs to reach into and rewrite a schema's persisted state; a merge that leaves a stale, unread foreign key is no different from any other stale metadata the drop-and-recreate step discards |
| Stop writing `CLUSTER_NODES`; actively remove any inherited key on every response | ✗ Rejected (reversed after initial acceptance, on architect review) — builds and maintains removal machinery, a tracked follow-up issue, and a dedicated manual test gate to solve a problem the operational upgrade path already solves without any code |
| Treat `adapterNotes` as a general-purpose cache for anything convenient at pushdown time | ✗ Rejected — the de facto status quo that produced `CLUSTER_NODES`, and the next convenient value would follow it in |

### Consequences

This rule reverses the superseded ADR's decision to record the node count as `CLUSTER_NODES`, while retaining that ADR's `UdfContext::node_count()` source and its `0 => 1` floor. No tombstone constant, no active-removal code path, and no tracked cleanup issue exist for this — a virtual schema created before this change keeps a stale `CLUSTER_NODES` key in its persisted notes indefinitely, unread and harmless, until the schema is dropped and recreated on the new adapter version.

## ADR: Capture the Handshake Read in `dispatch`; Pass a Value, Never `ctx`, into Async Planning

**ID:** capture-node-count-handshake-in-dispatch-pass-value-not-ctx-into-async-planning
**Plan:** refactor-pushdown-node-count
**Status:** Accepted

### Context

`node_count()` is a synchronous UDF-handshake read that may block on the UDF host, and `handle_pushdown_request` is `async`. `dispatch`'s pushdown arm already captures `ctx.script_schema()` and the resolved CONNECTION credentials before `rt.block_on` for exactly this reason.

### Decision

`dispatch`'s pushdown arm calls `cluster_nodes_from_context(ctx)` before `rt.block_on`, and `handle_pushdown_request` gains a plain `cluster_nodes: usize` parameter instead of reading `ctx` itself.

### Options Considered

| Option | Verdict |
|--------|---------|
| Capture in `dispatch`, thread a plain `usize` into async planning | ✓ Chosen — joins the two sibling captures (`script_schema`, CONNECTION config) already established at this boundary, and keeps async planning code free of ambient reads and of any dependency on the UDF delivery mechanism |
| Pass `&mut dyn UdfContext` into `handle_pushdown_request` and read `node_count()` there | ✗ Rejected — the obvious shortcut, but it re-introduces a synchronous, potentially-blocking handshake read inside the tokio runtime and couples async planning code to the delivery mechanism |

### Consequences

`cluster_nodes_from_context(ctx: &dyn UdfContext) -> usize` becomes the single owner of the node-count decision, applying the `0 => 1` floor and widening to `usize`. `handle_pushdown_request`'s arity changes, which the compiler enumerates across all 19 `build_adapter_notes`-adjacent call sites during implementation.

## ADR: Accept a Live Node Count as a Deliberate Behaviour Change on a Resized Cluster

**ID:** accept-live-node-count-as-deliberate-behaviour-change-on-resized-cluster
**Plan:** refactor-pushdown-node-count
**Status:** Accepted

### Context

Reading the node count from the live handshake at every `pushdown`, rather than from a value frozen at `createVirtualSchema` time, changes observable behaviour on a cluster that is resized after the virtual schema is created: the shard fan-out now tracks the resize immediately rather than waiting for an operator-run `REFRESH`.

### Decision

Treat the shift from a create-time-frozen node count to a per-pushdown live one as intended, and state it in `plan.md` § Impact rather than suppressing it.

### Options Considered

| Option | Verdict |
|--------|---------|
| Accept and document the live-tracking behaviour change | ✓ Chosen — for a fixed node count `G` is byte-identical, satisfying the issue's constraint; the divergence appears only on a resize, where the new behaviour is strictly more correct |
| Preserve the frozen semantics exactly (write the note, refresh it only on `REFRESH`) | ✗ Rejected — deliberately keeps a stale cluster property, contradicting the mission's stateless rule |

### Consequences

An operator watching shard counts across a cluster resize will observe `G` change without an intervening `REFRESH`. A schema created on one node and later grown to a four-node cluster stops planning single-node fan-outs immediately rather than only after an explicit `REFRESH`.
