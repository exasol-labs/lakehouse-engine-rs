# Backlog

Follow-up work not yet scheduled into a plan. Each entry records the finding and
proposed direction so it can be promoted into a `/speq:plan` without re-deriving
the analysis.

---

## BL-001: JOIN pushdown

**Raised by:** `add-capability-alignment` (2026-06-22)
**Status:** Open — deliberately out of scope; `JOIN*` capabilities not advertised

Today a query joining two VS tables is not pushed down: Exasol issues a separate
row-scan pushdown per table and joins the results in its core engine (correct, no
pushdown benefit). Pushing the join into DataFusion requires both join inputs in
one scan invocation, which the single-table file-sharding model does not provide.

### Phase 1 — broadcast inner equi-join (feasible)

Resolve the small side fully, replicate it into every scan invocation, join each
fact-table shard against the full copy in node-local DataFusion. Correct with no
cross-shard exchange. Advertise only `JOIN`, `JOIN_TYPE_INNER`, `JOIN_CONDITION_EQUI`;
add a small-side size threshold and a replicate-into-scan-spec mechanism. Covers the
star-schema case and reuses the existing `vs-expression` translator for the condition.

### Phase 2 — shuffle/partitioned join (hard; gate on benchmarks)

Large/large joins need both tables hash-partitioned on the join key so matching keys
co-locate per shard — a sharding-model redesign with no cross-shard exchange today,
and files can't be assigned to key-shards without reading them first. Exasol already
joins large/large well, so the pushdown win is mostly Iceberg pruning. Investigate
only if benchmarks justify it.
