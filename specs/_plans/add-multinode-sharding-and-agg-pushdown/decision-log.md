# Decision Log: add-multinode-sharding-and-agg-pushdown

Date: 2026-06-21

## Interview

**Q:** One plan or two for multi-node IPROC sharding and aggregation pushdown?
**A:** One combined plan (`add-multinode-sharding-and-agg-pushdown`).

**Q:** How should the adapter know the cluster node count for IPROC partitioning?
**A:** Fetch via connect-back during `createVirtualSchema` (`SELECT NPROC()`), persist the result as a VS property named `CLUSTER_NODES`. At pushdown time, read `CLUSTER_NODES` from the VS properties to decide how many shards to create. Default to 1 if not set / the fetch fails (backward-compatible single-node behaviour).

**Q:** VS property name and default?
**A:** `CLUSTER_NODES`, default = 1.

**Q:** Which aggregate functions are in scope?
**A:** COUNT(*)/COUNT(col), SUM(col), MIN(col)/MAX(col), AVG(col).

## Design Decisions

### [1] Cluster node count captured once at createVirtualSchema as CLUSTER_NODES

- **Decision:** Run `SELECT NPROC()` over connect-back during `createVirtualSchema`, store the result as the `CLUSTER_NODES` VS property; default to 1 on any failure. Read it at pushdown time to choose the shard count.
- **Alternatives:** Fetch `NPROC()` on every pushdown (rejected — per-query connect-back latency for a value stable across a VS lifetime); require the user to set a static node-count property (rejected — error-prone, drifts from reality).
- **Rationale:** Node count is stable for the VS lifetime; one connect-back keeps the hot pushdown path free of an extra round-trip; default 1 leaves the single-node path untouched when the cluster is single-node or the fetch fails.
- **Promotes to ADR:** yes

### [2] IPROC fan-out via derived VALUES + GROUP BY IPROC(), shard_key

- **Decision:** Express the cross-node fan-out as a single scan-driving query: a derived `VALUES` table of (shard_key, per-shard ScanSpec) rows, with the SET UDF invoked per group under `GROUP BY IPROC(), shard_key`, so Exasol places each shard on a distinct node.
- **Alternatives:** `UNION ALL` of N separate UDF SELECTs (rejected — does not guarantee node placement, bloats SQL); one UDF row per file (rejected — no node-level batching). The brief assumed a `strata-rs` IPROC pattern to mirror; exploration found none — `strata-rs` uses a single-invocation cache UDF (`CACHE_QUERY`) with zero IPROC/NPROC use, so the fan-out was designed from Exasol's native SET-UDF distribution idiom.
- **Rationale:** `GROUP BY IPROC()` is the idiomatic Exasol mechanism for distributing SET-UDF work across nodes and keeps the whole fan-out in one query the optimizer can place.
- **Promotes to ADR:** yes

### [3] Balanced disjoint partition capped at file count, no empty shards

- **Decision:** Partition the resolved file list into `min(CLUSTER_NODES, file_count)` balanced, disjoint, fully-covering shards; never emit a scan invocation for an empty shard.
- **Alternatives:** Always emit exactly `CLUSTER_NODES` shards (rejected — empty shards waste a node and an Iceberg session); one shard per file (rejected — loses node-level batching when files ≫ nodes).
- **Rationale:** Preserves the file-level no-overlap invariant, uses every node when files allow, and avoids wasted invocations.
- **Promotes to ADR:** no

### [4] Partial/merge aggregate decomposition; AVG as (sum,count) pair

- **Decision:** Split each aggregate into a node-local partial (in the scan UDF) and an Exasol-side merge (wrapper SQL): COUNT→SUM(partial_count), SUM→SUM(partial_sum), MIN→MIN(partial_min), MAX→MAX(partial_max). AVG is emitted as a (partial_sum, partial_count) pair and divided in the wrapper as `SUM(sum)/SUM(count)` with a count=0 → NULL guard.
- **Alternatives:** Full aggregate in one UDF on one node (rejected — does not scale, defeats the point); emit per-shard average and average the averages (rejected — incorrect for unequal shard sizes).
- **Rationale:** Partial+merge is exactly mergeable for COUNT/SUM/MIN/MAX and cuts transfer to one row per node; AVG requires the sum/count pair because averages of averages are wrong; the zero-guard preserves single-node NULL semantics.
- **Promotes to ADR:** yes

### [5] Aggregate caps advertised with detection fallback to row scanning

- **Decision:** Advertise single-group COUNT/SUM/MIN/MAX/AVG aggregate capabilities, but for any pushdown shape the UDF cannot compute (GROUP BY, DISTINCT, HAVING) fall back to row scanning for that query.
- **Alternatives:** Advertise nothing and never push aggregates (rejected — the deferred goal); advertise and hard-fail on unsupported shapes (rejected — breaks queries Exasol could still answer by scanning rows).
- **Rationale:** Advertising lets Exasol delegate the cheap common cases; the fallback keeps correctness for everything else.
- **Promotes to ADR:** no

### [6] iproc-sharding as its own feature/domain

- **Decision:** Capture cross-node file sharding as a new `parallelism/iproc-sharding` feature rather than folding it into `pushdown-planning`.
- **Alternatives:** Add the sharding scenarios under `pushdown-planning` (rejected — mixes the cross-node concern with translation/pushdown analysis).
- **Rationale:** Sharding is reused by both row and aggregate queries and carries its own invariants (disjoint, balanced, no empty shards) that deserve first-class scenarios.
- **Promotes to ADR:** no

## Review Findings

<!-- Populated by speq-implement after code review. -->
