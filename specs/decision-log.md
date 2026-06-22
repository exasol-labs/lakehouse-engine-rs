# Architecture Decision Records

<!-- ADRs are numbered sequentially starting from ADR-001. Never renumber. -->
<!-- recorder-agent appends new ADRs from plan decision logs. -->

---

## ADR-001: One Crate / One .so with Two Named Entry Points

**Date:** 2026-06-21
**Plan:** `add-datafusion-iceberg-scan-pushdown`
**Status:** Accepted

### Context

The VS adapter and the DataFusion scan SET UDF needed to be deployed to Exasol BucketFS. The pre-0.14.0 shape of the SDK required one `.so` per entry point, meaning two crates and two separate BucketFS uploads. language-container-rs 0.14.0 introduced support for multiple named entry points per `.so` (commit a11795a, live two-entry E2E in d67c977).

### Decision

Ship the VS adapter and the DataFusion scan SET UDF as two `#[exasol_udf]` entry points in a single `cdylib` crate that builds to one `.so`. One artifact is uploaded to BucketFS and both Exasol scripts reference it.

### Options Considered

| Option | Verdict |
|--------|---------|
| One crate / one `.so` with two entry points | ✓ Chosen — 0.14.0 supports it; one artifact halves deploy surface and exercises the new capability under test |
| Two crates / two `.so` files (pre-0.14.0 strata-rs shape) | ✗ Rejected — doubles BucketFS upload surface; unnecessary given 0.14.0 capability |

### Consequences

One BucketFS upload suffices for both scripts. The build target is single-crate, which simplifies the Makefile and workspace. Both entry points share the same compiled dependencies, reducing binary size. The new multi-entry-point capability is directly exercised and validated by this plan.

---

## ADR-002: Adapter Drives a Scan SET UDF (Not a Cache-Populating Library Call)

**Date:** 2026-06-21
**Plan:** `add-datafusion-iceberg-scan-pushdown`
**Status:** Accepted

### Context

The VS adapter must return a pushdown response to Exasol. In the sibling project strata-rs, the adapter calls a `populate_cache()` library function via connect-back and returns a plain `SELECT` from a cache table. The mission explicitly lists caching and materialization as non-goals; the PoC hypothesis is DataFusion-in-UDF as the distributed execution substrate.

### Decision

The adapter's `pushdown` response is SQL that invokes the scan SET UDF with an explicit file list. The UDF runs DataFusion and emits rows directly to Exasol. No cache table is populated.

### Options Considered

| Option | Verdict |
|--------|---------|
| Adapter returns SQL invoking a DataFusion scan SET UDF | ✓ Chosen — proves the PoC hypothesis; execution lives in DataFusion as intended |
| Mirror strata-rs: populate_cache() via connect-back, return SELECT from cache | ✗ Rejected — caching/materialization are explicit mission non-goals; would not test DataFusion-in-UDF execution path |

### Consequences

DataFusion runs inside the UDF for every query — no cached results, no background population. Each query is fully stateless and independent. The approach directly tests the central PoC hypothesis. Results are never stale but there is no acceleration from result reuse.

---

## ADR-003: Resolve Metadata Once in the Adapter; Pass an Explicit File List to the UDF

**Date:** 2026-06-21
**Plan:** `add-datafusion-iceberg-scan-pushdown`
**Status:** Accepted

### Context

The Iceberg snapshot and data-file list must be resolved from the catalog before scanning. The question is where: inside each UDF invocation (once per node) or once in the adapter during pushdown planning and handed to the UDF as an explicit argument.

### Decision

The adapter resolves the Iceberg snapshot and data-file list exactly once during `pushdown` and passes the file list to the scan UDF as an explicit argument. The UDF never discovers files itself.

### Options Considered

| Option | Verdict |
|--------|---------|
| Resolve once in the adapter; pass explicit file list to UDF | ✓ Chosen — satisfies the mission constraint (once per query, not once per node); creates the seam multi-node sharding will later exploit |
| Each UDF invocation re-resolves metadata from the catalog | ✗ Rejected — violates the mission constraint; causes N-node duplicate metadata fetches; breaks the sharding seam |

### Consequences

Metadata resolution is single-threaded and happens before the scan UDF runs, adding latency for the catalog round-trip. The explicit file-list argument is the exact seam that multi-node IPROC file sharding will later partition across nodes without touching the UDF internals.

---

## ADR-004: Value-Only Boundary with Batch-by-Batch Incremental Emit

**Date:** 2026-06-21
**Plan:** `add-datafusion-iceberg-scan-pushdown`
**Status:** Accepted

### Context

The scan UDF links its own copy of the Arrow library, giving it different Arrow `TypeId`s from any Arrow copy in the SDK or other `.so`s. Passing Arrow `RecordBatch` or array types across the `.so` boundary is therefore unsafe. Additionally, materializing the full result set before emitting risks memory blowups for large scans.

### Decision

Convert each Arrow `RecordBatch` to SDK `Value` rows inside the UDF and `ctx.emit` them before fetching the next batch; then drop each batch. Rely on the 4,000,000-byte auto-flush. No Arrow type crosses the `.so` boundary.

### Options Considered

| Option | Verdict |
|--------|---------|
| Convert batch-by-batch to Value; emit incrementally; drop each batch | ✓ Chosen — Arrow-safe across the boundary; streaming avoids memory blowup; matches project-wide invariant in CLAUDE.md/mission |
| Collect the full result set, then emit | ✗ Rejected — violates streaming constraint; risks materializing two full copies (Arrow + Value) in memory |
| Pass Arrow types directly across the .so boundary | ✗ Rejected — Arrow TypeId instability across dynamic-library boundary causes undefined behavior |

### Consequences

Peak memory per UDF invocation is bounded by one Arrow batch plus one Value row buffer rather than the full result set. The SDK's 4,000,000-byte auto-flush handles backpressure without manual tracking. Implementors must never collect all batches before emitting.

---

## ADR-005: Single Authoritative DataFusion-to-Exasol Type Mapping with JSON Fallback

**Date:** 2026-06-21
**Plan:** `add-datafusion-iceberg-scan-pushdown`
**Status:** Accepted

### Context

Exasol has no array, list, struct, or map type. Iceberg tables may contain Arrow columns of these types (List, Struct, Map, Binary, Decimal256, etc.). The mapping from Arrow types to Exasol types must be applied consistently in two places: the `createVirtualSchema` schema declaration and the scan UDF's Arrow→Value conversion. If these diverge, declared column types and emitted value types disagree, causing runtime errors.

### Decision

A single authoritative Arrow-to-Exasol mapping table governs both `createVirtualSchema` schema declaration and the scan's Arrow→Value conversion. Compatible Arrow types map directly to native Exasol types. Incompatible types (List, LargeList, FixedSizeList, Struct, Map, Union, Binary, LargeBinary, FixedSizeBinary, Duration, Time32, Time64, Interval, Decimal256, and out-of-range Decimal128 where p>36 or s>36) are serialized to a JSON string in the scan UDF and declared as `VARCHAR(2000000)` in the schema response.

### Options Considered

| Option | Verdict |
|--------|---------|
| Single shared mapping table: compatible types direct, incompatibles via JSON VARCHAR | ✓ Chosen — every Iceberg column is surfaced; declared and emitted types always agree; complex Parquet data is queryable as JSON strings |
| Reject or error on incompatible columns at createVirtualSchema | ✗ Rejected — makes tables with list/struct columns unusable; user framing requires all columns to be surfaced |
| Drop incompatible columns from the virtual table | ✗ Rejected — silently hides data; user framing expects vectors, lists, and structs to be accessible as JSON |

### Consequences

Every Iceberg column is surfaced in the virtual schema, even complex types. Incompatible columns arrive as queryable JSON strings typed as `VARCHAR(2000000)`. The single shared mapping table is the source of truth for both the adapter and the scan; any future type additions must update both code sites. Out-of-range Decimal128 values incur a JSON serialization round-trip.

---

## ADR-006: Cluster Node Count Captured Once at createVirtualSchema via adapterNotes

**Date:** 2026-06-21
**Plan:** `add-multinode-sharding-and-agg-pushdown`
**Status:** Accepted

### Context

The adapter needs the active Exasol cluster node count at pushdown time to decide how many file shards to create. The count must be obtainable without a per-query connect-back (which would add latency on the hot pushdown path). Exasol 2025.2.1 silently drops adapter-returned `schemaMetadata.properties` — they never appear in any catalog view and are not round-tripped to the adapter. The `adapterNotes` field, by contrast, is persisted by Exasol and returned to the adapter via `schemaMetadataInfo.adapterNotes` on every subsequent pushdown request.

### Decision

Run `SELECT NPROC()` over connect-back once during `createVirtualSchema`, store the result as the `CLUSTER_NODES` entry in the response's `adapterNotes` (stringified JSON). Default to 1 on any connect-back failure. Read `CLUSTER_NODES` from `schemaMetadataInfo.adapterNotes` at pushdown time to choose the shard count.

### Options Considered

| Option | Verdict |
|--------|---------|
| Fetch NPROC() once at createVirtualSchema; store in adapterNotes | ✓ Chosen — node count is stable for the VS lifetime; adapterNotes is persisted and round-tripped; one connect-back keeps the hot pushdown path free of extra latency |
| Store node count in schemaMetadata.properties | ✗ Rejected — Exasol 2025.2.1 silently drops adapter-returned properties; they are never persisted or round-tripped |
| Fetch NPROC() on every pushdown via connect-back | ✗ Rejected — per-query connect-back latency for a value stable across the VS lifetime |
| Require user to set a static node-count property | ✗ Rejected — error-prone; drifts from reality as the cluster scales |

### Consequences

One connect-back at schema creation time; zero connect-back overhead at pushdown time. The node count is queryable via `SYS.EXA_ALL_VIRTUAL_SCHEMAS.ADAPTER_NOTES`. Default of 1 leaves the single-node execution path unchanged when the cluster is single-node or the connect-back fails.

---

## ADR-007: IPROC Fan-Out via Derived VALUES + GROUP BY IPROC(), shard_key

**Date:** 2026-06-21
**Plan:** `add-multinode-sharding-and-agg-pushdown`
**Status:** Accepted

### Context

The adapter must distribute N file shards across N Exasol cluster nodes so each node's DataFusion UDF invocation scans only its own shard. Exasol's IPROC() function identifies the current execution node; GROUP BY over IPROC() causes Exasol to route each group to a distinct node when driving a SET UDF. No existing pattern in the sibling project strata-rs covers IPROC-based fan-out (strata-rs uses a single-invocation cache UDF with no IPROC/NPROC use), so the fan-out was designed from Exasol's native SET-UDF distribution idiom.

### Decision

Express the cross-node fan-out as a single scan-driving query: a derived `VALUES` table of `(shard_key, per-shard ScanSpec)` rows, with the scan SET UDF invoked per group under `GROUP BY IPROC(), shard_key`. Exasol places each group on a distinct node, routing each shard's rows to the correct DataFusion UDF invocation.

### Options Considered

| Option | Verdict |
|--------|---------|
| Derived VALUES + GROUP BY IPROC(), shard_key | ✓ Chosen — idiomatic Exasol mechanism for distributing SET-UDF work across nodes; one query the optimizer can place |
| UNION ALL of N separate UDF SELECTs | ✗ Rejected — does not guarantee node placement; bloats SQL linearly with shard count |
| One UDF row per file | ✗ Rejected — no node-level batching; loses the shard-level data locality |

### Consequences

Node placement is guaranteed by Exasol's GROUP BY IPROC() routing. The fan-out is expressed in one query, keeping the planning layer simple. Adding more shards increases the VALUES rows, not the query structure.

---

## ADR-008: Partial/Merge Aggregate Decomposition; AVG as (sum, count) Pair

**Date:** 2026-06-21
**Plan:** `add-multinode-sharding-and-agg-pushdown`
**Status:** Accepted

### Context

Aggregate pushdown must produce correct results across multiple shards. A naively per-shard average and then averaged-again across shards gives wrong results for unequal shard sizes. COUNT/SUM/MIN/MAX each have a well-defined partially-mergeable decomposition; AVG does not but can be decomposed into a (sum, count) pair that merges correctly.

### Decision

Split each supported aggregate into a node-local partial (computed in the scan UDF) and an Exasol-side merge (wrapper SQL): COUNT → SUM(partial_count), SUM → SUM(partial_sum), MIN → MIN(partial_min), MAX → MAX(partial_max). AVG is emitted as a (partial_sum, partial_count) pair and divided in the wrapper as `SUM(sum)/SUM(count)` with a zero-count NULL guard.

### Options Considered

| Option | Verdict |
|--------|---------|
| Partial+merge decomposition; AVG as (sum, count) pair | ✓ Chosen — exactly mergeable for COUNT/SUM/MIN/MAX; AVG sum/count pair is the only correct cross-shard decomposition; cuts transfer to one row per node |
| Full aggregate in one UDF on one node | ✗ Rejected — does not scale; defeats the sharding purpose |
| Emit per-shard average and average the averages | ✗ Rejected — mathematically incorrect for unequal shard sizes |

### Consequences

Network transfer is bounded to one partial-result row per shard, regardless of how many rows each shard scans. AVG requires two output columns per aggregated column, adding wrapper SQL complexity. The zero-count NULL guard preserves single-node AVG semantics for empty tables.
