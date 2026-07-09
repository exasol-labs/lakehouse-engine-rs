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
| Two crates / two `.so` files (the sibling project's pre-0.14.0 shape) | ✗ Rejected — doubles BucketFS upload surface; unnecessary given 0.14.0 capability |

### Consequences

One BucketFS upload suffices for both scripts. The build target is single-crate, which simplifies the Makefile and workspace. Both entry points share the same compiled dependencies, reducing binary size. The new multi-entry-point capability is directly exercised and validated by this plan.

---

## ADR-002: Adapter Drives a Scan SET UDF (Not a Cache-Populating Library Call)

**Date:** 2026-06-21
**Plan:** `add-datafusion-iceberg-scan-pushdown`
**Status:** Accepted

### Context

The VS adapter must return a pushdown response to Exasol. In the sibling project, the adapter calls a `populate_cache()` library function via connect-back and returns a plain `SELECT` from a cache table. The mission explicitly lists caching and materialization as non-goals; the PoC hypothesis is DataFusion-in-UDF as the distributed execution substrate.

### Decision

The adapter's `pushdown` response is SQL that invokes the scan SET UDF with an explicit file list. The UDF runs DataFusion and emits rows directly to Exasol. No cache table is populated.

### Options Considered

| Option | Verdict |
|--------|---------|
| Adapter returns SQL invoking a DataFusion scan SET UDF | ✓ Chosen — proves the PoC hypothesis; execution lives in DataFusion as intended |
| Mirror the sibling project: populate_cache() via connect-back, return SELECT from cache | ✗ Rejected — caching/materialization are explicit mission non-goals; would not test DataFusion-in-UDF execution path |

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
**Status:** Superseded by ADR-048

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

The adapter must distribute N file shards across N Exasol cluster nodes so each node's DataFusion UDF invocation scans only its own shard. Exasol's IPROC() function identifies the current execution node; GROUP BY over IPROC() causes Exasol to route each group to a distinct node when driving a SET UDF. No existing pattern in the sibling project covers IPROC-based fan-out (the sibling project uses a single-invocation cache UDF with no IPROC/NPROC use), so the fan-out was designed from Exasol's native SET-UDF distribution idiom.

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

---

## ADR-009: Standalone `crates/vs-expression` Crate for Expression Translation

**Date:** 2026-06-22
**Plan:** `add-group-by-and-sql-comprehension`
**Status:** Accepted

### Context

The VS adapter needed to translate Exasol pushdown expression-JSON nodes (column references, literals, comparison predicates, logical connectives, arithmetic, CAST, IN, BETWEEN, LIKE, IS NULL) into DataFusion SQL fragments for both filter pushdown and GROUP BY key rendering. The existing walker lived in `adapter/predicate.rs` inside `lakehouse-engine`, tightly coupled to engine internals and unreusable by the sibling project.

### Decision

Create a standalone workspace crate (`crates/vs-expression`) containing the full serde_json expression-node walker. The crate has no lakehouse-engine internals in its API; its only dependencies are `serde_json` and `exasol-udf-sdk` (for `UdfError`). It exposes three public entry points: `render_expression` (raising), `render_expression_safe` (None on failure), and `render_df_filter_safe` (None on failure or trivially-true result). Delete `adapter/predicate.rs` and replace all its callers with `vs_expression::render_df_filter_safe`.

### Options Considered

| Option | Verdict |
|--------|---------|
| Standalone `crates/vs-expression` crate with no engine-internal deps | ✓ Chosen — clean, testable, reusable by the sibling project; supports future monorepo convergence |
| Extend `adapter/predicate.rs` inline | ✗ Rejected — blocks reuse by the sibling project; keeps expression logic coupled to engine internals |
| Add a SQL-parser dependency (sqlparser-rs) as the IR | ✗ Rejected — user declined; overweight for a narrow translation job; serde_json walker is proven |

### Consequences

Expression translation is a separate, testable, reusable unit. The three-function public API (raising, safe, filter-safe) is stable and minimal. Long-term monorepo convergence with the sibling project is straightforward. `adapter/predicate.rs` is deleted; any future predicate coverage goes in `vs-expression`.

---

## ADR-010: GROUP BY Group-Key Values Emitted as Plain Columns; Wrapper Groups on Column Refs

**Date:** 2026-06-22
**Plan:** `add-group-by-and-sql-comprehension`
**Status:** Accepted

### Context

A grouped aggregate pushdown requires the scan UDF to emit partial rows that the outer wrapper SQL can re-group and merge. The group-key values must survive the partial-to-merge handoff with correct identity. Two approaches were considered: re-render the same GROUP BY expression in the wrapper SQL, or emit the computed group-key values as plain output columns and have the wrapper group on those column positions.

### Decision

The scan UDF emits computed group-key values as plain output columns named `GK_0`, `GK_1`, ... in spec order. The outer wrapper SQL groups on the same `GK_n` column names, avoiding any re-rendering of the original expression. The EMITS declaration in the scan-driving SQL names the group-key columns first, followed by the `PARTIAL_*` aggregate columns.

### Options Considered

| Option | Verdict |
|--------|---------|
| Emit group-key computed values as plain GK_n columns; wrapper groups on column refs | ✓ Chosen — eliminates expression-rendering consistency risk between partial scan and merge; wrapper only needs positional column refs |
| Re-render GROUP BY expression in the outer wrapper SQL | ✗ Rejected — any mismatch between UDF-side and wrapper-side rendering produces wrong grouping; brittle across expression types |
| Emit group keys by column name only | ✗ Rejected — breaks expression group keys that produce a computed value without a stable source column name |

### Consequences

The column-value contract between the scan UDF and the outer wrapper is purely positional. Adding a new group-key expression type requires no wrapper SQL changes. The GK_n naming convention is a stable protocol between adapter and scan UDF.

---

## ADR-011: Memory Safety via Metadata-Sized DataFusion Pool and Spill-or-Hardcap Backstop

**Date:** 2026-06-22
**Plan:** `add-group-by-and-sql-comprehension`
**Status:** Accepted

### Context

High-cardinality GROUP BY queries inside a scan UDF risk OOM-crashing the UDF process when DataFusion's intermediate state exceeds the per-instance memory budget. Exasol enforces a per-process heap limit via `setrlimit(RLIMIT_RSS)` (default 4096 MB) and stalls additional concurrent VMs once usage hits 80% of it (`swigengine.cc:1574-1595`). The per-instance limit is exposed in UDF metadata via `ctx.memory_limit()` (bytes; `0` = unbounded/unknown sentinel), provided by `language-container-rs:add-memory-limit-metadata`.

### Decision

The scan UDF reads `ctx.memory_limit()` and sizes the DataFusion `RuntimeEnv` `MemoryPool` to approximately 0.6 of it, leaving headroom below the engine's 80% stall threshold; falls back to a conservative 1024 MB default when the limit is `0`/unknown. A `/tmp` probe (tmpfs detection via `/proc/mounts` + `statvfs` free space) selects the pool variant: real disk with free space → `FairSpillPool` + `DiskManager` rooted at `/tmp` (completes at any cardinality); tmpfs or insufficient space → `GreedyMemoryPool` returning a clean `ResourcesExhausted` error. Any `/tmp` spill is transient per-invocation scratch, never persistent state.

### Options Considered

| Option | Verdict |
|--------|---------|
| Metadata-sized pool (~0.6 of limit) + spill-or-hardcap | ✓ Chosen — bounds to real per-instance limit; spill gives correctness at any cardinality; hardcap path fails cleanly; engine self-throttles at 80% |
| File-count cardinality guard (`GROUP_BY_CARDINALITY_LIMIT`) | ✗ Rejected — heuristic with no statistical basis; removed entirely |
| Per-shard emitted-group cap with UDF abort | ✗ Rejected — produces partial results; adds abort logic |
| Unbounded pool | ✗ Rejected — OOM-crashes the UDF process at high cardinality |

### Consequences

Memory use is bounded without a heuristic. Spill lets high-cardinality grouped queries complete on nodes with real-disk `/tmp`. Nodes with tmpfs (e.g., certain Docker setups) get a clean error instead of a crash. The 0.6 fraction leaves the engine's per-process concurrency stall mechanism free to throttle concurrent instances before they individually OOM.

---

## ADR-012: Oversubscribed `GROUP BY shard_key` Work-Unit Sharding (Supersedes ADR-007)

**Date:** 2026-06-22
**Plan:** `add-group-by-and-sql-comprehension`
**Status:** Accepted

### Context

ADR-007 shipped `GROUP BY IPROC(), shard_key` to distribute scan shards across nodes. A corrected reading of the Exasol engine internals (verified in `exasol-db`/`script-languages`) shows that groups drive UDF *invocations*, not OS processes; parallel instances on a node are a fixed VM pool sized to `NR_OF_CORES` (`primitives.cpp:267`, `swigengine.cc:1147-1184`), and groups are multiplexed onto it (`set_function.cpp:240-260`). `GROUP BY IPROC()` yields exactly one group per node (`misc_primitives.cpp:98-132`), capping parallelism at node count and leaving a node's other cores idle.

### Decision

Replace `GROUP BY IPROC(), shard_key` with `GROUP BY shard_key` over G oversubscribed work-unit shards, where `G = node_count × parallelism_factor` (`parallelism_factor` is a VS property, default 8), capped at 300 and clamped to `[1, file_count]`. The 300 cap keeps the group set in Exasol's round-robin distribution regime; above `max_dynamic_group_count` (default 300) Exasol hash-partitions groups instead (`globalgroupbyset5.cpp:295-341`). The `parallelism/iproc-sharding` feature is renamed to `parallelism/work-unit-sharding`. The balanced `partition_files` split is reused, passing G instead of node_count. The NPROC node-count capture (ADR-006) is retained to feed G.

### Options Considered

| Option | Verdict |
|--------|---------|
| `GROUP BY shard_key` with G = node_count × parallelism_factor capped 300 | ✓ Chosen — uses each node's full VM pool; stays in round-robin distribution regime (G ≤ 300) |
| `GROUP BY IPROC()` (ADR-007) | ✗ Rejected — caps parallelism at node count; leaves per-node cores idle |
| `GROUP BY IPROC(), shard_key` | ✗ Rejected — `shard_key` alone is sufficient; IPROC() adds no benefit once shard groups oversubscribe the node count |
| Uncapped G | ✗ Rejected — above 300, Exasol hash-partitions groups (unbalanced) |

### Consequences

Shard groups spread round-robin across nodes AND multiplex onto each node's core pool. The two-level grouping composes cleanly: inner `shard_key` parallelizes the scan, DataFusion performs the user GROUP BY inside each shard invocation, the outer wrapper merges partials by user group keys. The `parallelism_factor` VS property (default 8) gives operators a tuning knob without code changes.

---

## ADR-013: Cross-Repo Dependency on `language-container-rs:add-memory-limit-metadata`

**Date:** 2026-06-22
**Plan:** `add-group-by-and-sql-comprehension`
**Status:** Accepted

### Context

ADR-011's memory-pool sizing requires `ctx.memory_limit() -> u64` (bytes; `0` = unbounded/unknown sentinel), an accessor that belongs in the `exasol-udf-sdk` rather than being reimplemented in `lakehouse-engine-rs`. The accessor is added by the sibling-repo plan `language-container-rs:add-memory-limit-metadata`. Until the corresponding `exasol-udf-sdk` release is published, the pool-sizing code falls back to the `0`-sentinel path (1024 MB default budget). The SDK is currently pinned at 0.14.0; the accessor lands in the next release.

### Decision

Consume `UdfContext::memory_limit()` from the published `exasol-udf-sdk` release when it lands; do not reimplement the metadata proto deserialization locally. Until the SDK version carrying the accessor is published, the call site passes the `0` sentinel and the pool-sizing code uses the 1024 MB default. The version bump is a one-line `Cargo.toml` change.

### Options Considered

| Option | Verdict |
|--------|---------|
| Depend on `language-container-rs:add-memory-limit-metadata` SDK release | ✓ Chosen — single source of truth; avoids divergent wire-protocol decodings across repos |
| Reimplement the proto deserialization locally | ✗ Rejected — duplicates SDK responsibility; risks drift in the wire-protocol decoding |

### Consequences

The live `ctx.memory_limit()` path is gated on a sibling-repo SDK release. Until that release, the pool-sizing code operates against the 1024 MB default. The dependency is explicit and recorded; the one-line version bump unblocks the live path without touching any scan logic.

---

## ADR-014: Capability Invariant — Advertise Only What the Engine Can Back Correctly

**Date:** 2026-06-22
**Plan:** `add-capability-alignment`
**Status:** Accepted

### Context

The adapter's `CAPABILITIES` list and the `crates/vs-expression` translator had drifted in both directions: some advertised names did not exist in Exasol's vocabulary (`FN_PRED_GREATER`/`FN_PRED_GREATEREQUAL`), while many DataFusion-supported functions were not advertised at all. Over-advertising a function that is translated wrongly produces silent correctness bugs; under-advertising only costs performance.

### Decision

Establish the invariant that every name in `CAPABILITIES` must round-trip — either the `vs-expression` translator emits a correct DataFusion fragment for it, or the aggregate planner emits a correct shard-associative partial/merge plan. Any audit of the capability set starts from this rule: additions require a working translator path, and removals are required when no such path exists.

### Options Considered

| Option | Verdict |
|--------|---------|
| Capability invariant: advertise only what the engine backs correctly | ✓ Chosen — over-advertising is a silent correctness bug; under-advertising is only a performance loss |
| Maximise advertised set and rely on Exasol ignoring wrongly-handled names | ✗ Rejected — any wrongly-translated capability produces incorrect query results silently |

### Consequences

The capability list is now a contract: adding a new `FN_*` entry requires a translator arm or aggregate decomposition path to be implemented first. Removing capability names requires confirming the translator arm produces incorrect or no output. Future reviewers have an explicit rule to apply rather than guessing.

---

## ADR-015: Remove FN_PRED_GREATER and FN_PRED_GREATEREQUAL from CAPABILITIES

**Date:** 2026-06-22
**Plan:** `add-capability-alignment`
**Status:** Accepted

### Context

`FN_PRED_GREATER` and `FN_PRED_GREATEREQUAL` were present in the adapter's `CAPABILITIES` list. Verification against `virtual-schema-common-java/doc/development/api/capabilities_list.md` confirmed that neither name exists in Exasol's capability vocabulary. Exasol normalises `a > b` to `b < a` and `a >= b` to `b <= a` before the pushdown request reaches the adapter, so the adapter never receives nodes with these names in practice. Advertising non-existent capability names is misleading dead capability that future reviewers would re-litigate.

### Decision

Delete `FN_PRED_GREATER` and `FN_PRED_GREATEREQUAL` from `CAPABILITIES`. The `predicate_greater` and `predicate_greaterequal` translator arms in `vs-expression` are retained as defensive no-ops (they are never reached in practice but do no harm).

### Options Considered

| Option | Verdict |
|--------|---------|
| Remove both names from CAPABILITIES; keep translator arms as defensive no-ops | ✓ Chosen — the names are not in Exasol's vocabulary; advertising them is misleading |
| Leave them (Exasol ignores unknown names) | ✗ Rejected — future reviewer would re-litigate; inconsistent with the capability invariant (ADR-014) |

### Consequences

The capability list no longer contains names outside Exasol's vocabulary. The translator arms for `predicate_greater(equal)` stay as a safety net for any future Exasol version that might emit them, but are otherwise unreachable.

---

## ADR-016: STDDEV/VARIANCE Pushdown via (count, sum, sum_sq) Sufficient Statistics

**Date:** 2026-06-22
**Plan:** `add-capability-alignment`
**Status:** Accepted

### Context

The STDDEV and VARIANCE family of aggregates are not directly shard-associative: averaging per-shard standard deviations is mathematically incorrect for unequal shard sizes. However, the three sufficient statistics `COUNT(col)`, `SUM(col)`, and `SUM(col*col)` are individually shard-associative (each merges via SUM) and together allow exact reconstruction of variance and standard deviation in the outer wrapper.

### Decision

Advertise the STDDEV/VARIANCE family (`FN_AGG_STDDEV`, `FN_AGG_STDDEV_POP`, `FN_AGG_STDDEV_SAMP`, `FN_AGG_VARIANCE`, `FN_AGG_VAR_POP`, `FN_AGG_VAR_SAMP`) by decomposing each into a `(COUNT(col), SUM(col), SUM(col*col))` sufficient-statistics triple emitted per shard. The outer wrapper reconstructs variance as `(SUM(sum_sq) - SUM(sum)^2/SUM(cnt)) / d` (where `d` is `SUM(cnt)` for population forms and `SUM(cnt)-1` for sample forms) and standard deviation as its square root, with NULL guards for zero count and single-sample cases.

### Options Considered

| Option | Verdict |
|--------|---------|
| Sufficient-statistics decomposition: (count, sum, sum_sq) per shard | ✓ Chosen — exactly shard-associative; reconstructs exact result within float tolerance |
| Per-shard stddev then average the per-shard values | ✗ Rejected — mathematically incorrect for unequal shard sizes |
| Skip statistical aggregates entirely | ✗ Rejected — leaves easily supportable DataFusion capabilities off the table |

### Consequences

Statistical aggregates push down correctly across shards. The wrapper SQL is more complex (three partial columns per statistical aggregate plus the reconstruction formula). NULL/zero-count guards are required to avoid division-by-zero and negative-radicand edge cases. The approach is consistent with the partial/merge pattern already used for AVG (ADR-008).

---

## ADR-017: HAVING Applied in the Outer Merge Wrapper Only

**Date:** 2026-06-22
**Plan:** `add-capability-alignment`
**Status:** Accepted

### Context

With sharded partial aggregation, each shard emits one partial-aggregate row per group. The final HAVING predicate must be evaluated after the per-shard partials are merged into the true group aggregate. Applying HAVING inside the per-shard scan would silently discard groups whose aggregate value only clears the HAVING threshold after cross-shard merge, producing wrong results.

### Decision

Render the HAVING predicate via the shared `vs-expression` translator and apply it only in the OUTER wrapper SQL that merges the per-shard partial-aggregate rows. Never include a HAVING clause in the per-shard partial scan query. A HAVING predicate the adapter cannot translate is omitted from the wrapper SQL; Exasol retains it as a correctness backstop.

### Options Considered

| Option | Verdict |
|--------|---------|
| HAVING in the outer merge wrapper only | ✓ Chosen — HAVING is logically evaluated after grouping is complete; "complete" with sharded partials means post-merge |
| HAVING applied per shard | ✗ Rejected — discards groups that only clear the threshold after cross-shard merge; produces wrong results |

### Consequences

The wrapper SQL carries the HAVING clause; the per-shard partial scan SQL never does. This is the same structural pattern as the outer-wrapper aggregate merge in ADR-008 and ADR-016: logic that requires the full group picture goes in the wrapper, not the shard.

---

## ADR-018: Source Credentials from an Exasol CONNECTION Object (Mirror the Sibling Project's CONNECTION Convention)

**Date:** 2026-06-23
**Plan:** `add-glue-catalog-sigv4-connection`
**Status:** Accepted

### Context

The engine previously read catalog URI and S3 credentials straight from plain VS properties (`CATALOG_URI`, `ACCESS_KEY`, `SECRET_KEY`, etc.). This means credentials appear in the `CREATE VIRTUAL SCHEMA` SQL text, are visible to anyone who can read the query profile, and cannot be rotated without re-issuing the DDL. The sibling project already uses Exasol CONNECTION objects to solve this problem, with `ctx.connection(name)` returning `{address, password}` where the password is a JSON credential block.

### Decision

Read the catalog URI and all S3/signing credentials from `ctx.connection(<CATALOG_CONNECTION>)`. The `address` field is the catalog endpoint; the `password` field is a JSON object parsed for `warehouse`, `endpoint`, `region`, `access_key`, `secret_key`, and optional `session_token`/`path_style`/`use_sigv4`/`use_vended_credentials`. Both adapter entry points (`createVirtualSchema` and `pushdown`) resolve credentials through this path. Error messages never echo the password text.

### Options Considered

| Option | Verdict |
|--------|---------|
| CONNECTION object via `ctx.connection` (mirror the sibling project) | ✓ Chosen — keeps secrets out of SQL text; Exasol access-controls the CONNECTION; mirrors the existing sibling convention |
| Keep reading plain VS properties | ✗ Rejected — leaks credentials into `CREATE VIRTUAL SCHEMA` text and query profile; no rotation without DDL change |
| Inject credentials via request JSON | ✗ Rejected — not how the SDK surfaces them; would require a custom request-shape convention |

### Consequences

The `CATALOG_CONNECTION` VS property is now required. The old plain-property credential path (`extract_connection_props`) is removed. All credential-carrying code paths must ensure the password text never appears in error output. The opt-out flags (`use_sigv4`, `use_vended_credentials`) default to false so existing MinIO/REST stacks continue to work with a CONNECTION that omits them.

---

## ADR-019: Self-Issue a SigV4-Signed load_table GET Instead of Using RestCatalogBuilder

**Date:** 2026-06-23
**Plan:** `add-glue-catalog-sigv4-connection`
**Status:** Accepted

### Context

To query AWS Glue's Iceberg REST catalog the adapter must (a) SigV4-sign catalog HTTP requests and (b) recover the short-lived vended S3 credentials from the `load_table` response's `storage_credentials` block. Research confirmed that `iceberg-catalog-rest` 0.9.1 accepts only a plain `reqwest::Client` via `RestCatalogBuilder::with_client`, dispatches it internally with no per-request signing seam, and silently drops the `storage_credentials` field from `LoadTableResult`.

### Decision

For the Glue path, the adapter issues the `load_table` GET itself using a SigV4-signing `reqwest` client, deserializes the public `iceberg_catalog_rest::LoadTableResult` type, and extracts vended credentials from the `storage_credentials[*].config` block (longest-prefix match, with fallback to the flat `config` map). The unsigned/non-vended path continues using the existing `RestCatalogBuilder` flow unchanged.

### Options Considered

| Option | Verdict |
|--------|---------|
| Self-issued signed GET + parse `LoadTableResult` | ✓ Chosen — only clean in-tree path; accesses the public type; recovers `storage_credentials` |
| `RestCatalogBuilder::with_client` (plain client) | ✗ Rejected — 0.9.1 has no per-request signing seam; internal dispatch bypasses middleware |
| Fork `iceberg-catalog-rest` | ✗ Rejected — heavier maintenance burden; risk of diverging from upstream |

### Consequences

The Glue catalog path does not use `RestCatalogBuilder` for the signed load-table call. Unsigned paths are unchanged. The `LoadTableResult` deserialization depends on the `iceberg-catalog-rest` public type remaining stable; if the crate later exposes a signing hook this custom path can be removed.

---

## ADR-020: Apply Vended Credentials via merge_vended_into_storage in the Planning Layer

**Date:** 2026-06-23
**Plan:** `add-glue-catalog-sigv4-connection`
**Status:** Accepted

### Context

When `use_vended_credentials` is true, the Glue `load_table` response carries short-lived STS credentials (`s3.access-key-id`, `s3.secret-access-key`, `s3.session-token`) that must be used for data-file reads instead of the static CONNECTION credentials. These must be resolved once per query — not once per shard or node — to honour the stateless-UDF and resolve-once invariants.

### Decision

After the self-issued signed `load_table` GET (see ADR-019), the adapter extracts the vended keys from `storage_credentials[*].config` (longest-prefix match) or the flat `config` fallback, then calls `merge_vended_into_storage(static, vended)` to produce a storage block in which the STS keys override the static `access_key`/`secret_key`/`session_token` while the static `endpoint`, `region`, and `path_style` are preserved. This merged block is embedded in every per-shard `ScanSpec` by the planning layer; the scan UDF never contacts the catalog or re-requests credentials.

### Options Considered

| Option | Verdict |
|--------|---------|
| Resolve vended creds once in the planning layer, embed in each ScanSpec | ✓ Chosen — honours resolve-once and stateless-UDF invariants; mirrors the sibling project's shape |
| Rely on `iceberg-catalog-rest` to auto-apply vended creds | ✗ Rejected — 0.9.1 silently drops `storage_credentials`; not viable |
| Re-vend per node inside the scan UDF | ✗ Rejected — violates resolve-once invariant; adds catalog access to the UDF |

### Consequences

Each `ScanSpec` carries the final storage credentials (static or vended). The scan UDF is credential-passive: it configures its S3 store from the spec and never re-authenticates. Vended STS tokens have a limited lifetime; if a query takes longer than the token TTL, the scan will fail — this is accepted as a known limitation of the resolve-once design.

---

## ADR-021: Separate cloud-e2e Cargo Feature with Skip-When-Absent Semantics

**Date:** 2026-06-23
**Plan:** `add-glue-catalog-sigv4-connection`
**Status:** Accepted

### Context

The local Docker E2E suite (`exasol-e2e` feature) is designed to FAIL when its Exasol + MinIO + REST stack is down — this is intentional. Adding a cloud smoke/perf test for AWS Glue to the same feature would break that contract, because a cloud account is not always attached in every developer or CI environment.

### Decision

Gate the Glue smoke and performance tests behind a new `cloud-e2e` cargo feature, distinct from `exasol-e2e`. When the AWS credential environment variables are absent, the cloud tests skip (early return, no failure, no network call). The local Docker suite's fail-when-down semantics are unchanged.

### Options Considered

| Option | Verdict |
|--------|---------|
| New `cloud-e2e` cargo feature with skip-when-absent | ✓ Chosen — keeps the two harnesses orthogonal; safe to run in CI without cloud creds |
| Reuse `exasol-e2e` feature | ✗ Rejected — mixing in a skip-when-absent test breaks the local fail-when-down contract |

### Consequences

Developers must explicitly pass `--features cloud-e2e` to run the Glue smoke test. The feature is safe to enable in CI pipelines that inject AWS credentials as secrets and omit them otherwise. The skip path must not attempt any network call so absence of credentials is truly zero-cost.

---

## ADR-022: Byte-Balanced Sharding via LPT Greedy Assignment

**Date:** 2026-06-24
**Plan:** `change-shard-parallelism`
**Status:** Accepted

### Context

The previous `partition_files` split files into G equal-count groups. Equal file count does not mean equal scan work: a shard of three 1 GB files does far more I/O than a shard of three 1 KB files, so the slowest shard (straggler) dominates wall-clock time. Iceberg's `FileScanTask` already reports `file_size_in_bytes` at zero extra metadata cost.

### Decision

Replace count-balanced `partition_files` with `partition_files_by_bytes`, which sorts files by `file_size_in_bytes` descending (treating size 0 as 1 byte) and greedily assigns each to the shard whose running byte total is currently smallest (Longest-Processing-Time-first heuristic). The shard shape (`Vec<Vec<String>>`) is unchanged, so all downstream SQL builders are unaffected.

### Options Considered

| Option | Verdict |
|--------|---------|
| LPT greedy byte balancing | ✓ Chosen — O(n log n), deterministic, near-optimal makespan minimisation |
| Strict prefix-sum equal-byte split (file order preserved) | ✗ Rejected — a single large file can badly skew shards |
| Full DP optimum partition | ✗ Rejected — overkill for a balancing heuristic |
| Keep count-balanced split | ✗ Rejected — equal file count ≠ equal scan work; straggler dominates latency |

### Consequences

Shards are balanced by cumulative bytes rather than file count, reducing straggler-dominated tail latency. A file with `file_size_in_bytes == 0` is weighted as 1 byte so it is assigned to the lightest shard and never dropped from the scan. The resolve→partition seam is the only code path that changes; the shard shape and all SQL builders are unaffected.

---

## ADR-023: Hardware-Aware Default Parallelism Factor (`max(NR_OF_CORES × 2, 8)`)

**Date:** 2026-06-24
**Plan:** `change-shard-parallelism`
**Status:** Accepted (core-count capture superseded by ADR-049; the `max(NR_OF_CORES × 2, 8)` default formula itself is unchanged)

### Context

The previous default `PARALLELISM_FACTOR` was the magic constant 8. Per-node parallelism is bounded by a fixed VM pool sized to `NR_OF_CORES` (verified against the Exasol engine internals). A default unrelated to actual core count under-subscribes large nodes and may over-subscribe tiny nodes.

### Decision

Capture `NR_OF_CORES` via `SELECT PARAM_VALUE('NR_OF_CORES')` during `createVirtualSchema` (in the same connect-back session already opened for `SELECT NPROC()`). Use `max(NR_OF_CORES × 2, 8)` as the default `PARALLELISM_FACTOR` when the property is absent or invalid. An explicitly supplied `PARALLELISM_FACTOR` property always overrides the default.

### Options Considered

| Option | Verdict |
|--------|---------|
| `max(NR_OF_CORES × 2, 8)` default | ✓ Chosen — two-times oversubscription absorbs stragglers; floor at 8 preserves prior behaviour when cores unavailable |
| Keep static constant 8 | ✗ Rejected — magic number unrelated to the node's actual core pool |
| Multiplier × 1 | ✗ Rejected — leaves no headroom for straggler absorption |
| Multiplier × 4 | ✗ Rejected — excessive session-startup overhead per interview |

### Consequences

The default shard count scales with the cluster's real core pool, making it sensible for the deployed hardware without requiring operator intervention. The floor at 8 ensures that a dev/single-core VM or a failed `NR_OF_CORES` lookup (→0) does not collapse the factor. The user retains full control via the `PARALLELISM_FACTOR` property.

---

## ADR-024: Per-Instance CPU Bound via Two Orthogonal VS Properties, Defaulting to 1

**Date:** 2026-06-24
**Plan:** `change-shard-parallelism`
**Status:** Accepted

### Context

DataFusion's `SessionConfig::new()` defaults `target_partitions` to the host core count. With Exasol multiplexing up to `NR_OF_CORES` concurrent UDF instances per node, the effective thread count is `instances × target_partitions` ≈ `NR_OF_CORES²` (e.g. 32 × 32 = 1024 on a 32-core node) — massive oversubscription that thrashes the node. Tokio was already `new_current_thread()` (correct), but `target_partitions` was never overridden.

### Decision

Expose two independent VS properties — `DATAFUSION_TARGET_PARTITIONS` (→ DataFusion `target_partitions`) and `DATAFUSION_THREADS_PER_UDF` (→ Tokio runtime kind / worker threads). Both are stored as `DF_TARGET_PARTITIONS` / `DF_THREADS_PER_UDF` in `adapterNotes`, round-tripped into new optional `ScanSpec` fields (`df_target_partitions`, `df_threads_per_udf`, both `#[serde(default)]` to 1), and consumed in the scan UDF. With the defaults each UDF instance uses exactly one core; the cluster-level shard fan-out provides the parallelism. The recommended scale-up formula `max(1, floor(NR_OF_CORES / parallelism_factor))` is documented guidance only — the defaults stay 1 unless the operator overrides them.

### Options Considered

| Option | Verdict |
|--------|---------|
| Two orthogonal VS properties, both default 1 | ✓ Chosen — each is a clean operator lever; default 1+1 = one core per instance |
| A single combined "df_parallelism" knob | ✗ Rejected — DataFusion partition count and Tokio worker threads are orthogonal |
| Leave `target_partitions` at DataFusion's default | ✗ Rejected — this is the `instances × cores` oversubscription bug |
| Auto-derive `target_partitions` from cores in code | ✗ Rejected — hides cross-layer coupling, removes operator control |

### Consequences

By default each scan UDF instance uses exactly one core (one DataFusion partition, one Tokio thread). The cluster-level shard fan-out is the single, predictable source of parallelism and a node is never oversubscribed by default. Operators may raise both settings for workloads that benefit from intra-instance parallelism. The `ScanSpec` fields are optional with serde defaults so pre-existing serialized specs deserialize unchanged (backward-compatible ABI extension).

## ADR-025: Subtract a Constant Container-Overhead Before Applying the Memory-Pool Fraction

**Date:** 2026-06-24
**Plan:** `change-memory-pool-sizing`
**Status:** Accepted

### Context

`build_runtime_env` sized the DataFusion memory pool to `0.6 × ctx.memory_limit()`. `ctx.memory_limit()` is the per-process `RLIMIT_RSS` cap, which counts everything resident in the process — the Rust SLC binary, shared libraries, allocator arenas, and stacks — before DataFusion's allocator takes over. The Rust SLC container consumes roughly 150 MB of that budget at startup. Treating the full RSS limit as DataFusion-available systematically over-allocates the pool, pushing each instance closer to the engine's 80% concurrency-stall threshold and increasing OOM risk on dense nodes.

### Decision

The new positive-limit formula is `budget = max(fraction × (limit − overhead_bytes), MIN_POOL_FLOOR_BYTES)`. The overhead is subtracted as an absolute byte count before applying the fraction. Both `MEMORY_POOL_FRACTION` (default `0.6`) and `INSTANCE_OVERHEAD_MB` (default `200`) are VS properties, recorded in `adapterNotes`, round-tripped at pushdown, and carried in each per-shard `ScanSpec`. Only `ctx.memory_limit()` is read at UDF runtime.

### Options Considered

| Option | Verdict |
|--------|---------|
| Subtract a constant overhead before applying the fraction | ✓ Chosen — overhead is constant in absolute bytes, not proportional to the limit |
| Lower the fraction (e.g. 0.5) to implicitly account for overhead | ✗ Rejected — a proportional reduction is wrong as the limit scales; a constant subtraction models reality |
| No change (keep `0.6 × limit`) | ✗ Rejected — systematically over-allocates by ~150 MB per instance |

### Consequences

The pool budget is `fraction × (limit − overhead)` for any positive-limit invocation. Subtracting overhead can only lower the budget, so the handbrake invariant (`budget < 0.8 × limit`) is preserved for any non-negative overhead. The formula stays correct as the RSS limit scales. The zero-limit fallback (`DEFAULT_BUDGET_BYTES = 1 GiB`) is unchanged. Both inputs are VS-configurable and default-safe.

## ADR-026: Default `INSTANCE_OVERHEAD_MB` to 200 MB

**Date:** 2026-06-24
**Plan:** `change-memory-pool-sizing`
**Status:** Accepted

### Context

The empirical Rust SLC container overhead at startup (binary + shared libraries + allocator arenas + stacks) is approximately 150 MB. A default for `INSTANCE_OVERHEAD_MB` must be chosen conservatively: under-sizing the overhead causes the pool budget to be too large and risks OOM; over-sizing it merely gives DataFusion a slightly smaller pool.

### Decision

Default `INSTANCE_OVERHEAD_MB` to `200` MB. This adds a ~50 MB cushion over the empirical 150 MB estimate to absorb variance from shared-library versions, allocator arena growth, and stack usage, without materially shrinking the DataFusion pool relative to the pre-change `0.6 × limit` formula.

### Options Considered

| Option | Verdict |
|--------|---------|
| 200 MB | ✓ Chosen — 50 MB margin over the empirical estimate; OOM risk lowered without material pool reduction |
| 150 MB (empirical estimate) | ✗ Rejected — no margin for variance; under-sizing risks OOM, which is strictly worse than a slightly smaller pool |
| 256 MB | ✗ Rejected — unnecessarily large; shrinks the pool by an extra 56 MB versus 200 MB with no additional safety |

### Consequences

On the default 4096 MB per-instance limit the new budget is `0.6 × (4096 − 200) = 2338 MB`, versus `0.6 × 4096 = 2458 MB` previously — a ~120 MB reduction, well within the engine's `0.8 × 4096 = 3277 MB` handbrake. Operators may tune `INSTANCE_OVERHEAD_MB` via a VS property if their container footprint is measurably different.

## ADR-027: Confine Multi-Table VS Change to the VS-Adapter Layer; Scan Crate Unchanged

**Date:** 2026-06-24
**Plan:** `change-multi-table-virtual-schema`
**Status:** Accepted

### Context

Expanding the virtual schema from a single fixed table (`TABLE_NAME`) to an entire Iceberg namespace required choosing where in the stack to make the change. The verified Exasol VS protocol behaviour is that Exasol issues one single-table pushdown per table, even for JOINs — Exasol joins per-table result sets itself. This means each pushdown is already single-table and the scan layer (`ScanSpec`, `CatalogProps`, the scan UDF, and the sharding/fan-out SQL) does not need to be widened.

### Decision

Keep `ScanSpec`, `CatalogProps`, the scan UDF, and the sharding/fan-out SQL unchanged. Multi-table capability is implemented entirely in the VS-adapter layer: table identity moves from a create-time-fixed property to a per-pushdown value derived from `involvedTables[0].name` via the `TABLE_MAP` in `adapterNotes`.

### Options Considered

| Option | Verdict |
|--------|---------|
| Confine the change to the VS-adapter layer; scan crate unchanged | ✓ Chosen — the Exasol protocol already issues one single-table pushdown per table; widening the scan seam is dead complexity |
| Carry multiple tables in a single `ScanSpec`/pushdown | ✗ Rejected — Exasol issues one single-table pushdown per table even for JOINs, so a multi-table scan seam would never be invoked |

### Consequences

The scan crate and UDF are unchanged and continue to handle exactly one Iceberg table per invocation. Future multi-table scan pushdown (e.g. DataFusion JOIN) remains a separate plan. The VS-adapter layer alone carries the per-table identity routing.

## ADR-028: Persist Exasol-Name to Iceberg-Identifier Map in adapterNotes (Strategy B)

**Date:** 2026-06-24
**Plan:** `change-multi-table-virtual-schema`
**Status:** Accepted

### Context

Pushdown requests carry `involvedTables[0].name` as an uppercased, `__`-flattened Exasol table name. Recovering the original-cased, multi-level Iceberg `TableIdent` from this name at pushdown time requires either re-listing the catalog or reading a pre-built map. Strategy (A) re-lists the namespace at pushdown and matches case-insensitively. Strategy (B) records the `EXASOL_NAME → original-cased Iceberg identifier` map in `adapterNotes` at create time and reads it back at pushdown.

### Decision

Use strategy (B): `createVirtualSchema` enumerates the namespace (required regardless to build `schemaMetadata.tables`) and records a `TABLE_MAP` in `adapterNotes`; pushdown reads it back without a second catalog round-trip. `adapterNotes` is the proven persisted round-trip channel (`CLUSTER_NODES`, `NR_OF_CORES`, etc.).

### Options Considered

| Option | Verdict |
|--------|---------|
| adapterNotes `TABLE_MAP` (strategy B) | ✓ Chosen — no per-query catalog call; exact casing and multi-level path recovered deterministically; collision-detectable at create time; reuses proven channel |
| Re-list namespace at pushdown and case-insensitive match (strategy A) | ✗ Rejected — adds a catalog call per query; requires implementing signed `list_namespaces`/`list_tables` for the SigV4/Glue path (only `load_table` is signed today); casing recovery is heuristic, not exact |

### Consequences

`adapterNotes` grows by one `TABLE_MAP` entry (a JSON object mapping Exasol names to Iceberg identifiers). Pushdown never re-lists the catalog; recovery of original casing and multi-level namespace path is exact. `__` name collisions are detected at create time and fail loudly. Iceberg view support remains deferred (iceberg-rust 0.9.1 `Catalog` trait has no `list_views`).

## ADR-029: Sound-Partial Iceberg Predicate Translation — Strict OR/NOT Handling

**Date:** 2026-06-24
**Plan:** `add-iceberg-predicate-pruning`
**Status:** Accepted

### Context

Translating the Exasol WHERE predicate into an `iceberg::expr::Predicate` for file-level pruning requires a policy for nodes that cannot be translated (e.g. LIKE, REGEXP_LIKE, scalar-function predicates). The policy must never produce a predicate that drops result rows — only a predicate that keeps too many files is safe. The subtle correctness trap is in `OR` and `NOT`: pruning on only the translatable branch of an `OR` can skip files that the untranslatable branch could match. `NOT` of an unknown expression similarly cannot be safely negated.

### Decision

`to_iceberg_predicate` returns `Option<Predicate>` where `None` = "no constraint — treat as no-op". Under `AND` a `None` child is dropped and the other child is kept (dropping a conjunct under AND only widens the surviving file set — sound). Under `OR`, ANY `None` child collapses the whole `OR` to `None` (pruning on the translatable branch alone would wrongly skip files containing rows that match the untranslatable branch). `NOT` of a `None` child returns `None` (cannot soundly negate an unknown). Leaves translate only when the column resolves in the Iceberg schema and a type-matching `Datum` can be built; otherwise `None`.

### Options Considered

| Option | Verdict |
|--------|---------|
| Sound-partial: drop untranslatable nodes; strict OR/NOT collapse | ✓ Chosen — DataFusion is the correctness backstop; less pruning is always safe; the strict OR/NOT rule is the correctness core |
| Decline pushdown / error when any node is untranslatable | ✗ Rejected — forfeits the optimisation for any query with a LIKE; DataFusion already handles correctness |
| Prune on the translatable branch of an OR alone | ✗ Rejected — unsound: a row matching the untranslatable branch may live in any pruned file; would silently drop result rows |

### Consequences

Any query with an `OR` involving a non-translatable predicate receives no Iceberg pruning — it falls back to full file resolution while DataFusion applies the full filter. This is safe and correct. Queries with translatable `AND` conjuncts alongside LIKE predicates do receive pruning on the translatable part. The contract mirrors `render_df_filter_safe`'s existing conservative approach.

## ADR-030: New `adapter/iceberg_predicate.rs` Module; `iceberg-rust` Types Stay out of `vs-expression`

**Date:** 2026-06-24
**Plan:** `add-iceberg-predicate-pruning`
**Status:** Accepted

### Context

Iceberg file-level pruning requires constructing `iceberg::expr::Predicate` values from the Exasol filter JSON. Two candidate homes exist: the shared `crates/vs-expression` crate (already parses and translates Exasol filter JSON for DataFusion) or a new lakehouse-engine-specific module. The `vs-expression` crate is designed to be shared with the sibling project and is intentionally free of `iceberg-rust` dependencies.

### Decision

Author a dedicated `crates/lakehouse-engine/src/adapter/iceberg_predicate.rs` module in lakehouse-engine. It consumes the same raw Exasol filter JSON that the DataFusion path reads and emits `Option<iceberg::expr::Predicate>`. The `vs-expression` crate is not extended.

### Options Considered

| Option | Verdict |
|--------|---------|
| New `adapter/iceberg_predicate.rs` in lakehouse-engine | ✓ Chosen — keeps `iceberg-rust` types out of the cross-project-shared `vs-expression` crate; iceberg coupling lives only where iceberg is already a dependency |
| Extend `vs-expression` to also emit `iceberg::expr::Predicate` | ✗ Rejected — would add `iceberg-rust` as a dependency of `vs-expression`, polluting the cross-project sharing contract |

### Consequences

`vs-expression` remains `iceberg-rust`-free and sharable with the sibling project unchanged. The Iceberg predicate translation is a lakehouse-engine concern co-located with the rest of the file-resolution path. Any future sibling project needing Iceberg pruning would add its own translator or trigger a monorepo consolidation.

## ADR-031: Adopt Arrow-IPC `emit_batch` on the Raw-Row Scan Path

**Date:** 2026-06-26
**Plan:** `change-bounded-remote-scans`
**Status:** Accepted

### Context

The raw-row scan path previously converted each `RecordBatch` to a `Vec<Value>` and emitted rows one at a time. For large result sets (e.g. a ~60M-row join on the live AWS Glue cluster) this held two full copies of every batch in memory simultaneously: the original `RecordBatch` and the `Vec<Value>` copy built from it. This double-materialization was measured as a root cause of the OOM-induced VM crashes observed on the live 3-node cluster.

### Decision

Emit each `RecordBatch` via the SDK's `EmitBatch` API (the `emit-arrow` feature, SDK 0.19.0), which serializes the batch to Arrow IPC bytes internally. Only IPC bytes cross the `.so` boundary; no typed Arrow objects and no `Vec<Value>` intermediate are present on the raw-row path. Each batch is fetched, emitted, and dropped before the next is fetched. (The migration landed on 0.19.0, which also introduced the live debug surface; the plan was drafted against 0.18.0 docs.)

### Options Considered

| Option | Verdict |
|--------|---------|
| Arrow-IPC `emit_batch` via `EmitBatch` API | ✓ Chosen — removes the `Vec<Value>` copy entirely; IPC bytes preserve the Arrow-TypeId-ABI safety rule (no typed Arrow objects cross the `.so` boundary) |
| Keep row-by-row `Vec<Value>` emit, shrink batches only | ✗ Rejected — still holds two full copies of every batch at peak; reduces footprint but does not eliminate double-materialization |
| Emit raw Arrow types across the `.so` boundary | ✗ Rejected — violates the Arrow-TypeId stability contract; Arrow `TypeId` is not stable across dynamic-library boundaries |

### Consequences

Peak per-batch memory footprint on the raw-row path is approximately halved (one `RecordBatch` at a time, no simultaneous `Vec<Value>` copy). A `normalize_view_types` pass (Utf8View→Utf8, BinaryView→Binary) is required before `emit_batch` because DataFusion 58 can produce view types that the IPC encoder rejects; this is applied as a pre-emit normalization step.

## ADR-032: Surface `ResourcesExhausted` as a Distinct Clean Error, Not a Storage Error

**Date:** 2026-06-26
**Plan:** `change-bounded-remote-scans`
**Status:** Accepted

### Context

When the DataFusion memory pool is exhausted (a `ResourcesExhausted` condition) and `/tmp` is not spill-capable disk (as on the live cluster where `/tmp` is RAM-backed tmpfs), the UDF had no spill backstop. The existing `redact_storage_error` path reclassified any scan error — including `ResourcesExhausted` — as "assigned data could not be read", masking the true cause and leaving the operator without actionable information to right-size cores or parallelism.

### Decision

Classify a `ResourcesExhausted` condition before the storage-redaction step and surface it as a clean memory-exhaustion error, distinct from the "assigned data could not be read" wrapping. The classification is applied on all scan error paths (both the raw-row path and all five partial-aggregate error sites). Credential redaction is applied on both the memory-exhaustion and storage-error paths.

### Options Considered

| Option | Verdict |
|--------|---------|
| Classify `ResourcesExhausted` before redaction; surface as memory error | ✓ Chosen — gives the operator the true cause; enables right-sizing of cores/parallelism; satisfies the mission's "usable engine" constraint |
| Let `ResourcesExhausted` fall through the existing `redact_storage_error` path | ✗ Rejected — reclassifies a genuine memory bound as a misleading storage-read failure; the operator cannot distinguish OOM from a corrupted/missing file |

### Consequences

Operators running queries that hit the memory pool ceiling on a tmpfs cluster receive a clean error identifying memory/resource exhaustion rather than a confusing storage error. The bounded clean error is the correct backstop when `/tmp` cannot spill; `probe_tmp_spill` returning `NoDisk` for tmpfs remains correct and is not changed.

## ADR-033: Bound Parquet Decode Working Set via `batch_size` in `session_config_for_spec`

**Date:** 2026-06-26
**Plan:** `change-bounded-remote-scans`
**Status:** Accepted

### Context

DataFusion's `GreedyMemoryPool` / `FairSpillPool` bound aggregation, sort, and join memory — but NOT the Parquet→Arrow decode and scan buffers. On the live AWS Glue cluster, Parquet decode for wide tables at the DataFusion default batch size was a measured root cause of peak memory spikes that pushed instances past the per-node free limit before the pool could throttle them. The `batch_size` session config is the lever DataFusion exposes to bound that out-of-pool working set.

### Decision

Set `batch_size` in `session_config_for_spec` from a `df_batch_size` field carried in the `ScanSpec`. A spec lacking the field deserializes to a conservative built-in default (backward compatible). A sub-1 value is clamped to 1. The setting is applied on both the raw-row scan path and the partial-aggregate path, since both decode Parquet source files.

### Options Considered

| Option | Verdict |
|--------|---------|
| Set `batch_size` in `session_config_for_spec` from spec field | ✓ Chosen — bounds the out-of-pool decode working set; spec-sourced so the VS can tune it per deployment; backward-compatible default for existing specs |
| Rely on the memory pool alone | ✗ Rejected — the pool does not account for Parquet→Arrow decode buffers; high-cardinality wide tables can exceed the per-instance limit before the pool throttles |

### Consequences

Per-batch peak memory on Parquet decode is bounded by `batch_size` rather than left at the DataFusion default (8192 rows). The conservative default shrinks per-instance footprint at the cost of slightly more DataFusion scheduling overhead per batch. The `df_batch_size` field follows the same JSON round-trip + backward-compat-default pattern as `df_target_partitions`.

## ADR-034: AUTO/FIXED Threading Mode Resolved at the Thin VS; Integers Only to the UDF

**Date:** 2026-06-27
**Plan:** `change-engine-throughput`
**Status:** Accepted

### Context

DataFusion's `SessionConfig::new()` defaults `target_partitions` to the host core count. Without an explicit setting, each UDF instance spawns core-count partitions, oversubscribing the node by `(instances × cores)`. The prior behaviour supplied explicit `DATAFUSION_TARGET_PARTITIONS` / `DATAFUSION_THREADS_PER_UDF` VS properties with a FIXED numeric default, but there was no safe auto-derive path that respects the per-node UDF fan-out, and no way for an operator to pin values for repeatable benchmarks without giving up the safety invariant.

### Decision

A `DATAFUSION_THREADING_MODE` VS property (values `AUTO` or `FIXED`, default `AUTO`, case-insensitive) selects how thread/partition budgets are computed at `createVirtualSchema` time. In `AUTO` mode the adapter derives `df_threads_per_udf = max(1, floor(NR_OF_CORES / udf_instances_per_node))` with `df_target_partitions` held in lockstep, so `(udf_instances_per_node × df_threads_per_udf) ≤ NR_OF_CORES`. In `FIXED` mode, supplied values are used verbatim (each defaulting to `max(NR_OF_CORES, 1)` when absent). Only the resolved integer fields ever reach the scan UDF — the UDF stays mode-agnostic.

### Options Considered

| Option | Verdict |
|--------|---------|
| AUTO/FIXED mode in the adapter; integers only to the UDF | ✓ Chosen — honors "auto but overridable" (Q3); preserves thin-VS / stateless-UDF boundary; lockstep prevents oversubscription in AUTO; operator can pin for benchmarks in FIXED |
| Always-auto (no override) | ✗ Rejected — operator cannot pin values for repeatable benchmark sweeps |
| Always-manual (FIXED only) | ✗ Rejected — easy to misconfigure and oversubscribe the node |
| Bake a fixed thread count at compile time | ✗ Rejected — no config-driven tuning without recompilation |

### Consequences

Operators get a safe default (AUTO) that does not oversubscribe a node and a FIXED lever for benchmark sweeps. The UDF layer is unchanged: it consumes `df_threads_per_udf` and `df_target_partitions` as integers regardless of how they were derived. Note: AUTO's anti-oversubscription premise is most valuable for CPU/memory-bound workloads; I/O-bound far-VPC scans can benefit from deliberate oversubscription (see ADR-038).

## ADR-035: Telemetry Built on Archived Checkpoint Infrastructure; Default OFF

**Date:** 2026-06-27
**Plan:** `change-engine-throughput`
**Status:** Accepted

### Context

Throughput bottleneck attribution required phase timing (startup / object-storage import / send-back/emit) per UDF VM, but the project had no instrumentation surface. A fresh telemetry module would duplicate concurrency-safety and per-PID isolation work already proven in the `archive/udf-diagnostics-checkpoints` branch (`scan/diagnostics.rs`: per-PID file isolation, monotonic sequence, best-effort RSS sampling). The lc-rs 0.19.0 debug surface provided the per-VM-tagged emit channel keyed by `LAKEHOUSE_UDF_DEBUG_LEVEL`.

### Decision

Restore `scan/diagnostics.rs` from `archive/udf-diagnostics-checkpoints` and add three monotonic-clock phase accumulators (startup / object-storage import / send-back) wired at existing checkpoint sites. Gate all emission on the lc-rs debug level (`LAKEHOUSE_UDF_DEBUG_LEVEL`); emit nothing at the production default `info`. Every telemetry write is best-effort and never fails a scan.

### Options Considered

| Option | Verdict |
|--------|---------|
| Restore archived checkpoint infra; add phase boundaries | ✓ Chosen — reuses proven concurrency-safe, per-PID, crash-durable infra (Q2); zero production overhead; object-storage import phase isolates S3 travel cost for future in-VPC plan |
| Fresh telemetry module from scratch | ✗ Rejected — duplicates already-proven concurrency-safety and per-PID isolation; Q2 explicitly endorsed reuse |

### Consequences

Zero production overhead: final benchmark runs execute with telemetry OFF. The object-storage-import timing becomes the measurement lever for the future S3-in-VPC plan. The archived `diagnostics.rs` checkpoint trail (~170 lines) is restored but has zero production callers beyond the telemetry functions; flagged as trimmable-to-telemetry-only if the dormant checkpoint trail is unwanted (see review findings in the plan decision log).

## ADR-036: Decode-Emit Overlap Buffer Is Conditional / Measure-First; Not Committed

**Date:** 2026-06-27
**Plan:** `change-engine-throughput`
**Status:** Accepted

### Context

A bounded producer/consumer queue between the DataFusion stream and the emit calls could theoretically overlap S3 read latency with SLC send-back time. However, the `scan-execution` streaming discipline (fetch-one / emit / drop) was designed to bound memory, and adding a buffer introduces concurrency complexity and a held-in-memory batch per slot.

### Decision

Do NOT build the bounded `DF_MAX_BUFFERED_BATCHES` producer/consumer buffer. The `scan-execution` spec describes it only as a conditional capability gated on phase-telemetry evidence (see `datafusion-scan/scan-execution-telemetry`). The committed scenario is authored only after the gate passes: the phase telemetry must show that the emit phase and the object-storage import phase are both material AND serialized, and that decoupling them yields a measured throughput gain.

### Options Considered

| Option | Verdict |
|--------|---------|
| Commit buffer only after telemetry gate passes | ✓ Chosen — avoids speculative work; the gate failed (ADR-037): emit ~2ms vs import ~650ms, so the buffer yields essentially zero gain on the far-VPC workload |
| Build the buffer immediately | ✗ Rejected per Q1 — measure first; the S3-in-VPC plan + telemetry must first show read and emit don't already overlap and that decoupling pays |

### Consequences

The streaming discipline (fetch-one / emit / drop) stays intact. Memory is bounded by the `batch_size` lever. The gate result (ADR-037) confirmed this decision: the scan is overwhelmingly import-bound on the far-VPC path; overlapping a ~2ms emit with a ~650ms import yields essentially zero wall-clock gain.

## ADR-037: Scope Split — Engine Features Get Specs; Benchmarks and Sweeps Are Tasks Only

**Date:** 2026-06-27
**Plan:** `change-engine-throughput`
**Status:** Accepted

### Context

The throughput plan combined two kinds of deliverables: (1) engine feature changes with stable behavioral contracts (threading mode, telemetry capability, repartition-free pipeline guarantee, Parquet pruning flag, adapter-notes recording), and (2) benchmark harness, synthetic micro-benchmarks, parameter sweeps, and baseline measurements that measure the engine but are not themselves engine behaviors.

### Decision

Spec deltas cover only engine feature changes (Threading Mode, On-Demand Phase Telemetry, Raw-Scan Pipeline Shape, Parquet Pruning, AdapterNotes recording). The E2E/benchmark harness, synthetic emit/scan benchmarks, parameter sweeps, baseline measurement, and the empirical >1-thread test live in the plan's task list (Tasks 5–9) with no spec scenarios.

### Options Considered

| Option | Verdict |
|--------|---------|
| Spec engine features only; tasks for benchmark/harness/sweep | ✓ Chosen — specs describe product behavior; measurement tooling evolves faster and is not a behavioral contract |
| Spec benchmark behavior too | ✗ Rejected — benchmarks measure the engine, they are not engine capabilities; spec should describe behavior under test, not the test rig |

### Consequences

The spec library remains a description of product behavior, not a test harness manifest. Measurement tooling (bench scripts, sweep drivers, report formats) can evolve without requiring spec changes or ADR entries.

## ADR-038: AUTO Threading Default Safe but Not Fastest for I/O-Bound Remote Scans; FIXED Recommended

**Date:** 2026-06-27
**Plan:** `change-engine-throughput`
**Status:** Accepted

### Context

The threading sweep on the live AWS Glue cluster (NR_OF_CORES=4, PARALLELISM_FACTOR=8, lineitem ~1.7 GB) produced: 1/1 thread/partition → Q4 12.45s; 2/2 → 10.52s; 4/4 → 8.94s (best); 8/8 → 10.02s. AUTO derives `max(1, floor(4/8)) = 1` for this configuration, which is +39% slower than the optimal 4/4. The root cause: the scan is I/O-bound (S3 across the VPC), so threads overlap S3 read latency rather than competing for CPU — more threads per instance help up to ≈NR_OF_CORES even though `instances × threads` exceeds the VS-reported core count in this regime.

### Decision

Keep AUTO as the spec'd general safety default (never oversubscribes a CPU-bound or memory-bound workload). Document that the measured-optimal config for far-VPC I/O-bound remote scans is `DATAFUSION_THREADING_MODE=FIXED` with `DATAFUSION_THREADS_PER_UDF = DATAFUSION_TARGET_PARTITIONS = NR_OF_CORES`. The bench harness and recommended production config for remote scans use FIXED. A future "I/O-aware AUTO" that deliberately oversubscribes when read-bound is a potential follow-up.

### Options Considered

| Option | Verdict |
|--------|---------|
| AUTO safety default + FIXED lever + documented recommendation | ✓ Chosen — principled: safety by default, tunable by measurement; avoids hardcoding oversubscription for all deployments |
| Change AUTO to always oversubscribe | ✗ Rejected — correct for I/O-bound remote scans, wrong for CPU/memory-bound or local-storage deployments |
| Remove AUTO, FIXED only | ✗ Rejected — loses the safety invariant for deployments that don't tune |

### Consequences

Operators running far-VPC remote scans should set `DATAFUSION_THREADING_MODE=FIXED` with threads=cores for best throughput. The AUTO default remains correct and safe for all other workload shapes. The empirical result (8 instances × 4 threads = 32 concurrent threads on a 4-core node yields best results) documents that the I/O-bound assumption breaks the core-count anti-oversubscription heuristic.

## ADR-039: Throughput Bottleneck Is Far-VPC S3 Read Latency; UDF Engine Is Not the Limiter

**Date:** 2026-06-27
**Plan:** `change-engine-throughput`
**Status:** Accepted

### Context

Live-cluster baseline was ~0.2 GB/s, 5× short of the 1 GB/s target. After delivering all engine-side levers (Parquet `pushdown_filters`, lean repartition-free plan, row-group/page pruning, optimal threading, projection/partial-agg pushdown), the measured improvement was ~30–40% rather than the 5× needed to reach 1 GB/s.

### Decision

The engine-side levers in this plan are delivered and measurably improve throughput, but the ~5× gap is dominated by S3 read latency across the VPC, confirmed three independent ways: (a) phase telemetry: object-storage import ≈650ms vs emit ≈2ms per shard VM — the scan is overwhelmingly import-bound; (b) threading results: throughput improves by overlapping S3 waits, not by adding compute; (c) native Exasol `IMPORT FROM PARQUET` of the same lineitem files reaches the same ~0.17 GB/s ceiling (10.07s wall-clock) — the VS path is competitive or faster via pushdown. Moving S3 into the VPC is the highest-value next throughput lever.

### Options Considered

| Option | Verdict |
|--------|---------|
| Continue optimizing the UDF engine path | ✗ Rejected — measurement confirms UDF layer is not the limiter; native IMPORT reaches the same ceiling |
| Move S3 into the VPC (separate future plan) | ✓ Endorsed — the only lever that can close the ~5× gap; removes the dominant latency source |

### Consequences

The throughput plan is complete as an engine-side optimization effort. The next throughput action is the S3-in-VPC plan. The UDF layer overhead (vs native IMPORT) is small enough that the VS path is not the bottleneck. The IMPORT FROM PARQUET benchmark result is recorded as the reference ceiling for future comparison.

## ADR-040: Catalog Auth Credentials Live on `ConnectionCreds`, Never on the UDF-Boundary Payload

**Date:** 2026-06-29
**Plan:** `add-rest-catalog-oauth-auth`
**Status:** Accepted

### Context

REST-catalog authentication (a static bearer `token` or an OAuth2 `client_id`/`client_secret` exchange) had to be threaded into the unsigned catalog build. `CatalogProps` and `StorageProps` are serialized into `ScanSpec`, which crosses the stateless UDF boundary; the scan UDF never calls the catalog. Catalog secrets must never cross that boundary.

### Decision

Carry `token`, `client_id`, `client_secret`, `oauth2_server_uri`, `scope` (all `Option<String>`) on `ConnectionCreds`, strictly within the planning layer. Inject the corresponding `iceberg-catalog-rest` props inside `build_rest_catalog`, which already receives `creds` via `resolve_file_list`. No auth field is ever placed in `CatalogProps`/`StorageProps`/`ScanSpec`.

### Options Considered

| Option | Verdict |
|--------|---------|
| Auth fields on `ConnectionCreds`, injected at catalog-build time | ✓ Chosen — keeps secrets in the planning layer; preserves the stateless-UDF boundary |
| Widen `CatalogProps`/`ScanSpec` with the auth fields | ✗ Rejected — would carry catalog secrets across the UDF boundary the scan node never needs |

### Consequences

The "UDFs are stateless and never authenticate to catalogs" architecture boundary holds. A unit test (`scan_spec_carries_no_catalog_auth_props`) guards the invariant that no auth field name or value appears in a serialized scan spec.

## ADR-041: SigV4 and Catalog Token/OAuth Authentication Are Mutually Exclusive, Rejected at Validation

**Date:** 2026-06-29
**Plan:** `add-rest-catalog-oauth-auth`
**Status:** Accepted

### Context

The AWS Glue SigV4 path signs the `load_table` request with static AWS credentials and bypasses `RestCatalogBuilder`; catalog token/OAuth authenticates to the REST catalog itself through the builder props. A CONNECTION that enables both expresses two conflicting strategies, and the SigV4 path would silently drop the token/OAuth props.

### Decision

Reject a CONNECTION that sets `use_sigv4` while also supplying any catalog-auth field (`token`, or `client_id`/`client_secret`), with a credential-safe error. The check runs before the SigV4 S3-field guard so the dominant configuration conflict surfaces first.

### Options Considered

| Option | Verdict |
|--------|---------|
| Explicit mutual-exclusivity error | ✓ Chosen — an operated engine must not silently ignore supplied credentials |
| Let SigV4 win and silently ignore token/OAuth | ✗ Rejected — a silent misconfiguration trap |

### Consequences

Misconfigured CONNECTIONs fail fast with a clear message. `has_catalog_auth()` returns true even on partial OAuth (lone `client_id`), so a SigV4 + partial-OAuth combo trips this guard rather than the incomplete-OAuth branch.

## ADR-042: Static S3 Fields Are Unconditionally Optional; `warehouse` the Only Always-Required Field

**Date:** 2026-06-29
**Plan:** `add-rest-catalog-oauth-auth`
**Status:** Accepted

### Context

`REQUIRED_CRED_KEYS` required all five of `warehouse`, `endpoint`, `region`, `access_key`, `secret_key`. Source review of `iceberg-catalog-rest` 0.9.1 showed catalog auth and S3 storage credentials are fully orthogonal: `authenticate()` supports a no-auth mode and `use_vended_credentials` governs S3 vending independently — even an unauthenticated catalog can vend S3 credentials. The flat five-field requirement was pre-existing over-strictness that rejected valid vended/token/OAuth configurations.

### Decision

Reduce base required-field validation to `warehouse` only. The four S3 fields become optional at the base level, independent of catalog auth and `use_vended_credentials`. (The SigV4 path retains a conditional requirement — see ADR-043.)

### Options Considered

| Option | Verdict |
|--------|---------|
| `warehouse`-only base requirement; S3 fields optional | ✓ Chosen — matches the crate's auth/storage orthogonality; loosening never rejects a previously valid password |
| Keep all five always required | ✗ Rejected — forces dummy S3 values for vended/token/OAuth catalogs |
| Require S3 only when no catalog auth present | ✗ Rejected — a no-auth catalog can still vend, so the conditional mismodels the crate |

### Consequences

Existing static-S3 connections continue to validate and behave identically (they already supply all five fields); only acceptance widens. Backward compatibility is verified by test.

## ADR-043: When SigV4 Is Enabled, `access_key`/`secret_key`/`region` Are Required (Orthogonal to Vending)

**Date:** 2026-06-29
**Plan:** `add-rest-catalog-oauth-auth`
**Status:** Accepted

### Context

ADR-042 loosens base validation to `warehouse`-only. But the Glue path signs the `load_table` request with exactly `access_key`/`secret_key`/`region` (`sign_request`, service `glue`) BEFORE any vended credentials are swapped in. Without a guard, a `use_sigv4` connection missing those — previously caught by the flat `REQUIRED_CRED_KEYS` — would pass validation and fail later with an opaque signing error: a Glue-path regression.

### Decision

When `use_sigv4` is true, require `access_key`, `secret_key`, and `region` to be present and non-empty; reject with a credential-safe error naming the missing field(s) and referencing SigV4. Apply this regardless of `use_vended_credentials` (the static creds sign the catalog request first). `endpoint` is excluded — it is not fed to the signer.

### Options Considered

| Option | Verdict |
|--------|---------|
| Conditional SigV4 guard on the three signer fields | ✓ Chosen — restores the SigV4 safety net precisely scoped to the fields the signer consumes |
| Rely on ADR-042's `warehouse`-only validation for all cases | ✗ Rejected — reintroduces an opaque late signing failure on the Glue path |

### Consequences

Non-SigV4 cases stay as loose as ADR-042 specifies; the SigV4 path keeps its fail-fast validation. The guard holds even with vending enabled.

---

## ADR-044: Unify Table Loading Behind One Auth-Mode-Agnostic Self-Issued `loadTable` GET

**Date:** 2026-06-30
**Plan:** `change-vended-credentials-auth-orthogonal`
**Status:** Accepted

### Context

`resolve_file_list` previously extracted vended S3 credentials ONLY on the `use_sigv4` branch by self-issuing a signed `loadTable` GET. The unsigned branch called `iceberg-catalog-rest` 0.9.1's `RestCatalog::load_table`, which returns only a `Table` and silently discards the response `config`/`storage_credentials`. Because there is no public hook to recover those fields, the crate path cannot surface vended creds — so on the no-auth, static-bearer-token, and OAuth2 paths the adapter shipped static storage to every scan spec, and vended STS credentials never reached DataFusion. For Databricks Unity Catalog managed storage — where no usable static S3 creds exist — this was a hard failure.

### Decision

Replace the `use_sigv4` if/else split in `resolve_file_list` with a single `load_table_any_auth` function that returns the raw `LoadTableResult`. Its auth arm is chosen by catalog-auth mode: SigV4 signature | `Authorization: Bearer <token>` | OAuth2-grant-derived bearer | none. The one response feeds both Iceberg file planning and vended-credential extraction on every mode. Vended extraction becomes a single cross-cutting step gated solely on `use_vended_credentials`, never on the catalog-auth mode.

### Options Considered

| Option | Verdict |
|--------|---------|
| Single auth-mode-agnostic `load_table_any_auth` returning raw `LoadTableResult` | ✓ Chosen — one response feeds both planning and vending on every mode; eliminates the `RestCatalog`-drops-config problem; satisfies the `use_vended_credentials` orthogonality principle |
| Keep `RestCatalog::load_table` for unsigned modes and layer vending on top | ✗ Rejected — `iceberg-catalog-rest` 0.9.1's `load_table` returns a `Table` and discards `config`/`storage_credentials` with no public hook to recover them; the crate path structurally cannot vend |

### Consequences

Every catalog-auth mode now self-issues the `loadTable` GET and returns the raw response. `use_vended_credentials` is completely orthogonal to authentication: vended extraction runs on no-auth, static bearer token, OAuth2, and SigV4 identically. The `load_table_signed` function and both `use_sigv4` if/else branches in `resolve_file_list` / `resolve_table_schema` are removed as dead code. SigV4/Glue skips the `/v1/config` prefix lookup and uses the warehouse ARN directly; non-SigV4 uses the `overrides.prefix` from the config endpoint, falling back to an empty prefix (matching the REST spec) rather than the warehouse.

---

## ADR-045: Perform the OAuth2 Client-Credentials Grant In-Adapter

**Date:** 2026-06-30
**Plan:** `change-vended-credentials-auth-orthogonal`
**Status:** Accepted

### Context

To support OAuth2 client-credentials as a catalog-auth mode in the unified `load_table_any_auth` loader (ADR-044), the adapter must obtain a bearer token before issuing the self-issued `loadTable` GET. The `iceberg-catalog-rest` crate's internal token cache (`authenticate()`, `exchange_credential_for_token`) is `pub(crate)` and tied to the crate's own request pipeline; it cannot authenticate a self-issued GET issued outside the crate's `HttpClient`. With ADR-044 requiring a self-issued GET on every mode, the only viable path is to perform the grant directly in the adapter.

### Decision

The adapter issues its own form-encoded `client_credentials` POST (`grant_type=client_credentials`, `client_id`, `client_secret`, optional `scope`) to `oauth2_server_uri` or the catalog default token endpoint (`{catalog_uri}/v1/oauth/tokens`), returning the `access_token` used as the `Authorization: Bearer` header for the self-issued `loadTable` GET. The grant runs once per query (resolve-once-per-query); the `client_secret` and obtained bearer token are redacted from every error path.

### Options Considered

| Option | Verdict |
|--------|---------|
| Perform the OAuth2 grant in-adapter via a direct `client_credentials` POST | ✓ Chosen — orthogonality requires OAuth2 to be covered alongside bearer and no-auth on the self-issued GET path; the grant is small and runs once per query |
| Reuse `iceberg-catalog-rest`'s internal token cache / `authenticate()` | ✗ Rejected — the crate's machinery is `pub(crate)` and bound to its own request pipeline; it cannot authenticate a self-issued request |

### Consequences

The adapter is responsible for the OAuth2 client-credentials round-trip on every query where `client_id` + `client_secret` are supplied. Token refresh and re-vending are not implemented (resolve-once-per-query; STS lifetime far exceeds a single query). The `client_secret` and obtained access token must never appear in error messages or SQL output — enforced by `redact_catalog_auth_error`. No new crate dependencies are required (uses `reqwest`, already a dependency).

---

## ADR-046: Field-Id-Based Column Projection via a PhysicalExprAdapter, Not the Iceberg Reader

**Date:** 2026-07-01
**Plan:** `fix-scan-field-id-projection`
**Status:** Accepted

### Context

The scan engine bound columns by physical Parquet column name, which diverges from the Iceberg column-projection spec (field-id based). Under a rename, physical `score` and current logical `rating` share field-id 2 but not a name, so binding failed — returning wrong or missing data. The correct fix is to bind by Iceberg field-id. Two mechanisms were evaluated: iceberg-rust's `ArrowReader` / `iceberg-datafusion`, and a custom `PhysicalExprAdapter` installed on the `ListingTable`. The two-Arrow-versions constraint (DataFusion/SDK on arrow/parquet 58; iceberg 0.9.1 on aliased arrow 57) rules out the iceberg-rust reader path, because iceberg types cannot cross into the DataFusion session. DataFusion 54 dictates the `PhysicalExprAdapter` mechanism: `with_schema_adapter_factory` is a deprecated no-op in DataFusion 54.

### Decision

Bind columns by Iceberg field-id inside a custom `FieldIdExprAdapter` installed on the `ListingTable` via `ListingTableConfig::with_expr_adapter_factory`, keeping the whole fix in arrow-58 / DataFusion. The adapter resolves each logical column to its physical Parquet column by matching the logical field's `PARQUET:field_id` against the physical fields' `PARQUET:field_id`, independent of physical name. The Parquet opener applies the adapter per file, so files with divergent physical layouts within one shard each bind correctly.

### Options Considered

| Option | Verdict |
|--------|---------|
| Custom `FieldIdExprAdapter` + `ListingTableConfig::with_expr_adapter_factory` | ✓ Chosen — only clean path; stays within arrow-58/DataFusion; adapter is applied per file so divergent physical layouts within one shard bind correctly |
| iceberg-rust `ArrowReader` / `iceberg-datafusion` | ✗ Rejected — iceberg 0.9.1 uses aliased arrow 57; arrow types cannot cross into the DataFusion 54 session (arrow TypeId boundary) |
| `with_schema_adapter_factory` (deprecated) | ✗ Rejected — deprecated no-op in DataFusion 54; does not apply the adapter |

### Consequences

Field-id projection is correct across Iceberg schema evolution (renamed, dropped, added columns). No new crate dependencies are required: `datafusion_physical_expr_adapter` traits are already available via DataFusion 54, and `parquet::arrow::PARQUET_FIELD_ID_META_KEY` is on the arrow-58 side. The per-file adapter application means a single `ListingTable` over files with divergent physical layouts is handled correctly without pre-reading each file's schema.

---

## ADR-047: Override Resolution Only; Reuse DefaultPhysicalExprAdapter for Everything Else

**Date:** 2026-07-01
**Plan:** `fix-scan-field-id-projection`
**Status:** Accepted

### Context

With the `FieldIdExprAdapter` approach selected (ADR-046), the scope of the custom adapter had to be defined. The adapter must handle renamed columns (bind by field-id), but also nullable columns absent from older files (NULL-fill), columns with type divergence (cast), and required columns missing from a file (clean error). Reimplementing all of these behaviors from scratch duplicates the battle-tested `DefaultPhysicalExprAdapter` logic.

### Decision

`FieldIdExprAdapter` overrides only the column-resolution step (field-id-first, with a simple physical-name fallback when a file field carries no embedded `PARQUET:field_id`) and delegates null-fill (nullable column absent from a file → NULL literal), type-diff → cast, and required-missing → clean error to `DefaultPhysicalExprAdapter`. The per-column spec data carried across the UDF boundary is `{field_id, name, arrow_type, nullable}` with no Iceberg `initial-default` (deferred to #27). The `schema.name-mapping.default` table property is also not parsed (deferred to #28).

### Options Considered

| Option | Verdict |
|--------|---------|
| Override resolution only; delegate null-fill/cast/error to `DefaultPhysicalExprAdapter` | ✓ Chosen — keeps the change minimal and correct; added-nullable NULL-fill and added-required clean-error fall out for free from the default adapter |
| Reimplement a full custom schema adapter | ✗ Rejected — duplicates battle-tested DataFusion behavior; introduces new code paths for null-fill and cast that could diverge from DataFusion semantics |

### Consequences

The `FieldIdExprAdapter` is a thin wrapper: only the resolution mapping changes. Null-fill, cast, and required-missing-error behavior is inherited from `DefaultPhysicalExprAdapter` and stays in sync with DataFusion upgrades automatically. Out-of-scope behaviors (#27 initial-default fill, #28 name-mapping property) are tracked as separate issues and do not block this change.

---

## ADR-048: Source Cluster Node Count from `UdfContext::node_count()`, Not a Connect-Back `SELECT NPROC()` (Supersedes ADR-006)

**Date:** 2026-07-01
**Plan:** `fix-createvs-cores-nodecount`
**Status:** Accepted

### Context

`resolve_cluster_nodes` (ADR-006) obtained the active node count over a connect-back session (`SELECT NPROC()`) inside a closure whose `?` also propagated the sibling `PARAM_VALUE('NR_OF_CORES')` query's failure. Because `PARAM_VALUE` is not a real Exasol function and always errors, the shared `?` discarded the valid node count too, collapsing `(cluster_nodes, nr_of_cores)` to `(1, 0)` on every real cluster (issue #32). SDK 0.20.0 exposes `UdfContext::node_count() -> u32` from the live UDF handshake metadata, making the node count available in-process with no SQL round-trip, session, or failure mode.

### Decision

Read the active node count from `UdfContext::node_count()` in-process, mapping the neutral `0` (no live handshake — stub/test double/broken handshake) to a `CLUSTER_NODES` default of `1`; any live cluster (single-node included) reports `≥ 1` and is used verbatim. Delete the connect-back branch, the `CONNECTION_NAME` VS property, and the `SELECT NPROC()` query and its `nproc_value_to_count` parsing helper entirely — no defensive connect-back fallback is retained.

### Options Considered

| Option | Verdict |
|--------|---------|
| `UdfContext::node_count()` in-process, `0 → 1` default | ✓ Chosen — the handshake already carries the node count; no session, auth, or transaction; no query that can fail-and-discard the value |
| Keep `SELECT NPROC()` over connect-back (ADR-006 original) | ✗ Rejected — root cause of issue #32's shared-closure `?` discarding the node count; strictly less reliable than an in-process read |
| Keep connect-back as a defensive fallback behind `node_count()` | ✗ Rejected — grep-verified `CONNECTION_NAME`/connect-back is single-purpose (topology only) in this crate; a fallback re-introduces the exact fragile SQL path being removed |

### Consequences

`resolve_cluster_nodes` opens no read-only SQL session for topology discovery. The `CONNECTION_NAME` VS property is no longer read; existing VS instances that set it for this purpose ignore it silently. `CLUSTER_NODES` is now correct on every real cluster (previously collapsed to `1` whenever `NR_OF_CORES` was unset). The `CATALOG_CONNECTION` credential mechanism is untouched.

---

## ADR-049: Source Per-Node Core Count from `available_parallelism()`, Not the Bogus `PARAM_VALUE('NR_OF_CORES')` Connect-Back Query (Supersedes the Core-Count Capture in ADR-023)

**Date:** 2026-07-01
**Plan:** `fix-createvs-cores-nodecount`
**Status:** Accepted

### Context

ADR-023's default-parallelism-factor design captured `NR_OF_CORES` via `SELECT PARAM_VALUE('NR_OF_CORES')` over the same connect-back session used for the node count. `PARAM_VALUE` is not a real Exasol function — the query always fails, and its failure discarded the node count too (see ADR-048; issue #32). The `max(NR_OF_CORES × 2, 8)` default formula itself is unaffected; only the acquisition source for `NR_OF_CORES` changes. `available_parallelism()` is already trusted for the scan UDF's DataFusion `target_partitions` under ADR-023's sibling precedent on the same target clusters.

### Decision

Read the per-node core count from `std::thread::available_parallelism()` on the executing node when the `NR_OF_CORES` override is absent/invalid; treat an unavailable reading as `0` ("unknown"), preserving the downstream parallelism-factor floor-of-8 contract unchanged. Do not add a live-cluster verification task for `available_parallelism()` accuracy. Delete the `SELECT PARAM_VALUE('NR_OF_CORES')` query and its `varchar_value_to_u32` parsing helper.

### Options Considered

| Option | Verdict |
|--------|---------|
| `available_parallelism()` in-process, `0` = unknown sentinel | ✓ Chosen — `PARAM_VALUE` was the root cause of issue #32; `available_parallelism()` is already proven for DataFusion `target_partitions` in this codebase on the same clusters |
| Keep `SELECT PARAM_VALUE('NR_OF_CORES')` over connect-back | ✗ Rejected — not a real Exasol function; never worked; discarded the node count via the shared-closure `?` |
| Add a live-cluster verification task for `available_parallelism()` | ✗ Rejected — redundant given the existing ADR-023 precedent trusting the same source on the same target clusters |

### Consequences

`NR_OF_CORES` auto-detection no longer depends on any SQL session. The `NR_OF_CORES` override contract and its precedence over auto-detection are unchanged (see the plan's decision-log entry [4], not promoted to ADR). The parallelism-factor default formula (`max(NR_OF_CORES × 2, 8)`, ADR-023) is unaffected by this change.

---

## ADR-050: Full Positional Reorder Threading select-list Index Through Grouped-Aggregate Detection

**Date:** 2026-07-01
**Plan:** `fix-grouped-agg-select-order`
**Status:** Accepted

### Context

`detect_group_by_aggregates` walked `pushdownRequest.selectList` and split it into two disjoint lists — `group_keys` and `plans` — discarding each item's original select-list index. `build_grouped_aggregate_scan_sql` then assembled the outer merge SELECT unconditionally keys-first (`gk_select.chain(merge_items)`). Exasol validates the outer merge SELECT positionally against `selectListDataTypes`; whenever an aggregate preceded or interleaved with a group key in the original select list, the adapter's keys-first output was transposed relative to it, producing `SQL Error [04000]: ... Data type mismatch in column number N` (issue #33). The bug had three broken sub-cases sharing one root cause: aggregate before a single key, interleaved multi-key GROUP BY, and an expression group key after an aggregate.

### Decision

Extend `detect_group_by_aggregates` to carry each `selectList` item's original index and classification (group-key projection vs aggregate, with which slot). `build_grouped_aggregate_scan_sql` places the already-computed, already-typed group-key cast expressions and merged-aggregate expressions into the outer SELECT / GROUP BY at the ordinal position dictated by that index, for any interleaving. The inner fan-out (EMITS clause and per-shard scan) stays keys-first and unchanged — it is matched only against itself, never against the user's select list.

### Options Considered

| Option | Verdict |
|--------|---------|
| Full positional reorder threading select-list index through detection | ✓ Chosen — #33's root cause is general column transposition; fixes all three sub-cases (aggregate-before-key, interleaved multi-key, expression-key-after-aggregate) in one change |
| Narrow patch handling only "aggregate before a single group key" | ✗ Rejected — leaves interleaved multi-key and expression-key-after-aggregate broken; only masks the reported repro |

### Consequences

The outer wrapper SELECT, its cast list, and its GROUP BY list now assemble in the user's `selectList` order for any arrangement of keys and aggregates, matching Exasol's positional `selectListDataTypes` check. The inner fan-out EMITS clause and the scan UDF's per-shard SELECT remain keys-first and unaffected (see ADR-051). A HAVING-over-aggregates rendering gap was discovered during E2E verification and fixed in the same change (HAVING containing an aggregate was silently dropped; now rendered against the merge decomposition, fail-closed to native execution when unrenderable) — not itself promoted to a separate ADR, as it is a direct consequence of the same outer-wrapper assembly path.

---

## ADR-051: Keep the Wire Spec (`ScanSpec` / `AggregatePlan`) and Scan UDF Side Unchanged for Grouped-Aggregate Select-List Ordering

**Date:** 2026-07-01
**Plan:** `fix-grouped-agg-select-order`
**Status:** Accepted

### Context

Fixing the outer-wrapper column transposition (ADR-050) raised the question of whether the wire contract between the adapter and the scan UDF — `ScanSpec.group_keys`, `ScanSpec.aggregates`, the inner fan-out EMITS clause, `build_grouped_partial_agg_sql`, and the scan UDF's emit loop — also needed to carry select-list ordering end-to-end. This was independently re-verified (not taken on faith) against `crates/lakehouse-engine/src/scan/mod.rs`.

### Decision

Do not change `ScanSpec.group_keys` / `ScanSpec.aggregates`, the inner fan-out EMITS clause, `build_grouped_partial_agg_sql`, or the scan UDF's emit loop. Confine the fix entirely to the adapter's outer-merge assembly (ADR-050).

### Options Considered

| Option | Verdict |
|--------|---------|
| Keep the wire spec and scan UDF side keys-first and unchanged | ✓ Chosen — verified `build_grouped_partial_agg_sql` (L390-423) and the emit loop (L344-368) are keys-first on both the DataFusion SELECT and the emit order, matched only against the fan-out EMITS clause and never against the user `selectList` |
| Add ordering metadata to the wire spec so keys/aggregates interleave end-to-end | ✗ Rejected — the scan side never sees the user's select order; changing the wire shape would be churn with no correctness benefit |

### Consequences

The scan UDF and the wire contract between it and the adapter are untouched by this fix, minimizing the change surface and risk. The inner fan-out and per-shard scan remain self-consistent keys-first structures, matched only against each other.

---

## ADR-052: Two-Argument `LAKEHOUSE_SCAN(common, files)` — Shard-Invariant Spec Emitted Once, Per-Shard Files Only in `VALUES`

**Date:** 2026-07-02
**Plan:** `fix-scan-spec-shard-dedup`
**Status:** Accepted

### Context

`build_fan_out_inner_with_spec` serialized the full `ScanSpec` — credentials, projection, filter, aggregates, group_keys, logical_schema, emit_exa_types, and all `df_*`/memory tuning knobs — into every shard's single-argument `LAKEHOUSE_SCAN(spec VARCHAR(2000000))` invocation. Only `files` varies between shards; on a wide fan-out (G capped at 300 per the sharding model) the shard-invariant payload was repeated up to ~300 times in one generated statement, risking Exasol statement-size limits and multiplying the credential surface (issue #25). A full field audit (recorded in the plan) additionally found `ScanSpec.catalog: CatalogProps` had no production scan-UDF reader — only tests referenced it; all catalog interaction happens adapter-side before the UDF runs.

### Decision

Split `LAKEHOUSE_SCAN` into a two-argument signature: `LAKEHOUSE_SCAN(common VARCHAR, files VARCHAR)`. The adapter serializes the shard-invariant common spec exactly once as a SELECT-list literal shared by every shard, and places only each shard's file-URI JSON in the `VALUES` rows; `run_scan` reads both arguments and reconstitutes a `ScanSpec` via `ScanSpec::from_parts(common, files)`. Only `Value::String` still crosses the `.so` boundary. Drop `ScanSpec.catalog: CatalogProps` entirely — `ScanSpec` no longer carries a catalog `uri`/`warehouse`/`table` block; the `CatalogProps` type itself stays, still used adapter-side. The field audit's result: `files` is the sole per-shard field, `catalog` is dropped, and every remaining field (`projection`, `filter`, `limit`, `aggregates`, `group_keys`, `emit_exa_types`, `logical_schema`, `storage`, and the `df_*`/memory tuning knobs) is invariant and belongs in the common blob. For grouped queries the common blob is built once with `limit = None`, structurally guaranteeing the "LIMIT never in per-shard partial" invariant (the per-shard LIMIT-strip closure is removed as redundant). No single-argument backward-compatibility path is kept: the `.so`, SLC, and adapter deploy together and the scan SET SCRIPT DDL is recreated per deployment, so no in-flight spec crosses a version boundary.

### Options Considered

| Option | Verdict |
|--------|---------|
| Two-argument UDF: invariant common literal (emitted once) + per-shard files | ✓ Chosen — collapses the ~300× repetition to one common literal per statement; multi-arg UDFs need no new infrastructure (`exasol-udf-macros` 0.20.0 `input(a: T, b: T, ...)`) and only `Value::String` crosses the boundary |
| Connect-back to fetch credentials at scan time | ✗ Rejected — connect-back was deliberately removed from this project by issue #32 / ADR-048; do not reintroduce it |
| Stage the common spec as a BucketFS file referenced by all shards | ✗ Rejected — adds state/staging to a stateless, disposable UDF |
| Keep the single-argument form | ✗ Rejected — that is the bug being fixed |
| Keep `ScanSpec.catalog: CatalogProps` for symmetry / future use | ✗ Rejected — YAGNI; no production scan-UDF reader, and it is a credential-adjacent field that should not sit on the UDF-boundary payload |

### Consequences

The generated fan-out statement carries the shard-invariant payload — including credentials, projection, filter, aggregates, and tuning knobs — exactly once instead of once per shard, shrinking statement size and the credential surface on wide fan-outs. `ScanSpec` carries no catalog block at all, strengthening the existing "catalog auth never in any scan spec" guarantee. The scan SET SCRIPT DDL and every direct invocation move to the two-argument signature; there is no dual-read path, so the `.so`/SLC/adapter must deploy together (already true for this stateless, disposable UDF).

---

## ADR-053: Compact 2-Tuple `(path, size)` Per-Shard File Encoding

**Date:** 2026-07-02
**Plan:** `change-scan-spec-files-payload`
**Status:** Accepted

### Context

`ScanSpec.files` carried bare absolute file-URI strings (`Vec<String>`). Each generated pushdown SQL statement repeated the same ~40–70-char table-location prefix once per file across a fan-out capped at 300 shards (issue #45), and because only the path travelled, the scan UDF's `ListingTable` had to issue a per-file object-store `HEAD` to recover the byte size the adapter had already resolved from the Iceberg manifest and then discarded in `partition_files_by_bytes` (issue #29).

### Decision

Change `ScanSpec.files` from `Vec<String>` to `Vec<(String, u64)>`; the JSON wire form is a compact array of `[path, size]` pairs — exactly what serde produces for the tuple, with no custom (de)serializer. `files_json`/`files_from_json` and `partition_files_by_bytes` are retyped end-to-end to carry the pair instead of the bare path.

### Options Considered

| Option | Verdict |
|--------|---------|
| Compact 2-tuple `[path, size]` array | ✓ Chosen — minimal bytes, serde-native for a `Vec<(String,u64)>`, positional pairing cannot desynchronize |
| Struct-per-file objects `[{"path":...,"size":...}]` | ✗ Rejected — self-describing but roughly 3× the bytes per entry on a payload this repeats across up to 300 shards |
| Parallel arrays `{paths:[...],sizes:[...]}` | ✗ Rejected — compact but easy to desynchronize (a path and its size can drift apart) and awkward to shard |

### Consequences

Every per-shard file entry now carries its byte size alongside its path at no meaningful cost in wire size. `partition_files_by_bytes` must propagate the tuple through sharding rather than dropping the size after using it to balance shards. There is no dual-format decoder for the old bare-string shape (see ADR-056): the adapter that writes a spec and the UDF that reads it ship in the same `.so`.

---

## ADR-054: Carry the Iceberg Table Root Once in the Common Spec; Emit Paths Relative

**Date:** 2026-07-02
**Plan:** `change-scan-spec-files-payload`
**Status:** Accepted

### Context

Every per-shard file path repeated the full Iceberg table-location prefix (`table.metadata().location()`), even though that prefix is identical for every file in the query and is already resolved once at the same seam that vends the table's storage credentials (issue #45). Carrying it per file is pure duplicated overhead on a fan-out that can reach 300 shards.

### Decision

Add `table_root: String` (`#[serde(default)]`, empty ⇒ all-absolute) to `CommonScanSpec` and `ScanSpec`. The adapter threads the already-resolved `result.metadata.location()` out of `resolve_file_list` into the spec builder. Because the root is shard-invariant, it is serialized exactly ONCE in the common blob, never per shard. See ADR-055 for the strip/reconstruct rule this enables.

### Options Considered

| Option | Verdict |
|--------|---------|
| `table_root` field in the common (shard-invariant) blob | ✓ Chosen — the root is already computed at the resolve-once seam as the vended-credential anchor, so forwarding it is free; shard-invariant data belongs in the common blob per the two-argument split (ADR-052) |
| Repeat the full absolute prefix on every file path | ✗ Rejected — this is the status-quo bug (#45) |
| A separate BucketFS-staged prefix table | ✗ Rejected — adds persisted state to a stateless, disposable UDF |

### Consequences

The common spec grows by one string field, serialized once per query regardless of shard count. Every per-shard path can now be emitted relative to this root (ADR-055), which is where the actual byte savings materialize. A legacy or root-less spec (empty `table_root`) degrades safely to "treat every path as absolute."

---

## ADR-055: Strip-If-Prefix / Absolute-Passthrough Path Reconstruction

**Date:** 2026-07-02
**Plan:** `change-scan-spec-files-payload`
**Status:** Accepted

### Context

Iceberg data-file paths are NOT guaranteed to live under `table.metadata().location()` — `write.data.path`, `write.object-storage.enabled` hash injection, and migrated/Databricks layouts can place files elsewhere. Given the table root is now carried once (ADR-054), the adapter needs a rule for when it is safe to strip that root from a file path, and the UDF needs the symmetric rule for reconstructing the absolute path.

### Decision

In the adapter, strip `table_root` from a file path ONLY when `path.starts_with(table_root)` AND the match falls on a real path-segment boundary (the root ends with `/`, or the remainder begins with `/`) — otherwise the path is stored absolute and unchanged. This boundary check (found during code review, R.1) prevents a sibling-prefix false match, e.g. root `s3://bucket/tbl` must not strip from a file under `s3://bucket/tbl-other/...`. In the UDF's `register_files`, reconstruct symmetrically: an entry containing `://` is absolute and parses as-is; a relative entry is joined onto `table_root` (trailing `/` normalized) before `ListingTableUrl::parse`. A shard MAY mix relative and absolute entries. Regression-tested by `sibling_prefix_paths_are_not_relativized`.

### Options Considered

| Option | Verdict |
|--------|---------|
| Strip only on a real path-segment-boundary prefix match; else keep absolute | ✓ Chosen — captures the common-case byte win while staying correct for `write.data.path`, object-storage hash injection, migrated/Databricks layouts, and sibling-prefix false matches |
| Assume all data files live under `metadata.location()`; always strip / always join | ✗ Rejected — simpler but INCORRECT: Iceberg does not guarantee this |

### Consequences

Path stripping is conditional and reversible: the reconstructed absolute path always equals the original resolved data-file URI. A per-shard payload may legitimately mix relative and absolute entries within the same query. The path-segment-boundary refinement (beyond plain `starts_with`) was added during implementation code review and is covered by its own regression test rather than being a purely design-time decision.

---

## ADR-056: Supply File Sizes via a Spec-Backed `ObjectStore` `head()` Wrapper, Keeping `ListingTable` + Field-ID Adapter

**Date:** 2026-07-02
**Plan:** `change-scan-spec-files-payload`
**Status:** Accepted

### Context

With each per-shard file's byte size now available from the spec (ADR-053), the scan UDF no longer needs to issue a per-file object-store `HEAD` to discover it (issue #29) — but the registration path (`ListingTableConfig::new_with_multi_paths(...).with_expr_adapter_factory(FieldIdExprAdapterFactory)`) must keep working, since field-id-based column projection depends on `ListingTable`. Confirmed against DataFusion 54.0.0 + object_store 0.13.2 source: for an exact-file (non-collection) URL, `ListingTableUrl::list_prefixed_files` calls `store.head(&path)` per path and does NOT cache that branch (only the collection/`list` branch uses `list_with_cache`), so an override there is consulted on every query and issues no network HEAD. `last_modified` is not read for scan correctness — ParquetExec reads by known size via `get`/`get_range`; the only consumer of `last_modified` is the optional `FileStatisticsCache`, irrelevant to a per-query disposable UDF.

**Implementation deviation confirmed during coding:** in `object_store` 0.13.2, `head` is NOT itself an `ObjectStore` trait method — it is the auto-implemented `ObjectStoreExt` blanket method that dispatches to `get_opts(GetOptions { head: true, .. })`. The no-HEAD wrapper therefore overrides `get_opts` (short-circuiting the `head: true` case with the spec-backed `ObjectMeta` and delegating every other case to the inner store) rather than overriding a `head()` method directly. This still suppresses the network HEAD exactly as designed, because `store.head(&path)` calls `get_opts` internally. The crate manifest also gained an `async-trait` dependency, required to implement object_store's `#[async_trait] ObjectStore` trait for the wrapper.

### Decision

Keep the existing `ListingTable` + `with_expr_adapter_factory(FieldIdExprAdapterFactory)` wiring. Wrap the registered `AmazonS3` store in a thin `ObjectStore` that intercepts the `head: true` `get_opts` case to return an `ObjectMeta` built from the spec's known size (`last_modified = chrono::Utc.timestamp_nanos(0)`, `e_tag = None`, `version = None`), delegating every other call to the inner store. Register the wrapper in the session `RuntimeEnv`'s `ObjectStoreRegistry` under the same `ObjectStoreUrl`.

### Options Considered

| Option | Verdict |
|--------|---------|
| Spec-backed `ObjectStore` wrapper (overriding the `get_opts`/`head:true` path), keeping `ListingTable` | ✓ Chosen — additive; leaves the entire existing registration and field-id projection path untouched; verified not cached so it is consulted on every query |
| `PartitionedFile::new(path, size)` + `FileScanConfigBuilder::with_expr_adapter(...)` | Kept as documented fallback, not chosen as primary — VIABLE (the builder exposes the same `PhysicalExprAdapterFactory` trait, so field-id projection is retained) but replaces `ListingTable` wholesale, a much larger change |
| Leave the per-file HEAD in place | ✗ Rejected — this is the bug (#29) |

### Consequences

The scan UDF issues no per-file object-store `HEAD` before scanning; `scan-execution-field-id-projection` scenarios keep passing unchanged because the wrapper is purely additive. The wrapper's correctness rests on the object_store 0.13.2 `head`-is-`ObjectStoreExt`-over-`get_opts` behavior confirmed during implementation, not on a literal `head()` trait method — a future object_store upgrade that changes this dispatch would need to be re-verified against this ADR.

---

## ADR-057: One `S3_MAX_CONNECTIONS` Knob, Not a Dual Per-File/Per-Node Pair

**Date:** 2026-07-02
**Plan:** `add-scan-connection-concurrency`
**Status:** Accepted

### Context

The native Exasol `IMPORT FROM PARQUET` importer exposes a dual concurrency model: `MaxConnections` (parallel reads within a file) plus `MaxConcurrentReads` (files in parallel per node). The scan UDF's object-store connection concurrency had no operator-facing knob at all — `build_s3_store` built `AmazonS3Builder` with zero HTTP client tuning, leaving fetch concurrency entirely to defaults and to DataFusion's `target_partitions` file-group splitting. The operator hypothesis driving this plan was a single lever: "max file connections per node to saturate network/IO."

### Decision

Expose a single operator VS property `S3_MAX_CONNECTIONS` (mirroring the native importer's `MaxConnections` vocabulary) rather than mirroring the native importer's full dual model.

### Options Considered

| Option | Verdict |
|--------|---------|
| Single `S3_MAX_CONNECTIONS` knob | ✓ Chosen — covers the stated single-lever hypothesis; avoids unproven complexity |
| Two properties mirroring `MaxConnections` + `MaxConcurrentReads` | ✗ Rejected — a second axis is unproven complexity; defer until a benchmark shows one knob is insufficient |

### Consequences

Establishes the project convention that new tuning axes ship as one operator knob until a benchmark proves a second is needed (YAGNI). The knob follows the exact `PARALLELISM_FACTOR` property → `adapterNotes` → shard-invariant common-spec round-trip precedent, so the scan UDF stays resolution-agnostic.

---

## ADR-058: Apply the Connection Budget via `object_store` `ClientOptions`, Not DataFusion `target_partitions`

**Date:** 2026-07-02
**Plan:** `add-scan-connection-concurrency`
**Status:** Accepted

### Context

Object-store connection concurrency (how many concurrent HTTP connections to S3 a scan instance keeps warm) is a distinct throughput axis from CPU decode/compute concurrency, which the existing DataFusion thread/partition budget (`datafusion-scan/scan-execution-threading`) already governs. The resolved `S3_MAX_CONNECTIONS` budget needed a concrete mechanism to reach the object store's HTTP client.

### Decision

Size the budget onto the S3 client through `AmazonS3Builder::with_client_options(ClientOptions)` (`object_store` 0.13.2, method confirmed present), targeting the HTTP connection pool.

### Options Considered

| Option | Verdict |
|--------|---------|
| `AmazonS3Builder::with_client_options(ClientOptions)` | ✓ Chosen — the object-store HTTP client pool is what genuinely maps to "concurrent fetches from S3 per UDF instance" |
| DataFusion `target_partitions` file-group splitting | ✗ Rejected — that is the CPU/threading axis, already a separate knob |
| `datafusion.execution.meta_fetch_concurrency` | ✗ Rejected — only affects schema/stats reads, not data-scan throughput |

### Consequences

Records that object-store connection concurrency is a first-class tuning axis distinct from the DataFusion thread/partition budget. The budget applies uniformly on both the raw-row scan path and the partial-aggregate path, since both decode Parquet fetched over the same object store.

---

## ADR-059: Confounded Benchmark Evidence Is Incorporated as Rationale Plus a Named Re-Gate Task, Never Immediate Scope Expansion

**Date:** 2026-07-02
**Plan:** `add-scan-connection-concurrency`
**Status:** Accepted

### Context

A 2026-07-01 180M-row / 60-file full-`lineitem` benchmark found native `IMPORT INTO` (~80.4 s) outperforming the VS full-emit `CREATE TABLE AS SELECT *` (~151 s) by ~1.9× — a full raw-row emit workload, differently shaped from the original aggregate-path benchmark. That run recorded the confounded `CLUSTER_NODES=1`, the pre-0.20.1 `ctx.node_count()==0` handshake bug this same plan's dependency bump (Task 1) fixes. It was therefore unknown whether the 151 s/80.4 s gap was under-sharding (would close on the dep bump) or a genuine emit-path bottleneck (e.g. `Int64→Decimal128` coercion).

### Decision

Fold the new evidence into the plan as reinforcing rationale for the existing deliverables, add one named validation task (re-run the 60-file comparison after the dependency bump lands) to isolate the confound, and document the emit-path coercion optimization as evidence-gated deferred work. Do not expand code scope to build the emit-path optimization now.

### Options Considered

| Option | Verdict |
|--------|---------|
| Rationale + named re-gate task + evidence-gated deferred-work doc | ✓ Chosen — isolates the confound before committing to new scope |
| Expand this plan to build the emit-path optimization immediately | ✗ Rejected — YAGNI; no confirmed emit-bound root cause, only a confounded measurement |
| Ignore the new evidence | ✗ Rejected — it materially reshapes the rationale and surfaces a real open question |

### Consequences

Codifies the project rule: new benchmark evidence confounded by an in-flight fix is incorporated as rationale plus a named post-fix re-gate task and evidence-gated deferred-work docs, never as immediate scope expansion, until the confound is isolated.

---

## ADR-060: Advertise `AGGREGATE_GROUP_BY_TUPLE`, Reversing the Prior Exclusion

**Date:** 2026-07-03
**Plan:** `fix-multi-column-group-by-pushdown`
**Status:** Accepted

### Context

The adapter's grouped-aggregate detection, per-key type resolution, and scan-driving SQL builder already handle an arbitrary number of group keys, but `AGGREGATE_GROUP_BY_TUPLE` was excluded from `CAPABILITIES` (per ADR/decision [4] in the 2026-06-22 `add-group-by-and-sql-comprehension` decision log). With the capability absent, Exasol never sends a multi-key GROUP BY as a pushdown request; instead it falls back to a raw row scan that Exasol aggregates itself, shipping every raw row over the network and defeating the reduction in network transfer that grouped pushdown exists to provide (issue #53).

### Decision

Add `AGGREGATE_GROUP_BY_TUPLE` to `CAPABILITIES` so Exasol sends multi-key GROUP BY queries as pushdown requests, reversing the prior exclusion.

### Options Considered

| Option | Verdict |
|--------|---------|
| Advertise `AGGREGATE_GROUP_BY_TUPLE` | ✓ Chosen — the multi-key detection and SQL-building path already exists; the capability flag was the only thing gating it |
| Keep it excluded | ✗ Rejected — multi-column GROUP BY is extremely common; the raw-scan fallback defeats the purpose of grouped pushdown |

### Consequences

A GROUP BY over two or more keys is now pushed down as node-local partial aggregation rather than falling back to a raw row scan, at the cost of the multi-key path needing to be proven correct end-to-end (see ADR-061).

---

## ADR-061: Verify the N-Key Grouped Pushdown Path Before Trusting the Capability Flag

**Date:** 2026-07-03
**Plan:** `fix-multi-column-group-by-pushdown`
**Status:** Accepted

### Context

Issue #53 explicitly noted that the N≥2 group-key path "has not been verified end-to-end," because Exasol never sent a multi-key pushdown request while `AGGREGATE_GROUP_BY_TUPLE` was absent. Advertising the capability (ADR-060) without first verifying `detect_group_by_aggregates`, `group_key_exasol_types`, and `build_grouped_aggregate_scan_sql` against a real multi-key request risked shipping latent defects — group-key ordering, per-key type resolution, and HAVING/LIMIT interaction were all unproven for N≥2 keys.

### Decision

Treat the capability flip as requiring a verification spike across the detection, per-key type-resolution, and scan-SQL-building code paths, budgeting for real bug fixes rather than assuming a one-line flag change would suffice, and add end-to-end test coverage (including EXPLAIN-based pushdown-occurred assertions) for expression-valued group keys, interleaved key/aggregate ordering, HAVING + LIMIT combined with multi-key grouping, and high-cardinality/spill behavior of the node-local partial aggregate.

### Options Considered

| Option | Verdict |
|--------|---------|
| Verification spike + full E2E coverage before shipping the flag | ✓ Chosen — proves the multi-key path actually works rather than assuming it does |
| Ship the flag alone | ✗ Rejected — issue #53 explicitly flagged the N≥2 path as unverified; shipping it blind risks latent multi-key defects |

### Consequences

The multi-key grouped-aggregate path is proven correct (ordering, per-key types, HAVING/LIMIT, spill behavior) before being exposed to Exasol, at the cost of a wider verification/test-authoring scope than a bare capability-flag change.

---

## ADR-062: Fix Scoped to the Constant-Projection-Over-Group-By Shape, Not General Nested/Subquery Aggregate Pushdown

**Date:** 2026-07-03
**Plan:** `fix-nested-aggregate-pushdown`
**Status:** Accepted

### Context

Issue #52 reported a crash on `SELECT COUNT(*) FROM (SELECT L_ORDERKEY, COUNT(*) AS cnt FROM t GROUP BY L_ORDERKEY) t2` — an outer aggregate over an inner grouped-aggregate sub-select. `specs/mission.md` lists "Join pushdown, complex query rewrites" as explicitly out of scope, so any fix had to avoid growing pushdown surface area into general subquery/nested-aggregate composition.

### Decision

Bound the fix to making this specific SQL shape correct-or-safe (correct composed pushdown, or fall back to a non-pushed row-scan) rather than building general multi-level nested-aggregate/subquery pushdown composition as a new adapter capability.

### Options Considered

| Option | Verdict |
|--------|---------|
| Bounded fix: correct-or-safe for this shape only | ✓ Chosen — matches mission's exclusion of complex query rewrites; lower-risk than growing pushdown surface area |
| Add general subquery-pushdown composition as a new capability | ✗ Rejected — out of scope per mission; unbounded scope growth for a single reported defect |

### Consequences

The adapter gains a targeted guard/fix for the constant-projection-over-`GROUP BY` shape (see ADR-063) without adding a general subquery-composition capability. Other nested/subquery shapes not matching this pattern continue to rely on the existing fallback-to-row-scan behavior, which remains within mission scope.

---

## ADR-063: Constant-Projection-Over-`GROUP BY` Placeholder Drives the Existing Grouped Scan Instead of the Row-Scan Path

**Date:** 2026-07-03
**Plan:** `fix-nested-aggregate-pushdown`
**Status:** Accepted

### Context

Empirical capture against the local Exasol Docker + MinIO + Iceberg REST stack showed Exasol does not send a nested `from`/sub-select for `SELECT COUNT(*) FROM (SELECT id, COUNT(*) AS cnt FROM t GROUP BY id) t2`. Instead it sends one flat `pushdownRequest` with `aggregationType: "group_by"`, a real `groupBy: [ID]`, and a `selectList` containing a single `literal_null` placeholder (the optimizer's "count the groups" rewrite, since neither `id` nor the inner `cnt` is needed by the outer query). `detect_group_by_aggregates` (`crates/lakehouse-engine/src/adapter/pushdown.rs:762`) rejected this shape because the placeholder's rendered SQL (`NULL`) matched no group key, falling through to the row-scan path's `extract_projection`, which pushed the rendered literal `NULL` in as a bare projection/`EMITS` column identifier — a phantom column DataFusion rejects (`Schema error: No field named "NULL"`). The row-scan fallback alternative was rejected as unsafe: it returns one row per source row, not per group, which is only accidentally correct when the group key happens to be unique (e.g. the seeded `events.id`) and silently wrong on any table with duplicate group-key values (e.g. TPC-H `LINEITEM.L_ORDERKEY`, the shape in issue #52).

### Decision

Extend `detect_group_by_aggregates`'s non-aggregate `selectList` arm to recognize a pure-literal placeholder item (e.g. `literal_null`) as a "count the groups" constant projection, driving the existing grouped-scan builder with an empty aggregate-plan list instead of forcing it through the group-key-match path (which requires the rendered item to equal a group key). This preserves one-row-per-distinct-group output, which is what Exasol's outer `COUNT(*)` needs to count correctly, for any group-key cardinality. As defence-in-depth, `extract_projection`'s literal-item arm no longer pushes a rendered literal such as `NULL` as a bare projection/`EMITS` column name on the row-scan path.

### Options Considered

| Option | Verdict |
|--------|---------|
| (a) Correct-parsing: treat the literal placeholder as a constant group-count projection and keep driving the grouped scan | ✓ Chosen — correct for all group-key cardinalities; the defect is a translatable-expression-used-as-column-identifier bug, not a malformed request |
| (b) Tighten the guard so the row-scan fallback engages | ✗ Rejected as unsafe — returns raw row count, not distinct-group count, on any table with duplicate group-key values; only accidentally correct on unique-key data |
| Return an error to force native retry | ✗ Rejected — a VS has no native data path, so this just fails a query that Athena/Trino/Spark all answer correctly |

### Consequences

`GROUP BY`-pushdown requests whose `selectList` is a lone constant/literal placeholder (the "count the groups" optimizer rewrite) now drive the grouped scan and return correct per-group-cardinality results instead of crashing. The regression test additionally covers a duplicate-key group column (not just the unique-key seeded `events.id`) to discriminate the correct grouped fix from the unsafe row-scan fallback, since both incidentally return the same count on unique-key data.

---

## ADR-064: Expression Aggregate Arguments Carried on a New `arg_expr` Field, Rendered via `render_expression`

**Date:** 2026-07-03
**Plan:** `add-count-distinct-and-expression-aggregate-pushdown`
**Status:** Accepted

### Context

`SUM(LENGTH(L_COMMENT))`-shaped aggregates could not be pushed down because `AggregatePlan`'s argument capture only accepted a bare column reference, forcing a fall back to a full raw row-scan that ships every projected column to Exasol. The `crates/vs-expression` translator's `render_expression` mechanism already renders arbitrary DataFusion SQL fragments for GROUP BY keys via `detect_group_by_aggregates`, so the same mechanism could extend to aggregate arguments. Two carrier options existed: overload the existing `column: Option<String>` field to also hold rendered SQL, or add a new field alongside it.

### Decision

Add `AggregatePlan.arg_expr: Option<String>` holding the rendered DataFusion SQL fragment, keeping `column: Option<String>` for the bare-column fast path. The scan side uses `arg_expr` verbatim (no `quote_ident`); partial/merge column types for an expression-argument aggregate come from the aggregate's declared type in the parallel top-level `selectListDataTypes` array instead of a source-column type lookup. Applies to `SUM`/`MIN`/`MAX`/`AVG`/`COUNT(col)`; `COUNT(*)` has no argument and is unaffected. An argument the translator cannot render soundly causes the adapter to fall back to row scanning rather than emit an incorrect partial/merge plan.

### Options Considered

| Option | Verdict |
|--------|---------|
| New `arg_expr: Option<String>` field alongside `column` | ✓ Chosen — backward-compatible serde; the bare-column fast path (and its exact source-column type lookup) is untouched; the translator remains the single source of expression-rendering truth, mirroring GROUP BY keys |
| Overload `column` to also carry rendered SQL | ✗ Rejected — bare-column `MIN`/`MAX` partials rely on the exact source-column Exasol type looked up by name; overloading would break that lookup and the existing JSON round-trip |

### Consequences

Aggregate pushdown now decomposes `COUNT(expr)`/`SUM(expr)`/`MIN(expr)`/`MAX(expr)`/`AVG(expr)` over any translator-renderable scalar expression into the same shard-associative partial/merge plan as their bare-column forms, instead of forcing a full row-scan fallback. Partial/merge typing for these aggregates is sourced from `selectListDataTypes` rather than a column lookup, which is a new typing path that must stay in sync with the declared select-list types.

---

## ADR-065: COUNT(DISTINCT) Merged by a Scalar UDF Fed via LISTAGG of Per-Shard JSON Arrays

**Date:** 2026-07-03
**Plan:** `add-count-distinct-and-expression-aggregate-pushdown`
**Status:** Accepted

### Context

`COUNT(DISTINCT col)` over the whole table (no GROUP BY) previously fell back to a full raw row-scan. Decomposing it across shards requires each shard to compute its local distinct value set and the wrapper to union those sets without shipping raw rows or crossing the `.so` boundary with an Arrow type, and without building bespoke SQL rewriting (an explicit mission non-goal).

### Decision

Add a new `AggKind::CountDistinct`. Per shard, compute the LOCAL distinct value set via `array_agg(DISTINCT col)` inside the existing DataFusion scan, excluding NULLs, and serialize it to a JSON array VARCHAR — one partial value per shard, preserving the one-row-per-shard partial wire shape. Merge in the outer wrapper SQL via a new scalar entry point `LAKEHOUSE_DISTINCT_MERGE_COUNT`, fed the shard partials joined with Exasol's native `LISTAGG` into a JSON array-of-arrays string, mixed into the same merge SELECT as the SUM/MIN/MAX partials as an ordinary scalar call. The merge UDF parses the array-of-arrays, unions the elements into a set, and returns its cardinality.

### Options Considered

| Option | Verdict |
|--------|---------|
| Scalar merge UDF fed via `LISTAGG` of per-shard JSON arrays | ✓ Chosen — preserves the one-row-per-shard partial wire shape; only a JSON string crosses the `.so` boundary; the array-of-arrays framing lets JSON escaping handle separator/quote hazards |
| A SET merge UDF with its own grouping protocol | ✗ Rejected — reintroduces a grouping protocol the design explicitly wanted to avoid and complicates queries with multiple `COUNT(DISTINCT)` columns |
| Bespoke SQL string-splitting or `CONNECT BY` hierarchical rewrite | ✗ Rejected — an explicit non-goal (complex query rewrites) |

### Consequences

Single-group `COUNT(DISTINCT col)` is now pushed down instead of falling back to a row scan, via a third scalar entry point (`LAKEHOUSE_DISTINCT_MERGE_COUNT`) added to the same `.so` (see also the packaging spec delta). Grouped `COUNT(DISTINCT)` (inside a GROUP BY) remains out of scope and continues to fall back to row scanning. The design introduces a new merge-time dependency on `LISTAGG`'s output size ceiling, which interacts with the per-shard safety cap (ADR-066).

---

## ADR-066: Execution-Time Per-Shard Safety Cap for COUNT(DISTINCT), With a Clean Error on Overflow

**Date:** 2026-07-03
**Plan:** `add-count-distinct-and-expression-aggregate-pushdown`
**Status:** Accepted

### Context

Once `COUNT(DISTINCT col)` is advertised and pushed down (ADR-065), a high-cardinality column (e.g. an order-key-like column) could otherwise accumulate an unbounded per-shard distinct set, risking memory exhaustion or a serialized JSON value exceeding the `VARCHAR(2000000)` wire limit. The mission's bounded-execution stance requires a clean `ResourcesExhausted`-style error over an OOM crash. Iceberg NDV (number-distinct-values) statistics are not reliably available, so a plan-time decline based on NDV was not a dependable primary mechanism.

### Decision

Enforce a mandatory per-shard execution-time safety cap: 100,000 distinct elements AND 1,048,576 bytes (1 MiB) serialized, whichever trips first. On overflow, the scan UDF aborts that shard with a clean bounded-resource error naming the offending column and the cap exceeded, rather than emitting a truncated (silently wrong) distinct set or continuing to accumulate until the process runs out of memory. No plan-time NDV-based decline is implemented as a primary mechanism.

### Options Considered

| Option | Verdict |
|--------|---------|
| Execution-time per-shard cap (element count + serialized bytes) → clean error | ✓ Chosen — safer default given unreliable Iceberg NDV stats; bounds both pre-serialization memory/CPU and the wire value size with headroom under `VARCHAR(2000000)` |
| Plan-time NDV-based decline to row scan | ✗ Rejected as primary — Iceberg NDV stats are not reliably available; may be considered as a future secondary optimization |

### Consequences

A standalone high-cardinality `COUNT(DISTINCT col)` that previously fell to a (slow but correct) row scan now, once `FN_AGG_COUNT_DISTINCT` is advertised, gets pushed down and may fail the cap with a clean error instead of completing via row scan — an accepted behavioural regression for that specific shape, consistent with the mission's bounded-execution stance. The merge side is separately bounded by `LISTAGG`'s output ceiling. The target use case (low-cardinality dimension columns) is unaffected.

---

## ADR-067: Two-Column Arithmetic Aggregate Gap Is Fixed by Capability Advertisement, Not New Machinery

**Date:** 2026-07-04
**Plan:** `add-arithmetic-aggregate-pushdown-and-benchmark-suite`
**Status:** Accepted

### Context

`SUM(l_extendedprice * l_discount)` (a non-join TPC-H Q6 shape) fell back to a full raw row-scan of both operand columns even though the expression-argument SUM partial/merge machinery (`arg_column_or_expr`, `col_type_for`, `sum_emit_type`) already existed. Reading the code showed the actual blocker: `capabilities.rs` advertised `FN_MOD` but none of the arithmetic binary-operator capabilities, so Exasol's optimizer never constructed a scalar-function pushdown node for `+`/`-`/`*`/`/` at all — it silently requested the raw operand columns instead.

### Decision

Fix the gap by advertising `FN_ADD`/`FN_SUB`/`FN_MULT`/`FN_FLOAT_DIV` and reconciling the translator's operator-name matching, reusing the existing expression-argument SUM partial/merge machinery unchanged.

### Options Considered

| Option | Verdict |
|--------|---------|
| Advertise the missing arithmetic capabilities and reconcile operator names | ✓ Chosen — the downstream decomposition path already exists; the blocker was purely in what the adapter advertises, so this is the smallest sound change |
| Build a dedicated two-column-product decomposition path | ✗ Rejected — redundant new subsystem; `AggKind::Sum` + `arg_expr` already covers a two-column arithmetic argument |
| Assume the translator lacked binary-arithmetic support and build it | ✗ Rejected — reading `crates/vs-expression` showed `ADD`/`SUB`/`MUL`/`FLOAT_DIV` were already rendered; the gap was advertisement, not translation |

### Consequences

`SUM(col_a OP col_b)` for `OP ∈ {*, +, -, /}` now decomposes into the shard-associative partial/merge plan instead of a raw two-column row-scan fallback. Advertising the operators globally (not scoped to SUM arguments) also enables arithmetic in filter/select-list/group-key positions, since Exasol's capability model has no position-scoped advertisement — accepted as a safe, net-positive side effect because untranslatable nodes still fall back correctness-safely.

---

## ADR-068: Arithmetic Operator-Name Reconciliation Is a Hard Live-Verification Gate, Not an Assumption

**Date:** 2026-07-04
**Plan:** `add-arithmetic-aggregate-pushdown-and-benchmark-suite`
**Status:** Accepted

### Context

The `crates/vs-expression` translator matched multiplication as `"MUL"`, based on hand-crafted unit-test JSON — never exercised live, because the capability was unadvertised. Exasol's actual capability/name vocabulary uses `FN_MULT`. Shipping the advertise-and-decompose fix on the unverified `"MUL"` assumption risked advertising `FN_MULT` while the translator declined the resulting `"MULT"` node, silently degrading to row-scan fallback with no speedup (correct but pointless).

### Decision

Made live capture of the exact Exasol `function_scalar` name for `+`/`-`/`*`/`/` a hard gate on the capability/translator change (task 1.1), and require the capability set and the translator's matched-name set to stay in lockstep (enforced by a dedicated test).

### Options Considered

| Option | Verdict |
|--------|---------|
| Verify live before coding, keep capability set and translator names in lockstep | ✓ Chosen — the whole perf win hinges on the translator recognizing what Exasol actually sends |
| Trust the existing `"MUL"` unit tests / spec | ✗ Rejected — those tests used hand-crafted JSON never exercised against a live advertised capability, so the assumed name was unverified and plausibly wrong |

### Consequences

Live capture (decision-log finding [7]) was structurally unsatisfiable in the literal "observe the node" sense (Exasol won't emit an unadvertised node), so verification proceeded via the already-advertised `FN_MOD` naming convention (`FN_<X>` → node name `"<X>"`) plus a native-SQL DECIMAL-inference probe — equally strong evidence without a speculative capability deploy. This confirmed `"MUL"` was wrong and pinned the fix to `"MULT"`, with `ADD`/`SUB`/`FLOAT_DIV` already correct.

---

## ADR-069: Parallelism-Factor Sweep Is Evidence-Gated; a Validated No-Op Is an Acceptable Outcome

**Date:** 2026-07-04
**Plan:** `add-arithmetic-aggregate-pushdown-and-benchmark-suite`
**Status:** Accepted

### Context

A prior diagnostic flagged a possible 10-30% gain from increasing `BENCH_PARALLELISM_FACTOR` (oversubscription hides emit-ack stall), but marked it UNVERIFIED — plausible only if the workload is emit-ack-latency-bound, zero if CPU-decode-bound. `bench/.env` pins the factor at 8, below the code's own `max(cores*2,8)=16` default. Changing a parallelism default without evidence risks regressing non-join queries.

### Decision

Ship the sweep (factor 8/16/24 vs Q2/Q3/Q5 + a Q9b regression check) as an explicit validation task that precedes any default change; only change `bench/.env` / `resolve_parallelism_factor` if the evidence shows a real, repeatable improvement without a Q9b regression, and record the finding either way.

### Options Considered

| Option | Verdict |
|--------|---------|
| Evidence-gated sweep with a no-op as an acceptable result | ✓ Chosen — avoids shipping a speculative default change that could regress non-join queries |
| Hardcode `BENCH_PARALLELISM_FACTOR=16` (or change the code default) now | ✗ Rejected — the 10-30% gain was explicitly unverified and the workload's bottleneck (emit-ack vs CPU-decode) was unknown |

### Consequences

The sweep (`bench/parallelism_sweep.sh`), run twice against the live test1 cluster, found pf16 flat (within run-to-run noise, no consistent direction) and pf24 a real, repeatable regression (Q3 and Q9b both worse in both runs) — over-oversubscription adds scheduling overhead that outweighs any stall-hiding benefit. `bench/.env`'s factor of 8 and the code's `resolve_parallelism_factor` default were left unchanged; ruling out this optimization on solid evidence is treated as a valid, durable result, with the sweep tooling itself the lasting artifact.

---

## ADR-070: Raw-Scan Projection Gets an Explicit `ProjectionItem` Tag Instead of a Syntactic Heuristic

**Date:** 2026-07-04
**Plan:** `add-arithmetic-aggregate-pushdown-and-benchmark-suite`
**Status:** Accepted

### Context

`extract_projection` pushed both bare column names and rendered scalar-expression fragments into the same `Vec<String>` (`spec.projection`). `build_scan_sql` then treated every entry as a bare identifier, wrapping a rendered expression fragment like `("SCORE" * 2)` in `quote_ident(...)` and producing a phantom column name — reproduced live as `e2e_selectlist_expression_pushdown` failing with `F-UDF-CL-RUST-9001 ... No field named "(""SCORE"" * 2)"`. The two projection-item kinds were structurally indistinguishable downstream, the same "column vs. rendered expression looks like a string either way" problem `AggregatePlan`'s `column`/`arg_expr` split already solves for aggregates.

### Decision

Change `ScanSpec.projection` / `CommonScanSpec.projection` from `Vec<String>` to `Vec<ProjectionItem>`, a `#[serde(untagged)]` enum with `Column(String)` and `Expr { expr: String }`, tagged at the point `extract_projection` already knows the distinction. `build_scan_sql` matches the variant: `Column` keeps its existing CAST-for-JSON-fallback + `quote_ident` behavior; `Expr` is spliced verbatim, exactly like `spec.filter` and `agg_arg_sql`.

### Options Considered

| Option | Verdict |
|--------|---------|
| Explicit `ProjectionItem` tag (`Column`/`Expr`), mirroring `AggregatePlan` | ✓ Chosen — the distinction is already known upstream; tagging it there is the same established pattern as `column`/`arg_expr` |
| Syntactic heuristic in `build_scan_sql` (contains `(`/quote/operator → expression) | ✗ Rejected — fragile; an exotic quoted Exasol column name would be misclassified |
| "Is it a field in the Arrow schema?" as the discriminator | ✗ Rejected — implicit, and silently changes the defensive path for a bare column absent from the schema |
| A parallel `Vec<bool>` / `Vec<Option<String>>` alongside the string vec | ✗ Rejected — positional-alignment fragility, worse than a single tagged type |

### Consequences

`#[serde(untagged)]` keeps the wire format for the common all-columns case byte-for-byte unchanged (a `Column` still serializes as a bare JSON string), and `From<&str>`/`From<String>`/`PartialEq<&str>` keep existing call sites and assertions compiling unchanged — only the row-scan test helpers, `build_scan_driving_sql` call sites, and `extract_projection`-output assertions needed edits. The `column`/`arg_expr`/`ProjectionItem` pattern (never overload one string field for both a bare identifier and rendered SQL) is now the established convention for anything crossing the adapter→scan spec boundary.

---

## ADR-071: Close the NQ4 Top-N Loss by Advertising ORDER_BY_COLUMN + a Partial/Merge Top-N

**Date:** 2026-07-04
**Plan:** `add-topn-pushdown`

**Status:** Accepted

### Context

NQ4 (`SELECT L_ORDERKEY, L_EXTENDEDPRICE FROM lineitem ORDER BY L_EXTENDEDPRICE DESC LIMIT 20`) lost to Trino 12.03s vs 4.71s on TPC-H sf=30 test1 because the adapter advertises no `ORDER_BY*` capability, so Exasol never delegates the ordering and the adapter raw-emits the whole table for Exasol to sort. The join-pushdown non-goal does not apply here — it is a non-join, single-table loss, and the standing directive (carried over from the sibling arithmetic-pushdown plan) is to optimize legitimately-fixable non-join losses.

### Decision

Advertise `ORDER_BY_COLUMN` and push `ORDER BY <bare projected col(s)> LIMIT n` down as a per-shard bounded top-N (each shard runs its own `ORDER BY … LIMIT n`) merged by an Exasol-side outer `ORDER BY … LIMIT n`, reusing the SHAPE of the existing aggregate partial/merge machinery rather than its aggregate-specific code.

### Options Considered

| Option | Verdict |
|--------|---------|
| Advertise `ORDER_BY_COLUMN` + per-shard bounded top-N + outer merge | ✓ Chosen — the blocker is purely the unadvertised capability; this is the smallest sound change and mirrors the just-shipped aggregate partial/merge shape |
| Leave it as a raw scan and accept the loss | ✗ Rejected — a non-join, single-table query squarely within the standing "optimize where legitimately possible" directive |
| Change file-sharding to co-locate top rows | ✗ Rejected — violates the sharding-architecture non-goal |
| Build a general ORDER BY pushdown (expression keys, offset, ordered aggregates) | ✗ Rejected as over-scope — column top-N covers the target and the common shape |

### Consequences

NQ4 now flips from lakehouse-engine-rs's largest competitive loss to a win: 12.03s → 2.13s (5.65x), also beating Trino's 4.71s. The new path is a new partial/merge variant sitting alongside the aggregate path, not a new architecture.

---

## ADR-072: Whether the Top-N Change Is Pure Optimization or Also a Latent Correctness Fix Is Gated on Live Capture

**Date:** 2026-07-04
**Plan:** `add-topn-pushdown`

**Status:** Accepted

### Context

Reading the code alone shows `extract_limit` unconditionally reads `pushdownRequest.limit.numElements` and the row-scan branch pushes that limit to every shard via the common spec with no ORDER-BY-awareness — so IF Exasol ever sent a bare `limit` alongside an unpushed `order_by` today, the adapter would silently truncate to an arbitrary per-shard subset. Whether this ever happens in practice is unknown from the code alone.

### Decision

Make a live `EXPLAIN VIRTUAL` capture of the NQ4 shape against test1 (task A1) a hard gate, before writing any code, to determine whether Exasol pushes a bare `limit` for an ORDER BY query it can't also delegate today.

### Options Considered

| Option | Verdict |
|--------|---------|
| Live-capture gate before coding | ✓ Chosen — the sibling plan's methodology is live-capture-first, and the cost of assuming wrong is a silent correctness bug |
| Infer "safe" from the correct-but-slow captured NQ4 result and skip verification | ✗ Rejected — the code path shows a real theoretical truncation risk that must be verified, not assumed |

### Consequences

A1 confirmed Exasol structurally withholds `limit` whenever the accompanying `order_by` can't also be delegated — today's code path can never exercise the truncation danger, so this plan is pure optimization, not a bugfix. The defensive invariant (ADR-074) is adopted anyway, because advertising `ORDER_BY_COLUMN` is exactly what starts putting `order_by` + `limit` together in future requests.

---

## ADR-073: Advertise ORDER_BY_COLUMN Only; ORDER_BY_EXPRESSION and LIMIT_WITH_OFFSET Stay Absent

**Date:** 2026-07-04
**Plan:** `add-topn-pushdown`

**Status:** Accepted

### Context

The NQ4 target and the common top-N shape are both served by column sort keys with no OFFSET. Advertising expression ordering and/or an OFFSET would add rendering and bounded-sort-with-skip complexity with no evidenced need.

### Decision

Advertise only `ORDER_BY_COLUMN`. Keep `ORDER_BY_EXPRESSION` and `LIMIT_WITH_OFFSET` unadvertised so Exasol structurally never pushes an expression sort key or an OFFSET the adapter has no path for.

### Options Considered

| Option | Verdict |
|--------|---------|
| `ORDER_BY_COLUMN` only | ✓ Chosen — matches the codebase's "advertise only what the backing path supports" discipline; makes unsupported shapes structurally impossible in the request rather than something to defensively decline at runtime |
| Also advertise `ORDER_BY_EXPRESSION` and/or `LIMIT_WITH_OFFSET` | ✗ Rejected — no evidenced need, adds complexity |

### Consequences

The capability surface stays exactly as wide as the backing implementation. Unprojected/expression sort keys and OFFSET queries simply never appear in the request shape the adapter must handle.

---

## ADR-074: Never Push a Bare Per-Shard LIMIT Ahead of a Global Sort

**Date:** 2026-07-04
**Plan:** `add-topn-pushdown`

**Status:** Accepted

### Context

Once `ORDER_BY_COLUMN` is advertised, Exasol will send `order_by` + `limit` together for ordered queries. A bare per-shard limit pushed ahead of a global sort would let each shard return an arbitrary (not top-ranked) subset, silently truncating the true top-N for any ORDER BY shape the adapter does not match as a top-N.

### Decision

Emit the per-shard row limit ONLY alongside the matching per-shard `ORDER BY` (the matched top-N shape). For any ORDER-BY-carrying request the adapter does not match as a top-N, withhold the per-shard limit and leave row selection to the Exasol-side ordering.

### Options Considered

| Option | Verdict |
|--------|---------|
| Per-shard limit gated on a matching per-shard sort | ✓ Chosen — the invariant that makes advertising the capability safe across every shape, not just the optimized one |
| Keep pushing the per-shard limit whenever a `limit` is present (today's row-scan behavior) | ✗ Rejected — becomes unsafe the moment `order_by` can accompany a `limit` |

### Consequences

Asserted directly as a spec scenario and a unit test (`order_by_present_without_topn_match_withholds_per_shard_limit`). During implementation (B5), a related but distinct gap was found live: an ORDER BY over an unprojected column combined with LIMIT returns wrong (unsorted, untruncated) results because Exasol does not always re-apply its own backstop ordering once it has delegated both clauses together — tracked as a known residual, not a regression, since the old behavior was fail-silent and the new one fails loud.

---

## ADR-075: Returned SQL for the Matched Top-N Is Self-Contained, Not Dependent on an Exasol Re-Sort Backstop

**Date:** 2026-07-04
**Plan:** `add-topn-pushdown`

**Status:** Accepted

### Context

The pre-existing model for `LIMIT`/`HAVING` relies on Exasol re-applying the clause it pushed as a correctness backstop. For the top-N path, depending on "does Exasol re-sort?" is an avoidable class of risk, and the outer wrapper already exists for the shard fan-out.

### Decision

The matched top-N path returns an outer `SELECT <proj> FROM (<fan-out>) ORDER BY <keys> LIMIT n` that fully specifies the final ordering itself, rather than relying on Exasol to re-apply the pushed `ORDER BY`.

### Options Considered

| Option | Verdict |
|--------|---------|
| Self-contained outer `ORDER BY … LIMIT n` | ✓ Chosen — removes the matched path's dependence on Exasol's re-sort behavior; the outer wrapper already exists so adding the ordering is cheap |
| Rely on the Exasol backstop, as `LIMIT`/`HAVING` do | Kept as the safety net for the UNmatched decline shapes only, not for the matched path |

### Consequences

Live verification (C1) confirmed the outer merge SQL renders its own final `ORDER BY … LIMIT` independent of Exasol. During implementation (B5), the unmatched/decline shapes were found to need the same defensive treatment for the grouped-aggregate path, which had never rendered an `ORDER BY` at all and relied entirely on an Exasol backstop that does not always apply — fixed by rendering an explicit final `ORDER BY`/`LIMIT` on every path that can receive an `order_by`-carrying request.

---

## ADR-076: Direction and NULL Placement Must Be Rendered Identically Per-Shard and in the Merge

**Date:** 2026-07-04
**Plan:** `add-topn-pushdown`

**Status:** Accepted

### Context

The distributed top-N is exact only if the per-shard bounded sort and the Exasol-side merge sort induce the identical ranking. If the scan UDF's default NULL placement differs from Exasol's, the per-shard cut and the merge disagree near NULLs and the top-N silently diverges from single-node results.

### Decision

Render an explicit `ASC`/`DESC` and `NULLS FIRST`/`NULLS LAST` on BOTH the per-shard `ORDER BY` (scan UDF) and the outer merge `ORDER BY` (adapter), using the direction/NULL semantics captured live from Exasol's wire shape (`isAscending`, `nullsLast`), rather than relying on either engine's default NULL ordering.

### Options Considered

| Option | Verdict |
|--------|---------|
| Explicit direction + NULL placement on both sides | ✓ Chosen — the one subtle correctness detail where a silent default mismatch would corrupt ranking near NULLs |
| Render only direction, let NULL ordering default on each side | ✗ Rejected — if DataFusion's and Exasol's defaults differ, per-shard and merge disagree on ranking |

### Consequences

Covered by a dedicated NULL-placement unit test (`ordered_scan_sql_preserves_desc_and_null_placement`) and verified live against test1 (C1: outer merge rendered `ORDER BY "L_EXTENDEDPRICE" DESC NULLS FIRST LIMIT 20`, matching the per-shard spec).

---

## ADR-077: Decline the Top-N Shape When a Sort Key Column Needs the JSON-Fallback VARCHAR Cast

**Date:** 2026-07-04
**Plan:** `add-topn-pushdown`

**Status:** Accepted

### Context

Discovered during implementation (B3b/B4): for a sort key column whose Arrow type needs the JSON-fallback VARCHAR cast (List/Struct/out-of-range-Decimal/etc.), the per-shard `ORDER BY` binds against the real native value in the FROM-clause row source, but the emitted, projected value is a JSON string. Exasol's outer merge only ever sees that emitted JSON string and re-ranks it lexicographically — so per-shard and merge would disagree on ranking and silently corrupt the global top-N, even though each shard's own local top-N stays internally correct. Not triggered by NQ4 (`L_EXTENDEDPRICE` is a plain in-range DECIMAL), but a ship-blocking gap for any future ordered-top-N over a fallback-typed column.

### Decision

`detect_topn` resolves each sort key column's Arrow type from the resolved logical schema and declines the whole ordered-top-N shape (falling back to the safe raw-scan path) whenever that type needs the JSON fallback cast, or when the column is absent from the logical schema.

### Options Considered

| Option | Verdict |
|--------|---------|
| Decline the top-N shape for JSON-fallback-typed sort keys | ✓ Chosen — always correctness-safe; worst case a rare shape falls back to the raw scan |
| Emit the sort key uncast and cast only a duplicate projection column | ✗ Rejected — needs extra trailing EMITS columns dropped by the outer SELECT, disproportionate to a shape no evidenced query hits |
| Sort the merge on the pre-cast value | ✗ Rejected — impossible; Exasol only ever receives the emitted representation |

### Consequences

Covered by a unit test (`json_fallback_typed_sort_key_declines_topn`). The guard is evaluated on the same type info the scan path already uses for its own `needs_json_fallback` cast decision, so it becomes fully load-bearing the moment the logical-schema tag vocabulary is enriched to preserve richer types beyond the current out-of-range-Decimal case.

---

## ADR-078: Shape-Aware Zero-Files Short-Circuit via a Hoisted Plan Decision

**Date:** 2026-07-04
**Plan:** `fix-aggregate-pushdown-empty-file-pruning`

**Status:** Accepted

### Context

`handle_pushdown` resolved the file list once, then short-circuited on zero files by returning the raw row-scan empty shape (`empty_pushdown_sql`) unconditionally — before the aggregate-shape detection (`detect_group_by_aggregates`, single-group `detect_aggregates`) ran later in the function. For an aggregate or grouped-aggregate request, this made the short-circuit return the wrong column count/shape, which Exasol rejected as a positional pushdown mismatch (`sqlCode 04000`). Both detection functions are pure over `pushdown_req` and do not depend on the resolved files, so every input needed to synthesize a shape-correct empty result was already available without any file.

### Decision

Move the request-shape decision ahead of the `files.is_empty()` short-circuit and dispatch to three empty-result builders (grouped zero-row / single-group one-row / row-scan projection), reusing the existing `detect_*` and type helpers so the empty shape is always derived from the same sources as the non-empty shape.

### Options Considered

| Option | Verdict |
|--------|---------|
| Hoist the plan-shape decision and dispatch to shape-specific empty builders | ✓ Chosen — detection is pure and file-independent, so it can be hoisted with no new I/O, and reusing shared helpers guarantees empty/non-empty shape parity |
| Pass an "is-aggregate" flag into `empty_pushdown_sql` only | ✗ Rejected — still would not carry grouped shape or per-`AggKind` semantics |
| Let the empty case fall through the normal fan-out with zero shards | ✗ Rejected — a single-group `COUNT` merges as `SUM(PARTIAL_count)` over zero fan-out rows = `NULL` (wrong, should be `0`), and a grouped fan-out over zero shards is malformed |

### Consequences

Empty and non-empty column shapes can never drift apart, since both derive from the same detection and type helpers. Covered by unit tests for each plan shape (`empty_files_single_group_aggregate_emits_zero_and_null_row`, `empty_files_count_distinct_emits_zero_no_merge_udf`, `empty_files_grouped_aggregate_emits_zero_rows_grouped_shape`, `empty_files_shape_matches_non_empty_plan_priority`) and end-to-end tests confirming Exasol accepts the response over a real all-pruned query.

---

## ADR-079: Pin iceberg 0.10.0-rc.2 via git tag, not a crates.io exact-version pin

**Date:** 2026-07-06
**Plan:** `change-iceberg-rust-0-10-bump`
**Status:** Accepted

### Context

The bump target, iceberg-rust 0.10.0-rc.2, is a pre-release whose API can still churn. Verification during planning showed crates.io publishes `iceberg`/`iceberg-catalog-rest`/`iceberg-storage-opendal` only up to 0.9.1 — 0.10.0-rc.2 exists only as a git tag (`v0.10.0-rc.2`, commit `be6cc96eaeb1cac4574cabb11ea6e1e92e0aad45`) of `apache/iceberg-rust`, never published to crates.io. The interview's intent was that a later RC/GA bump be deliberate and reviewed, not automatic.

### Decision

Pin `iceberg`, `iceberg-catalog-rest`, and `iceberg-storage-opendal` to the git tag `v0.10.0-rc.2` (commit `be6cc96eaeb1cac4574cabb11ea6e1e92e0aad45`) of `apache/iceberg-rust`, rather than a crates.io version string.

### Options Considered

| Option | Verdict |
|--------|---------|
| Git tag pin (`v0.10.0-rc.2`) | ✓ Chosen — the only mechanism that resolves the RC at all; the tag is immutable and human-readable, so a later RC/GA requires an explicit edit |
| `version = "=0.10.0-rc.2"` from crates.io | ✗ Rejected — crates.io does not publish this version; it does not resolve |
| Bare `rev = <sha>` | ✗ Rejected — resolves the same commit but loses the human-readable tag reference |

### Consequences

The dependency pin is immutable and self-documenting; bumping to a later RC or the GA release is an explicit, reviewed edit rather than something that happens automatically on `cargo update`.

---

## ADR-080: Unify the production/iceberg arrow tree on 58; do not bump the workspace arrow major

**Date:** 2026-07-06
**Plan:** `change-iceberg-rust-0-10-bump`
**Status:** Accepted

### Context

iceberg 0.9.1 linked arrow 57, creating a split with the rest of the workspace on arrow 58. Verification against the iceberg 0.10.0-rc.2 tag's `Cargo.toml` confirmed it is on arrow/parquet 58, so the split can be collapsed by the bump alone. A tempting alternative was to instead move the whole workspace to arrow 59 to match `tpchgen-arrow` 3.0.0.

### Decision

Let the bump collapse the production arrow tree onto arrow 58 with no change to the workspace `arrow`, `datafusion`, or SDK pins.

### Options Considered

| Option | Verdict |
|--------|---------|
| Keep workspace on arrow 58; let iceberg 0.10 collapse onto it | ✓ Chosen — eliminates the 57/58 split with zero change to datafusion, the SDK, or other pins |
| Move workspace arrow to 59 (matching tpchgen-arrow 3.0.0) | ✗ Rejected — drags datafusion 54, `exasol-udf-sdk` 0.20.2, and iceberg 0.10 (all arrow-58) off their pinned tree; far larger and out-of-scope |

### Consequences

The arrow-57/58 split that existed solely because of iceberg 0.9.1 is fully eliminated. No other workspace dependency pin needs to move, keeping this a scoped, low-risk bump.

---

## ADR-081: Drop `tpchgen-arrow`; build arrow-58 batches from `tpchgen` core directly

**Date:** 2026-07-06
**Plan:** `change-iceberg-rust-0-10-bump`
**Status:** Accepted

### Context

`tpchgen-arrow` publishes 2.0.2 (arrow 57) and 3.0.0 (arrow 59) but no arrow-58 release, while the iceberg 0.10 writer now expects arrow-58 `RecordBatch`es. Keeping `tpchgen-arrow` at either version would leave a second, divergent arrow tree in the dev/e2e graph and require an Arrow IPC bridge that does not exist today. `tpchgen` core, by contrast, is a pure row generator with zero dependencies — no arrow at all (verified in `Cargo.lock`).

### Decision

Remove the `tpchgen-arrow` dependency entirely and construct arrow-58 `RecordBatch`es directly in `seed.rs`/`tpch_loader.rs` from `tpchgen` core, using the workspace `arrow` 58 builders.

### Options Considered

| Option | Verdict |
|--------|---------|
| Drop `tpchgen-arrow`; hand-build arrow-58 batches from `tpchgen` core | ✓ Chosen — reaches a genuinely single arrow-58 tree with no cross-tree hand-off; bounded, test-only cost (~100–200 lines of column builders) |
| Keep `tpchgen-arrow` 2.0.2 (arrow 57) + Arrow IPC bridge | ✗ Rejected — leaves arrow 57 permanently in the dev lock and introduces an IPC round-trip that doesn't exist today |
| Bump `tpchgen-arrow` to 3.0.0 (arrow 59) + IPC bridge | ✗ Rejected — introduces arrow 59, a tree newer than the workspace's 58, plus generator API churn plus the same IPC bridge; strictly worse |
| Drop the TPC-H loader | ✗ Rejected — it backs the live smoke test |

### Consequences

The dev/e2e dependency graph collapses onto a single arrow-58 tree — no arrow 57, no arrow 59, no IPC bridge anywhere. A future workspace arrow bump needs no coordinated `tpchgen-arrow` release, since generator batches are now built with the workspace arrow directly. The cost is bounded, test-only code in `seed.rs`/`tpch_loader.rs`; no production code is affected.

---

## ADR-082: Reference the Broadcast Join Dimension Side by File List, Not Materialized Rows

**Date:** 2026-07-06
**Plan:** `add-join-pushdown-broadcast`
**Status:** Accepted

### Context

Broadcast inner equi-join pushdown needs the small (dimension) side's data available to every fact-side shard's DataFusion session with no cross-shard exchange. The VS layer must stay thin (translation, pushdown analysis, parallelization planning) with all execution living in the UDF, per the repo's architecture boundaries, and the shard-invariant common spec is repeated to every shard invocation, so its size directly costs per-shard payload.

### Decision

Carry the dimension side's full file list, table root, and logical schema in the shard-invariant common spec; every shard invocation re-scans that file list itself and joins it node-locally against its fact-file subset, reusing the existing `register_files` path.

### Options Considered

| Option | Verdict |
|--------|---------|
| Carry the dimension side by file-list reference in the common spec | ✓ Chosen — keeps the VS thin (file-list resolution only, no execution), avoids a large VARCHAR blob repeated to every shard, reuses `register_files`; the bounded small side makes per-shard re-scans cheap |
| Materialize the dimension rows in the VS to Arrow IPC, base64-embed them in the common spec | ✗ Rejected — moves execution into the VS layer and repeats a large blob to every shard invocation |

### Consequences

Every shard invocation performs its own dimension-side file read, bounded by the broadcast threshold (`JOIN_BROADCAST_MAX_BYTES`, default 128 MiB), so the redundant work stays small by construction. The common spec gains a join block (table root, file list, logical schema, join type, rendered condition) that is absent for non-join specs.

---

## ADR-083: Ineligible Joins Fall Back to Deterministic Unaccelerated SQL, Not an Error

**Date:** 2026-07-06
**Plan:** `add-join-pushdown-broadcast`
**Status:** Accepted

### Context

Exasol capabilities are advertised once and statically; once `JOIN`/`JOIN_TYPE_INNER`/`JOIN_CONDITION_EQUI` are advertised, Exasol pushes every inner equi-join to the adapter, including shapes outside the broadcast contract (small side over threshold, or a shape the translator/guard cannot serve). The adapter must decide what to return for those ineligible pushdowns without regressing currently-working join queries.

### Decision

For any inner equi-join the adapter cannot broadcast, emit SQL that scans each table independently through its own sharded scan-UDF fan-out subquery and lets Exasol's core engine join the two results (`INNER JOIN ... ON <condition>`). A hard error (relying on Exasol's native retry) is reserved for the genuine last resort, when even that fallback SQL cannot be built.

### Options Considered

| Option | Verdict |
|--------|---------|
| Deterministic unaccelerated two-scan SQL fallback | ✓ Chosen — reproduces today's (pre-JOIN-capability) behavior deterministically; correctness stays inside adapter control |
| Always decline ineligible joins with an error and rely on Exasol re-planning | ✗ Rejected — Exasol does not cleanly re-plan on an adapter error; a hard error risks failing currently-working join queries |

### Consequences

The adapter carries two distinct join-rendering paths (broadcast fan-out and unaccelerated two-scan), each independently tested. Post-implementation E2E testing (ADR-085) found the first-cut two-scan rendering itself needed a correction; that refinement supersedes the rendering half of this decision's fallback path without changing the fallback-vs-error routing decided here.

---

## ADR-084: Shard Only the Fact Side; the Large-Side Sharding Model Is Unchanged

**Date:** 2026-07-06
**Plan:** `add-join-pushdown-broadcast`
**Status:** Accepted

### Context

Broadcast join pushdown (Phase 1 of backlog BL-001) needed to decide how much of the existing single-table file-sharding model to change to support joins, given Phase 2 (large/large shuffle join) is explicitly out of scope.

### Decision

The larger (fact) side keeps the existing G work-unit `GROUP BY shard_key` file-sharding exactly as the single-table path does; only the small (dimension) side's delivery (ADR-082) and the in-UDF join execution are new. No cross-shard exchange is introduced.

### Options Considered

| Option | Verdict |
|--------|---------|
| Shard only the fact side; reuse the existing sharding model unchanged | ✓ Chosen — broadcast is correct with no cross-shard exchange because the full small side is available to every shard; keeps Phase 2 (shuffle) fully out of scope |
| Re-partition either side by join key (shuffle join) | ✗ Rejected — out of scope for BL-001 Phase 1 |

### Consequences

The existing file-sharding/parallelism model (`parallelism/work-unit-sharding`) is untouched by this plan. Broadcast join pushdown is additive: new join branches in the adapter and scan UDF, no change to how the fact side's files are partitioned into shards.

---

## ADR-085: Two-Scan Fallback Renders Table-Qualified Columns, Independent of the Disjoint-Column Guard

**Date:** 2026-07-07
**Plan:** `add-join-pushdown-broadcast`
**Status:** Accepted

### Context

Supersedes the rendering half of ADR-083's two-scan fallback decision for shared-column-name joins; the broadcast path's bare-name rendering and disjoint-column guard are unchanged. Live E2E testing against a real Exasol Docker container surfaced a regression: a join between two tables that share a column name (e.g. both have `id`) hard-failed once `JOIN` was advertised. The disjoint-column-name guard (which exists only to keep the BROADCAST path's bare-name rendering against a combined in-UDF DataFusion schema unambiguous) correctly rejected the broadcast rendering, but the two-scan fallback wrongly reused that same guard-gated bare-name rendering and returned a hard `Err` instead of falling back — regressing a previously-working query, since Exasol does not retry natively on that error.

### Decision

The two-scan fallback renders its own join condition, WHERE filter, select list, GROUP BY, HAVING, and ORDER BY with table-qualified references (`"LHS_FACT"."COL"` / `"LHS_DIM"."COL"`), resolved from each `column` node's `tableName` against the side that owns it — never against a combined bare-name schema, and not gated on the disjoint-column guard (that guard governs broadcast eligibility only). Implemented by annotating each `column` node with a `tableAlias` and teaching the shared `vs-expression` translator to emit `"ALIAS"."NAME"` when `tableAlias` is present, bare name otherwise (the single-table path is byte-for-byte unchanged). A disjoint-guard failure is a plain reason the broadcast path is unavailable, not an error; a hard `Err` (native retry) is reserved for a condition that cannot be rendered against either a bare or a qualified schema.

### Options Considered

| Option | Verdict |
|--------|---------|
| Give the two-scan fallback its own table-qualified rendering, independent of the broadcast disjoint-column guard | ✓ Chosen — the two-scan path is Exasol's own engine joining two already-materialized sub-results, which resolves table-qualified references natively even on a shared column name; fixes the regression without touching the broadcast path |
| Keep the shared bare-name rendering and widen the disjoint-column guard | ✗ Rejected — would keep declining (via hard error) legitimate shared-column joins that the two-scan path can serve correctly |

### Consequences

`vs-expression`'s column renderer gained an optional `tableAlias` (bare name when absent, so the single-table and broadcast paths are unaffected). The two-scan fallback is now correct for shared-column-name joins. This decision is closely paired with ADR-086 (aggregate-over-join routing), found and fixed in the same post-implementation E2E pass.

---

## ADR-086: Aggregate-Over-Join Routes Through the Qualified Two-Scan Path, Not a Decline

**Date:** 2026-07-07
**Plan:** `add-join-pushdown-broadcast`
**Status:** Accepted

### Context

The same post-implementation E2E pass that found ADR-085's regression also found `SELECT COUNT(*), MIN(o.O_ORDERDATE) FROM CUSTOMER JOIN ORDERS ON ...` (the plan's own second Manual Testing example) failing with "Expected number of columns is 2 but pushdown query has 5": the fallback ignored the aggregate select list and emitted the full cross-table row projection instead.

### Decision

Any join request that carries an aggregate, GROUP BY, ORDER BY, LIMIT, or HAVING (`join_requires_exasol_postprocessing`) is routed to the two-scan path regardless of broadcast eligibility, because none of those can ride the broadcast in-UDF join (which renders only projection + filter + join condition). The two-scan wrapper renders the aggregate select list as ordinary Exasol SQL over the materialized join (`SELECT <aggregates> FROM (fact fan-out) JOIN (dim fan-out) ON ... [GROUP BY ...] [HAVING ...] [ORDER BY ...] [LIMIT ...]`), so Exasol evaluates the aggregate over the joined-and-materialized rows — exactly the pre-JOIN-capability behavior. The aggregate function name is spliced verbatim (it is Exasol's own); only its column argument is table-qualified.

### Options Considered

| Option | Verdict |
|--------|---------|
| Route aggregate/GROUP BY/ORDER BY/LIMIT/HAVING-bearing joins through the two-scan path unconditionally | ✓ Chosen — matches pre-JOIN-capability behavior exactly and avoids a hard error Exasol does not cleanly re-plan |
| Decline aggregate-over-join pushdowns with an error | ✗ Rejected — same reasoning as ADR-083: a hard error risks regressing currently-working queries |

### Consequences

Aggregate-over-join, GROUP BY-over-join, ORDER BY-over-join, and LIMIT/HAVING-over-join queries are all served by the qualified two-scan wrapper (ADR-085), never by the broadcast in-UDF join. Both regressions found in this E2E pass (ADR-085 and this one) are covered by new host and E2E tests.
## ADR-087: Keep DataFusion `ParquetSource`; Apply Positional Deletes via a Per-File Base `ParquetAccessPlan`

**Date:** 2026-07-06
**Plan:** `add-positional-delete-application`
**Status:** Accepted

### Context

Issue #11 identified a silent-correctness bug: the scan collapsed each Iceberg `FileScanTask` to a bare `(path, size)` pair and discarded its `.deletes`, so every merge-on-read query returned pre-delete rows with no error. A fix needed to apply Iceberg positional deletes on read (tracked as issue #68) without giving up DataFusion's own scan engine — projection/filter/LIMIT pushdown, row-group and page pruning, statistics, and streaming.

### Decision

Do NOT swap in iceberg-rust's `ArrowReader` / `iceberg-datafusion` `IcebergTableScan`. Keep DataFusion's `ParquetSource` as the scan engine and apply positional deletes by attaching a per-data-file `ParquetAccessPlan` (a base row selection) via `PartitionedFile::with_extensions`; the Parquet opener intersects predicate/bloom-filter/row-group/page pruning on top of the injected selection rather than the selection defeating that pruning.

### Options Considered

| Option | Verdict |
|--------|---------|
| Per-file base `ParquetAccessPlan` on DataFusion's `ParquetSource` | ✓ Chosen — DataFusion 54 exposes this access-plan seam natively (verified at `datafusion-datasource-parquet-54.0.0` opener/mod.rs and access_plan.rs); the injected selection composes with pushdown instead of disabling it |
| iceberg-rust `ArrowReader` / `iceberg-datafusion` `IcebergTableScan` | ✗ Rejected — loses DataFusion projection/filter/LIMIT pushdown, row-group/page pruning, statistics, and streaming, and re-plans files inside the scan, breaking file-level work assignment and the resolve-once seam |

### Consequences

Positional-delete application composes with all existing pushdown and pruning rather than bypassing it, so performance on the delete-free path is unaffected and the delete-carrying path keeps the same pruning behavior (cf. apache/iceberg-rust#2376). The approach requires vendoring the positions-to-`RowSelection` construction (see ADR-089 area of the plan's decision log) since it isn't a public dependency surface.

---

## ADR-088: Unify the Scan Provider on the Custom `ParquetSource`-Backed `TableProvider`, Gated by a Plan-Shape Test

**Date:** 2026-07-06
**Plan:** `add-positional-delete-application`
**Status:** Accepted

### Context

Attaching a per-file `ParquetAccessPlan` requires building a `FileScanConfig` directly, which the prior `ListingTable`-based registration path does not permit. The scan needed either one unified provider for all files (delete-free and merge-on-read alike) or two divergent registration paths gated on whether a file carries deletes.

### Decision

Build the custom `ParquetSource`-backed `TableProvider` for every scan, delete-free and merge-on-read alike, replacing `ListingTable` in file registration — unless a plan-shape/pruning-preservation test shows a noticeable regression on the delete-free path, in which case fall back to a conditional path (`ListingTable` for delete-free, the custom provider only when deletes are present).

### Options Considered

| Option | Verdict |
|--------|---------|
| Unified custom `TableProvider` for all scans | ✓ Chosen — one code path, simpler; the plan-shape/pruning-preservation test is the objective gate that would trigger falling back |
| Conditional from the start (`ListingTable` for delete-free, custom provider only for MOR) | ✗ Rejected as the default — retained as the documented fallback if the unified path regresses the delete-free plan shape |

### Consequences

The delete-free scan path now goes through the same provider code as the merge-on-read path, so a regression there would affect every query, not just delete-carrying ones — the plan-shape test exists specifically to catch that before it ships. If the fallback is ever triggered, the codebase gains a second, conditional registration path.

---

## ADR-089: Plan-Time Fail-Loud at the Manifest / `DataFile` Level Is the Authoritative Correctness Gate for Unsupported Deletes

**Date:** 2026-07-06
**Plan:** `add-positional-delete-application`
**Status:** Accepted

### Context

The engine cannot apply every Iceberg delete mechanism — equality deletes, Puffin/v3 deletion vectors, and ORC/Avro data or delete files are out of scope. Before this feature, none of these were detected, so the engine would silently return pre-delete rows for tables using them. `plan_files` drops the Puffin discriminator, so detection at scan/read time cannot reliably distinguish a deletion vector from a Parquet positional delete.

### Decision

Detect unsupported delete mechanisms at plan time, in the adapter, at the manifest / `DataFile` level (where the Puffin discriminator and file format are still visible) — before building or returning any scan-driving SQL. This plan-time detection is the authoritative gate. A lightweight scan-time check is retained only as cheap defense-in-depth.

### Options Considered

| Option | Verdict |
|--------|---------|
| Plan-time detection at the manifest/`DataFile` level, with a scan-time backstop | ✓ Chosen — reliable, because the discriminator and file format are still visible at that level; fails before any SQL is emitted or any node is engaged |
| Read-time-only detection on `FileScanTaskDeleteFile` | ✗ Rejected as the sole guard — `plan_files` drops the Puffin discriminator, making a deletion vector indistinguishable from a Parquet positional delete once the scan spec is built |

### Consequences

An unsupported-delete query now fails immediately, cleanly, and without emitting scan-driving SQL or any credential — closing the silent-correctness gap for those mechanisms too. Equality deletes and deletion vectors remain explicit future work under issue #11; adding support for them later means extending the same manifest-level detection point.

---

## ADR-090: Minimal Scan-Spec Surface for Delete Support — Per-File References Only

**Date:** 2026-07-06
**Plan:** `add-positional-delete-application`
**Status:** Accepted

### Context

Applying positional deletes needs enough information for the scan UDF to read and interpret each data file's associated delete files, but the wire format between the adapter and the scan UDF is deliberately kept minimal (no catalog access from the UDF, no repeated per-shard fields). A more expansive design would have carried a serialized Iceberg `Schema` and a bound `BoundPredicate` into the scan spec to support delete application generically.

### Decision

Add only per-file positional-delete references (path, byte size, delete content type) to the per-shard `files` argument. Keep `logical_schema` and the existing `FieldIdExprAdapter` exactly as they are; do not carry a serialized Iceberg `Schema` or a `BoundPredicate`. Legacy `(path, size)` entries deserialize with an empty delete list, preserving backward compatibility within one deploy.

### Options Considered

| Option | Verdict |
|--------|---------|
| Per-file delete references only (path, byte size, content type) | ✓ Chosen — keeps the wire format lean; DataFusion already does its own pushdown from the SQL filter, and the existing field-id adapter already handles schema evolution |
| Carry a serialized Iceberg `Schema` + bound `BoundPredicate` | ✗ Rejected — unnecessary weight; this was the design of an earlier, rejected broader plan (`add-iceberg-delete-application`) |

### Consequences

The scan spec's delete-related surface stays minimal and backward-compatible: a delete-free table produces a byte-identical common spec to before this feature, and legacy per-shard entries without deletes still reconstitute correctly. Any future delete mechanism support (equality deletes, deletion vectors) will need to extend this same minimal per-file surface rather than reintroducing the rejected schema/predicate approach.

---

## ADR-091: Per-Side Predicate Pushdown for Joins by `tableName` Conjunct Attribution

**Date:** 2026-07-07
**Plan:** `add-join-pushdown-broadcast`
**Status:** Accepted

### Context

PR #70 review (antonireus) found three performance-only gaps in the join-pushdown paths — the plan was correct as-is, but each side of a join was over-scanning. (1) Both join routes resolve their file lists through `resolve_join_sides`, which passed `filter_json: None` to `resolve_file_list`, so neither side got the Iceberg manifest pruning (partition/bounds elimination) the single-table path gets from `filter_json_raw`. (2) The unaccelerated two-scan fallback built each leg's `ScanSpec` with `filter: None`, so each leg full-scanned and shipped every row to Exasol to filter. (3) That fallback also projected each table's FULL involved-column set, shipping columns no clause referenced. The `None` filter was originally justified because a pushed WHERE "may reference either table's columns" — but that rationale only holds for the *combined* predicate.

The naive fix "just pass the whole WHERE JSON to each side" is UNSOUND here: `to_iceberg_predicate` resolves columns by NAME only (via `extract_column`, which ignores `tableName`), so on a shared column name (both tables have an `ID`) it would resolve the other side's `dimension.ID = 5` conjunct against THIS side's `ID` and wrongly prune fact files — the same shared-column-name hazard ADR-085 already had to defend against.

### Decision

For an inner equi-join (all this PR builds), attribute each WHERE conjunct to a side by its column nodes' `tableName` — the identical signal `annotate_columns_with_alias`/`build_join_alias_map` (ADR-085) already use — and push only side-local conjuncts down per side. A conjunct is side-local to side X iff EVERY column it references is tagged `tableName = X`; cross-table conjuncts, the equi-join condition, and any OR spanning both tables are withheld from both sides and applied only by the outer wrapper's WHERE (unchanged — still the correctness backstop). Because attribution is by table identity, a side-local conjunct contains only that side's columns, so `to_iceberg_predicate`/`render_df_filter_safe` resolve it against that side's own schema and the shared-column-name case stays correct. Concretely: (a) thread each side's side-local sub-predicate into `resolve_one_join_side`'s `resolve_file_list` for manifest pruning (both routes); (b) set the two-scan fallback leg's `ScanSpec.filter` to its bare-name side-local predicate for DataFusion row-group pruning + row filtering; (c) narrow each fallback leg's projection to the columns the outer wrapper actually references for that side (SELECT + condition + full WHERE + GROUP BY/HAVING/ORDER BY), dropping the rest. This is purely additive pruning; the broadcast SQL builder is untouched.

### Options Considered

| Option | Verdict |
|--------|---------|
| Pass the whole WHERE JSON to each side and rely on `to_iceberg_predicate` dropping unknown columns | ✗ Rejected — unsound on a shared column name: name-only resolution applies the other side's conjunct to this side and prunes wrongly |
| `tableName`-based conjunct attribution; push only side-local conjuncts (filter + manifest pruning); narrow projection to referenced columns | ✓ Chosen — sound for inner equi-joins (a one-side conjunct is a necessary survival condition for that side's rows), reuses the ADR-085 attribution signal, and keeps shared-column-name joins correct |

### Consequences

Both join routes now get free Iceberg manifest pruning per side; the two-scan fallback filters and footer-prunes each leg before emitting and ships only referenced columns — closing the pruning regression versus pre-PR single-table pushdown. The narrowed projection deliberately includes the FULL WHERE's columns (not just side-local ones) because the outer wrapper still renders the whole predicate qualified, and an absent SELECT list keeps every column (`SELECT *`). Pruned byte totals also feed side selection and the broadcast threshold, which only makes both more accurate. All new logic is `tableName`-driven and unit-tested for the shared-column-name (`EVENTS.ID` ⋈ `LABELS.ID`) case so it cannot regress the ADR-085 fix.

**E2E-surfaced correction (the fan-out filter must render BARE).** The live cluster sends every column node with BOTH `tableName` (e.g. `FACT_ORDERS`) AND the query's `tableAlias` (e.g. `O` for `FROM fact_orders o`), and the `vs-expression` translator emits `"ALIAS"."NAME"` whenever `tableAlias` is present. The first cut pushed the side-local predicate into the two-scan leg's `ScanSpec.filter` via `render_df_filter_safe` unchanged, so it rendered `("O"."O_ORDERDATE" …)` — but a per-side fan-out is a SINGLE-TABLE scan whose relation exposes BARE uppercase columns (`scan_target` wrapped in an unaliased derived table), so the alias-qualified reference failed to resolve (`No field named "O"."O_ORDERDATE"`), regressing every filtered join. Fix: strip `tableAlias` (`strip_table_alias`) before rendering the leg's filter, so it is bare exactly like the single-table scan path. The outer two-scan wrapper is unaffected — its `render_df_filter_qualified` re-qualifies each column to `LHS_FACT`/`LHS_DIM` (overwriting the native alias) against each side's own fan-out subquery. The broadcast path is likewise untouched: it keeps rendering the native `tableAlias`, which the in-UDF `build_join_sql` resolves against its two registered sides — a mechanical regression test now pins both behaviors (bare in the fan-out, native-alias-preserving in broadcast). Iceberg manifest pruning (`to_iceberg_predicate`) is alias-agnostic (it resolves by bare column `name`), so Finding 1 needed no stripping.

---

## ADR-092: N-Table Inner Joins Fall Back to an N-Scan Unaccelerated Wrapper (Generalizes ADR-083 to N Tables)

**Date:** 2026-07-07
**Plan:** `fix-join-decline-hard-fail`
**Status:** Superseded by ADR-095

### Context

Issue #76: a pushdown over an inner join spanning three or more involved tables (Q1
`supplier⋈nation⋈region`, Q2 `customer⋈orders⋈lineitem`, NQ3
`part⋈partsupp⋈supplier⋈nation`) hard-failed with `F-UDF-CL-RUST-9001: join pushdown
declined: the join spans more than two tables …`, originating from
`JoinShape::Ineligible(TooManyTables)` in `handle_pushdown`. The `exasol-udf-macros`
0.20.3 FFI shim erases every `UdfError` variant to return code 1, which the UDF host
surfaces as a hard SQL error — there is no native-retry path in this repo or the SDK for
a declined pushdown. This is the same false premise ADR-083 rejected and ADR-085/086
fixed for the two-table case; the feature spec already required N-table fallback
behavior, so the >2-table decline was a spec-vs-implementation mismatch, not a design
gap. The alternative of advertising fewer join capabilities so Exasol never pushes
multi-table joins was rejected because it would regress the two-table broadcast benefit
and still not match the already-written spec.

### Decision

A pushdown over an inner join spanning three or more involved tables is served by
materializing each table through its own sharded scan-UDF fan-out and reconstructing the
original inner join in Exasol's core engine — never by returning an error. An error is
reserved for a shape whose fallback genuinely cannot be built (a non-inner join node, an
involved table absent from `TABLE_MAP` or carrying no column metadata, or a
condition/clause the translator cannot render).

### Options Considered

| Option | Verdict |
|--------|---------|
| N-scan unaccelerated fallback for 3+ table inner joins | ✓ Chosen — closes #76, honors the existing spec wording, extends the proven ADR-083/085/086 "never wrong, only unaccelerated" pattern from two tables to N |
| Keep declining `TooManyTables` (status quo) | ✗ Rejected — hard-fails a query class the spec already requires to work |
| Advertise fewer join capabilities so Exasol never pushes multi-table joins | ✗ Rejected — regresses the two-table broadcast benefit and does not match the already-written spec |

### Consequences

A 3+ table inner-join pushdown now always returns a valid pushdown response instead of
an error; Exasol's core engine reconstructs the join over N independently-scanned
sub-results. The two-table broadcast and two-scan paths are unaffected. Future
multi-table query classes (Q1/Q2/NQ3-shaped joins) become usable without requiring a
broadcast N-way join to be built first.

---

## ADR-093: N-Scan Wrapper Renders as Cross-Join + Conjunctive Table-Qualified WHERE

**Date:** 2026-07-07
**Plan:** `fix-join-decline-hard-fail`
**Status:** Superseded by ADR-095

### Context

Generalizing the two-table unaccelerated fallback (ADR-085/086) to N tables requires
choosing how to reconstruct an arbitrary all-inner nested join tree over N
independently-scanned fan-out subqueries. A chained `INNER JOIN … ON` tree that
faithfully reproduces the pushed join tree would require ON-scope bookkeeping so each
condition references only tables already introduced earlier in the chain — error-prone
for arbitrary trees.

### Decision

The N-scan wrapper renders as `SELECT <qualified select list> FROM (fan0) "LHS_T0",
(fan1) "LHS_T1", … WHERE <all N-1 join conditions AND-conjoined with the qualified
residual filter> [GROUP BY …] [HAVING …] [ORDER BY …] [LIMIT …]`, with every column
reference table-qualified from its `tableName` via the ADR-085 alias-annotation
machinery.

### Options Considered

| Option | Verdict |
|--------|---------|
| Cross-join + conjunctive qualified WHERE | ✓ Chosen — provably equivalent to any join-tree ordering for all-inner joins, order-agnostic, no ON-scope bookkeeping; Exasol's optimizer turns equi-conditioned cross joins into hash joins |
| Chained `INNER JOIN … ON` tree reproducing the pushed join tree | ✗ Rejected — requires ON-scope bookkeeping so each condition references only tables already introduced; error-prone for arbitrary trees and buys nothing since Exasol re-optimizes anyway |

### Consequences

The builder need not track which tables each condition spans, simplifying the
implementation to N-entry alias-map construction plus conjunctive WHERE assembly. The
existing ADR-085 qualified-rendering machinery (`render_expression_qualified`,
`render_df_filter_qualified`, `qualified_join_select_items`/`_group_by`/`_having`/`_order_by`)
is reused wholesale, so correctness on shared column names carries over unchanged.

---

## ADR-094: Freeze the Two-Table Join Path; Add the N-Table Path Additively

**Date:** 2026-07-07
**Plan:** `fix-join-decline-hard-fail`
**Status:** Superseded by ADR-095

### Context

The N-table fallback could either be built by retrofitting N-table support into the
existing `EligibleJoin`/`JoinSides`/`build_unaccelerated_join_sql` two-table
structures, or added as a separate, additive path alongside them. The two-table
broadcast and two-scan fallback (ADR-081..086) are working, live-tested code with real
regression coverage (`has_two_scan_wrapper` and friends).

### Decision

Add `JoinShape::MultiTable(MultiTableJoin)` + `plan_multi_table_join` +
`build_n_scan_join_sql` for N≥3, leaving the two-table `Eligible`/`JoinSides`/
`build_unaccelerated_join_sql`/`build_two_scan_join_sql`/`LHS_FACT`/`LHS_DIM` path and
all its ADR-081..086 tests byte-for-byte unchanged.

### Options Considered

| Option | Verdict |
|--------|---------|
| Additive `JoinShape::MultiTable` path, two-table path frozen | ✓ Chosen — confines the change to new, independently-testable units and guarantees the two-table broadcast benefit and its live-tested regressions stay intact |
| Retrofit N tables into the existing `EligibleJoin`/`JoinSides` structures | ✗ Rejected — churns the working two-table broadcast + two-scan code and its E2E assertions, raising regression risk for no benefit |

### Consequences

The two-table broadcast and two-scan paths carry zero risk of regression from this
change. Future planners extending N-table join behavior should build on the
`MultiTable`/`plan_multi_table_join`/`build_n_scan_join_sql` path rather than
re-unifying it with the two-table path, unless a broadcast N-way join is deliberately
pursued (currently out of scope per the mission's "no N-table broadcast" non-goal).

---

## ADR-095: Single Unified N≥2 Unaccelerated Join Renderer (Supersedes ADR-092, ADR-093, ADR-094)

**Date:** 2026-07-08
**Plan:** `fix-join-decline-hard-fail`
**Status:** Accepted

### Context

PR #78 code review found that ADR-094's additive design — a frozen two-table path
(`plan_eligible_join`/`build_unaccelerated_join_sql`/`build_two_scan_join_sql`,
`LHS_FACT`/`LHS_DIM`) alongside a separate N≥3 path
(`plan_multi_table_join`/`build_n_scan_join_sql`, `LHS_T0..`) — is architecturally
unsound: the rendering gap that caused issue #76 existed in BOTH implementations, and
the first fix touched only one of them. Because the adapter advertises
`JOIN`/`JOIN_TYPE_INNER`/`JOIN_CONDITION_EQUI` statically with no per-query opt-out,
Exasol pushes every inner equi-join of any arity, so any divergence between the two
renderers is a latent correctness gap waiting to be hit at whichever arity was not
fixed.

### Decision

Collapse the two join implementations into one. `detect_join` yields a single join
shape carrying the N (≥2) resolved involved tables and the N-1 join conditions;
`handle_pushdown` routes through one `plan_join`, which computes broadcast eligibility
(N==2, small side ≤ `JOIN_BROADCAST_MAX_BYTES`, no Exasol postprocessing) as a property
and, when eligible, takes the broadcast fan-out — otherwise calls the SOLE fallback
renderer `build_n_scan_join_sql` (`LHS_T0..LHS_T{N-1}`, cross-join + conjunctive
table-qualified WHERE per ADR-093's technique, ADR-091 per-side predicate pushdown,
ADR-085 qualified rendering). `build_unaccelerated_join_sql`, `build_two_scan_join_sql`,
`resolve_join_sides`, the `Eligible`/`MultiTable` `JoinShape` split, and the
`LHS_FACT`/`LHS_DIM` alias scheme are removed. The two-table fallback is now exactly
N=2, structurally, not by coincidence. This supersedes ADR-092 (outcome retained: a 3+
table inner join never errors), ADR-093 (rendering technique retained, restated as part
of the one renderer), and ADR-094 (its "freeze and add additively" decision is
reversed).

### Options Considered

| Option | Verdict |
|--------|---------|
| Single unified N≥2 renderer; broadcast an inner optimization | ✓ Chosen — a single renderer cannot diverge from itself; "two-table = N=2" becomes structural |
| Keep the additive two-path design (ADR-094) | ✗ Rejected — the two copies already drifted and shipped the #76/PR-78 bug; retrofitting the aggregate fix into both would leave the same two-copies risk for the next fix |

### Consequences

There is exactly one unaccelerated join rendering implementation for all inner joins of
arity N≥2. Existing two-scan tests (`has_two_scan_wrapper`, `LHS_FACT`/`LHS_DIM`)
migrate to `LHS_T0`/`LHS_T1` aliases; the two-table SQL shape is otherwise unchanged.
Any future join-rendering fix lands once, not twice.

---

## ADR-096: `vs-expression` Renders Aggregate Function Nodes at the Shared Seam

**Date:** 2026-07-08
**Plan:** `fix-join-decline-hard-fail`
**Status:** Accepted

### Context

The actual root cause of the PR #78 defect was not join arity: a grouped-aggregate
select list over a join whose select item is a SCALAR FUNCTION WRAPPING AGGREGATES —
e.g. `ROUND(100.0 * SUM(CASE WHEN l_returnflag='R' THEN 1 ELSE 0 END) / COUNT(*), 2)` —
declined at every arity (single-table, two-table, N-table).
`render_selectlist_item_qualified` (`pushdown.rs:4486`) only special-cased a top-level
`function_aggregate`; anything else recursed into
`vs_expression::render_expression_safe` → `render_expression_inner`
(`vs-expression/src/lib.rs:100`), which had a `function_scalar` arm (ROUND, arithmetic,
CASE) but no `function_aggregate` arm, so recursion into a nested `SUM`/`COUNT` hit the
unsupported-node catch-all → `Err`/`None` → decline.

### Decision

Add a `function_aggregate` arm to `render_expression_inner`: splice the aggregate
`name` verbatim (uppercased — not translated like a scalar function), render
`COUNT(*)` for empty/star arguments, render each argument by recursion, honor
`distinct: true` → `COUNT(DISTINCT arg)`, and qualify column arguments via the ADR-085
`tableAlias` annotation. Unify `render_selectlist_item_qualified` and
`render_aggregate_qualified` (`pushdown.rs`) onto this path so a top-level aggregate and
a nested aggregate render identically, keeping the top-level output byte-compatible
with the shapes it already handled.

### Options Considered

| Option | Verdict |
|--------|---------|
| Aggregate arm at the shared `vs-expression` seam | ✓ Chosen — the seam is shared by all arities and any future caller; one fix repairs single-table, two-table, and N-table simultaneously |
| Special-case scalar-over-aggregate only in the join select-list path (`pushdown.rs`) | ✗ Rejected — leaves the identical gap for single-table nested aggregates and any future caller of `vs-expression` |
| Keep declining scalar-over-aggregate select items | ✗ Rejected — it is a valid, expected TPC-H-shaped query and there is no native retry (ADR-097) to fall back on |

### Consequences

A scalar expression wrapping one or more aggregates renders correctly regardless of
join arity. Top-level and nested aggregate rendering are consistent by construction.
The single-table partial/merge aggregate decomposition paths (`pushdown-planning`,
`-count-distinct`, `-expression-aggregate`, `-grouped-agg`) are unaffected: they detect
a top-level `function_aggregate` before recursing into `vs-expression`, so the new arm
only changes behavior for aggregates nested inside another expression — exactly the
case that previously errored.

---

## ADR-097: Advertised Capability Must Render — Purge the Native-Retry Fiction

**Date:** 2026-07-08
**Plan:** `fix-join-decline-hard-fail`
**Status:** Accepted

### Context

15 `UdfError::User` join/aggregate decline sites in `pushdown.rs` (lines 2211, 2231,
2253, 2614, 4161, 4819, 4867, 4883, 5168, 5181, 5277, 5309, 5329, 5358, 5415) were
framed as "Exasol will retry the query natively." This is false: the
`exasol-udf-macros` FFI shim erases every `UdfError::User` into a hard
`F-UDF-CL-RUST-9001` client-facing SQL error; Exasol never re-plans on an adapter error
(ADR-083, ADR-085 already established this for the two-table case). A unit test at
`pushdown.rs:7936` asserted `msg.contains("retry")`, encoding the false framing into a
regression check.

### Decision

Remove the native-retry framing from all 15 sites. Delete the sites whose shapes now
always render after ADR-095/ADR-096 (the join-arity and aggregate-nesting gaps that
motivated them no longer exist). Reword the genuine last-resort errors — a non-inner
join node in the tree, an involved table absent from `TABLE_MAP` or carrying no column
metadata, or a condition/clause `vs-expression` cannot render — as plain hard
client-facing errors with no retry. Adopt the governing principle: for each advertised
capability the adapter MUST always be able to render what Exasol may push, or MUST NOT
advertise it; "decline at runtime and hope Exasol retries" is not a valid third option.
Update the `msg.contains("retry")` test to assert the corrected wording.

### Options Considered

| Option | Verdict |
|--------|---------|
| Purge retry framing; hard error only when truly unrenderable | ✓ Chosen — truthful error semantics; removes a recurring source of "just decline and hope" bugs; the protocol has no decline-and-retry response |
| Keep the "retry natively" wording | ✗ Rejected — it is false, as ADR-083/085 already established, and the false framing was encoded into a regression test that would need to keep being "fixed" around |

### Consequences

Every remaining hard-error decline site in the join/aggregate pushdown path states
plainly that it is a hard error with no native retry. The advertised-capability-must-
render principle applies to all future capability additions, not just joins.

---

## ADR-098: Full Push-Down of Grouped Scalar-Over-Aggregate Select Items (Primary), Qualified Wrapper as Residual Fallback

**Date:** 2026-07-08
**Plan:** `fix-scalar-over-aggregate-grouped-pushdown`
**Status:** Accepted

### Context

Issue #82: a single-table grouped query whose select list contains a scalar function
wrapping aggregates (e.g. `ROUND(100.0 * SUM(CASE …) / COUNT(*), 2)`) hard-failed
through the Virtual Schema with a `04000` pushdown column-count mismatch.
`detect_group_by_aggregates` classified such an item as neither a top-level aggregate,
a literal, nor a group-key projection, returning `None` and falling through to a bare
raw full-row scan — which returns the wrong column count for a `group_by` request. Two
candidate fixes existed: (a) decompose the item's inner aggregates into the existing
partial `AggregatePlan` machinery and render the scalar wrapper over the merged
partials in the outer wrapper, reusing the grouped partial/merge architecture and
`render_having_over_merge`'s aggregate-rewrite machinery; or (b) route the whole
grouped scalar-over-aggregate through a qualified single-table wrapper (Exasol
aggregates over a materialized sharded raw scan), analogous to the join fallback.

### Decision

Push down a scalar-over-aggregate grouped select item by folding its inner aggregates
into the existing partial `AggregatePlan` decomposition and rendering the scalar
wrapper over the merged partials in the outer wrapper, at the item's original
`selectList` ordinal. Fall back to a qualified single-table wrapper only when an inner
aggregate is genuinely undecomposable (`DISTINCT`, a non-numeric stat argument, an
untranslatable argument, or a non-aggregate/non-group-key node) — never a bare raw
row scan for a grouped request.

### Options Considered

| Option | Verdict |
|--------|---------|
| (a) Full push-down: decompose inner aggregates into partials, render scalar wrapper over merged partials | ✓ Chosen — consistent with the existing grouped partial/merge architecture, reuses `AggregatePlan` decomposition and the scan-UDF layout, and preserves node-local aggregation (mission's minimal-network-transfer requirement) |
| (b) Route the whole grouped scalar-over-aggregate through the qualified single-table wrapper unconditionally | ✗ Rejected — ships every matching row per group to Exasol, defeating node-local decomposition even for the plain aggregates in the same query; retained only as the residual-shape safety net |

### Consequences

A grouped select item with a scalar wrapping aggregates decomposes and pushes down
like a top-level aggregate, keeping node-local aggregation for the common case. A
grouped decline for a genuinely undecomposable shape never emits a
column-count-mismatched bare row scan — it emits a qualified single-table wrapper with
the correct column count instead.

---

## ADR-099: Generalize `render_having_over_merge` to Descend Scalars for Select-List Merge Rendering

**Date:** 2026-07-08
**Plan:** `fix-scalar-over-aggregate-grouped-pushdown`
**Status:** Accepted

### Context

The existing HAVING merge-rewrite renderer, `render_having_over_merge`, already
rewrites a top-level `function_aggregate` node to its merged `PARTIAL_*` expression
matched to the decomposed `AggregatePlan` list. Its gap: `render_having_operand`'s
catch-all delegated a scalar function or arithmetic node wrapping an aggregate to
`render_expression`, which renders the nested aggregate verbatim over source columns —
absent from the outer wrapper — rather than over the merged partials. A grouped
select-list scalar-over-aggregate item is structurally the same problem: render the
surrounding scalar/arithmetic structure while rewriting each aggregate leaf to its
merged `PARTIAL_*` expression.

### Decision

Generalize `render_having_operand` so a `function_scalar`/arithmetic node recurses
into a merge-aware renderer (`render_scalar_over_merge`) that rewrites every nested
`function_aggregate` to its merged expression, matched to the `AggregatePlan` list by
equality (kind + argument), preserving the scalar/arithmetic structure around it. The
same renderer serves both the grouped select list and HAVING, so a top-level bare
aggregate and a nested aggregate are rewritten by one consistent path.

### Options Considered

| Option | Verdict |
|--------|---------|
| Generalize `render_having_over_merge`/`render_having_operand` to descend scalars | ✓ Chosen — one renderer serves both the select list and HAVING, avoiding the two-copy divergence PR #78 diagnosed for the join renderers; fixes a scalar-over-aggregate inside HAVING as a side effect |
| A new, independent select-list merge renderer parallel to the HAVING one | ✗ Rejected — duplicates the aggregate→merged rewrite logic and risks drift between the two copies |

### Consequences

A scalar function wrapping one or more aggregates, whether in a grouped select list or
a HAVING clause, is rendered by the same merge-aware path and never references a
source column absent from the outer wrapper. Top-level and nested aggregate rewriting
are consistent by construction.
