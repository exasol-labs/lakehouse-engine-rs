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

---

## ADR-009: Standalone `crates/vs-expression` Crate for Expression Translation

**Date:** 2026-06-22
**Plan:** `add-group-by-and-sql-comprehension`
**Status:** Accepted

### Context

The VS adapter needed to translate Exasol pushdown expression-JSON nodes (column references, literals, comparison predicates, logical connectives, arithmetic, CAST, IN, BETWEEN, LIKE, IS NULL) into DataFusion SQL fragments for both filter pushdown and GROUP BY key rendering. The existing walker lived in `adapter/predicate.rs` inside `lakehouse-engine`, tightly coupled to engine internals and unreusable by the sibling `strata-rs` project.

### Decision

Create a standalone workspace crate (`crates/vs-expression`) containing the full serde_json expression-node walker. The crate has no lakehouse-engine internals in its API; its only dependencies are `serde_json` and `exasol-udf-sdk` (for `UdfError`). It exposes three public entry points: `render_expression` (raising), `render_expression_safe` (None on failure), and `render_df_filter_safe` (None on failure or trivially-true result). Delete `adapter/predicate.rs` and replace all its callers with `vs_expression::render_df_filter_safe`.

### Options Considered

| Option | Verdict |
|--------|---------|
| Standalone `crates/vs-expression` crate with no engine-internal deps | ✓ Chosen — clean, testable, reusable by strata-rs; supports future monorepo convergence |
| Extend `adapter/predicate.rs` inline | ✗ Rejected — blocks strata-rs reuse; keeps expression logic coupled to engine internals |
| Add a SQL-parser dependency (sqlparser-rs) as the IR | ✗ Rejected — user declined; overweight for a narrow translation job; serde_json walker is proven |

### Consequences

Expression translation is a separate, testable, reusable unit. The three-function public API (raising, safe, filter-safe) is stable and minimal. Long-term monorepo convergence with strata-rs is straightforward. `adapter/predicate.rs` is deleted; any future predicate coverage goes in `vs-expression`.

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

High-cardinality GROUP BY queries inside a scan UDF risk OOM-crashing the UDF process when DataFusion's intermediate state exceeds the per-instance memory budget. Exasol enforces a per-process heap limit via `setrlimit(RLIMIT_RSS)` (default 4096 MB) and stalls additional concurrent VMs once usage hits 80% of it (`[redacted]`). The per-instance limit is exposed in UDF metadata via `ctx.memory_limit()` (bytes; `0` = unbounded/unknown sentinel), provided by `language-container-rs:add-memory-limit-metadata`.

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

ADR-007 shipped `GROUP BY IPROC(), shard_key` to distribute scan shards across nodes. A corrected reading of the Exasol engine internals (verified in `[redacted]`/`script-languages`) shows that groups drive UDF *invocations*, not OS processes; parallel instances on a node are a fixed VM pool sized to `NR_OF_CORES` (`[redacted]`, `[redacted]`), and groups are multiplexed onto it (`[redacted]`). `GROUP BY IPROC()` yields exactly one group per node (`[redacted]`), capping parallelism at node count and leaving a node's other cores idle.

### Decision

Replace `GROUP BY IPROC(), shard_key` with `GROUP BY shard_key` over G oversubscribed work-unit shards, where `G = node_count × parallelism_factor` (`parallelism_factor` is a VS property, default 8), capped at 300 and clamped to `[1, file_count]`. The 300 cap keeps the group set in Exasol's round-robin distribution regime; above `max_dynamic_group_count` (default 300) Exasol hash-partitions groups instead (`[redacted]`). The `parallelism/iproc-sharding` feature is renamed to `parallelism/work-unit-sharding`. The balanced `partition_files` split is reused, passing G instead of node_count. The NPROC node-count capture (ADR-006) is retained to feed G.

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
