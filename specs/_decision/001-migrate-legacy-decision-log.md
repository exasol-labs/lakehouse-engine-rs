# Decisions: migrate-legacy-decision-log

## ADR: One Crate / One .so with Two Named Entry Points

**ID:** one-crate-one-so-with-two-named-entry-points
**Plan:** `add-datafusion-iceberg-scan-pushdown`
**Status:** Accepted

### Context

The VS adapter and the DataFusion scan SET UDF both deploy to Exasol BucketFS. language-container-rs 0.14.0 supports multiple named entry points per `.so`.

### Decision

Both entry points ship as `#[exasol_udf]` functions in one `cdylib` crate that builds one `.so`. Both Exasol scripts reference that one artifact.

### Options Considered

| Option | Verdict |
|--------|---------|
| Two crates and two `.so` files | Rejected: doubles the BucketFS upload surface and is unnecessary since 0.14.0 |

### Consequences

One upload serves both scripts, and both entry points share compiled dependencies.

---

## ADR: Adapter Drives a Scan SET UDF (Not a Cache-Populating Library Call)

**ID:** adapter-drives-a-scan-set-udf-not-a-cache-populating-library-call
**Plan:** `add-datafusion-iceberg-scan-pushdown`
**Status:** Accepted

### Context

The VS adapter must return a pushdown response to Exasol. The sibling project calls `populate_cache()` over connect-back and returns a `SELECT` from a cache table. The mission lists caching and materialization as non-goals.

### Decision

The adapter's `pushdown` response is SQL that invokes the scan SET UDF with an explicit file list. The UDF runs DataFusion and emits rows directly to Exasol, and no cache table is populated.

### Options Considered

| Option | Verdict |
|--------|---------|
| Populate a cache via connect-back and select from it | Rejected: caching is a mission non-goal and does not exercise DataFusion-in-UDF execution |

### Consequences

Every query is stateless and independent. Results are never stale, and there is no acceleration from result reuse.

---

## ADR: Resolve Metadata Once in the Adapter; Pass an Explicit File List to the UDF

**ID:** resolve-metadata-once-in-the-adapter-pass-an-explicit-file-list-to-the-udf
**Plan:** `add-datafusion-iceberg-scan-pushdown`
**Status:** Accepted

### Context

The Iceberg snapshot and data-file list must be resolved from the catalog before scanning, either in each UDF invocation or once in the adapter.

### Decision

The adapter resolves the snapshot and data-file list once during `pushdown` and passes the file list to the scan UDF as an argument. The UDF never discovers files itself.

### Options Considered

| Option | Verdict |
|--------|---------|
| Each UDF invocation re-resolves metadata | Rejected: violates the once-per-query mission constraint and duplicates metadata fetches per node |

### Consequences

The catalog round-trip adds latency before the scan runs. The explicit file list is the seam that multi-node file sharding partitions.

---

## ADR: Single Authoritative DataFusion-to-Exasol Type Mapping with JSON Fallback

**ID:** single-authoritative-datafusion-to-exasol-type-mapping-with-json-fallback
**Plan:** `add-datafusion-iceberg-scan-pushdown`
**Status:** Accepted

### Context

Exasol has no array, list, struct, or map type. The Arrow-to-Exasol mapping applies in both the `createVirtualSchema` declaration and the scan's Arrow-to-Value conversion, and a divergence causes runtime errors.

### Decision

One mapping table governs both sites. Compatible Arrow types map to native Exasol types. Incompatible types (List, Struct, Map, Union, Binary, Duration, Time, Interval, Decimal256, and Decimal128 with precision or scale above 36) are serialized to a JSON string and declared as `VARCHAR(2000000)`.

### Options Considered

| Option | Verdict |
|--------|---------|
| Reject incompatible columns at `createVirtualSchema` | Rejected: makes tables with list or struct columns unusable |
| Drop incompatible columns | Rejected: silently hides data |

### Consequences

Every Iceberg column is surfaced. A future type addition must update both code sites, and out-of-range Decimal128 values pay a JSON serialization round-trip.

---

## ADR: Cluster Node Count Captured Once at createVirtualSchema via adapterNotes

**ID:** cluster-node-count-captured-once-at-createvirtualschema-via-adapternotes
**Plan:** `add-multinode-sharding-and-agg-pushdown`
**Status:** Superseded by source-cluster-node-count-from-udfcontext-node-count-not-a-connect-back-select-nproc-supersedes-adr-006

### Context

The adapter needs the cluster node count at pushdown time to choose the shard count, without a per-query connect-back. Exasol 2025.2.1 silently drops adapter-returned `schemaMetadata.properties`, but it persists `adapterNotes` and returns them on every pushdown request.

### Decision

The adapter runs `SELECT NPROC()` over connect-back once during `createVirtualSchema` and stores it as `CLUSTER_NODES` in `adapterNotes`, defaulting to 1 on failure. Pushdown reads `CLUSTER_NODES` to choose the shard count.

### Options Considered

| Option | Verdict |
|--------|---------|
| Store the count in `schemaMetadata.properties` | Rejected: Exasol drops adapter-returned properties |
| Fetch `NPROC()` on every pushdown | Rejected: adds connect-back latency for a value stable over the VS lifetime |
| Require a static node-count property | Rejected: error-prone and drifts as the cluster scales |

### Consequences

Pushdown has no connect-back overhead. The default of 1 keeps single-node execution unchanged when the cluster is single-node or the connect-back fails.

---

## ADR: IPROC Fan-Out via Derived VALUES + GROUP BY IPROC(), shard_key

**ID:** iproc-fan-out-via-derived-values-group-by-iproc-shard-key
**Plan:** `add-multinode-sharding-and-agg-pushdown`
**Status:** Accepted

### Context

The adapter must distribute N file shards across N cluster nodes so each node's UDF invocation scans only its shard. Grouping over `IPROC()` makes Exasol route each group to a distinct node when driving a SET UDF.

### Decision

The fan-out is one scan-driving query: a derived `VALUES` table of `(shard_key, per-shard ScanSpec)` rows, with the scan SET UDF invoked under `GROUP BY IPROC(), shard_key`.

### Options Considered

| Option | Verdict |
|--------|---------|
| UNION ALL of N UDF SELECTs | Rejected: does not guarantee node placement and grows the SQL with shard count |
| One UDF row per file | Rejected: no node-level batching and loses shard-level locality |

### Consequences

Exasol guarantees node placement, and more shards add `VALUES` rows without changing the query structure.

---

## ADR: Partial/Merge Aggregate Decomposition; AVG as (sum, count) Pair

**ID:** partial-merge-aggregate-decomposition-avg-as-sum-count-pair
**Plan:** `add-multinode-sharding-and-agg-pushdown`
**Status:** Accepted

### Context

Aggregate pushdown must stay correct across shards. COUNT, SUM, MIN, and MAX merge directly, but averaging per-shard averages is wrong for unequal shard sizes.

### Decision

Each aggregate splits into a node-local partial computed in the scan UDF and an Exasol-side merge in the wrapper SQL: COUNT and SUM merge with SUM, MIN with MIN, MAX with MAX. AVG is emitted as a (sum, count) pair and divided in the wrapper, with a NULL guard for zero count.

### Options Considered

| Option | Verdict |
|--------|---------|
| Full aggregate in one UDF on one node | Rejected: does not scale |
| Average the per-shard averages | Rejected: incorrect for unequal shard sizes |

### Consequences

Transfer is one partial row per shard. AVG needs two output columns, and the NULL guard preserves single-node AVG semantics for empty tables.

---

## ADR: Standalone `crates/vs-expression` Crate for Expression Translation

**ID:** standalone-crates-vs-expression-crate-for-expression-translation
**Plan:** `add-group-by-and-sql-comprehension`
**Status:** Accepted

### Context

The adapter must translate Exasol pushdown expression-JSON nodes into DataFusion SQL fragments for filter pushdown and GROUP BY key rendering. The existing walker sat inside the engine's `adapter/predicate.rs`, coupled to engine internals and unusable by the sibling project.

### Decision

The expression walker lives in a standalone workspace crate, `vs-expression`, with no engine-internal dependencies. It exposes a raising render, a safe render, and a safe filter render, and `adapter/predicate.rs` is deleted.

### Options Considered

| Option | Verdict |
|--------|---------|
| Extend `adapter/predicate.rs` inline | Rejected: keeps expression logic coupled to the engine and blocks reuse |
| Use a SQL-parser dependency (sqlparser-rs) as the IR | Rejected: overweight for a narrow translation job |

### Consequences

Future predicate coverage goes in `vs-expression`. Expression translation is a separate, testable, reusable unit.

---

## ADR: GROUP BY Group-Key Values Emitted as Plain Columns; Wrapper Groups on Column Refs

**ID:** group-by-group-key-values-emitted-as-plain-columns-wrapper-groups-on-column-refs
**Plan:** `add-group-by-and-sql-comprehension`
**Status:** Accepted

### Context

A grouped aggregate pushdown needs partial rows that the outer wrapper SQL can re-group and merge, with group-key identity preserved.

### Decision

The scan UDF emits computed group-key values as plain columns `GK_0`, `GK_1`, and so on, ahead of the `PARTIAL_*` aggregate columns. The wrapper SQL groups on those columns.

### Options Considered

| Option | Verdict |
|--------|---------|
| Re-render the GROUP BY expression in the wrapper | Rejected: a mismatch between UDF-side and wrapper-side rendering gives wrong grouping |
| Emit group keys by source column name | Rejected: computed expression keys have no stable source column name |

### Consequences

The contract between scan UDF and wrapper is positional, so a new group-key expression type needs no wrapper change.

---

## ADR: Memory Safety via Metadata-Sized DataFusion Pool and Spill-or-Hardcap Backstop

**ID:** memory-safety-via-metadata-sized-datafusion-pool-and-spill-or-hardcap-backstop
**Plan:** `add-group-by-and-sql-comprehension`
**Status:** Accepted

### Context

High-cardinality GROUP BY queries can OOM-crash the UDF process. Exasol enforces a per-process heap limit (default 4096 MB), reported by `ctx.memory_limit()` (`0` means unknown), and stalls further concurrent VMs at 80% of it.

### Decision

The scan UDF sizes the DataFusion memory pool to about 0.6 of the reported limit, or 1024 MB when the limit is unknown. If `/tmp` is real disk with free space, the pool spills to disk and completes at any cardinality. If `/tmp` is tmpfs or full, the pool is hard-capped and returns a clean `ResourcesExhausted` error. Spill files are per-invocation scratch only.

### Options Considered

| Option | Verdict |
|--------|---------|
| File-count cardinality guard | Rejected: a heuristic with no statistical basis |
| Per-shard emitted-group cap with UDF abort | Rejected: produces partial results |
| Unbounded pool | Rejected: OOM-crashes the UDF at high cardinality |

### Consequences

Nodes with tmpfs `/tmp` get a clean error instead of a crash. The 0.6 fraction leaves room for the engine to throttle concurrency before an instance OOMs.

---

## ADR: Oversubscribed `GROUP BY shard_key` Work-Unit Sharding (Supersedes ADR-007)

**ID:** oversubscribed-group-by-shard-key-work-unit-sharding-supersedes-adr-007
**Plan:** `add-group-by-and-sql-comprehension`
**Status:** Accepted
**Supersedes:** iproc-fan-out-via-derived-values-group-by-iproc-shard-key

### Context

Groups drive UDF invocations, not OS processes. Each node runs a fixed VM pool sized to `NR_OF_CORES` and multiplexes groups onto it. `GROUP BY IPROC()` yields one group per node, so parallelism stops at the node count and other cores idle.

### Decision

The scan groups on `shard_key` alone over G work-unit shards, where G is node count times the `parallelism_factor` VS property (default 8), capped at 300 and clamped to `[1, file_count]`. Above `max_dynamic_group_count` (default 300) Exasol hash-partitions groups unevenly instead of distributing them round-robin. The feature is named `parallelism/work-unit-sharding`, and the balanced file split and the node-count capture are reused.

### Options Considered

| Option | Verdict |
|--------|---------|
| `GROUP BY IPROC()` or `GROUP BY IPROC(), shard_key` | Rejected: caps parallelism at node count, and `shard_key` alone suffices once shards oversubscribe nodes |
| Uncapped G | Rejected: above 300 Exasol hash-partitions groups unevenly |

### Consequences

Shard groups spread round-robin across nodes and multiplex onto each node's cores. DataFusion runs the user GROUP BY inside each shard and the outer wrapper merges the partials. `parallelism_factor` is an operator tuning knob.

---

## ADR: Capability Invariant — Advertise Only What the Engine Can Back Correctly

**ID:** capability-invariant-advertise-only-what-the-engine-can-back-correctly
**Plan:** `add-capability-alignment`
**Status:** Accepted

### Context

The adapter's `CAPABILITIES` list and the `vs-expression` translator had drifted. Some advertised names did not exist in Exasol's vocabulary, and many supported functions were not advertised. Over-advertising a wrongly translated function causes silent wrong results, while under-advertising only costs performance.

### Decision

Every name in `CAPABILITIES` must have a correct path: a translator arm that emits a correct DataFusion fragment, or an aggregate plan with a correct shard-associative partial and merge. Additions need a working path, and names without one are removed. `FN_PRED_GREATER` and `FN_PRED_GREATEREQUAL` are deleted.

### Options Considered

| Option | Verdict |
|--------|---------|
| Maximise the advertised set and rely on Exasol | Rejected: wrongly translated capabilities return silently wrong results |

### Consequences

A new `FN_*` entry needs its translator arm or decomposition first.

---

## ADR: STDDEV/VARIANCE Pushdown via (count, sum, sum_sq) Sufficient Statistics

**ID:** stddev-variance-pushdown-via-count-sum-sum-sq-sufficient-statistics
**Plan:** `add-capability-alignment`
**Status:** Accepted

### Context

STDDEV and VARIANCE are not shard-associative, but `COUNT(col)`, `SUM(col)`, and `SUM(col*col)` each merge via SUM and together reconstruct variance exactly.

### Decision

The STDDEV and VARIANCE family is advertised by emitting a `(COUNT, SUM, SUM of squares)` triple per shard. The outer wrapper computes variance as `(SUM(sum_sq) - SUM(sum)^2/SUM(cnt)) / d`, with `d` equal to the count for population forms and count minus 1 for sample forms. Standard deviation is its square root, with NULL guards for zero and single-sample counts.

### Options Considered

| Option | Verdict |
|--------|---------|
| Average per-shard standard deviations | Rejected: incorrect for unequal shard sizes |
| Skip statistical aggregates | Rejected: leaves supportable capabilities off |

### Consequences

The wrapper SQL carries three partial columns per statistical aggregate and the NULL guards that avoid division by zero and negative radicands.

---

## ADR: Source Credentials from an Exasol CONNECTION Object (Mirror the Sibling Project's CONNECTION Convention)

**ID:** source-credentials-from-an-exasol-connection-object-mirror-the-sibling-project-s-connection-convention
**Plan:** `add-glue-catalog-sigv4-connection`
**Status:** Accepted

### Context

The engine read the catalog URI and S3 credentials from plain VS properties. Credentials then appear in the `CREATE VIRTUAL SCHEMA` text and the query profile, and rotation requires new DDL. The sibling project solves this with Exasol CONNECTION objects.

### Decision

The adapter reads the catalog URI and all S3 and signing credentials from the CONNECTION named by `CATALOG_CONNECTION`. The address is the catalog endpoint, and the password is a JSON block holding the credentials and options. Both `createVirtualSchema` and `pushdown` resolve credentials this way, and error messages never echo the password text.

### Options Considered

| Option | Verdict |
|--------|---------|
| Keep plain VS properties | Rejected: leaks credentials into DDL text and the query profile, and rotation needs a DDL change |
| Inject credentials via request JSON | Rejected: the SDK does not surface them that way |

### Consequences

`CATALOG_CONNECTION` is required and the plain-property credential path is removed. The `use_sigv4` and `use_vended_credentials` flags default to false, so existing MinIO and REST stacks work with a CONNECTION that omits them.

---

## ADR: Byte-Balanced Sharding via LPT Greedy Assignment

**ID:** byte-balanced-sharding-via-lpt-greedy-assignment
**Plan:** `change-shard-parallelism`
**Status:** Accepted

### Context

Splitting files into equal-count shards does not equalize scan work, so the slowest shard dominates wall-clock time. Iceberg's `FileScanTask` already reports `file_size_in_bytes`.

### Decision

Files are sorted by size descending, with size 0 treated as 1 byte, and each is assigned to the shard with the smallest running byte total (Longest-Processing-Time-first). The shard shape is unchanged, so downstream SQL builders are unaffected.

### Options Considered

| Option | Verdict |
|--------|---------|
| Strict prefix-sum equal-byte split | Rejected: one large file can skew shards |
| Full DP optimum partition | Rejected: overkill for a balancing heuristic |
| Keep count-balanced split | Rejected: equal file count is not equal scan work |

### Consequences

A zero-size file counts as 1 byte, so it goes to the lightest shard and is never dropped.

---

## ADR: Hardware-Aware Default Parallelism Factor (`max(NR_OF_CORES × 2, 8)`)

**ID:** hardware-aware-default-parallelism-factor-max-nr-of-cores-2-8
**Plan:** `change-shard-parallelism`
**Status:** Superseded by source-per-node-core-count-from-available-parallelism-not-the-bogus-param-value-nr-of-cores-connect-back-query-supersedes-the-core-count-capture-in-adr-023

### Context

The default `PARALLELISM_FACTOR` was the constant 8. Per-node parallelism is bounded by a VM pool sized to `NR_OF_CORES`, so a fixed default under-subscribes large nodes and may over-subscribe tiny ones.

### Decision

The adapter reads `NR_OF_CORES` with `SELECT PARAM_VALUE('NR_OF_CORES')` during `createVirtualSchema`, in the existing connect-back session. The default `PARALLELISM_FACTOR` is `max(NR_OF_CORES × 2, 8)`, and an explicit property overrides it.

### Options Considered

| Option | Verdict |
|--------|---------|
| Keep the constant 8 | Rejected: unrelated to the node's core pool |
| Multiplier of 1 | Rejected: no headroom to absorb stragglers |
| Multiplier of 4 | Rejected: excessive session-startup overhead |

### Consequences

The floor of 8 keeps a single-core VM or a failed lookup (0) from collapsing the factor.

---

## ADR: Subtract a Constant Container-Overhead Before Applying the Memory-Pool Fraction

**ID:** subtract-a-constant-container-overhead-before-applying-the-memory-pool-fraction
**Plan:** `change-memory-pool-sizing`
**Status:** Accepted

### Context

The memory pool was sized to `0.6 × ctx.memory_limit()`. That limit is the per-process `RLIMIT_RSS` cap, which also counts the Rust SLC binary, shared libraries, allocator arenas, and stacks, about 150 MB at startup. Ignoring that overhead over-allocates the pool and raises OOM risk on dense nodes.

### Decision

The pool budget is `max(fraction × (limit − overhead_bytes), MIN_POOL_FLOOR_BYTES)`. `MEMORY_POOL_FRACTION` (default `0.6`) and `INSTANCE_OVERHEAD_MB` (default `200`) are VS properties carried in each `ScanSpec`.

### Options Considered

| Option | Verdict |
|--------|---------|
| Lower the fraction to account for overhead | Rejected: the overhead is constant in bytes, not proportional to the limit |
| Keep `0.6 × limit` | Rejected: over-allocates by about 150 MB per instance |

### Consequences

Subtracting overhead only lowers the budget, so the invariant `budget < 0.8 × limit` holds. The zero-limit fallback of 1 GiB is unchanged.

---

## ADR: Confine Multi-Table VS Change to the VS-Adapter Layer; Scan Crate Unchanged

**ID:** confine-multi-table-vs-change-to-the-vs-adapter-layer-scan-crate-unchanged
**Plan:** `change-multi-table-virtual-schema`
**Status:** Accepted

### Context

The virtual schema grows from one fixed table to a whole Iceberg namespace. Exasol issues one single-table pushdown per table, even for JOINs, and joins the result sets itself.

### Decision

`ScanSpec`, `CatalogProps`, the scan UDF, and the fan-out SQL stay unchanged. Table identity moves from a create-time property to a per-pushdown value taken from `involvedTables[0].name` through the `TABLE_MAP` in `adapterNotes`.

### Options Considered

| Option | Verdict |
|--------|---------|
| Carry multiple tables in one `ScanSpec` | Rejected: Exasol never sends a multi-table pushdown, so the seam would go unused |

### Consequences

The scan UDF handles one Iceberg table per invocation. Multi-table scan pushdown, such as a DataFusion JOIN, is a separate plan.

---

## ADR: Persist Exasol-Name to Iceberg-Identifier Map in adapterNotes (Strategy B)

**ID:** persist-exasol-name-to-iceberg-identifier-map-in-adapternotes-strategy-b
**Plan:** `change-multi-table-virtual-schema`
**Status:** Accepted

### Context

A pushdown carries an uppercased, `__`-flattened Exasol table name. The original-cased, multi-level Iceberg `TableIdent` must be recovered either by re-listing the catalog (A) or from a map stored at create time (B).

### Decision

`createVirtualSchema` already enumerates the namespace, so it stores a `TABLE_MAP` of Exasol names to Iceberg identifiers in `adapterNotes`, and pushdown reads it back.

### Options Considered

| Option | Verdict |
|--------|---------|
| Re-list the namespace at pushdown and match case-insensitively | Rejected: adds a catalog call per query, needs signed list calls on the Glue path, and recovers casing heuristically |

### Consequences

Pushdown makes no catalog list call and recovers casing and namespace path exactly. `__` name collisions fail at create time. Iceberg views stay unsupported because the iceberg-rust `Catalog` trait has no `list_views`.

---

## ADR: Sound-Partial Iceberg Predicate Translation — Strict OR/NOT Handling

**ID:** sound-partial-iceberg-predicate-translation-strict-or-not-handling
**Plan:** `add-iceberg-predicate-pruning`
**Status:** Accepted

### Context

File-level pruning translates the Exasol WHERE predicate into an `iceberg::expr::Predicate`. A predicate that drops result rows is wrong, while one that keeps too many files is safe. Pruning on only the translatable branch of an `OR` can skip files the untranslatable branch matches.

### Decision

Translation returns `None` for "no constraint". Under `AND`, a `None` child is dropped. Under `OR`, any `None` child makes the whole `OR` `None`. `NOT` of `None` is `None`. A leaf translates only when its column resolves in the Iceberg schema and a type-matching `Datum` can be built.

### Options Considered

| Option | Verdict |
|--------|---------|
| Decline pushdown when any node is untranslatable | Rejected: forfeits pruning for any query with a LIKE, and DataFusion already guarantees correctness |
| Prune on the translatable branch of an `OR` | Rejected: unsound, it silently drops matching rows |

### Consequences

An `OR` with an untranslatable branch gets no Iceberg pruning, and DataFusion applies the full filter. Translatable `AND` conjuncts still prune.

---

## ADR: New `adapter/iceberg_predicate.rs` Module; `iceberg-rust` Types Stay out of `vs-expression`

**ID:** new-adapter-iceberg-predicate-rs-module-iceberg-rust-types-stay-out-of-vs-expression
**Plan:** `add-iceberg-predicate-pruning`
**Status:** Accepted

### Context

Iceberg pruning builds `iceberg::expr::Predicate` values from the Exasol filter JSON. `vs-expression` is shared with the sibling project and has no `iceberg-rust` dependency.

### Decision

A dedicated engine module translates the Exasol filter JSON into an optional Iceberg predicate. `vs-expression` is not extended.

### Options Considered

| Option | Verdict |
|--------|---------|
| Extend `vs-expression` to emit Iceberg predicates | Rejected: adds `iceberg-rust` as a dependency of the shared crate |

### Consequences

A sibling project needing Iceberg pruning would write its own translator or trigger a monorepo consolidation.

---

## ADR: Adopt Arrow-IPC `emit_batch` on the Raw-Row Scan Path

**ID:** adopt-arrow-ipc-emit-batch-on-the-raw-row-scan-path
**Plan:** `change-bounded-remote-scans`
**Status:** Accepted

### Context

The raw-row path converted each `RecordBatch` to a `Vec<Value>`, holding two full copies of every batch. This was a measured root cause of OOM-induced VM crashes on the live 3-node cluster.

### Decision

The raw-row path emits each `RecordBatch` through the SDK's `EmitBatch` API (`emit-arrow` feature), which serializes to Arrow IPC bytes. Only IPC bytes cross the `.so` boundary. Each batch is fetched, emitted, and dropped before the next.

### Options Considered

| Option | Verdict |
|--------|---------|
| Keep row-by-row `Vec<Value>` emit with smaller batches | Rejected: still holds two copies at peak |
| Emit Arrow types across the `.so` boundary | Rejected: Arrow `TypeId` is unstable across the boundary |

### Consequences

Peak per-batch memory is about halved. A `normalize_view_types` pass (Utf8View to Utf8, BinaryView to Binary) runs before `emit_batch`, because DataFusion can produce view types the IPC encoder rejects.

---

## ADR: Surface `ResourcesExhausted` as a Distinct Clean Error, Not a Storage Error

**ID:** surface-resourcesexhausted-as-a-distinct-clean-error-not-a-storage-error
**Plan:** `change-bounded-remote-scans`
**Status:** Accepted

### Context

When the DataFusion memory pool is exhausted and `/tmp` cannot spill (tmpfs on the live cluster), `redact_storage_error` reported the failure as "assigned data could not be read". That hid the true cause from the operator.

### Decision

The scan classifies `ResourcesExhausted` before storage redaction and reports it as a clean memory-exhaustion error. The classification applies on the raw-row path and all partial-aggregate error sites, and credential redaction applies on both paths.

### Options Considered

| Option | Verdict |
|--------|---------|
| Let `ResourcesExhausted` fall through `redact_storage_error` | Rejected: a memory bound looks like a storage failure, and the operator cannot tell OOM from a missing file |

### Consequences

On a tmpfs cluster the bounded clean error is the backstop when `/tmp` cannot spill. `probe_tmp_spill` returning `NoDisk` for tmpfs is unchanged.

---

## ADR: Bound Parquet Decode Working Set via `batch_size` in `session_config_for_spec`

**ID:** bound-parquet-decode-working-set-via-batch-size-in-session-config-for-spec
**Plan:** `change-bounded-remote-scans`
**Status:** Accepted

### Context

The DataFusion memory pools bound aggregation, sort, and join memory, but not Parquet-to-Arrow decode buffers. On the live Glue cluster, wide-table decode at the default batch size spiked memory past the per-node limit before the pool could throttle.

### Decision

`session_config_for_spec` sets `batch_size` from a `df_batch_size` field in the `ScanSpec`. A missing field falls back to a conservative default, and a value below 1 is clamped to 1. It applies on both the raw-row and partial-aggregate paths.

### Options Considered

| Option | Verdict |
|--------|---------|
| Rely on the memory pool alone | Rejected: the pool does not account for decode buffers |

### Consequences

The conservative default lowers per-instance footprint at the cost of slightly more scheduling overhead per batch (DataFusion default: 8192 rows).

---

## ADR: AUTO/FIXED Threading Mode Resolved at the Thin VS; Integers Only to the UDF

**ID:** auto-fixed-threading-mode-resolved-at-the-thin-vs-integers-only-to-the-udf
**Plan:** `change-engine-throughput`
**Status:** Accepted

### Context

Without an explicit setting, each UDF instance spawns core-count partitions and oversubscribes the node. The earlier explicit properties had a fixed numeric default, no safe auto-derivation, and no way to pin values for repeatable benchmarks without losing the safety invariant.

### Decision

The `DATAFUSION_THREADING_MODE` VS property (`AUTO` or `FIXED`, default `AUTO`) selects how budgets are computed at `createVirtualSchema`. In `AUTO`, the adapter sets `df_threads_per_udf = max(1, floor(NR_OF_CORES / udf_instances_per_node))` and keeps `df_target_partitions` equal to it. In `FIXED`, supplied values are used verbatim, each defaulting to `max(NR_OF_CORES, 1)`. The scan UDF receives only the resolved integers and stays mode-agnostic. `AUTO` stays the general safety default for CPU-bound and memory-bound workloads.

### Options Considered

| Option | Verdict |
|--------|---------|
| Always auto | Rejected: operators cannot pin values for benchmark sweeps |
| Always manual | Rejected: easy to misconfigure and oversubscribe |
| Fixed thread count at compile time | Rejected: no tuning without recompiling |

### Consequences

I/O-bound far-VPC scans can benefit from deliberate oversubscription (see ADR-038).

---

## ADR: Telemetry Built on Archived Checkpoint Infrastructure; Default OFF

**ID:** telemetry-built-on-archived-checkpoint-infrastructure-default-off
**Plan:** `change-engine-throughput`
**Status:** Accepted

### Context

Attributing throughput bottlenecks needs phase timing (startup, object-storage import, send-back) per UDF VM. The `archive/udf-diagnostics-checkpoints` branch already has concurrency-safe, per-PID instrumentation, and language-container-rs 0.19.0 provides a per-VM-tagged debug channel keyed by `LAKEHOUSE_UDF_DEBUG_LEVEL`.

### Decision

The engine restores the archived `scan/diagnostics.rs` and adds three monotonic-clock phase accumulators at existing checkpoint sites. Emission is gated on the debug level, so nothing is emitted at the production default `info`. Telemetry writes are best-effort and never fail a scan.

### Options Considered

| Option | Verdict |
|--------|---------|
| Write a fresh telemetry module | Rejected: duplicates proven concurrency-safety and per-PID isolation |

### Consequences

Production overhead is zero. The restored checkpoint trail (about 170 lines) has no production callers beyond the telemetry functions and can be trimmed to telemetry only.

---

## ADR: Decode-Emit Overlap Buffer Is Conditional / Measure-First; Not Committed

**ID:** decode-emit-overlap-buffer-is-conditional-measure-first-not-committed
**Plan:** `change-engine-throughput`
**Status:** Accepted

### Context

A bounded producer/consumer queue between the DataFusion stream and the emit calls could overlap S3 read latency with send-back time. It adds concurrency complexity and holds a batch in memory per slot, against the fetch-one, emit, drop discipline that bounds memory.

### Decision

The engine does not build the `DF_MAX_BUFFERED_BATCHES` buffer. It becomes a requirement only if phase telemetry shows that emit and object-storage import are both material and serialized, and that decoupling them gains throughput. The gate failed: emit took about 2 ms against about 650 ms for import on the far-VPC workload (ADR-037).

### Options Considered

| Option | Verdict |
|--------|---------|
| Build the buffer now | Rejected: measure first, since telemetry must show decoupling pays |

### Consequences

Memory stays bounded by `batch_size`, and the scan is import-bound on the far-VPC path.

---

## ADR: Catalog Auth Credentials Live on `ConnectionCreds`, Never on the UDF-Boundary Payload

**ID:** catalog-auth-credentials-live-on-connectioncreds-never-on-the-udf-boundary-payload
**Plan:** `add-rest-catalog-oauth-auth`
**Status:** Accepted

### Context

REST-catalog authentication (a static bearer `token` or an OAuth2 `client_id` and `client_secret`) must reach the catalog build. `CatalogProps` and `StorageProps` are serialized into `ScanSpec`, which crosses the UDF boundary, and the scan UDF never calls the catalog.

### Decision

The auth fields (`token`, `client_id`, `client_secret`, `oauth2_server_uri`, `scope`) live on `ConnectionCreds` within the planning layer and are injected when the REST catalog is built. No auth field enters `CatalogProps`, `StorageProps`, or `ScanSpec`.

### Options Considered

| Option | Verdict |
|--------|---------|
| Widen `CatalogProps` or `ScanSpec` with the auth fields | Rejected: carries catalog secrets across a boundary the scan node never needs |

### Consequences

A unit test guards that no auth field name or value appears in a serialized scan spec.

---

## ADR: SigV4 and Catalog Token/OAuth Authentication Are Mutually Exclusive, Rejected at Validation

**ID:** sigv4-and-catalog-token-oauth-authentication-are-mutually-exclusive-rejected-at-validation
**Plan:** `add-rest-catalog-oauth-auth`
**Status:** Accepted

### Context

The Glue SigV4 path signs `load_table` with static AWS credentials and bypasses `RestCatalogBuilder`, which is where token and OAuth props apply. A CONNECTION that enables both would have the SigV4 path silently drop the token or OAuth props.

### Decision

Validation rejects a CONNECTION that sets `use_sigv4` together with any catalog-auth field, with a credential-safe error. The check runs before the SigV4 S3-field guard.

### Options Considered

| Option | Verdict |
|--------|---------|
| Let SigV4 win and ignore token or OAuth | Rejected: a silent misconfiguration trap |

### Consequences

`has_catalog_auth()` is true even for partial OAuth, such as a lone `client_id`, so a SigV4 plus partial-OAuth CONNECTION trips this guard.

---

## ADR: Static S3 Fields Are Unconditionally Optional; `warehouse` the Only Always-Required Field

**ID:** static-s3-fields-are-unconditionally-optional-warehouse-the-only-always-required-field
**Plan:** `add-rest-catalog-oauth-auth`
**Status:** Accepted

### Context

`REQUIRED_CRED_KEYS` required `warehouse`, `endpoint`, `region`, `access_key`, and `secret_key`. In `iceberg-catalog-rest`, catalog auth and S3 credentials are orthogonal, and even an unauthenticated catalog can vend S3 credentials. The five-field rule rejected valid vended, token, and OAuth configurations.

### Decision

Base validation requires only `warehouse`. The four S3 fields are optional regardless of catalog auth and `use_vended_credentials`. The SigV4 path keeps a conditional requirement (ADR-043).

### Options Considered

| Option | Verdict |
|--------|---------|
| Keep all five required | Rejected: forces dummy S3 values for vended, token, and OAuth catalogs |
| Require S3 only when no catalog auth is present | Rejected: a no-auth catalog can still vend |

### Consequences

Existing static-S3 connections validate as before, and only acceptance widens.

---

## ADR: Unify Table Loading Behind One Auth-Mode-Agnostic Self-Issued `loadTable` GET

**ID:** unify-table-loading-behind-one-auth-mode-agnostic-self-issued-loadtable-get
**Plan:** `change-vended-credentials-auth-orthogonal`
**Status:** Accepted

### Context

Vended S3 credentials were extracted only on the SigV4 branch, which self-issued the `loadTable` GET. The unsigned branch used `RestCatalog::load_table`, which discards the response `config` and `storage_credentials`. So no-auth, bearer-token, and OAuth2 paths never passed vended credentials to DataFusion, a hard failure for Databricks Unity Catalog managed storage with no static S3 credentials.

### Decision

One `load_table_any_auth` function returns the raw `LoadTableResult` for every auth mode: SigV4 signature, bearer token, OAuth2-derived bearer, or none. That one response feeds Iceberg file planning and vended-credential extraction. Extraction is gated only on `use_vended_credentials`, never on the auth mode.

### Options Considered

| Option | Verdict |
|--------|---------|
| Keep `RestCatalog::load_table` for unsigned modes and add vending on top | Rejected: it returns a `Table` and discards `config` and `storage_credentials` with no public hook |

### Consequences

`use_vended_credentials` is orthogonal to authentication, and `load_table_signed` and the `use_sigv4` branches are removed. SigV4/Glue skips the `/v1/config` prefix lookup and uses the warehouse ARN directly. Other modes use `overrides.prefix` from the config endpoint, falling back to an empty prefix as the REST spec says, not to the warehouse.

---

## ADR: Perform the OAuth2 Client-Credentials Grant In-Adapter

**ID:** perform-the-oauth2-client-credentials-grant-in-adapter
**Plan:** `change-vended-credentials-auth-orthogonal`
**Status:** Accepted

### Context

The unified loader (ADR-044) needs a bearer token before its self-issued `loadTable` GET. The token cache in `iceberg-catalog-rest` is `pub(crate)` and bound to the crate's own request pipeline, so it cannot authenticate a self-issued GET.

### Decision

The adapter issues its own form-encoded `client_credentials` POST to `oauth2_server_uri`, or to the catalog default `{catalog_uri}/v1/oauth/tokens`, and uses the returned `access_token` as the bearer for the GET. The grant runs once per query, and the `client_secret` and token are redacted from every error path.

### Options Considered

| Option | Verdict |
|--------|---------|
| Reuse the `iceberg-catalog-rest` token cache | Rejected: it cannot authenticate a self-issued request |

### Consequences

Token refresh and re-vending are not implemented, because STS lifetime far exceeds one query. `redact_catalog_auth_error` keeps the secret and token out of messages. No new dependency is needed, since `reqwest` is already present.

---

## ADR: Field-Id-Based Column Projection via a PhysicalExprAdapter, Not the Iceberg Reader

**ID:** field-id-based-column-projection-via-a-physicalexpradapter-not-the-iceberg-reader
**Plan:** `fix-scan-field-id-projection`
**Status:** Accepted

### Context

The scan bound columns by physical Parquet column name, but the Iceberg spec binds by field-id. After a rename, physical `score` and logical `rating` share field-id 2 but not a name, so the scan returned wrong or missing data. iceberg-rust uses an aliased arrow 57 while DataFusion and the SDK use arrow 58, so iceberg types cannot cross into the DataFusion session. DataFusion 54 deprecates `with_schema_adapter_factory` as a no-op.

### Decision

A custom `FieldIdExprAdapter` installed on the `ListingTable` through `with_expr_adapter_factory` binds each logical column to the physical Parquet column with the same `PARQUET:field_id`, independent of name. The Parquet opener applies it per file, so files with different physical layouts in one shard each bind correctly.

### Options Considered

| Option | Verdict |
|--------|---------|
| iceberg-rust `ArrowReader` or `iceberg-datafusion` | Rejected: arrow types cannot cross the arrow 57 / 58 boundary |
| `with_schema_adapter_factory` | Rejected: deprecated no-op in DataFusion 54 |

### Consequences

Projection is correct across renamed, dropped, and added columns. No new dependency is needed, and no per-file schema pre-read is needed.

---

## ADR: Override Resolution Only; Reuse DefaultPhysicalExprAdapter for Everything Else

**ID:** override-resolution-only-reuse-defaultphysicalexpradapter-for-everything-else
**Plan:** `fix-scan-field-id-projection`
**Status:** Accepted

### Context

The `FieldIdExprAdapter` (ADR-046) must bind renamed columns, and also NULL-fill nullable columns absent from older files, cast divergent types, and fail cleanly on a missing required column. `DefaultPhysicalExprAdapter` already implements those.

### Decision

`FieldIdExprAdapter` overrides only column resolution: field-id first, with a physical-name fallback when a file field has no `PARQUET:field_id`. It delegates null-fill, casting, and the required-missing error to `DefaultPhysicalExprAdapter`. The per-column spec carries `{field_id, name, arrow_type, nullable}` with no `initial-default` (deferred to #27), and the `schema.name-mapping.default` property is not parsed (deferred to #28).

### Options Considered

| Option | Verdict |
|--------|---------|
| Reimplement a full custom schema adapter | Rejected: duplicates DataFusion behavior and risks diverging from it |

### Consequences

Null-fill, cast, and error behavior stay in sync with DataFusion upgrades.

---

## ADR: Source Cluster Node Count from `UdfContext::node_count()`, Not a Connect-Back `SELECT NPROC()` (Supersedes ADR-006)

**ID:** source-cluster-node-count-from-udfcontext-node-count-not-a-connect-back-select-nproc-supersedes-adr-006
**Plan:** `fix-createvs-cores-nodecount`
**Status:** Accepted
**Supersedes:** cluster-node-count-captured-once-at-createvirtualschema-via-adapternotes

### Context

The node count came from `SELECT NPROC()` over connect-back, in a closure whose `?` also propagated the failure of the sibling `PARAM_VALUE('NR_OF_CORES')` query. `PARAM_VALUE` is not a real Exasol function and always errors, so the valid node count was discarded and `(cluster_nodes, nr_of_cores)` collapsed to `(1, 0)` on every real cluster (#32). SDK 0.20.0 exposes `UdfContext::node_count()` from the live handshake.

### Decision

The adapter reads the node count from `UdfContext::node_count()` in-process. The value `0` (no live handshake) maps to a `CLUSTER_NODES` default of 1, and any live value is used as is. The connect-back branch, the `CONNECTION_NAME` VS property, and the `SELECT NPROC()` query are deleted, with no fallback.

### Options Considered

| Option | Verdict |
|--------|---------|
| Keep `SELECT NPROC()` over connect-back | Rejected: root cause of #32 and less reliable than an in-process read |
| Keep connect-back as a fallback behind `node_count()` | Rejected: connect-back served only topology, and a fallback reintroduces the fragile SQL path |

### Consequences

Topology discovery opens no SQL session. Existing VS instances that set `CONNECTION_NAME` ignore it silently. `CATALOG_CONNECTION` is untouched.

---

## ADR: Source Per-Node Core Count from `available_parallelism()`, Not the Bogus `PARAM_VALUE('NR_OF_CORES')` Connect-Back Query (Supersedes the Core-Count Capture in ADR-023)

**ID:** source-per-node-core-count-from-available-parallelism-not-the-bogus-param-value-nr-of-cores-connect-back-query-supersedes-the-core-count-capture-in-adr-023
**Plan:** `fix-createvs-cores-nodecount`
**Status:** Accepted
**Supersedes:** hardware-aware-default-parallelism-factor-max-nr-of-cores-2-8

### Context

ADR-023 captured `NR_OF_CORES` with `SELECT PARAM_VALUE('NR_OF_CORES')`, which always fails and discarded the node count too (ADR-048, #32). The `max(NR_OF_CORES × 2, 8)` formula is unaffected, and `available_parallelism()` is already trusted for the scan UDF's `target_partitions` on the same clusters.

### Decision

When the `NR_OF_CORES` override is absent or invalid, the adapter reads the core count from `std::thread::available_parallelism()` on the executing node, treating an unavailable reading as `0` (unknown), which keeps the floor of 8. The `PARAM_VALUE` query and its parsing helper are deleted.

### Options Considered

| Option | Verdict |
|--------|---------|
| Keep `PARAM_VALUE('NR_OF_CORES')` over connect-back | Rejected: not a real Exasol function, never worked |
| Add a live-cluster verification task for `available_parallelism()` | Rejected: redundant, since the same source is already trusted on the same clusters |

### Consequences

Core-count detection needs no SQL session. The `NR_OF_CORES` override and its precedence are unchanged.

---

## ADR: Full Positional Reorder Threading select-list Index Through Grouped-Aggregate Detection

**ID:** full-positional-reorder-threading-select-list-index-through-grouped-aggregate-detection
**Plan:** `fix-grouped-agg-select-order`
**Status:** Accepted

### Context

`detect_group_by_aggregates` split the select list into `group_keys` and `plans` and dropped each item's original index. The outer merge SELECT was then built keys-first. Exasol validates it positionally against `selectListDataTypes`, so an aggregate before or between group keys was transposed and failed with `Data type mismatch in column number N` (#33). Aggregate before a single key, interleaved multi-key GROUP BY, and an expression key after an aggregate share this root cause.

### Decision

Detection carries each select-list item's original index and classification. The outer SELECT, its casts, and its GROUP BY are assembled in select-list order for any interleaving. The inner fan-out (EMITS clause and per-shard scan) stays keys-first and unchanged, because it is matched only against itself. `ScanSpec.group_keys`, `ScanSpec.aggregates`, `build_grouped_partial_agg_sql`, and the scan UDF emit loop are unchanged.

### Options Considered

| Option | Verdict |
|--------|---------|
| Patch only aggregate-before-a-single-key | Rejected: leaves interleaved and expression-key cases broken |

### Consequences

A HAVING that contains an aggregate was silently dropped, and now renders against the merge decomposition and falls back to native execution when unrenderable (ADR-051).

---

## ADR: Two-Argument `LAKEHOUSE_SCAN(common, files)` — Shard-Invariant Spec Emitted Once, Per-Shard Files Only in `VALUES`

**ID:** two-argument-lakehouse-scan-common-files-shard-invariant-spec-emitted-once-per-shard-files-only-in-values
**Plan:** `fix-scan-spec-shard-dedup`
**Status:** Accepted

### Context

The full `ScanSpec` was serialized into every shard's single-argument `LAKEHOUSE_SCAN` call, though only `files` varies. With up to 300 shards, the shard-invariant payload, credentials included, repeated up to about 300 times in one statement, risking statement-size limits (#25). `ScanSpec.catalog` also had no production reader in the scan UDF.

### Decision

`LAKEHOUSE_SCAN` takes two arguments: a common spec and a per-shard files value. The adapter serializes the common spec once as a SELECT-list literal and puts only each shard's file JSON in the `VALUES` rows. `run_scan` rebuilds the `ScanSpec` from both. `files` is the only per-shard field. `ScanSpec.catalog` is dropped, and the rest, including `limit`, goes in the common blob. For grouped queries the common blob has no limit, so a LIMIT never reaches a per-shard partial. No single-argument compatibility path is kept, because the `.so`, SLC, and adapter deploy together.

### Options Considered

| Option | Verdict |
|--------|---------|
| Connect-back to fetch credentials at scan time | Rejected: connect-back was removed by #32 (ADR-048) |
| Stage the common spec as a BucketFS file | Rejected: adds state to a stateless UDF |
| Keep the single-argument form | Rejected: it is the bug |
| Keep `ScanSpec.catalog` for future use | Rejected: no reader, and a credential-adjacent field does not belong in the UDF payload |

### Consequences

Statement size and credential surface shrink on wide fan-outs, and no catalog block reaches a scan spec. The scan DDL and every direct invocation use the two-argument form, so the `.so`, SLC, and adapter must deploy together.

---

## ADR: Compact 2-Tuple `(path, size)` Per-Shard File Encoding

**ID:** compact-2-tuple-path-size-per-shard-file-encoding
**Plan:** `change-scan-spec-files-payload`
**Status:** Accepted

### Context

`ScanSpec.files` held bare file URIs, so each statement repeated the table-location prefix once per file across up to 300 shards (#45). The UDF also issued an object-store `HEAD` per file to recover a size the adapter had already read from the manifest (#29).

### Decision

`ScanSpec.files` becomes a list of `(path, size)` pairs, serialized as a compact `[path, size]` array by serde. The file helpers and byte-balanced partitioning carry the pair end to end.

### Options Considered

| Option | Verdict |
|--------|---------|
| Struct-per-file objects | Rejected: about 3 times the bytes per entry on a payload repeated across up to 300 shards |
| Parallel arrays of paths and sizes | Rejected: a path and its size can desynchronize, and sharding is awkward |

### Consequences

No decoder for the old bare-string form exists (ADR-056), because the adapter and UDF ship in the same `.so`.

---

## ADR: Carry the Iceberg Table Root Once in the Common Spec; Emit Paths Relative

**ID:** carry-the-iceberg-table-root-once-in-the-common-spec-emit-paths-relative
**Plan:** `change-scan-spec-files-payload`
**Status:** Accepted

### Context

Every per-shard file path repeated the Iceberg table-location prefix, which is identical for all files and already resolved once where the storage credentials are vended (#45).

### Decision

`CommonScanSpec` and `ScanSpec` gain a `table_root` field, empty by default and meaning all paths are absolute. The adapter takes the already-resolved table location from `resolve_file_list` and serializes it once in the common blob. ADR-055 defines how paths are stripped and rebuilt.

### Options Considered

| Option | Verdict |
|--------|---------|
| Repeat the absolute prefix on every path | Rejected: this is the bug (#45) |
| Stage a prefix table in BucketFS | Rejected: adds persisted state to a stateless UDF |

### Consequences

A spec with an empty `table_root` treats every path as absolute. The byte savings come from the relative paths in ADR-055.

---

## ADR: Strip-If-Prefix / Absolute-Passthrough Path Reconstruction

**ID:** strip-if-prefix-absolute-passthrough-path-reconstruction
**Plan:** `change-scan-spec-files-payload`
**Status:** Accepted

### Context

Iceberg data files need not live under the table location: `write.data.path`, object-storage hash injection, and migrated or Databricks layouts can place them elsewhere. The adapter and the UDF need symmetric strip and rebuild rules.

### Decision

The adapter strips `table_root` from a path only when the path starts with it and the match ends on a path-segment boundary. Otherwise the path stays absolute, so root `s3://bucket/tbl` does not strip from `s3://bucket/tbl-other/...`. In `register_files`, the UDF treats an entry containing `://` as absolute and joins any other entry onto `table_root`. A shard may mix relative and absolute entries.

### Options Considered

| Option | Verdict |
|--------|---------|
| Assume all files live under the table location and always strip | Rejected: incorrect, since Iceberg does not guarantee it |

### Consequences

The reconstructed path always equals the original data-file URI. The sibling-prefix case has its own regression test.

---

## ADR: One `S3_MAX_CONNECTIONS` Knob, Not a Dual Per-File/Per-Node Pair

**ID:** one-s3-max-connections-knob-not-a-dual-per-file-per-node-pair
**Plan:** `add-scan-connection-concurrency`
**Status:** Accepted

### Context

The native Exasol Parquet importer has `MaxConnections` (parallel reads within a file) and `MaxConcurrentReads` (files in parallel per node). The scan UDF had no operator knob for object-store connection concurrency.

### Decision

The engine exposes one VS property, `S3_MAX_CONNECTIONS`. It follows the `PARALLELISM_FACTOR` route through `adapterNotes` into the common spec.

### Options Considered

| Option | Verdict |
|--------|---------|
| Two properties mirroring `MaxConnections` and `MaxConcurrentReads` | Rejected: a second axis is unproven until a benchmark shows one knob is insufficient |

### Consequences

A new tuning axis ships as one operator knob until a benchmark proves a second is needed.

---

## ADR: Apply the Connection Budget via `object_store` `ClientOptions`, Not DataFusion `target_partitions`

**ID:** apply-the-connection-budget-via-object-store-clientoptions-not-datafusion-target-partitions
**Plan:** `add-scan-connection-concurrency`
**Status:** Accepted

### Context

Object-store connection concurrency is a different throughput axis from CPU concurrency, which the DataFusion thread and partition budget governs.

### Decision

The `S3_MAX_CONNECTIONS` budget sets the S3 client's HTTP connection pool through `AmazonS3Builder::with_client_options`. It applies on both the raw-row and partial-aggregate paths.

### Options Considered

| Option | Verdict |
|--------|---------|
| DataFusion `target_partitions` file-group splitting | Rejected: that is the CPU axis, already a separate knob |
| `datafusion.execution.meta_fetch_concurrency` | Rejected: it affects only schema and stats reads |

### Consequences

Connection concurrency is a first-class tuning axis, separate from the thread and partition budget.

---

## ADR: Advertise `AGGREGATE_GROUP_BY_TUPLE`, Reversing the Prior Exclusion

**ID:** advertise-aggregate-group-by-tuple-reversing-the-prior-exclusion
**Plan:** `fix-multi-column-group-by-pushdown`
**Status:** Accepted

### Context

Grouped-aggregate detection and SQL building already handle any number of group keys, but `AGGREGATE_GROUP_BY_TUPLE` was excluded from `CAPABILITIES`. Exasol then never pushed a multi-key GROUP BY and instead ran a raw row scan, shipping every row over the network (#53).

### Decision

`AGGREGATE_GROUP_BY_TUPLE` is added to `CAPABILITIES`, so Exasol sends multi-key GROUP BY as a pushdown request.

### Options Considered

| Option | Verdict |
|--------|---------|
| Keep it excluded | Rejected: multi-column GROUP BY is common and the raw-scan fallback defeats grouped pushdown |

### Consequences

The multi-key path must be proven correct end to end (ADR-061).

---

## ADR: Constant-Projection-Over-`GROUP BY` Placeholder Drives the Existing Grouped Scan Instead of the Row-Scan Path

**ID:** constant-projection-over-group-by-placeholder-drives-the-existing-grouped-scan-instead-of-the-row-scan-path
**Plan:** `fix-nested-aggregate-pushdown`
**Status:** Accepted

### Context

For `SELECT COUNT(*) FROM (SELECT id, COUNT(*) FROM t GROUP BY id)`, Exasol sends one flat `group_by` pushdown whose `selectList` is a single `literal_null` placeholder, as the optimizer's "count the groups" rewrite. `detect_group_by_aggregates` rejected it because `NULL` matched no group key. The row-scan path then pushed `NULL` as a phantom column that DataFusion rejects. A row-scan fallback returns one row per source row, not per group, which is wrong for duplicate group-key values such as `LINEITEM.L_ORDERKEY` (#52).

### Decision

`detect_group_by_aggregates` treats a pure-literal select-list item as a constant "count the groups" projection and drives the existing grouped scan with an empty aggregate-plan list. This keeps one row per distinct group for any key cardinality. `extract_projection` also no longer pushes a rendered literal as a column name.

### Options Considered

| Option | Verdict |
|--------|---------|
| Tighten the guard so the row-scan fallback engages | Rejected: returns the raw row count, not the group count, for duplicate keys |
| Return an error to force native retry | Rejected: a VS has no native data path, so the query just fails |

### Consequences

The regression test covers a duplicate-key group column, because unique-key data cannot tell the grouped fix from the row-scan fallback.

---

## ADR: Expression Aggregate Arguments Carried on a New `arg_expr` Field, Rendered via `render_expression`

**ID:** expression-aggregate-arguments-carried-on-a-new-arg-expr-field-rendered-via-render-expression
**Plan:** `add-count-distinct-and-expression-aggregate-pushdown`
**Status:** Accepted

### Context

`SUM(LENGTH(L_COMMENT))`-shaped aggregates fell back to a raw row scan because `AggregatePlan` accepted only a bare column argument. `render_expression` already renders arbitrary fragments for GROUP BY keys.

### Decision

`AggregatePlan` gains `arg_expr: Option<String>` for the rendered fragment, and `column` stays for the bare-column path. The scan uses `arg_expr` verbatim. Partial and merge types for an expression argument come from the declared type in `selectListDataTypes`. It applies to SUM, MIN, MAX, AVG, and COUNT(col). An argument the translator cannot render soundly falls back to row scanning.

### Options Considered

| Option | Verdict |
|--------|---------|
| Overload `column` to also hold rendered SQL | Rejected: bare-column MIN and MAX partials look up the exact source type by name, which overloading would break |

### Consequences

Expression-argument aggregates use the shard-associative partial and merge plan. The `selectListDataTypes` typing path must stay in sync with the declared select-list types.

---

## ADR: Two-Column Arithmetic Aggregate Gap Is Fixed by Capability Advertisement, Not New Machinery

**ID:** two-column-arithmetic-aggregate-gap-is-fixed-by-capability-advertisement-not-new-machinery
**Plan:** `add-arithmetic-aggregate-pushdown-and-benchmark-suite`
**Status:** Accepted

### Context

`SUM(l_extendedprice * l_discount)` fell back to a raw two-column row scan although the expression-argument SUM machinery existed. `capabilities.rs` advertised `FN_MOD` but none of the arithmetic operators, so Exasol never built a pushdown node for `+`, `-`, `*`, or `/`.

### Decision

The adapter advertises `FN_ADD`, `FN_SUB`, `FN_MULT`, and `FN_FLOAT_DIV`, reconciles the translator's operator-name matching, and reuses the existing expression-argument SUM machinery.

### Options Considered

| Option | Verdict |
|--------|---------|
| Build a dedicated two-column-product decomposition | Rejected: `AggKind::Sum` with `arg_expr` already covers it |
| Build binary-arithmetic translation | Rejected: the translator already renders `ADD`, `SUB`, `MUL`, and `FLOAT_DIV` |

### Consequences

`SUM(col_a OP col_b)` pushes down. Exasol has no position-scoped advertisement, so arithmetic is also enabled in filter, select-list, and group-key positions. Untranslatable nodes still fall back safely.

---

## ADR: Raw-Scan Projection Gets an Explicit `ProjectionItem` Tag Instead of a Syntactic Heuristic

**ID:** raw-scan-projection-gets-an-explicit-projectionitem-tag-instead-of-a-syntactic-heuristic
**Plan:** `add-arithmetic-aggregate-pushdown-and-benchmark-suite`
**Status:** Accepted

### Context

`extract_projection` put bare column names and rendered expression fragments in one `Vec<String>`. `build_scan_sql` quoted every entry as an identifier, turning `("SCORE" * 2)` into a phantom column name and failing live with `No field named "(""SCORE"" * 2)"`.

### Decision

`ScanSpec.projection` and `CommonScanSpec.projection` hold `ProjectionItem` values, an untagged enum of `Column(String)` and `Expr { expr: String }`, tagged where `extract_projection` knows the kind. `Column` keeps its CAST-for-JSON-fallback and quoting. `Expr` is spliced verbatim, like `spec.filter`.

### Options Considered

| Option | Verdict |
|--------|---------|
| Syntactic heuristic in `build_scan_sql` | Rejected: fragile, and an exotic quoted column name would be misclassified |
| Check whether the item is a field in the Arrow schema | Rejected: implicit, and changes the defensive path for a bare column absent from the schema |
| Parallel `Vec<bool>` or `Vec<Option<String>>` | Rejected: positional alignment is fragile |

### Consequences

A `Column` still serializes as a bare JSON string, so the common wire format is unchanged. Never overload one string field for both a bare identifier and rendered SQL: the `column`, `arg_expr`, and `ProjectionItem` pattern is the convention for anything crossing the adapter-to-scan boundary.

---

## ADR: Close the NQ4 Top-N Loss by Advertising ORDER_BY_COLUMN + a Partial/Merge Top-N

**ID:** close-the-nq4-top-n-loss-by-advertising-order-by-column-a-partial-merge-top-n
**Plan:** `add-topn-pushdown`
**Status:** Accepted

### Context

NQ4 (`ORDER BY L_EXTENDEDPRICE DESC LIMIT 20` on one table) lost to Trino, 12.03s against 4.71s on TPC-H sf=30. The adapter advertised no `ORDER_BY*` capability, so it raw-emitted the whole table for Exasol to sort. The join-pushdown non-goal does not apply to a single-table loss.

### Decision

The adapter advertises `ORDER_BY_COLUMN` and pushes `ORDER BY <bare projected columns> LIMIT n` as a per-shard top-N merged by an outer Exasol `ORDER BY ... LIMIT n`. It reuses the shape of the aggregate partial and merge machinery, not its aggregate-specific code.

### Options Considered

| Option | Verdict |
|--------|---------|
| Leave the raw scan and accept the loss | Rejected: a single-table query within the standing directive to optimize non-join losses |
| Change file sharding to co-locate top rows | Rejected: violates the sharding-architecture non-goal |
| General ORDER BY pushdown (expression keys, offset, ordered aggregates) | Rejected: column top-N covers the target and the common shape |

### Consequences

NQ4 improved from 12.03s to 2.13s, ahead of Trino's 4.71s. The top-N path is a new partial and merge variant beside the aggregate path.

---

## ADR: Never Push a Bare Per-Shard LIMIT Ahead of a Global Sort

**ID:** never-push-a-bare-per-shard-limit-ahead-of-a-global-sort
**Plan:** `add-topn-pushdown`
**Status:** Accepted

### Context

With `ORDER_BY_COLUMN` advertised, Exasol sends `order_by` and `limit` together. A bare per-shard limit before a global sort lets each shard return an arbitrary subset and silently truncates the true top-N for any ORDER BY the adapter does not match as top-N.

### Decision

The adapter emits the per-shard limit only with the matching per-shard `ORDER BY`. For an ORDER-BY request it does not match as top-N, it withholds the per-shard limit and leaves row selection to Exasol's ordering.

### Options Considered

| Option | Verdict |
|--------|---------|
| Keep pushing the per-shard limit whenever a `limit` is present | Rejected: unsafe once `order_by` can accompany a `limit` |

### Consequences

A spec scenario and a unit test assert this. A known residual remains: ORDER BY over an unprojected column with LIMIT returns unsorted, untruncated results, because Exasol does not always re-apply its own ordering once both clauses are delegated. Previously this failed silently, and it now fails loudly.

---

## ADR: Returned SQL for the Matched Top-N Is Self-Contained, Not Dependent on an Exasol Re-Sort Backstop

**ID:** returned-sql-for-the-matched-top-n-is-self-contained-not-dependent-on-an-exasol-re-sort-backstop
**Plan:** `add-topn-pushdown`
**Status:** Accepted

### Context

For `LIMIT` and `HAVING`, the engine relies on Exasol re-applying the pushed clause as a correctness backstop. For top-N, depending on an Exasol re-sort is an avoidable risk, and an outer wrapper already exists for the shard fan-out.

### Decision

The matched top-N path returns an outer `SELECT <proj> FROM (<fan-out>) ORDER BY <keys> LIMIT n` that fully specifies the final ordering. It does not rely on Exasol re-applying the pushed `ORDER BY`. Every path that can receive an `order_by` request renders an explicit final `ORDER BY` and `LIMIT`, including the grouped-aggregate path, which had never rendered an `ORDER BY`.

### Options Considered

| Option | Verdict |
|--------|---------|
| Rely on the Exasol backstop, as `LIMIT` and `HAVING` do | Rejected for the matched path: the backstop does not always apply. It remains the safety net for unmatched decline shapes |

### Consequences

Live verification confirmed the outer merge SQL renders its own final `ORDER BY ... LIMIT`.

---

## ADR: Direction and NULL Placement Must Be Rendered Identically Per-Shard and in the Merge

**ID:** direction-and-null-placement-must-be-rendered-identically-per-shard-and-in-the-merge
**Plan:** `add-topn-pushdown`
**Status:** Accepted

### Context

A distributed top-N is exact only if the per-shard sort and the Exasol-side merge sort give the same ranking. If their default NULL placements differ, the per-shard cut and the merge disagree near NULLs.

### Decision

Both the per-shard `ORDER BY` (scan UDF) and the outer merge `ORDER BY` (adapter) render explicit `ASC` or `DESC` and `NULLS FIRST` or `NULLS LAST`, taken from Exasol's wire shape (`isAscending`, `nullsLast`). Neither engine's default NULL ordering is used.

### Options Considered

| Option | Verdict |
|--------|---------|
| Render only direction and default NULL ordering on each side | Rejected: differing defaults make the per-shard and merge rankings disagree |

### Consequences

A dedicated NULL-placement unit test covers this, and live verification matched the per-shard spec.

---

## ADR: Decline the Top-N Shape When a Sort Key Column Needs the JSON-Fallback VARCHAR Cast

**ID:** decline-the-top-n-shape-when-a-sort-key-column-needs-the-json-fallback-varchar-cast
**Plan:** `add-topn-pushdown`
**Status:** Accepted

### Context

For a sort key whose Arrow type needs the JSON-fallback VARCHAR cast, the per-shard `ORDER BY` binds to the native value, but the emitted value is a JSON string. Exasol's merge re-ranks that string lexicographically, so the per-shard and merge rankings disagree and the global top-N is silently wrong. NQ4 is not affected, since `L_EXTENDEDPRICE` is a plain in-range DECIMAL.

### Decision

`detect_topn` resolves each sort key's Arrow type from the logical schema and declines the whole top-N shape, falling back to the raw scan, when the type needs the JSON fallback cast or the column is absent from the schema.

### Options Considered

| Option | Verdict |
|--------|---------|
| Emit the sort key uncast and cast a duplicate projection column | Rejected: needs extra trailing EMITS columns, disproportionate for a shape no known query hits |
| Sort the merge on the pre-cast value | Rejected: Exasol only receives the emitted representation |

### Consequences

A unit test covers this. The guard becomes fully load-bearing once the logical-schema tag vocabulary preserves richer types.

---

## ADR: Shape-Aware Zero-Files Short-Circuit via a Hoisted Plan Decision

**ID:** shape-aware-zero-files-short-circuit-via-a-hoisted-plan-decision
**Plan:** `fix-aggregate-pushdown-empty-file-pruning`
**Status:** Accepted

### Context

`handle_pushdown` returned the raw row-scan empty shape for zero files before aggregate-shape detection ran. For aggregate and grouped-aggregate requests that was the wrong column count, and Exasol rejected it (`sqlCode 04000`). Both detection functions are pure over the request and need no files.

### Decision

The request-shape decision moves ahead of the zero-files short-circuit, which dispatches to three empty-result builders: grouped zero-row, single-group one-row, and row-scan projection. They reuse the existing detection and type helpers, so empty and non-empty shapes derive from the same sources.

### Options Considered

| Option | Verdict |
|--------|---------|
| Pass an is-aggregate flag into `empty_pushdown_sql` | Rejected: carries neither grouped shape nor per-`AggKind` semantics |
| Run the normal fan-out with zero shards | Rejected: a single-group `COUNT` merges to `NULL` instead of 0, and a grouped fan-out over zero shards is malformed |

### Consequences

Empty and non-empty column shapes cannot drift apart. Unit tests cover each plan shape, and end-to-end tests confirm Exasol accepts the response for a fully pruned query.

---

## ADR: Pin iceberg 0.10.0-rc.2 via git tag, not a crates.io exact-version pin

**ID:** pin-iceberg-0-10-0-rc-2-via-git-tag-not-a-crates-io-exact-version-pin
**Plan:** `change-iceberg-rust-0-10-bump`
**Status:** Superseded by pin-iceberg-0-10-0-via-crates-io-registry-version-not-git-tag

### Context

The iceberg-rust release candidate is a pre-release whose API can still change. crates.io does not publish it, so it resolves only as a git tag of `apache/iceberg-rust`. A later release candidate or GA bump should be deliberate and reviewed.

### Decision

The `iceberg`, `iceberg-catalog-rest`, and `iceberg-storage-opendal` dependencies are pinned to the release candidate's git tag, not a crates.io version.

### Options Considered

| Option | Verdict |
|--------|---------|
| Exact crates.io version | Rejected: crates.io does not publish this version |
| Bare commit `rev` | Rejected: loses the human-readable tag |

### Consequences

The pin is immutable and self-documenting, and a later bump is an explicit edit, not a `cargo update` side effect.

---

## ADR: Drop `tpchgen-arrow`; build arrow-58 batches from `tpchgen` core directly

**ID:** drop-tpchgen-arrow-build-arrow-58-batches-from-tpchgen-core-directly
**Plan:** `change-iceberg-rust-0-10-bump`
**Status:** Accepted

### Context

`tpchgen-arrow` has releases for arrow 57 and arrow 59 but none for arrow 58, which the iceberg 0.10 writer expects. Keeping it leaves a second arrow tree in the dev and e2e graph and needs an Arrow IPC bridge. `tpchgen` core has no arrow dependency.

### Decision

The `tpchgen-arrow` dependency is removed. The seed and TPC-H loader code builds arrow-58 `RecordBatch`es from `tpchgen` core with the workspace arrow builders.

### Options Considered

| Option | Verdict |
|--------|---------|
| Keep `tpchgen-arrow` for arrow 57 with an IPC bridge | Rejected: leaves arrow 57 in the dev lock and adds an IPC round-trip |
| Use `tpchgen-arrow` for arrow 59 with an IPC bridge | Rejected: adds an arrow tree newer than the workspace's, API churn, and the same bridge |
| Drop the TPC-H loader | Rejected: it backs the live smoke test |

### Consequences

The dev and e2e graph has a single arrow-58 tree, and a future arrow bump needs no `tpchgen-arrow` release. The cost is about 100 to 200 lines of test-only column builders.

---

## ADR: Reference the Broadcast Join Dimension Side by File List, Not Materialized Rows

**ID:** reference-the-broadcast-join-dimension-side-by-file-list-not-materialized-rows
**Plan:** `add-join-pushdown-broadcast`
**Status:** Accepted

### Context

Broadcast inner equi-join pushdown needs the small dimension side available to every fact-side shard with no cross-shard exchange. The VS layer must stay thin, and the common spec is repeated to every shard, so its size costs per-shard payload.

### Decision

The shard-invariant common spec carries the dimension side's file list, table root, and logical schema. Each shard re-scans that file list itself and joins it locally against its fact-file subset, reusing `register_files`.

### Options Considered

| Option | Verdict |
|--------|---------|
| Materialize the dimension rows in the VS as base64 Arrow IPC in the common spec | Rejected: moves execution into the VS and repeats a large blob to every shard |

### Consequences

Each shard reads the dimension side itself, bounded by `JOIN_BROADCAST_MAX_BYTES` (default 128 MiB). The common spec gains a join block (table root, file list, logical schema, join type, rendered condition), absent for non-join specs.

---

## ADR: Ineligible Joins Fall Back to Deterministic Unaccelerated SQL, Not an Error

**ID:** ineligible-joins-fall-back-to-deterministic-unaccelerated-sql-not-an-error
**Plan:** `add-join-pushdown-broadcast`
**Status:** Accepted

### Context

Capabilities are advertised once and statically. Once `JOIN`, `JOIN_TYPE_INNER`, and `JOIN_CONDITION_EQUI` are advertised, Exasol pushes every inner equi-join to the adapter, including shapes the broadcast contract does not cover. Those must not regress working join queries.

### Decision

For an inner equi-join it cannot broadcast, the adapter emits SQL that scans each table through its own sharded fan-out subquery and lets Exasol join the results. A hard error is reserved for when even that fallback SQL cannot be built. ADR-085 corrects the fallback's rendering without changing this routing.

### Options Considered

| Option | Verdict |
|--------|---------|
| Decline ineligible joins with an error and rely on Exasol re-planning | Rejected: Exasol does not re-plan cleanly on an adapter error, so working join queries could fail |

### Consequences

The adapter keeps two join-rendering paths, broadcast fan-out and unaccelerated two-scan, each tested independently.

---

## ADR: Shard Only the Fact Side; the Large-Side Sharding Model Is Unchanged

**ID:** shard-only-the-fact-side-the-large-side-sharding-model-is-unchanged
**Plan:** `add-join-pushdown-broadcast`
**Status:** Accepted

### Context

Broadcast join pushdown (Phase 1 of BL-001) must decide how much of the single-table sharding model to change. Phase 2, a large-by-large shuffle join, is out of scope.

### Decision

The fact side keeps the existing `GROUP BY shard_key` work-unit sharding. Only the dimension side's delivery (ADR-082) and the in-UDF join are new, and no cross-shard exchange is added.

### Options Considered

| Option | Verdict |
|--------|---------|
| Re-partition either side by join key (shuffle join) | Rejected: out of scope for Phase 1 |

### Consequences

Broadcast join pushdown is additive, and the work-unit sharding model is untouched.

---

## ADR: Two-Scan Fallback Renders Table-Qualified Columns, Independent of the Disjoint-Column Guard

**ID:** two-scan-fallback-renders-table-qualified-columns-independent-of-the-disjoint-column-guard
**Plan:** `add-join-pushdown-broadcast`
**Status:** Accepted
**Supersedes:** ineligible-joins-fall-back-to-deterministic-unaccelerated-sql-not-an-error

### Context

This corrects the rendering of ADR-083's two-scan fallback for joins whose tables share a column name. The disjoint-column guard keeps the broadcast path's bare-name rendering unambiguous. The two-scan fallback reused that guard-gated rendering and returned a hard error, regressing a working query because Exasol does not retry natively on that error.

### Decision

The two-scan fallback renders its join condition, WHERE, select list, GROUP BY, HAVING, and ORDER BY with table-qualified references (`"LHS_FACT"."COL"`, `"LHS_DIM"."COL"`), resolved from each column's `tableName`. It is not gated on the disjoint-column guard, which governs broadcast eligibility only. The adapter annotates each `column` node with a `tableAlias`, and the shared `vs-expression` translator emits `"ALIAS"."NAME"` when it is present and the bare name otherwise. A guard failure means only that broadcast is unavailable. A hard error is reserved for a condition that cannot be rendered in either form.

### Options Considered

| Option | Verdict |
|--------|---------|
| Keep the shared bare-name rendering and widen the guard | Rejected: still errors on shared-column joins the two-scan path can serve |

### Consequences

The single-table and broadcast paths are unaffected, since the bare name is used when `tableAlias` is absent. ADR-086 covers a paired aggregate-over-join fix.

---

## ADR: Aggregate-Over-Join Routes Through the Qualified Two-Scan Path, Not a Decline

**ID:** aggregate-over-join-routes-through-the-qualified-two-scan-path-not-a-decline
**Plan:** `add-join-pushdown-broadcast`
**Status:** Accepted

### Context

`SELECT COUNT(*), MIN(o.O_ORDERDATE) FROM CUSTOMER JOIN ORDERS ON ...` failed with "Expected number of columns is 2 but pushdown query has 5". The fallback ignored the aggregate select list and emitted the full cross-table row projection.

### Decision

Any join request with an aggregate, GROUP BY, ORDER BY, LIMIT, or HAVING goes to the two-scan path whatever its broadcast eligibility, because the broadcast in-UDF join renders only projection, filter, and join condition. The two-scan wrapper renders the aggregate select list as ordinary Exasol SQL over the materialized join, so Exasol evaluates it as it did before the JOIN capability. The aggregate function name is spliced verbatim and only its column argument is table-qualified.

### Options Considered

| Option | Verdict |
|--------|---------|
| Decline aggregate-over-join pushdowns with an error | Rejected: same reason as ADR-083, a hard error risks regressing working queries |

### Consequences

Aggregate, GROUP BY, ORDER BY, LIMIT, and HAVING joins are served by the qualified two-scan wrapper (ADR-085), never by the broadcast join.

---

## ADR: Keep DataFusion `ParquetSource`; Apply Positional Deletes via a Per-File Base `ParquetAccessPlan`

**ID:** keep-datafusion-parquetsource-apply-positional-deletes-via-a-per-file-base-parquetaccessplan
**Plan:** `add-positional-delete-application`
**Status:** Accepted

### Context

The scan collapsed each Iceberg `FileScanTask` to a `(path, size)` pair and discarded its deletes, so merge-on-read queries silently returned pre-delete rows (#11, #68). Applying positional deletes must not give up DataFusion's projection, filter, and LIMIT pushdown, row-group and page pruning, statistics, or streaming.

### Decision

DataFusion's `ParquetSource` stays the scan engine. Positional deletes are applied through a per-data-file `ParquetAccessPlan` base row selection attached with `PartitionedFile::with_extensions`. The Parquet opener intersects its pruning with that selection.

### Options Considered

| Option | Verdict |
|--------|---------|
| iceberg-rust `ArrowReader` or `iceberg-datafusion` `IcebergTableScan` | Rejected: loses DataFusion pushdown, pruning, statistics, and streaming, and re-plans files inside the scan, breaking file-level work assignment and resolve-once |

### Consequences

Delete application composes with existing pushdown and pruning, and the delete-free path is unaffected. The positions-to-`RowSelection` construction must be vendored, since it is not a public dependency surface.

---

## ADR: Unify the Scan Provider on the Custom `ParquetSource`-Backed `TableProvider`, Gated by a Plan-Shape Test

**ID:** unify-the-scan-provider-on-the-custom-parquetsource-backed-tableprovider-gated-by-a-plan-shape-test
**Plan:** `add-positional-delete-application`
**Status:** Accepted

### Context

A per-file `ParquetAccessPlan` needs a directly built `FileScanConfig`, which the `ListingTable` registration path does not allow. The scan needs one provider for all files or two paths gated on whether a file has deletes.

### Decision

Every scan, delete-free or merge-on-read, uses the custom `ParquetSource`-backed `TableProvider` in place of `ListingTable`. If a plan-shape and pruning-preservation test shows a noticeable delete-free regression, the engine falls back to `ListingTable` for delete-free scans and the custom provider only when deletes are present.

### Options Considered

| Option | Verdict |
|--------|---------|
| Conditional paths from the start | Rejected as the default, kept as the fallback if the unified path regresses the delete-free plan shape |

### Consequences

A regression in the unified provider would affect every query, so the plan-shape test guards it. Triggering the fallback would add a second, conditional registration path.

---

## ADR: Plan-Time Fail-Loud at the Manifest / `DataFile` Level Is the Authoritative Correctness Gate for Unsupported Deletes

**ID:** plan-time-fail-loud-at-the-manifest-datafile-level-is-the-authoritative-correctness-gate-for-unsupported-deletes
**Plan:** `add-positional-delete-application`
**Status:** Accepted

### Context

The engine cannot apply equality deletes, Puffin or v3 deletion vectors, or ORC and Avro files, and previously did not detect them, so it silently returned pre-delete rows. `plan_files` drops the Puffin discriminator, so scan-time detection cannot tell a deletion vector from a Parquet positional delete.

### Decision

The adapter detects unsupported delete mechanisms at plan time, at the manifest and `DataFile` level where the discriminator and file format are visible, before building any scan-driving SQL. This is the authoritative gate. A lightweight scan-time check stays as defense in depth.

### Options Considered

| Option | Verdict |
|--------|---------|
| Read-time detection only | Rejected as the sole guard: the Puffin discriminator is gone once the scan spec is built |

### Consequences

An unsupported-delete query fails immediately without emitting SQL or credentials. Equality deletes and deletion vectors remain future work under #11, by extending the same detection point.

---

## ADR: Minimal Scan-Spec Surface for Delete Support — Per-File References Only

**ID:** minimal-scan-spec-surface-for-delete-support-per-file-references-only
**Plan:** `add-positional-delete-application`
**Status:** Accepted

### Context

The scan UDF needs each data file's delete files, but the adapter-to-UDF wire format is kept minimal. A broader design would carry a serialized Iceberg `Schema` and a `BoundPredicate`.

### Decision

The per-shard `files` argument gains only per-file positional-delete references (path, byte size, delete content type). `logical_schema` and `FieldIdExprAdapter` stay as they are, and no `Schema` or `BoundPredicate` is carried. Legacy `(path, size)` entries deserialize with an empty delete list.

### Options Considered

| Option | Verdict |
|--------|---------|
| Carry a serialized Iceberg `Schema` and a bound `BoundPredicate` | Rejected: unnecessary weight, and the design of the rejected `add-iceberg-delete-application` plan |

### Consequences

A delete-free table produces a byte-identical common spec to before. Future delete mechanisms must extend this per-file surface.

---

## ADR: Per-Side Predicate Pushdown for Joins by `tableName` Conjunct Attribution

**ID:** per-side-predicate-pushdown-for-joins-by-tablename-conjunct-attribution
**Plan:** `add-join-pushdown-broadcast`
**Status:** Accepted

### Context

Each side of a join over-scanned. Both join routes resolved file lists with no filter, so neither side got Iceberg manifest pruning. The two-scan fallback built each leg's `ScanSpec` with `filter: None`, so each leg shipped every row to Exasol. It also projected each table's full involved-column set. Passing the whole WHERE to each side is unsound: `to_iceberg_predicate` resolves columns by name only, so on a shared column name it applies the other side's conjunct to this side and wrongly prunes files.

### Decision

For an inner equi-join, the adapter attributes each WHERE conjunct to a side by its columns' `tableName`, the signal ADR-085 already uses. A conjunct is side-local when every column it references belongs to one table. Only side-local conjuncts are pushed per side. Cross-table conjuncts, the join condition, and any OR spanning both tables stay in the outer wrapper's WHERE, which remains the correctness backstop. The side-local predicate feeds manifest pruning on both routes and the two-scan leg's `ScanSpec.filter`. Each fallback leg's projection narrows to the columns the outer wrapper references for that side, including the full WHERE's columns. The leg filter strips `tableAlias` and renders bare, because a per-side fan-out is a single-table scan whose relation exposes bare column names. The broadcast path keeps the native alias.

### Options Considered

| Option | Verdict |
|--------|---------|
| Pass the whole WHERE to each side and rely on `to_iceberg_predicate` dropping unknown columns | Rejected: unsound on shared column names, since name-only resolution prunes wrongly |

### Consequences

Both routes get manifest pruning per side, and the fallback legs filter and prune before emitting. Pruned byte totals also make side selection and the broadcast threshold more accurate. Unit tests cover the shared-column-name case so ADR-085 cannot regress, and a regression test pins the bare rendering in the fan-out and the alias-preserving rendering in broadcast.

---

## ADR: N-Table Inner Joins Fall Back to an N-Scan Unaccelerated Wrapper (Generalizes ADR-083 to N Tables)

**ID:** n-table-inner-joins-fall-back-to-an-n-scan-unaccelerated-wrapper-generalizes-adr-083-to-n-tables
**Plan:** `fix-join-decline-hard-fail`
**Status:** Superseded by single-unified-n-2-unaccelerated-join-renderer-supersedes-adr-092-adr-093-adr-094

### Context

An inner join over three or more tables hard-failed with `F-UDF-CL-RUST-9001: join pushdown declined` (#76). The `exasol-udf-macros` FFI shim turns every `UdfError` into a hard SQL error, and no native-retry path exists for a declined pushdown. The spec already required N-table fallback.

### Decision

A pushdown over an inner join of three or more tables materializes each table through its own sharded scan fan-out and rebuilds the join in Exasol's core engine. An error is reserved for a shape whose fallback cannot be built: a non-inner join node, a table absent from `TABLE_MAP` or without column metadata, or a clause the translator cannot render.

### Options Considered

| Option | Verdict |
|--------|---------|
| Keep declining `TooManyTables` | Rejected: hard-fails a query class the spec requires |
| Advertise fewer join capabilities so Exasol never pushes multi-table joins | Rejected: regresses the two-table broadcast benefit and still does not match the spec |

### Consequences

The two-table broadcast and two-scan paths are unaffected.

---

## ADR: N-Scan Wrapper Renders as Cross-Join + Conjunctive Table-Qualified WHERE

**ID:** n-scan-wrapper-renders-as-cross-join-conjunctive-table-qualified-where
**Plan:** `fix-join-decline-hard-fail`
**Status:** Superseded by single-unified-n-2-unaccelerated-join-renderer-supersedes-adr-092-adr-093-adr-094

### Context

Rebuilding an all-inner join tree over N fan-out subqueries as a chained `INNER JOIN ... ON` needs ON-scope bookkeeping, so each condition references only tables already introduced. That is error-prone for arbitrary trees.

### Decision

The N-scan wrapper selects from the N fan-outs as a comma cross join. Its WHERE holds all N-1 join conditions and the qualified residual filter, with every column table-qualified from its `tableName` through the ADR-085 alias machinery.

### Options Considered

| Option | Verdict |
|--------|---------|
| Chained `INNER JOIN ... ON` tree reproducing the pushed join tree | Rejected: needs ON-scope bookkeeping, and Exasol re-optimizes anyway |

### Consequences

Cross join with conjunctive WHERE is equivalent to any join order for all-inner joins, and Exasol turns equi-conditioned cross joins into hash joins. The builder needs no per-condition table tracking, and the ADR-085 qualified-rendering functions are reused.

---

## ADR: Freeze the Two-Table Join Path; Add the N-Table Path Additively

**ID:** freeze-the-two-table-join-path-add-the-n-table-path-additively
**Plan:** `fix-join-decline-hard-fail`
**Status:** Superseded by single-unified-n-2-unaccelerated-join-renderer-supersedes-adr-092-adr-093-adr-094

### Context

N-table support could be retrofitted into the two-table join structures or added as a separate path. The two-table broadcast and two-scan code (ADR-081 to 086) is live-tested.

### Decision

A new `JoinShape::MultiTable` path serves N of 3 or more, and the two-table path and its tests stay unchanged.

### Options Considered

| Option | Verdict |
|--------|---------|
| Retrofit N tables into the existing two-table structures | Rejected: churns working code and its E2E assertions |

### Consequences

The two-table paths carry no regression risk. Extensions should build on the `MultiTable` path unless an N-way broadcast join is pursued, which the mission's "no N-table broadcast" non-goal currently excludes.

---

## ADR: Single Unified N≥2 Unaccelerated Join Renderer (Supersedes ADR-092, ADR-093, ADR-094)

**ID:** single-unified-n-2-unaccelerated-join-renderer-supersedes-adr-092-adr-093-adr-094
**Plan:** `fix-join-decline-hard-fail`
**Status:** Accepted
**Supersedes:** n-table-inner-joins-fall-back-to-an-n-scan-unaccelerated-wrapper-generalizes-adr-083-to-n-tables
**Supersedes:** n-scan-wrapper-renders-as-cross-join-conjunctive-table-qualified-where
**Supersedes:** freeze-the-two-table-join-path-add-the-n-table-path-additively

### Context

The additive design of ADR-094 kept two renderers, a frozen two-table path and an N of 3 or more path. The #76 rendering gap existed in both, and the first fix touched only one. The adapter advertises the inner equi-join capabilities statically, so Exasol pushes inner equi-joins of any arity, and any divergence between the renderers is a latent correctness gap.

### Decision

One join planner and one fallback renderer serve all inner joins of two or more tables. `detect_join` yields one join shape carrying N tables and N-1 conditions. `plan_join` takes the broadcast fan-out when N is 2, the small side is within `JOIN_BROADCAST_MAX_BYTES`, and no Exasol postprocessing is needed. Otherwise `build_n_scan_join_sql` renders the fallback, using ADR-093's cross-join technique, ADR-091's per-side predicate pushdown, and ADR-085's qualified rendering. The separate two-table fallback builders, the `Eligible` and `MultiTable` split, and the `LHS_FACT` and `LHS_DIM` aliases are removed. ADR-092's outcome stands: a join of 3 or more tables never errors.

### Options Considered

| Option | Verdict |
|--------|---------|
| Keep the additive two-path design (ADR-094) | Rejected: the two copies drifted and shipped the #76 and PR-78 bug, and a fix applied to both leaves the same risk |

### Consequences

Any join-rendering fix lands once. Existing two-scan tests migrate to `LHS_T0` and `LHS_T1`, and the two-table SQL shape is otherwise unchanged.

---

## ADR: `vs-expression` Renders Aggregate Function Nodes at the Shared Seam

**ID:** vs-expression-renders-aggregate-function-nodes-at-the-shared-seam
**Plan:** `fix-join-decline-hard-fail`
**Status:** Accepted

### Context

The root cause of the PR #78 defect was not join arity. A grouped select item that wraps aggregates in a scalar function, such as `ROUND(100.0 * SUM(CASE ...) / COUNT(*), 2)`, declined at every arity. `render_expression_inner` had a `function_scalar` arm but no `function_aggregate` arm, so a nested `SUM` or `COUNT` hit the unsupported-node catch-all.

### Decision

`render_expression_inner` gains a `function_aggregate` arm. It splices the aggregate name verbatim and uppercased, renders `COUNT(*)` for empty or star arguments, renders arguments by recursion, honors `distinct: true`, and qualifies column arguments through the ADR-085 `tableAlias`. Top-level and nested aggregate rendering share this path, and top-level output stays byte-compatible.

### Options Considered

| Option | Verdict |
|--------|---------|
| Special-case scalar-over-aggregate only in the join select-list path | Rejected: leaves the gap for single-table nested aggregates and other callers |
| Keep declining scalar-over-aggregate items | Rejected: a valid TPC-H-shaped query with no native retry (ADR-097) |

### Consequences

A scalar wrapping aggregates renders at any join arity. Single-table partial and merge decomposition is unaffected, because it detects a top-level `function_aggregate` before recursing into `vs-expression`.

---

## ADR: Advertised Capability Must Render — Purge the Native-Retry Fiction

**ID:** advertised-capability-must-render-purge-the-native-retry-fiction
**Plan:** `fix-join-decline-hard-fail`
**Status:** Accepted

### Context

Fifteen `UdfError::User` join and aggregate decline sites in `pushdown.rs` claimed "Exasol will retry the query natively". That is false: the FFI shim turns every `UdfError::User` into a hard `F-UDF-CL-RUST-9001` SQL error, and Exasol never re-plans on an adapter error (ADR-083, ADR-085). A unit test asserted `msg.contains("retry")`, encoding the false claim.

### Decision

The retry wording is removed from all 15 sites. Sites whose shapes now always render (ADR-095, ADR-096) are deleted. The remaining last-resort errors (a non-inner join node, a table absent from `TABLE_MAP` or without column metadata, a clause `vs-expression` cannot render) are plain hard errors. For each advertised capability the adapter must always render what Exasol may push, or must not advertise it. The retry test asserts the corrected wording.

### Options Considered

| Option | Verdict |
|--------|---------|
| Keep the "retry natively" wording | Rejected: it is false, and a regression test would have to be worked around |

### Consequences

The principle applies to every future capability. No commit may advertise `ORDER_BY_EXPRESSION` until every reachable ordered path renders an expression sort key faithfully or declines with a `User` error naming the key. Those paths are the declined row-scan wrapper, the grouped merge, the qualified single-table wrapper, and the N-scan join wrapper.

---

## ADR: Reuse the Iceberg Crate's NameMapping Deserializer

**ID:** reuse-the-iceberg-crate-s-namemapping-deserializer
**Plan:** `change-name-mapping-fallback`
**Status:** Accepted

### Context

The `schema.name-mapping.default` table property is JSON with kebab-case, an optional `field-id`, and nested `fields`. The `iceberg` crate already exports `NameMapping` and `MappedField` for exactly this shape.

### Decision

The engine parses the property with `serde_json::from_str::<iceberg::spec::NameMapping>`.

### Options Considered

| Option | Verdict |
|--------|---------|
| Hand-rolled serde struct | Rejected: reinvents a deserializer the dependency provides |

### Consequences

Parsing correctness tracks the `iceberg` crate's spec fidelity.

---

## ADR: Name-Mapping Resolution Slots Strictly Between Embedded Field-Id and Physical-Name Fallback

**ID:** name-mapping-resolution-slots-strictly-between-embedded-field-id-and-physical-name-fallback
**Plan:** `change-name-mapping-fallback`
**Status:** Accepted

### Context

Iceberg column-projection rule 2 applies name-mapping only to data files without field-id information. `rename_physical_to_logical` already matched an embedded `PARQUET:field_id` (highest priority) and fell back to the physical name (lowest). Name-mapping needed a precedence relative to both.

### Decision

In `rename_physical_to_logical`, name-mapping applies only to a physical field with no embedded `PARQUET:field_id`. It is tried after embedded field-id resolution and before the physical-name fallback, and it augments that fallback.

### Options Considered

| Option | Verdict |
|--------|---------|
| Consult name-mapping for every field, including those with an embedded id | Rejected: an embedded field-id is authoritative under the Iceberg spec |
| Replace the physical-name fallback with name-mapping | Rejected: breaks the no-mapping and uncovered-field cases |

### Consequences

The resolution order is embedded field-id, then name-mapping, then physical name. Existing no-mapping behavior is unchanged.

---

## ADR: Author Delete-Bearing Benchmark Tables via Apache Spark `DELETE FROM` on a v2 Merge-on-Read Table

**ID:** author-delete-bearing-benchmark-tables-via-apache-spark-delete-from-on-a-v2-merge-on-read-table
**Plan:** `add-delete-benchmark-flag`
**Status:** Accepted

### Context

The `BENCH_WITH_DELETES` benchmark variant needs Iceberg v2 merge-on-read TPC-H tables with about 5% of rows position-deleted per table, to measure the merge-on-read read path. No in-repo Iceberg writer could author position-delete files.

### Decision

The tables are authored with Apache Spark's Iceberg runtime: a `DELETE FROM` on a `write.delete.mode=merge-on-read`, format-version 2 copy of each TPC-H table, with a deterministic `<surrogate_key> % 20 = 0` predicate. Docker mode reuses the existing `apache/spark` compose fixture image, and remote mode reuses the EMR Serverless application used by `spark_compare.sh`.

### Options Considered

| Option | Verdict |
|--------|---------|
| PyIceberg `table.delete()` | Rejected: copy-on-write only, so it authors no position-delete files |
| iceberg-rust writer | Rejected: iceberg-rust 0.10 has no position-delete writer (`apache/iceberg-rust#340`) |

### Consequences

The benchmark depends on Spark only at authoring time, never at query time. The two authoring entry points are `scripts/spark-fixtures/create_tpch_deletes.sql` (docker) and `deploy/scripts/make_deletes_remote.py` (remote).

---

## ADR: Full Push-Down of Grouped Scalar-Over-Aggregate Select Items (Primary), Qualified Wrapper as Residual Fallback

**ID:** full-push-down-of-grouped-scalar-over-aggregate-select-items-primary-qualified-wrapper-as-residual-fallback
**Plan:** `fix-scalar-over-aggregate-grouped-pushdown`
**Status:** Accepted

### Context

A single-table grouped query with a scalar function wrapping aggregates, such as `ROUND(100.0 * SUM(CASE ...) / COUNT(*), 2)`, hard-failed with a `04000` column-count mismatch (#82). `detect_group_by_aggregates` classified the item as neither aggregate, literal, nor group key, returned `None`, and fell to a bare raw row scan that returns the wrong column count for a `group_by` request.

### Decision

The adapter folds the item's inner aggregates into the existing partial `AggregatePlan` decomposition and renders the scalar wrapper over the merged partials in the outer wrapper, at the item's original select-list position. It falls back to a qualified single-table wrapper only when an inner aggregate cannot be decomposed: `DISTINCT`, a non-numeric statistic argument, an untranslatable argument, or a non-aggregate non-group-key node. It never falls back to a bare row scan for a grouped request. `render_having_operand` recurses into a merge-aware renderer, `render_scalar_over_merge`, which rewrites each nested `function_aggregate` to its merged expression, matched to the `AggregatePlan` list by kind and argument.

### Options Considered

| Option | Verdict |
|--------|---------|
| Route every grouped scalar-over-aggregate through the qualified single-table wrapper | Rejected: ships every matching row per group to Exasol, defeating node-local aggregation. Kept only as the residual fallback |

### Consequences

A scalar-over-aggregate item pushes down like a top-level aggregate and keeps node-local aggregation. An undecomposable grouped shape gets a qualified wrapper with the correct column count.

---

## ADR: Split Fan-Out Is the Sole Scan Path, Applied Unconditionally

**ID:** split-fan-out-is-the-sole-scan-path-applied-unconditionally
**Plan:** `change-scan-fanout-to-scalar-emit`
**Status:** Accepted

### Context

Exasol materializes the `GROUP BY shard_key` raw-row emit fan-out into temp-DB RAM on the transparent VS path, so memory grows with scanned data volume. A spike showed that a nested `LAKEHOUSE_DISTRIBUTE_FILES` LUA SET distributor driving an outer ungrouped scalar `LAKEHOUSE_SCAN` keeps memory constant (about 2.5 GB, against 22 GB or more). The materialization affects every wrapper that drove the old SET scan.

### Decision

The distributor plus scalar scan is the only scan path, with no flag, VS property, or planner mode. It applies to every wrapper: raw scan, single-group aggregate, grouped aggregate, top-N, broadcast join, and the N-scan join fallback. Merge, group, and join logic moves to the outer ungrouped query over the scalar scan's partial rows. Single-shard plans skip the distributor and call the scalar scan directly.

### Options Considered

| Option | Verdict |
|--------|---------|
| Keep the spike's local A/B flag | Rejected: diagnostic scaffolding, dead weight once the fix is proven |
| Per-query mode switch | Rejected: adds a branch and maintenance burden for no benefit |

### Consequences

Every scan-driving wrapper shares one fan-out shape and scan-output memory stays constant. All wrappers convert in one plan, not incrementally.

---

## ADR: `LAKEHOUSE_SCAN` Itself Becomes SCALAR — One Scan Entry Point, Not Two

**ID:** lakehouse-scan-itself-becomes-scalar-one-scan-entry-point-not-two
**Plan:** `change-scan-fanout-to-scalar-emit`
**Status:** Accepted

### Context

Streaming-only scan output requires the scan UDF to run as a SCALAR EMIT script, not a SET script. The scan logic is identical for both, and only the DDL script type and the run loop differ.

### Decision

The existing `LAKEHOUSE_SCAN` symbol's DDL script type changes from SET to SCALAR in place.

### Options Considered

| Option | Verdict |
|--------|---------|
| Add a separate `LAKEHOUSE_SCAN_SCALAR` entry point | Rejected: doubles packaging and fingerprint surface for identical logic, and the two would drift |

### Consequences

The SET script type is retired from the scan path, and the symbol name is unchanged.

---

## ADR: The File Distributor Is a LUA SET Script, Not a Rust `.so` Entry Point

**ID:** the-file-distributor-is-a-lua-set-script-not-a-rust-so-entry-point
**Plan:** `change-scan-fanout-to-scalar-emit`
**Status:** Accepted

### Context

Fan-out still needs a `GROUP BY shard_key` relation so Exasol distributes shard groups round-robin across nodes. That relation only re-emits each shard's file-list string for the outer scalar scan, so it needs no DataFusion, Parquet access, or Rust scan logic.

### Decision

`LAKEHOUSE_DISTRIBUTE_FILES` is a pure LUA SET passthrough script with its own DDL, schema-qualified like the scan and distinct-merge scripts. It is not a Rust `.so` entry point.

### Options Considered

| Option | Verdict |
|--------|---------|
| Third Rust entry point in the `.so` | Rejected: adds Rust, packaging, and fingerprint surface for a pure re-emit |

### Consequences

The `.so` still exports exactly the adapter, scan, and scalar distinct-merge entry points. The distributor references no `.so` and declares no `%udf_object`, so its footprint is small and independent of data volume.

---

## ADR: N-Scan Join Fallback Renders `INNER JOIN … ON` With Greedy-Attach and Per-Leg Filter Pushdown

**ID:** n-scan-join-fallback-renders-inner-join-on-with-greedy-attach-and-per-leg-filter-pushdown
**Plan:** `change-scan-fanout-to-scalar-emit`
**Status:** Accepted

### Context

The unaccelerated join fallback rendered its FROM as a comma cross join with one flat WHERE. Converting every leg to the distributor and scalar-scan fan-out was a chance to render the join shape Exasol plans best and to push each side's own filters into its leg.

### Decision

The fallback FROM is a left-to-right `INNER JOIN ... ON` chain. Each join condition attaches at the earliest join point where every table it references is in scope, decided by the set of `tableName`s it touches and never by column name, so shared column names stay qualified. A join point with no resolvable condition renders `ON 1=1`. Each side's side-local WHERE conjuncts go into that side's fan-out leg as a DataFusion filter, and only residual conjuncts (cross-table, OR-spanning, or untagged) stay in the outer WHERE.

### Options Considered

| Option | Verdict |
|--------|---------|
| Keep the comma cross join and flat WHERE | Rejected: correct but leaves side-local filters unpushed and gives Exasol's optimizer a less favorable shape |

### Consequences

DataFusion prunes and filters each leg instead of Exasol filtering after full materialization.
