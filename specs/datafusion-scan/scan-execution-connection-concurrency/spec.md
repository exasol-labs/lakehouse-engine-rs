# Feature: DataFusion Scan Execution — Object-Store Connection Concurrency

Controls how many concurrent connections each scan UDF instance holds open to
the S3-compatible object store, so that a node's network / IO bandwidth is
saturated when data-file fetching — not CPU — is the throughput bottleneck.
The budget is a single operator-facing knob (mirroring Exasol's native
`IMPORT FROM PARQUET` `MaxConnections` parameter): an explicit positive value
pins it, otherwise the adapter derives a per-instance budget from the node's
capacity. Configuration is round-tripped from a VS property through
`adapterNotes` and the shard-invariant common spec argument to the scan UDF,
which applies it to the object store's HTTP client — an axis independent of the
CPU thread/partition budget of `datafusion-scan/scan-execution-threading`.

## Background

* The `ScanSpec` carries a shard-invariant `s3_max_connections` field (a
  positive integer connection-concurrency budget) that defaults to a
  conservative built-in value when absent from the JSON, so pre-existing scan
  specs remain backward-compatible.
* Data-file fetching from S3 is a distinct throughput axis from CPU work: the
  DataFusion thread/partition budget (`datafusion-scan/scan-execution-threading`)
  governs decode/compute concurrency, while this budget governs how many
  concurrent HTTP connections to the object store the instance may keep warm.
  With one DataFusion instance per node (`PARALLELISM_FACTOR=1` → `G = node_count`),
  a serial or under-concurrent fetch path can leave the node's network idle even
  when all cores are busy; raising the connection budget lets a single instance
  pull many byte ranges / files in parallel.
* The budget is applied to the object store's HTTP client options when the scan
  UDF builds its S3 store, on both the raw-row scan path and the partial-aggregate
  path (both decode source Parquet fetched over the same object store).
* The budget is resolved in the adapter at `createVirtualSchema` time from an
  `S3_MAX_CONNECTIONS` VS/connection property and recorded in `adapterNotes`. An
  explicit positive integer is used verbatim; an absent, empty, zero, or invalid
  value triggers an AUTO derivation from the node's core count and its per-node
  UDF-instance share (mirroring the AUTO thread-budget derivation in
  `datafusion-scan/scan-execution-threading`). Only the resolved integer reaches
  the scan UDF; the UDF stays resolution-agnostic.
* The budget travels in the shard-invariant common spec argument, serialized once
  for the whole work-unit shard fan-out (see `parallelism/work-unit-sharding`),
  never repeated per shard.
* Whether raising connection concurrency actually closes the gap to the native
  `IMPORT FROM PARQUET` throughput ceiling is an open empirical question answered
  by benchmark sweeps, not by this spec. This spec only guarantees the budget is
  selectable, correctly derived, round-tripped, and applied to the object store.
* See `datafusion-scan/scan-execution-memory-and-credentials` for how the S3
  object store is built from vended credentials, and
  `vs-adapter/create-virtual-schema-adapter-notes` for how the property is
  recorded in `adapterNotes`.
* **The budget is per built object store, and a join now builds one per side.** Issue #294 gives each join side its own inner object store so each reads through its own credential (`datafusion-scan/scan-execution-join`). Each inner store gets its own HTTP client, so each is configured with the budget N and a two-side join can hold up to 2N warm idle connections to one host rather than N.
* **Dividing the budget across the sides was rejected.** `budget / side_count` would silently halve fact-side fetch parallelism on EVERY broadcast join, making a tuning knob's effective value depend on whether the query happens to be a join — a data-dependent performance regression, in exchange for bounding a resource that is not the bottleneck.
* **What doubles is warm idle sockets, not concurrency.** The knob maps to `object_store` 0.13.2's `pool_max_idle_per_host`, which bounds how many established connections the pool keeps warm and reusable; `object_store` 0.13.2 exposes no hard in-flight ceiling. The bound this delta widens is therefore a socket / file-descriptor bound, not a request-rate bound.
* **A join whose sides live in different buckets already held 2N before this delta**, because two buckets already yielded two registered stores with two clients. This delta makes the shared-bucket join match that shape rather than introducing a new one.
* **The per-side store split does NOT split the delete-path semaphore.** The size-N limiter below is instance-level and shared across both sides' registrations, so the delete path's in-flight bound stays N even though the HTTP connection pools are now per side. What is per side is the credential a read goes out under, not how many reads may be in flight.
* The positional-delete pipeline issues object-store reads in TWO phases, and BOTH now draw
  from the one size-N limiter. Phase A (`collect_delete_positions`) reads each unique
  positional-delete file's body once. Phase B (`partitioned_files`) fetches each
  DELETE-CARRYING DATA FILE's Parquet footer to obtain the per-row-group row counts the base
  `ParquetAccessPlan` needs. Phase B previously awaited those footer fetches one at a time in a
  `for` loop, so a shard with K delete-carrying data files paid K serialized round-trips while a
  delete-free scan of the same files had its footers fetched concurrently by DataFusion's own
  opener. Issue [#165](https://github.com/exasol-labs/lakehouse-engine-rs/issues/165).
* One semaphore, two phases, one field. The limiter is named `delete_path_read_limiter`
  (`crates/lakehouse-engine/src/scan/positional_deletes.rs`), renamed from `delete_read_limiter`
  because it no longer bounds only reads OF delete files: it bounds every object-store read the
  delete path issues while preparing a delete-carrying scan. Adding a second size-N semaphore for
  Phase B would double the instance's in-flight bound to 2N and break the guarantee this feature
  already records for Phase A.
* Deadlock freedom rests on one property, not on phase ordering alone: every fan-out task
  acquires EXACTLY ONE permit, holds it across EXACTLY ONE object-store read, and releases it on
  completion. No task holds a permit while awaiting another permit, and no task awaits another
  task. Phase A also fully completes and drops its permits before Phase B's fan-out is
  constructed within a single `partitioned_files` call, but that ordering is a consequence of the
  code shape, not the safety argument — the no-hold-and-wait property is what makes contention
  between phases, and between the two concurrently-planned sides of a broadcast join, queue
  rather than deadlock.
* What the budget does NOT bound is unchanged: the Parquet opener's own data-file reads at
  execution time are bounded by the object store's HTTP client, which the same N configures. The
  semaphore is an application-level admission gate over the delete path's preparation reads only.

## Scenarios

### Scenario: Scan configures its object store from the resolved connection budget

* *GIVEN* a scan spec whose `s3_max_connections` field is a positive integer N
* *WHEN* the scan UDF builds its S3 object store for the files listed in its per-shard argument
* *THEN* the UDF SHALL configure the object store's HTTP client options with a connection-concurrency budget of N, so up to N concurrent connections to the object store are held warm per host rather than leaving the client at its default pooling behaviour
* *AND* the UDF SHALL apply that same budget of N to EACH object store it builds, so a two-side broadcast join — which builds one store per side to read each side through its own credential — holds up to 2N warm connections rather than N
* *AND* the UDF MUST NOT divide the budget across a join's sides, because the resulting per-side value would make a fact-side scan's fetch parallelism depend on whether the query is a join
* *AND* the budget SHALL apply on both the raw-row scan path and the partial-aggregate path, since both decode Parquet fetched over the same object store
* *AND* a credential value MUST NOT appear in any error the UDF surfaces while building the client options

### Scenario: Scan falls back to a built-in default budget when the field is absent

* *GIVEN* a scan spec whose JSON omits the `s3_max_connections` field
* *WHEN* the scan UDF deserializes the spec and builds its S3 object store
* *THEN* the UDF SHALL use a conservative built-in default connection-concurrency budget, clamped to at least 1
* *AND* the scan SHALL otherwise execute identically to the explicit-value path

### Scenario: FIXED value overrides the AUTO derivation at createVirtualSchema

* *GIVEN* a `createVirtualSchema` request whose `S3_MAX_CONNECTIONS` property is a positive integer M
* *WHEN* the adapter resolves the connection-concurrency budget
* *THEN* the adapter SHALL record M as the connection-concurrency budget, without applying the AUTO derivation
* *AND* the adapter SHALL record the resolved value in the `createVirtualSchema` `adapterNotes` so the per-shard scan spec carries an integer field the scan UDF consumes unchanged

### Scenario: AUTO derivation sizes the per-instance budget from node capacity

* *GIVEN* a `createVirtualSchema` request that supplies no positive-integer `S3_MAX_CONNECTIONS` property (absent, empty, zero, or invalid)
* *AND* a resolved per-node core count greater than 0 and a per-node UDF-instance share derived from the work-unit shard fan-out
* *WHEN* the adapter resolves the connection-concurrency budget
* *THEN* the adapter SHALL derive a per-instance connection-concurrency budget from the core count and the per-node UDF-instance share, mirroring the AUTO thread-budget derivation, so the budget scales with a node's capacity and the per-node instance share without collapsing below 1
* *AND* the adapter SHALL record the derived value in `adapterNotes`

### Scenario: AUTO derivation falls back to the default budget when the core count is unknown

* *GIVEN* a `createVirtualSchema` request that supplies no positive-integer `S3_MAX_CONNECTIONS` property
* *AND* a resolved per-node core count of 0 (the unknown / unavailable sentinel)
* *WHEN* the adapter resolves the connection-concurrency budget
* *THEN* the adapter SHALL fall back to the conservative built-in default budget rather than producing a zero or negative budget
* *AND* the adapter SHALL still return a successful `createVirtualSchema` response

### Scenario: Connection budget travels once in the shard-invariant common spec

* *GIVEN* a scan-driving query fanned across more than one work-unit shard
* *WHEN* the adapter serializes the scan-driving SQL arguments
* *THEN* the resolved connection-concurrency budget SHALL travel in the shard-invariant common spec argument, serialized EXACTLY ONCE for the whole fan-out, and MUST NOT be repeated in any per-shard argument
* *AND* the `ScanSpec` reconstituted for every shard SHALL carry the same connection-concurrency budget

### Scenario: The connection budget also bounds the positional-delete path's object-store reads

* *GIVEN* a scan spec whose `s3_max_connections` field is a positive integer N and whose assigned data files carry associated Parquet positional-delete files
* *WHEN* the scan UDF prepares positional deletes across every scan table it registers for the query — a single table, or both the fact and dimension sides of a broadcast join, which DataFusion may plan concurrently
* *THEN* a single fan-out limiter of size N — one semaphore constructed once per scan invocation and shared by every registered scan table — SHALL bound BOTH the Phase A delete-file body reads AND the Phase B data-file Parquet footer fetches that build the delete-carrying files' base access plans, so across every such fan-out active in one scan invocation AT MOST N of those object-store reads are in flight at any instant
* *AND* a SECOND size-N limiter SHALL NOT be introduced along either axis — not one per provider, because concurrently planned join-side scan leaves would then allow up to 2N in-flight reads, and not one per phase, because a Phase-B-private semaphore would let a single provider run N delete-file reads and N footer fetches at once; either breaks the instance-level bound
* *AND* every task in either fan-out SHALL acquire exactly ONE permit, hold it across exactly ONE object-store read, and release it on completion, holding no permit while awaiting another — so contention between the two phases, and between the two sides of a broadcast join, queues rather than deadlocks
* *AND* a data file carrying NO deletes SHALL NOT acquire a permit, because it issues no Phase B footer fetch
* *AND* this bound SHALL be an application-level concurrency limit, distinct from the HTTP client idle-pool size that the same N configures on the object store
