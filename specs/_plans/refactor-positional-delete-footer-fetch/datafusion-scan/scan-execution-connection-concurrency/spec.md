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
* The budget is applied to the object store's HTTP client options when the scan
  UDF builds its S3 store, on both the raw-row scan path and the partial-aggregate
  path (both decode source Parquet fetched over the same object store).
* See `datafusion-scan/scan-execution-memory-and-credentials` for how the S3
  object store is built from vended credentials, and
  `vs-adapter/create-virtual-schema-adapter-notes` for how the property is
  recorded in `adapterNotes`.

<!-- DELTA:NEW -->
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
<!-- /DELTA:NEW -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: The connection budget also bounds the positional-delete path's object-store reads

* *GIVEN* a scan spec whose `s3_max_connections` field is a positive integer N and whose assigned data files carry associated Parquet positional-delete files
* *WHEN* the scan UDF prepares positional deletes across every scan table it registers for the query — a single table, or both the fact and dimension sides of a broadcast join, which DataFusion may plan concurrently
* *THEN* a single fan-out limiter of size N — one semaphore constructed once per scan invocation and shared by every registered scan table — SHALL bound BOTH the Phase A delete-file body reads AND the Phase B data-file Parquet footer fetches that build the delete-carrying files' base access plans, so across every such fan-out active in one scan invocation AT MOST N of those object-store reads are in flight at any instant
* *AND* a SECOND size-N limiter SHALL NOT be introduced along either axis — not one per provider, because concurrently planned join-side scan leaves would then allow up to 2N in-flight reads, and not one per phase, because a Phase-B-private semaphore would let a single provider run N delete-file reads and N footer fetches at once; either breaks the instance-level bound
* *AND* every task in either fan-out SHALL acquire exactly ONE permit, hold it across exactly ONE object-store read, and release it on completion, holding no permit while awaiting another — so contention between the two phases, and between the two sides of a broadcast join, queues rather than deadlocks
* *AND* a data file carrying NO deletes SHALL NOT acquire a permit, because it issues no Phase B footer fetch
* *AND* this bound SHALL be an application-level concurrency limit, distinct from the HTTP client idle-pool size that the same N configures on the object store
<!-- /DELTA:CHANGED -->
