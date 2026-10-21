# Feature: DataFusion Scan Execution — Row-Position Delete Pipeline

The scan UDF applies every row-position delete mechanism through one pipeline. Each mechanism turns
its payload into the deleted row positions of one data file. The pipeline turns those positions into
a base `ParquetAccessPlan` on the data file's `PartitionedFile`, which the Parquet opener intersects
with predicate, row-group, and page pruning. Three mechanisms feed it: Iceberg positional-delete
files, Iceberg deletion vectors, and Delta deletion vectors.

## Background

* A data file's delete list is one format-neutral list of delete mechanisms, each naming itself
  (#342). The scan dispatches on the mechanism's own variant and never asks which table format
  produced the spec.
* Three variants are applied, and each one's payload handling is specified in its own feature:
  * Iceberg positional-delete files: `scan-read-path/scan-execution-positional-deletes` and
    `scan-read-path/scan-execution-positional-deletes-fanout`.
  * Iceberg deletion vectors: `scan-read-path/scan-execution-iceberg-deletion-vectors` (#12).
  * Delta deletion vectors: `scan-read-path/scan-execution-delta-deletion-vectors` (#320).
* The Iceberg equality-delete variant is refused. The plan-time adapter gate refuses it first
  (`file-planning/pushdown-file-pruning`), and the read-time backstop below is cheap defense in depth
  behind that gate.
* The pipeline runs in two phases.
  * Phase A reads every unique delete payload once and merges the positions into one map from
    data-file path to deleted row positions.
  * Phase B looks up that map per data file. For each delete-carrying data file it fetches the
    Parquet footer for the per-row-group row counts, builds a per-row-group `RowSelection`, and
    attaches it as the file's base `ParquetAccessPlan`.
* Every read, decode, and access-plan build is its own Tokio task on the scan runtime
  (`scan-runtime/scan-execution-threading`). A multi-thread runtime therefore spreads the delete
  path's CPU work across its workers, and a current-thread runtime keeps it on one core.
* Phase A and Phase B share one size-N connection-budget limiter per scan invocation, including both
  sides of a broadcast join. `scan-runtime/scan-execution-connection-concurrency` owns the budget
  knob (`s3_max_connections`).
* Phase B's footer fetches run concurrently (#165), and two properties keep that safe. Each fetch
  targets a distinct data-file path, so no two concurrent fetches contend for one session
  `FileMetadataCache` key. The fan-out also keeps the spec's file order in the `PartitionedFile`
  list it returns, so the shard's single `FileGroup` matches a serial build.
* The Parquet opener reads the base access plan and intersects predicate, bloom-filter, row-group,
  and page pruning on top of it, so deletes compose with pushdown instead of disabling it.
* Deletes are applied at the scan layer. The rows that the DataFusion filter, LIMIT, top-N, and
  aggregation see are already the post-delete rows, so no later stage can reintroduce a deleted row.
* See `datafusion-scan/scan-execution` for the base scan flow and the unified-provider plan shape,
  and `datafusion-scan/scan-execution-file-metadata` for the no-HEAD footer read.

## Scenarios

### Scenario: An unapplicable delete file is rejected with a clean error (read-time backstop)

* *GIVEN* a scan invocation whose assigned files include a delete mechanism the scan cannot apply, an Iceberg equality-delete file
* *WHEN* the scan UDF prepares that data file's scan
* *THEN* the UDF SHALL return a clean error that names the unsupported delete mechanism and the offending delete FILE BEFORE emitting any row for the affected data file
* *AND* the UDF SHALL dispatch on the delete mechanism's own variant rather than on the table format that produced the scan spec, so exactly THREE variants (the Iceberg positional delete, the Iceberg deletion vector, and the Delta deletion vector) reach the pipeline and every other variant is refused
* *AND* the UDF MUST NOT silently emit pre-delete rows for that file
* *AND* the error message MUST NOT contain any storage access key, secret key, or session token

### Scenario: All delete mechanisms converge on one position map and one access-plan pipeline

* *GIVEN* a shard whose assigned data files mix Iceberg data files carrying Parquet positional-delete files or deletion vectors with Delta data files carrying deletion vectors
* *WHEN* the scan UDF builds the shard's per-data-file access plans
* *THEN* Phase A SHALL accumulate EVERY mechanism into ONE map from data-file path to deleted row positions, and Phase B SHALL consume that map WITHOUT knowing which mechanism produced an entry
* *AND* the row-selection builder and the access-plan builder SHALL be the same for every mechanism, because a decoded deletion vector and an accumulated positional-delete set are both a bitmap of 0-based row positions in one data file
* *AND* every mechanism SHALL share the ONE size-N connection limiter that bounds delete-payload reads and data-file footer fetches, so a mixed shard's total in-flight object-store reads MUST NOT exceed N
* *AND* the read-time backstop SHALL run BEFORE any I/O, so an unapplicable delete anywhere in the shard fails loud before a single delete-file body, deletion-vector payload, or data-file footer is fetched
* *AND* the post-delete row set for every data file MUST be identical to applying that file's own mechanism alone

### Scenario: A delete-free data file scans through the same provider unchanged

* *GIVEN* a scan invocation whose assigned data files carry no delete mechanism
* *WHEN* the scan UDF registers those files
* *THEN* the UDF SHALL scan them through the same DataFusion `ParquetSource`-backed provider with NO base `ParquetAccessPlan` attached (an absent or all-selected access plan)
* *AND* the emitted rows SHALL be identical to a scan of the same files without delete support for the same query

### Scenario: Delete preparation spreads across the scan runtime's workers

* *GIVEN* a scan runtime with more than one worker thread and a shard whose data files carry row-position deletes
* *WHEN* the scan UDF reads and decodes those deletes and builds the access plans
* *THEN* each read and decode, and each data file's access-plan build, SHALL run as its own task on the runtime's workers, so the CPU work uses more than one core
* *AND* the UDF MUST NOT run that work on threads beyond the runtime's workers, so the instance stays within its thread budget
* *AND* the first failing task SHALL abort the remaining tasks and fail the scan before any row is emitted

### Scenario: Access-plan construction cost does not grow with the row-group count

* *GIVEN* a delete-carrying data file with many row groups
* *WHEN* the scan UDF builds the file's base `ParquetAccessPlan`
* *THEN* the UDF SHALL build every row group's selection in one pass over the file's deleted positions, so the cost grows with the deletes and not with deletes times row groups
* *AND* a row group with no deleted row SHALL be scanned without a selection, and a row group whose every row is deleted SHALL be skipped

### Scenario: Concurrent data-file footer fetches stay within the connection budget

* *GIVEN* a shard whose assigned data files include MORE delete-carrying data files than the resolved `s3_max_connections` budget N
* *WHEN* the scan UDF builds those files' base `ParquetAccessPlan`s, each of which needs its data file's per-row-group row counts from that file's Parquet footer
* *THEN* the UDF SHALL fetch those footers CONCURRENTLY rather than awaiting one before starting the next
* *AND* the number of concurrently in-flight footer fetches MUST NOT exceed N at any instant, counted across every fan-out active in the scan invocation: a single table, or both sides of a broadcast join sharing the ONE size-N limiter that also bounds the delete-payload reads
* *AND* the returned `PartitionedFile` list SHALL preserve the per-shard spec's assigned-file order, so each file occupies the same position in the shard's single `FileGroup` as it would under a serial build
* *AND* the post-delete row set SHALL be identical to a strictly serial footer fetch, because a data file's per-row-group row counts and its delete set are both independent of fetch order
* *AND* a footer fetch that fails SHALL surface as a credential-redacted user error naming the failure, with the remaining fetches abandoned, no partial access plan attached, and no row emitted for the shard

### Scenario: A delete-free data file still costs no footer fetch of its own

* *GIVEN* a shard mixing delete-carrying data files with data files that carry no delete mechanism
* *WHEN* the scan UDF builds the shard's `PartitionedFile` list
* *THEN* the UDF SHALL fetch a Parquet footer ONLY for the delete-carrying data files, because only those need a base `ParquetAccessPlan`
* *AND* a delete-free data file MUST NOT acquire a permit from the shared fan-out limiter, so delete-free files in a mixed shard neither consume nor wait on the connection budget
* *AND* each delete-free data file SHALL still appear in the returned `PartitionedFile` list, in its spec order, with no access plan attached
