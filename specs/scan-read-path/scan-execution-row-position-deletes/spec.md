# Feature: DataFusion Scan Execution, Row-Position Delete Pipeline

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
* Planning groups the shard's delete payloads and issues no request. A positional-delete file, a
  Delta sidecar, and a merged Puffin request are one payload each.
* A data file's deletes load when DataFusion opens the file. Its footer and its payloads load
  together, the positions become the file's base `ParquetAccessPlan`, and DataFusion then opens the
  file as it opens a delete-free one.
* A payload loads once. The first data file that needs it starts the load, later data files wait
  for it, and each takes its own positions. A data file's access plan is built once and shared by
  its byte ranges.
* A wrapper around the Parquet file source attaches the access plan. The scan's reader refuses a
  delete-carrying file that reaches it without one.
* The row groups of a positional-delete file decode as Tokio tasks on the scan runtime
  (`scan-runtime/scan-execution-threading`). Every other delete read runs on the stream that opens
  the data file.
* Every delete read takes one permit of the size-N limiter of the scan invocation.
  `scan-runtime/scan-execution-connection-concurrency` owns the budget knob (`s3_max_connections`).
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

### Scenario: All delete mechanisms converge on one access-plan pipeline

* *GIVEN* a shard whose assigned data files mix Iceberg data files carrying Parquet positional-delete files or deletion vectors with Delta data files carrying deletion vectors
* *WHEN* the scan UDF builds the shard's per-data-file access plans
* *THEN* EVERY mechanism SHALL yield deleted row positions per data file, and the access-plan builder SHALL consume them WITHOUT knowing which mechanism produced them
* *AND* the row-selection builder and the access-plan builder SHALL be the same for every mechanism, because a decoded deletion vector and an accumulated positional-delete set are both a bitmap of 0-based row positions in one data file
* *AND* every mechanism SHALL share the ONE size-N connection limiter that bounds delete-payload reads, so a mixed shard's in-flight delete reads MUST NOT exceed N
* *AND* the read-time backstop SHALL run BEFORE any I/O, so an unapplicable delete anywhere in the shard fails loud before anything is fetched
* *AND* the post-delete row set for every data file MUST be identical to applying that file's own mechanism alone

### Scenario: A delete-free data file scans through the same provider unchanged

* *GIVEN* a scan invocation whose assigned data files carry no delete mechanism
* *WHEN* the scan UDF registers those files
* *THEN* the UDF SHALL scan them through the same DataFusion `ParquetSource`-backed provider with NO base `ParquetAccessPlan` attached (an absent or all-selected access plan)
* *AND* the emitted rows SHALL be identical to a scan of the same files without delete support for the same query

### Scenario: Delete preparation spreads across the scan runtime's workers

* *GIVEN* a scan runtime with more than one worker thread and a shard whose data files carry positional-delete files
* *WHEN* the scan UDF reads and decodes those delete files
* *THEN* each row group of a delete file SHALL decode as its own task on the runtime's workers, so the CPU work uses more than one core
* *AND* the UDF MUST NOT run that work on threads beyond the runtime's workers, so the instance stays within its thread budget
* *AND* the first failing task SHALL abort the delete file's remaining tasks and fail the scan

### Scenario: Access-plan construction cost does not grow with the row-group count

* *GIVEN* a delete-carrying data file with many row groups
* *WHEN* the scan UDF builds the file's base `ParquetAccessPlan`
* *THEN* the UDF SHALL build every row group's selection in one pass over the file's deleted positions, so the cost grows with the deletes and not with deletes times row groups
* *AND* a row group with no deleted row SHALL be scanned without a selection, and a row group whose every row is deleted SHALL be skipped

### Scenario: A delete payload is read once for the data files that share it

* *GIVEN* a shard whose data files share a positional-delete file, a Delta sidecar, or a merged Puffin request
* *WHEN* the scan's streams open those data files, at the same time or one after another
* *THEN* the UDF SHALL read the shared payload once and apply to each data file its own positions
* *AND* a scan that a LIMIT stops early MUST NOT read a payload that only unopened data files need

### Scenario: A data file cut into byte ranges applies its deletes once

* *GIVEN* a delete-carrying data file that the scan cut into several byte ranges
* *WHEN* the scan's streams open those ranges
* *THEN* the UDF SHALL load the file's deletes and build its access plan once, and every range SHALL apply the part of the plan that covers its row groups
* *AND* the emitted rows SHALL be the file's rows without the deleted ones, each exactly once

### Scenario: Row-position deletes apply to files opened by concurrent streams

* *GIVEN* a shard of delete-carrying data files read by more streams than it has files
* *WHEN* the scan runs a full read, a top-N, a LIMIT, or a join of two delete-carrying tables
* *THEN* the result SHALL hold no deleted row and every other row
* *AND* a delete read or a reservation that fails after other files emitted rows SHALL fail the scan

### Scenario: A delete-carrying data file is never read without its access plan

* *GIVEN* a delete-carrying data file that reaches the scan's reader without a base `ParquetAccessPlan`, as a plan rewrite that dropped the file-source wrapper would cause
* *WHEN* DataFusion opens the file
* *THEN* the UDF SHALL fail the scan with an error naming the data file
* *AND* the UDF MUST NOT emit a row of that file
