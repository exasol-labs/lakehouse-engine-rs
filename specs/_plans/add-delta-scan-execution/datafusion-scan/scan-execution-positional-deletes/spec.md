# Feature: DataFusion Scan Execution — Iceberg Positional Delete Application

Extends the scan UDF so that Iceberg merge-on-read (MOR) **positional deletes** are applied on
read, so a query over a table with positional-delete files returns the post-delete row set.
The scan keeps DataFusion's own `ParquetSource` as the scan engine — projection/filter/LIMIT
pushdown, row-group and page pruning, statistics, and streaming are all preserved — and applies
positional deletes by attaching a per-data-file `ParquetAccessPlan` (a base row selection) to
that file's `PartitionedFile`, which the Parquet opener intersects with predicate/row-group/page
pruning rather than defeating it.

## Background

* **This delta is issue #320 and changes no Iceberg behavior.** The two-phase pipeline, the shared
  limiter, the row-selection builder, and the access-plan builder are unchanged. What changes is which
  delete mechanisms reach that pipeline: the Delta deletion vector now does, and its refusal arm is
  gone.
* The Iceberg equality-delete and Iceberg Puffin deletion-vector arms KEEP refusing. Applying them is
  tracked separately and is outside this delta.
* The Delta side of the pipeline — descriptor resolution, sidecar fetching, and bitmap decoding — is
  specified in `datafusion-scan/scan-execution-delta-deletion-vectors`. This delta specifies only what
  the SHARED pipeline guarantees once both mechanisms feed it.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: An unapplicable delete file is rejected with a clean error (read-time backstop)

* *GIVEN* a scan invocation whose assigned files include a delete mechanism the scan cannot apply — an Iceberg equality-delete file or an Iceberg Puffin deletion-vector payload
* *WHEN* the scan UDF prepares that data file's scan
* *THEN* the UDF SHALL return a clean error that names the unsupported delete mechanism BEFORE emitting any row for the affected data file
* *AND* the UDF SHALL dispatch on the delete mechanism's own variant rather than on the table format that produced the scan spec, so exactly TWO variants — the Iceberg positional delete and the Delta deletion vector — reach the delete-application pipeline and every other variant is refused, SUPERSEDING the recorded "exactly one variant" rule now that `datafusion-scan/scan-execution-delta-deletion-vectors` applies the Delta deletion vector
* *AND* the error for an Iceberg equality delete or a Puffin deletion vector SHALL keep naming the offending delete FILE
* *AND* the refusal MUST NOT name a Delta deletion vector and MUST NOT cite issue #320, SUPERSEDING the recorded clause that scoped that refusal to #320, because #320 is what implements it
* *AND* the UDF MUST NOT silently emit pre-delete rows for that file
* *AND* the error message MUST NOT contain any storage access key, secret key, or session token
<!-- /DELTA:CHANGED -->

<!-- DELTA:NEW -->
### Scenario: Both delete mechanisms converge on one position map and one access-plan pipeline

* *GIVEN* a shard whose assigned data files mix Iceberg data files carrying Parquet positional-delete files with Delta data files carrying deletion vectors
* *WHEN* the scan UDF builds the shard's per-data-file access plans
* *THEN* the read phase SHALL accumulate BOTH mechanisms into ONE map from data-file path to deleted row positions, and the access-plan phase SHALL consume that map WITHOUT knowing which mechanism produced an entry
* *AND* the row-selection builder and the access-plan builder SHALL be the SHIPPED ones, unchanged, because a decoded deletion vector and an accumulated positional-delete set are both a bitmap of 0-based row positions in one data file
* *AND* both mechanisms SHALL share the ONE size-N connection limiter that already bounds delete-file reads and data-file footer fetches, so a mixed shard's total in-flight object-store reads MUST NOT exceed N
* *AND* the read-time backstop SHALL still run BEFORE any I/O, so an unapplicable delete anywhere in the shard fails loud before a single delete-file body, deletion-vector file, or data-file footer is fetched
* *AND* the post-delete row set for every data file MUST be identical to applying that file's own mechanism alone
<!-- /DELTA:NEW -->
