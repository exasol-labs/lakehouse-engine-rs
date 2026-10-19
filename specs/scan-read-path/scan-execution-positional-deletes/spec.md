# Feature: DataFusion Scan Execution, Iceberg Positional Delete Application

Applies Iceberg merge-on-read positional deletes on read, so a query over a table with positional-delete files returns the post-delete row set. The scan keeps DataFusion's `ParquetSource` and attaches a per-data-file `ParquetAccessPlan` (a base row selection) to the file's `PartitionedFile`. The Parquet opener intersects that selection with predicate, row-group and page pruning.

## Background

* The scope is Parquet data files and Parquet positional-delete files, at `write.delete.granularity=file` and `write.delete.granularity=partition`. Equality deletes, Puffin deletion vectors, and ORC or Avro files fail loud. The authoritative gate is at plan time: see `file-planning/pushdown-file-pruning`.
* A positional-delete file has the columns `file_path` (Utf8, field-id `2147483546`) and `pos` (Int64, field-id `2147483545`). The scan keeps the rows whose `file_path` equals the data file being read and accumulates their `pos` values into a per-data-file delete set. The `file_path` filter is required for `partition` granularity, where one delete file references many data files.
* Several delete files can apply to one data file. The scan unions their positions.
* The delete set and the data file's per-row-group row counts, read from its Parquet footer, become a per-row-group Arrow `RowSelection`.
* A data file's delete list is one format-neutral list of delete mechanisms, each naming its own variant. The scan dispatches on the variant and never on the table format. The Iceberg positional-delete variant and the Delta deletion-vector variant are applied, and both feed one data-file-path to deleted-position map and one access-plan pipeline. The Iceberg equality-delete and Puffin deletion-vector variants are refused.
* The read-time backstop is defense in depth behind the plan-time gate. It returns a clean, credential-redacted error naming the unsupported mechanism.
* `scan-read-path/scan-execution-delta-deletion-vectors` specifies the Delta side. `scan-read-path/scan-execution-positional-deletes-fanout` specifies the concurrency bounds, the read-once-per-shard guarantee, and row-group pruning.
* See `datafusion-scan/scan-execution` for the base scan flow, `datafusion-scan/scan-execution-spec-reconstitution` for the delete-carrying wire format, and `datafusion-scan/scan-execution-file-metadata` for the no-HEAD footer read.

## Scenarios

### Scenario: Positional deletes remove flagged rows (file granularity)

* *GIVEN* a scan invocation whose assigned files include a Parquet data file paired with a Parquet positional-delete file (`write.delete.granularity=file`) that marks specific row positions in that data file as deleted
* *WHEN* the scan UDF runs over its assigned files
* *THEN* the UDF SHALL read the positional-delete file, accumulate the deleted `pos` values for that data file, and attach the resulting row selection to the data file's scan as a base `ParquetAccessPlan`
* *AND* the UDF MUST NOT emit any row whose position is marked deleted
* *AND* the UDF SHALL emit every non-deleted row of the data file unchanged

### Scenario: A partition-granularity delete file is filtered to the data file being read

* *GIVEN* a scan invocation whose assigned files include a Parquet positional-delete file (`write.delete.granularity=partition`) whose `file_path` column references several data files, including at least two files assigned to this shard
* *WHEN* the scan UDF applies that delete file to each assigned data file
* *THEN* for each data file the UDF SHALL retain only the delete-file rows whose `file_path` equals that data file's path, and SHALL apply those positions to that data file only
* *AND* the UDF MUST NOT delete a position of one data file using a delete-file row that references a different data file

### Scenario: Multiple delete files applying to one data file are unioned

* *GIVEN* a data file associated with two or more Parquet positional-delete files that each mark different row positions of that data file as deleted
* *WHEN* the scan UDF builds the delete set for that data file
* *THEN* the UDF SHALL union the deleted positions from every associated delete file into a single delete set for that data file
* *AND* the emitted rows SHALL exclude every position present in any of the associated delete files

### Scenario: A fully deleted data file yields no rows

* *GIVEN* a data file every one of whose row positions is marked deleted by its associated positional-delete file(s)
* *WHEN* the scan UDF runs over that data file
* *THEN* the UDF SHALL emit no rows from that data file
* *AND* the UDF MUST NOT error, because an empty post-delete result is a valid result

### Scenario: Positional deletes compose with projection, filter, LIMIT, and pruning

* *GIVEN* a scan spec whose data file carries positional deletes AND whose common spec carries a projection, a filter predicate, and a LIMIT
* *WHEN* the scan UDF builds the DataFusion plan with the base `ParquetAccessPlan` attached
* *THEN* the Parquet opener SHALL intersect the injected delete row selection WITH predicate, row-group, and page pruning, so a row group provably excluded by the predicate is still skipped and a deleted row is still removed
* *AND* the rows the filter, LIMIT, and any aggregation observe SHALL already be the post-delete rows
* *AND* the emitted result SHALL equal the result of applying the deletes, projection, filter, and LIMIT over the full data on a single node

### Scenario: An unapplicable delete file is rejected with a clean error (read-time backstop)

* *GIVEN* a scan invocation whose assigned files include a delete mechanism the scan cannot apply, an Iceberg equality-delete file or an Iceberg Puffin deletion-vector payload
* *WHEN* the scan UDF prepares that data file's scan
* *THEN* the UDF SHALL return a clean error that names the unsupported delete mechanism BEFORE emitting any row for the affected data file
* *AND* the UDF SHALL dispatch on the delete mechanism's own variant rather than on the table format that produced the scan spec, so exactly TWO variants, the Iceberg positional delete and the Delta deletion vector, reach the delete-application pipeline and every other variant is refused, SUPERSEDING the recorded "exactly one variant" rule now that `scan-read-path/scan-execution-delta-deletion-vectors` applies the Delta deletion vector
* *AND* the error for an Iceberg equality delete or a Puffin deletion vector SHALL keep naming the offending delete FILE
* *AND* the refusal MUST NOT name a Delta deletion vector and MUST NOT cite issue #320, SUPERSEDING the recorded clause that scoped that refusal to #320, because #320 is what implements it
* *AND* the UDF MUST NOT silently emit pre-delete rows for that file
* *AND* the error message MUST NOT contain any storage access key, secret key, or session token

### Scenario: Both delete mechanisms converge on one position map and one access-plan pipeline

* *GIVEN* a shard whose assigned data files mix Iceberg data files carrying Parquet positional-delete files with Delta data files carrying deletion vectors
* *WHEN* the scan UDF builds the shard's per-data-file access plans
* *THEN* the read phase SHALL accumulate BOTH mechanisms into ONE map from data-file path to deleted row positions, and the access-plan phase SHALL consume that map WITHOUT knowing which mechanism produced an entry
* *AND* the row-selection builder and the access-plan builder SHALL be the SHIPPED ones, unchanged, because a decoded deletion vector and an accumulated positional-delete set are both a bitmap of 0-based row positions in one data file
* *AND* both mechanisms SHALL share the ONE size-N connection limiter that already bounds delete-file reads and data-file footer fetches, so a mixed shard's total in-flight object-store reads MUST NOT exceed N
* *AND* the read-time backstop SHALL still run BEFORE any I/O, so an unapplicable delete anywhere in the shard fails loud before a single delete-file body, deletion-vector file, or data-file footer is fetched
* *AND* the post-delete row set for every data file MUST be identical to applying that file's own mechanism alone

### Scenario: A delete-free data file scans through the same provider unchanged

* *GIVEN* a scan invocation whose assigned data files carry no associated delete files
* *WHEN* the scan UDF registers those files
* *THEN* the UDF SHALL scan them through the same DataFusion `ParquetSource`-backed provider with NO base `ParquetAccessPlan` attached (an absent or all-selected access plan)
* *AND* the emitted rows SHALL be identical to the pre-feature delete-free scan for the same query

> The read-once-per-shard guarantee for a delete file referenced by multiple data files, the
> connection-budget concurrency bounds on delete-file reads and footer fetches, and row-group-level
> pruning of a partition-granularity delete file are specified in
> `scan-read-path/scan-execution-positional-deletes-fanout`.

### Scenario: The refactor preserves the delete-application safety invariants

* *GIVEN* a shard whose assigned data files carry associated Parquet positional-delete files at file or partition granularity
* *WHEN* the two-phase pipeline applies those deletes (Phase A reads each unique delete file once into a data-file-path → deleted-position map; Phase B looks up that map per data file with no delete-file I/O, and fetches each delete-carrying data file's own Parquet footer as a bounded-concurrent fan-out under the same shared limiter)
* *THEN* the post-delete row set for every data file MUST be identical to applying every associated delete file's positions per data file, unchanged by the read-once, concurrent, pruned restructure and unchanged by the concurrent Phase B footer fetch
* *AND* the read-time backstop rejecting non-positional deletes, credential redaction on every error path, and the no-object-store-HEAD invariant MUST all hold unchanged
* *AND* the backstop SHALL still run BEFORE any I/O, so an unapplicable delete anywhere in the shard fails loud before a single delete-file body or data-file footer is fetched
