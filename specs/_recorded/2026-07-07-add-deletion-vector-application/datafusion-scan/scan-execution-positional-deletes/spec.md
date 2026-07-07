# Feature: DataFusion Scan Execution — Iceberg Positional Delete Application

Extends the scan UDF so that Iceberg merge-on-read (MOR) **positional deletes** are applied on
read, so a query over a table with positional-delete files returns the post-delete row set.
The scan keeps DataFusion's own `ParquetSource` as the scan engine — projection/filter/LIMIT
pushdown, row-group and page pruning, statistics, and streaming are all preserved — and applies
positional deletes by attaching a per-data-file `ParquetAccessPlan` (a base row selection) to
that file's `PartitionedFile`, which the Parquet opener intersects with predicate/row-group/page
pruning rather than defeating it. This is the DataFusion-native alternative to swapping in
iceberg-rust's `ArrowReader`, and it exists to close the issue #11 silent-correctness bug for the
positional-delete case (tracked as issue #68).

## Background

* This feature closes the positional-delete half of the silent-correctness bug in issue #11:
  before this change the scan collapsed each iceberg `FileScanTask` to a bare `(path, size)` pair
  and discarded its `.deletes`, so every MOR query returned pre-delete rows with no error.
* Scope is **Parquet data files + Parquet positional-delete files only**, at both
  `write.delete.granularity=file` and `write.delete.granularity=partition`.
<!-- DELTA:CHANGED -->
* Equality deletes and ORC or Avro data or delete files are OUT OF SCOPE for THIS feature and
  MUST fail loud (the authoritative gate is at plan time — see `vs-adapter/pushdown-file-pruning`).
  v3 / Puffin deletion vectors are handled by the sibling feature
  `datafusion-scan/scan-execution-deletion-vectors`, which feeds its decoded delete positions into
  the SAME per-data-file union point and `RowSelection`/`ParquetAccessPlan` machinery described
  below; they are no longer rejected by the read-time backstop.
<!-- /DELTA:CHANGED -->
<!-- DELTA:CHANGED -->
* Each data file's associated delete files are resolved through its `deletes` references, each a
  `df` index into the shard's interned `deleteFiles` pool (see
  `datafusion-scan/scan-execution-spec-reconstitution` for the wire shape); a partition-granularity
  delete file shared by several data files is interned ONCE in the pool and referenced by `df` from
  each data file it applies to. A positional-delete reference carries no `offset`/`length` (those
  are present only for a blob-addressed deletion vector).
<!-- /DELTA:CHANGED -->
* The scan reads each associated positional-delete Parquet file (columns `file_path` Utf8,
  field-id `2147483546`, and `pos` Int64, field-id `2147483545`), keeps only the rows whose
  `file_path` equals the data file currently being read, and accumulates the `pos` values into a
  per-data-file delete set (an ascending set of deleted row positions). Filtering by `file_path`
  is REQUIRED for `partition` granularity, where one delete file references many data files.
* Multiple positional-delete files MAY apply to one data file; their deleted positions are
  unioned into a single delete set for that data file.
* The delete set + the data file's per-row-group row counts (from its Parquet footer) are
  converted into a per-row-group Arrow `RowSelection` and attached to the data file's
  `PartitionedFile` as a base `ParquetAccessPlan`. The Parquet opener reads this as the base
  access plan and intersects predicate/bloom-filter/row-group/page pruning ON TOP of it, so the
  injected row selection composes with pushdown rather than disabling it.
* **Sequence-number applicability is decided by the planning layer**, not the scan: the planning
  layer associates each delete file with exactly the data files it applies to. The scan MUST
  preserve that association verbatim and MUST NOT re-derive it.
* Deletes are applied at the scan/decode layer, so the rows the DataFusion filter, LIMIT, top-N,
  and aggregation operate on are already the post-delete rows — no downstream stage can
  reintroduce a deleted row.
* **Fail-loud backstop (read-time):** the primary, authoritative gate is at plan time; this
  scan-time check is cheap defense-in-depth. If an assigned delete file is neither a Parquet
  positional delete nor a v3 deletion vector (an equality-delete file or an unknown content type),
  the scan MUST return a clean, credential-redacted error naming the unsupported mechanism rather
  than silently returning pre-delete rows.
* See `datafusion-scan/scan-execution` for the base scan flow and the unified-provider plan
  shape, `datafusion-scan/scan-execution-spec-reconstitution` for the delete-carrying wire
  format, `datafusion-scan/scan-execution-file-metadata` for the no-HEAD footer read,
  `datafusion-scan/scan-execution-deletion-vectors` for the v3 deletion-vector path that shares
  this feature's union point, and `packaging/e2e-harness-positional-deletes` for the full-stack
  matrix.

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

<!-- DELTA:CHANGED -->
### Scenario: An unapplicable delete file is rejected with a clean error (read-time backstop)

* *GIVEN* a scan invocation whose assigned files include a delete file that is neither a Parquet positional delete nor a v3 deletion vector (an equality-delete file or an unknown delete content type)
* *WHEN* the scan UDF prepares that data file's scan
* *THEN* the UDF SHALL return a clean error that names the unsupported delete mechanism BEFORE emitting any row for the affected data file
* *AND* the UDF MUST NOT silently emit pre-delete rows for that file
* *AND* a v3 / Puffin deletion vector SHALL NOT be rejected by this backstop — it is applied by `datafusion-scan/scan-execution-deletion-vectors`
* *AND* the error message MUST NOT contain any storage access key, secret key, or session token
<!-- /DELTA:CHANGED -->

### Scenario: A delete-free data file scans through the same provider unchanged

* *GIVEN* a scan invocation whose assigned data files carry no associated delete files
* *WHEN* the scan UDF registers those files
* *THEN* the UDF SHALL scan them through the same DataFusion `ParquetSource`-backed provider with NO base `ParquetAccessPlan` attached (an absent or all-selected access plan)
* *AND* the emitted rows SHALL be identical to the pre-feature delete-free scan for the same query
