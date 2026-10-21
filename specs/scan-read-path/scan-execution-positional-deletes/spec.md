# Feature: DataFusion Scan Execution — Iceberg Positional-Delete File Application

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
* This feature covers **Parquet positional-delete files over Parquet data files**, at both
  `write.delete.granularity=file` and `write.delete.granularity=partition`. Equality deletes and
  ORC or Avro data or delete files are OUT OF SCOPE and MUST fail loud. The primary gate is at plan
  time (`file-planning/pushdown-file-pruning`). It checks each planned data file and each delete
  file iceberg-rust applies to it, so it relies on iceberg-rust's delete matching (ADR
  `unsupported-deletes-classified-from-planned-tasks`).
* The positions this feature reads feed the pipeline that every row-position delete mechanism
  shares (`scan-read-path/scan-execution-row-position-deletes`). That feature owns the variant
  dispatch, the read-time backstop, the convergence of all mechanisms on one position map, and the
  data-file footer fan-out. Iceberg deletion vectors are specified in
  `scan-read-path/scan-execution-iceberg-deletion-vectors`.
* The scan reads each associated positional-delete Parquet file (columns `file_path` Utf8,
  field-id `2147483546`, and `pos` Int64, field-id `2147483545`), keeps only the rows whose
  `file_path` equals the data file currently being read, and accumulates the `pos` values into a
  per-data-file delete set (an ascending set of deleted row positions). Filtering by `file_path`
  is REQUIRED for `partition` granularity, where one delete file references many data files.
* Multiple positional-delete files MAY apply to one data file; their deleted positions are
  unioned into a single delete set for that data file.
* **Sequence-number applicability is decided by the planning layer**, not the scan: the planning
  layer associates each delete file with exactly the data files it applies to. The scan MUST
  preserve that association verbatim and MUST NOT re-derive it.
* The adapter tags an Iceberg position delete stored in Puffin format as the Puffin
  deletion-vector variant, not as a positional delete, because the scan reads it by blob range.
* The read-once-per-shard guarantee, the delete-file read concurrency, and the row-group pruning
  of a partition-granularity delete file are specified in
  `scan-read-path/scan-execution-positional-deletes-fanout`.
* See `datafusion-scan/scan-execution-spec-reconstitution` for the delete-carrying wire format.

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
