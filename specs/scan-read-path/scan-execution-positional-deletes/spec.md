# Feature: DataFusion Scan Execution, Iceberg Positional-Delete File Application

Applies Iceberg merge-on-read positional deletes on read, so a query over a table with positional-delete files returns the post-delete row set. The scan keeps DataFusion's `ParquetSource` and attaches a per-data-file `ParquetAccessPlan` (a base row selection) to the file's `PartitionedFile`. The Parquet opener intersects that selection with predicate, row-group and page pruning.

## Background

* The scope is Parquet positional-delete files over Parquet data files, at `write.delete.granularity=file` and `write.delete.granularity=partition`. Equality deletes and ORC or Avro data or delete files fail loud. The primary gate is at plan time (`file-planning/pushdown-file-pruning`). It checks each planned data file and each delete file iceberg-rust applies to it, so it relies on iceberg-rust's delete matching.
* A positional-delete file has the columns `file_path` (Utf8, field-id `2147483546`) and `pos` (Int64, field-id `2147483545`). The scan keeps the rows whose `file_path` equals the data file being read and accumulates their `pos` values into a per-data-file delete set. The `file_path` filter is required for `partition` granularity, where one delete file references many data files.
* Iceberg spec, Position Delete Files: `file_path` "must match the `file_path` of the target data file in a manifest entry". The scan therefore matches it against the manifest path before the adapter's escaping (`file-planning/pushdown-planning-file-encoding`).
* Several delete files can apply to one data file. The scan unions their positions.
* The planning layer decides sequence-number applicability by associating each delete file with exactly the data files it applies to. The scan preserves that association and never re-derives it.
* The adapter tags an Iceberg position delete stored in Puffin format as the Puffin deletion-vector variant, not as a positional delete, because the scan reads it by blob range.
* The positions this feature reads feed the pipeline every row-position delete mechanism shares. `scan-read-path/scan-execution-row-position-deletes` owns the variant dispatch, the read-time backstop, the convergence of all mechanisms on one position map, and the data-file footer fan-out. `scan-read-path/scan-execution-iceberg-deletion-vectors` specifies Iceberg deletion vectors.
* `scan-read-path/scan-execution-positional-deletes-fanout` specifies the read-once-per-shard guarantee, the delete-file read concurrency, and row-group pruning. See `datafusion-scan/scan-execution-spec-reconstitution` for the delete-carrying wire format.

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
* *AND* `file_path` SHALL be compared with the data file's unescaped manifest path, so a data file whose key holds `%`, `#`, or `?` keeps its deletes
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
