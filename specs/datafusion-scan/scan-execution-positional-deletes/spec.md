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
  `write.delete.granularity=file` and `write.delete.granularity=partition`. Equality deletes,
  v3/Puffin deletion vectors, and ORC or Avro data or delete files are OUT OF SCOPE and MUST fail
  loud (the authoritative gate is at plan time — see `vs-adapter/pushdown-file-pruning`).
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
  scan-time check is cheap defense-in-depth. If an assigned delete file is not a Parquet
  positional delete (a Puffin / deletion-vector payload, an equality-delete file, or an unknown
  content type), the scan MUST return a clean, credential-redacted error naming the unsupported
  mechanism rather than silently returning pre-delete rows.
* See `datafusion-scan/scan-execution` for the base scan flow and the unified-provider plan
  shape, `datafusion-scan/scan-execution-spec-reconstitution` for the delete-carrying wire
  format, `datafusion-scan/scan-execution-file-metadata` for the no-HEAD footer read, and
  `packaging/e2e-harness-positional-deletes` for the full-stack matrix.

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

* *GIVEN* a scan invocation whose assigned files include a delete file that is not a Parquet positional delete (a Puffin / deletion-vector payload, an equality-delete file, or an unknown delete content type)
* *WHEN* the scan UDF prepares that data file's scan
* *THEN* the UDF SHALL return a clean error that names the unsupported delete mechanism BEFORE emitting any row for the affected data file
* *AND* the UDF MUST NOT silently emit pre-delete rows for that file
* *AND* the error message MUST NOT contain any storage access key, secret key, or session token

### Scenario: A delete-free data file scans through the same provider unchanged

* *GIVEN* a scan invocation whose assigned data files carry no associated delete files
* *WHEN* the scan UDF registers those files
* *THEN* the UDF SHALL scan them through the same DataFusion `ParquetSource`-backed provider with NO base `ParquetAccessPlan` attached (an absent or all-selected access plan)
* *AND* the emitted rows SHALL be identical to the pre-feature delete-free scan for the same query

### Scenario: A shared delete file is read once per shard regardless of referencing data-file count

* *GIVEN* a shard whose assigned data files include two or more files that reference the same partition-granularity Parquet positional-delete file
* *WHEN* the scan UDF applies positional deletes to the shard's data files
* *THEN* the UDF SHALL read that delete file's contents from object storage AT MOST ONCE for the whole shard, not once per referencing data file
* *AND* the delete positions the UDF applies to each referencing data file SHALL be identical to reading and filtering that delete file separately per data file
* *AND* the UDF MUST NOT issue an object-store HEAD for the delete file, because its size comes from the delete-file reference in the scan spec

### Scenario: Concurrent delete-file reads stay within the connection budget

* *GIVEN* a shard whose assigned data files reference more unique positional-delete files than the resolved `s3_max_connections` budget N
* *WHEN* the scan UDF reads those delete files to build its per-data-file delete sets
* *THEN* the number of concurrently in-flight delete-file object-store reads MUST NOT exceed N at any instant, counted across every delete-read fan-out active in the scan invocation — a single table, or both sides of a broadcast join sharing one size-N limiter
* *AND* the UDF SHALL read delete files concurrently up to N in flight rather than strictly one at a time
* *AND* the resulting per-data-file delete sets SHALL be identical to a strictly serial read, because unioning delete positions is order-independent

### Scenario: Row groups that cannot contain an assigned data file's deletes are skipped

* *GIVEN* a partition-granularity positional-delete file, sorted by (`file_path`, `pos`) as the Iceberg spec requires, whose per-row-group `file_path` min/max statistics show some row groups reference only data files absent from this shard
* *WHEN* the scan UDF reads that delete file
* *THEN* the UDF SHALL skip a row group whose `file_path` min/max bounds cannot overlap any assigned data file's path, decoding only the row groups that can contain an assigned data file's delete positions
* *AND* the skip test MUST be range-based: skip a row group only when the target data-file path sorts strictly before its `file_path` min statistic OR strictly after its `file_path` max statistic; the UDF MUST NOT treat a `min == max == target` row group as an exact single-value match or otherwise shortcut the range comparison
* *AND* a row group whose `file_path` statistics are ABSENT (not written, or disabled) MUST NOT be pruned; the UDF SHALL decode it
* *AND* the accumulated delete set for each assigned data file SHALL be identical to the set produced by decoding every row group without pruning, including when the `file_path` min/max statistics are truncated

> Iceberg table spec (https://iceberg.apache.org/spec/, Position Delete Files) is the normative basis for this pruning:
> - "Position delete files are required to be sorted by file and position, not a table order, and should set sort order id to null." A given data file's entries are therefore contiguous within one delete file, so row-group `file_path` min/max bounds are a safe skip.
> - "Column metrics can be used to determine whether a delete file's rows overlap the contents of a data file or a scan range." This is normative permission to skip row groups by `file_path` min/max bounds.
>
> Parquet statistics truncation keeps this skip safe only when it is range-based. A truncated min is rounded DOWN and a truncated max is rounded UP, so `[min, max]` stays a loose superset of the row group's true `file_path` values, and `target < min OR target > max` never wrongly skips a row group that could hold the target. An equality shortcut (`min == max == target`) is NOT safe: truncation can collapse two distinct long paths to an equal truncated min/max pair, so a match there does not prove the row group holds the target. A row group with ABSENT `file_path` statistics carries no bound and MUST be decoded.
>
> Page-level (page-index) pruning is a deliberate NON-goal of this plan: row-group-level pruning already exploits the spec-guaranteed sort, and positional-delete files are small. This is an optimization scope trim, not an Iceberg-spec deviation — the spec permits but does not require reader-side pruning at any granularity.

### Scenario: The refactor preserves the delete-application safety invariants

* *GIVEN* a shard whose assigned data files carry associated Parquet positional-delete files at file or partition granularity
* *WHEN* the two-phase pipeline applies those deletes (Phase A reads each unique delete file once into a data-file-path → deleted-position map; Phase B looks up that map per data file with no delete-file I/O)
* *THEN* the post-delete row set for every data file MUST be identical to applying every associated delete file's positions per data file, unchanged by the read-once, concurrent, pruned restructure
* *AND* the read-time backstop rejecting non-positional deletes, credential redaction on every error path, and the no-object-store-HEAD invariant MUST all hold unchanged
