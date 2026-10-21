# Feature: DataFusion Scan Execution — Positional-Delete File I/O Fan-Out and Pruning

Governs the object-store I/O shape of reading Iceberg Parquet positional-delete files: the
read-once-per-shard guarantee for a delete file that several data files reference, the
connection-budget bound on concurrent delete-file reads, and the row-group pruning that skips the
irrelevant row groups of a partition-granularity delete file. Which rows a delete file removes is
specified in `scan-read-path/scan-execution-positional-deletes`.

## Background

* These reads are Phase A of the pipeline that every row-position delete mechanism shares
  (`scan-read-path/scan-execution-row-position-deletes`). That feature owns Phase B, including the
  concurrent data-file footer fetches, and the size-N limiter that Phase A and Phase B share.
* Both deletion-vector mechanisms read under the same limiter with their own read-once discipline:
  the Delta mechanism for its sidecar fetches, and the Iceberg mechanism for its Puffin blob reads.
  Each decodes its own container format, so their fan-out scenarios live in
  `scan-read-path/scan-execution-delta-deletion-vectors` and
  `scan-read-path/scan-execution-iceberg-deletion-vectors` rather than here.
* See `scan-runtime/scan-execution-connection-concurrency` for the operator-facing budget knob
  itself (`s3_max_connections`); this feature governs how positional-delete file reads spend that
  budget, not how the budget is derived or configured.

## Scenarios

### Scenario: A shared delete file is read once per shard regardless of referencing data-file count

* *GIVEN* a shard whose assigned data files include two or more files that reference the same partition-granularity Parquet positional-delete file
* *WHEN* the scan UDF applies positional deletes to the shard's data files
* *THEN* the UDF SHALL read that delete file's contents from object storage AT MOST ONCE for the whole shard, not once per referencing data file
* *AND* the delete positions the UDF applies to each referencing data file SHALL be identical to reading and filtering that delete file separately per data file
* *AND* the UDF MUST NOT issue an object-store HEAD for the delete file, because its size comes from the delete-file reference in the scan spec
* *AND* the post-delete rows of the whole query SHALL be the same whether the data files that reference one delete file are assigned to one shard or split across several, because every shard applies the delete file to its own data files independently

### Scenario: Concurrent delete-file reads stay within the connection budget

* *GIVEN* a shard whose assigned data files reference more unique positional-delete files than the resolved `s3_max_connections` budget N
* *WHEN* the scan UDF reads those delete files to build its per-data-file delete sets
* *THEN* the number of concurrently in-flight delete-file object-store reads MUST NOT exceed N at any instant, counted across every delete-read fan-out active in the scan invocation — a single table, or both sides of a broadcast join sharing one size-N limiter
* *AND* the UDF SHALL read delete files concurrently up to N in flight rather than strictly one at a time
* *AND* the resulting per-data-file delete sets SHALL be identical to a strictly serial read, because unioning delete positions is order-independent

### Scenario: A delete file's row groups are read as separate requests

* *GIVEN* a positional-delete file with several row groups that can hold an assigned data file's deletes
* *WHEN* the scan UDF reads that delete file
* *THEN* the UDF SHALL read the footer first, then each such row group as its own request under its own permit of the shared limiter, so the row groups are read concurrently up to N in flight
* *AND* the UDF SHALL read the footer with the format's metadata size hint, so a footer and metadata that fit the hint cost ONE ranged GET rather than a footer GET followed by a metadata GET
* *AND* the UDF SHALL read only the `file_path` and `pos` columns, never the optional `row` column

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
