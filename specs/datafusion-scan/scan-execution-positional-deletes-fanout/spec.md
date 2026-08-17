# Feature: DataFusion Scan Execution — Positional Delete I/O Fan-Out and Pruning

Governs the object-store I/O SHAPE of applying positional deletes: the shared connection-budget
limiter that bounds concurrent delete-file reads and delete-carrying data-file footer fetches, the
read-once-per-shard guarantee for a delete file referenced by multiple data files, and the
row-group-level pruning that skips decoding a partition-granularity delete file's irrelevant row
groups. Split out of `datafusion-scan/scan-execution-positional-deletes` once that feature's
scenario count crossed this library's per-spec organization threshold; the delete-APPLICATION
guarantees (what gets removed, how mechanisms compose, the read-time backstop) stay in that sibling
feature.

## Background

* **Split from `datafusion-scan/scan-execution-positional-deletes`, issue #320.** That feature keeps
  every scenario about WHICH rows a delete mechanism removes and how the shared pipeline composes
  with projection/filter/LIMIT/pruning and the read-time backstop. This feature owns every scenario
  about the I/O SHAPE — concurrency bounds, read-once-per-shard, and row-group pruning — that the
  same pipeline exercises to build its per-data-file access plans.
* Phase A (`collect_delete_positions`) reads each unique delete file once into a data-file-path →
  deleted-position map. Phase B looks up that map per data file with no delete-file I/O of its own,
  but does fetch each delete-carrying data file's own Parquet footer for its per-row-group row
  counts. Phase B is no longer serial and no longer I/O-free: those footer fetches run as a
  bounded-concurrent fan-out under the shared limiter instead of a serial `for` loop. Issue
  [#165](https://github.com/exasol-labs/lakehouse-engine-rs/issues/165).
* Two properties make the concurrent fan-out safe to reason about. Each fan-out task targets a
  DISTINCT data-file path, so no two concurrent tasks contend for the same session
  `FileMetadataCache` key. And the fan-out preserves the per-shard spec's file ORDER in the
  `PartitionedFile` list it returns, so the shard's single `FileGroup` is byte-identical to the
  serial build and the concurrency cannot perturb emission order.
* Delete positions and access plans are unchanged by the restructure: the delete set for a data file
  is fixed by Phase A, and Phase B's footer fetch reads only the per-row-group row counts, which are
  a property of the file rather than of the fetch order.
* The Delta deletion-vector mechanism shares this same connection-budget limiter and read-once
  discipline for its own sidecar fetches — see `datafusion-scan/scan-execution-delta-deletion-vectors`
  — but decodes a different container format, so its fan-out scenarios live there rather than here.
* See `datafusion-scan/scan-execution-connection-concurrency` for the operator-facing budget knob
  itself (`s3_max_connections`); this feature governs how the positional-delete pipeline spends that
  budget, not how the budget is derived or configured.

## Scenarios

### Scenario: Concurrent data-file footer fetches stay within the connection budget

* *GIVEN* a shard whose assigned data files include MORE delete-carrying data files than the resolved `s3_max_connections` budget N
* *WHEN* the scan UDF builds those files' base `ParquetAccessPlan`s, each of which needs its data file's per-row-group row counts from that file's Parquet footer
* *THEN* the UDF SHALL fetch those footers CONCURRENTLY rather than awaiting one before starting the next
* *AND* the number of concurrently in-flight footer fetches MUST NOT exceed N at any instant, counted across every fan-out active in the scan invocation — a single table, or both sides of a broadcast join sharing the ONE size-N limiter that also bounds the delete-file reads
* *AND* the returned `PartitionedFile` list SHALL preserve the per-shard spec's assigned-file order, so each file occupies the same position in the shard's single `FileGroup` as it did under the serial build
* *AND* the post-delete row set SHALL be identical to a strictly serial footer fetch, because a data file's per-row-group row counts and its delete set are both independent of fetch order
* *AND* a footer fetch that fails SHALL surface as a credential-redacted user error naming the failure, with the remaining fetches abandoned, no partial access plan attached, and no row emitted for the shard

### Scenario: A delete-free data file still costs no footer fetch of its own

* *GIVEN* a shard mixing delete-carrying data files with data files that carry no associated delete files
* *WHEN* the scan UDF builds the shard's `PartitionedFile` list
* *THEN* the UDF SHALL fetch a Parquet footer ONLY for the delete-carrying data files, because only those need a base `ParquetAccessPlan`
* *AND* a delete-free data file MUST NOT acquire a permit from the shared fan-out limiter, so delete-free files in a mixed shard neither consume nor wait on the connection budget
* *AND* each delete-free data file SHALL still appear in the returned `PartitionedFile` list, in its spec order, with no access plan attached

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
