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

* The delete set + the data file's per-row-group row counts (from its Parquet footer) are
  converted into a per-row-group Arrow `RowSelection` and attached to the data file's
  `PartitionedFile` as a base `ParquetAccessPlan`.
* **Sequence-number applicability is decided by the planning layer**, not the scan: the planning
  layer associates each delete file with exactly the data files it applies to. The scan MUST
  preserve that association verbatim and MUST NOT re-derive it.
* See `datafusion-scan/scan-execution-connection-concurrency` for the shared fan-out limiter and
  `datafusion-scan/scan-execution-file-metadata` for the no-HEAD, single-request footer read.

<!-- DELTA:NEW -->
* Phase B is no longer serial and is no longer I/O-free. It performs no DELETE-FILE I/O — Phase A
  still reads every delete file exactly once up front — but it does fetch each DELETE-CARRYING
  data file's Parquet footer, and those fetches now run as a bounded-concurrent fan-out under the
  shared limiter instead of a serial `for` loop. Issue
  [#165](https://github.com/exasol-labs/lakehouse-engine-rs/issues/165).
* Two properties make the concurrent fan-out safe to reason about. Each fan-out task targets a
  DISTINCT data-file path, so no two concurrent tasks contend for the same session
  `FileMetadataCache` key. And the fan-out preserves the per-shard spec's file ORDER in the
  `PartitionedFile` list it returns, so the shard's single `FileGroup` is byte-identical to the
  serial build and the concurrency cannot perturb emission order.
* Delete positions and access plans are unchanged by the restructure: the delete set for a data
  file is fixed by Phase A, and Phase B's footer fetch reads only the per-row-group row counts,
  which are a property of the file rather than of the fetch order.
<!-- /DELTA:NEW -->

## Scenarios

<!-- DELTA:NEW -->
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
<!-- /DELTA:NEW -->

<!-- DELTA:CHANGED -->
### Scenario: The refactor preserves the delete-application safety invariants

* *GIVEN* a shard whose assigned data files carry associated Parquet positional-delete files at file or partition granularity
* *WHEN* the two-phase pipeline applies those deletes (Phase A reads each unique delete file once into a data-file-path → deleted-position map; Phase B looks up that map per data file with no delete-file I/O, and fetches each delete-carrying data file's own Parquet footer as a bounded-concurrent fan-out under the same shared limiter)
* *THEN* the post-delete row set for every data file MUST be identical to applying every associated delete file's positions per data file, unchanged by the read-once, concurrent, pruned restructure and unchanged by the concurrent Phase B footer fetch
* *AND* the read-time backstop rejecting non-positional deletes, credential redaction on every error path, and the no-object-store-HEAD invariant MUST all hold unchanged
* *AND* the backstop SHALL still run BEFORE any I/O, so an unapplicable delete anywhere in the shard fails loud before a single delete-file body or data-file footer is fetched
<!-- /DELTA:CHANGED -->
