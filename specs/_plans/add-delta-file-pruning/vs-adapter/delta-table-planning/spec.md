# Feature: Delta Table Planning

Resolves a Delta Lake table into the engine's existing `ScanSpec` shape at plan time, so file-level
sharding, the pushdown wire format, streaming emit, and the memory model are reused for the Delta
path exactly as they already are for Iceberg.

## Background

<!-- DELTA:NEW -->
* **This delta is issue #321, and it closes the LAST deferral this feature has carried since #319.**
  Filter-based file pruning is now implemented and owned by `vs-adapter/delta-file-pruning`. Three
  recorded statements are superseded, all of them assertions that pruning does NOT happen: the
  "**Per-file min/max statistics are OUT of scope**" bullet, the "filter-based file pruning is issue
  #321" half of the two-deferrals bullet, and the "**Filter-based file pruning stays deferred,
  unchanged**" bullet. This feature therefore records NO remaining pruning exception. Nothing else
  about Delta planning changes: log replay, partition values, deletion-vector descriptors,
  column-mapping binding keys, credential vending, the protocol gate, and type refusal are all
  untouched.
* **The no-stats-on-the-wire rule SURVIVES this delta, with a corrected justification.** The scan still
  carries no per-file minimum or maximum, because `delta_kernel` evaluates the bounds internally during
  log replay and hands back a selection vector — pruning completes before a file entry exists, so the
  stats wire shape this feature deferred never acquired a consumer and is not designed now either. The
  scenario clause below is CHANGED only in its reason, never in its obligation, so `ScanSpec` stays
  format-neutral per CLAUDE.md and every golden scan-spec encoding is unmoved.
<!-- /DELTA:NEW -->

## Scenarios

### Scenario: A Delta table resolves its current version's active data files

* *GIVEN* a Delta table whose transaction log holds more than one JSON commit, reachable through a
  credentialed object store at a table-root URL
* *WHEN* the Delta format reader resolves that table's scan
* *THEN* the reader SHALL resolve the log's CURRENT version and replay every JSON commit and any
  checkpoint up to it, and SHALL return exactly the data files active at that version — each entry
  carrying the `add` action's `path` verbatim and its `size` — so a file added at one version and
  removed at a later one is absent and a file added at a later version is present
* *AND* the reader SHALL return one entry per active path even when the same path is removed and
  re-added within one commit, because a Delta `DELETE` that writes a deletion vector emits a
  `remove` and an `add` for the identical path and a per-`add` collection would return that file
  twice
* *AND* the reader SHALL store each path verbatim, resolving it against no table root, because path
  reconstruction belongs to file registration (see
  `datafusion-scan/scan-execution-spec-reconstitution`)
* *AND* the returned scan SHALL carry the table root taken from the table's own catalog-reported
  storage location, so the shard-invariant common spec carries it once
<!-- DELTA:CHANGED -->
* *AND* the returned scan MUST NOT carry any per-file minimum or maximum statistic, because
  `delta_kernel` compares those bounds itself during log replay and returns a selection vector rather
  than values — so plan-time pruning (`vs-adapter/delta-file-pruning`) needs no wire shape for them,
  SUPERSEDING the recorded reason that stats-based pruning was deferred to issue #321
<!-- /DELTA:CHANGED -->
* *AND* the reader MUST NOT construct its own object store: it SHALL read the log through the store
  it is given, so the replay is exercised over a local filesystem store as well as over S3

### Scenario: The Delta reader is reached from production pushdown under the Unity Catalog kind

* *GIVEN* a virtual schema created with `CATALOG_KIND` set to `UNITY_CATALOG`, and a query against one
  of its Delta tables
* *WHEN* the adapter handles the resulting pushdown request
* *THEN* the adapter SHALL select the Delta format reader through the scan-source seam and SHALL plan
  the query from the `ResolvedScan` that reader returns
* *AND* the reader's resolved partition columns SHALL reach the shard-invariant common spec and its
  per-file partition values SHALL reach the per-shard file entries, so the deferred scan-side partition
  reconstruction this reader's contract names is satisfied by
  `datafusion-scan/scan-execution-partition-values` rather than left open
<!-- DELTA:CHANGED -->
* *AND* the reader SHALL now PRUNE the file list by the request's filter, on partition values and on
  per-file statistics alike (`vs-adapter/delta-file-pruning`), SUPERSEDING the recorded rule that it
  applies NO filter-based file pruning: a filter now narrows the files the scan reads as well as the
  rows it emits, so this feature records NO remaining pruning exception
<!-- /DELTA:CHANGED -->
* *AND* the reader SHALL now GATE the Delta reader protocol and reader-feature set
  (`vs-adapter/delta-reader-feature-gating`), SUPERSEDING the recorded rule that it performs no such
  gating: a table whose reader features this engine does not implement is no longer query-reachable,
  so this feature records NO remaining reader-feature exception
* *AND* the reader SHALL refuse a request that reads or emits a column whose Delta type this engine
  cannot render faithfully, per column rather than per table
  (`vs-adapter/delta-type-mapping`), so a table carrying one struct column stays queryable on its
  other columns
* *AND* every error the reader surfaces on this path MUST be returned as an error value, never raised
  as a panic, and MUST NOT contain any vended or static credential value
