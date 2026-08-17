# Feature: Delta Table Planning

Resolves a Delta Lake table into the engine's existing `ScanSpec` shape at plan time, so file-level
sharding, the pushdown wire format, streaming emit, and the memory model are reused for the Delta
path exactly as they already are for Iceberg.

## Background

* **This delta is issue #322.** The two deferrals this feature has recorded since #319 — Delta
  reader-feature gating and broad Delta type mapping — are both closed. Neither the log replay, the
  partition values, the deletion-vector descriptors, the column-mapping binding keys, nor the
  credential vending changes; what changes is that an unsupported table is now refused and a wider
  type surface is now mapped.
* **Gating moves to `vs-adapter/delta-reader-feature-gating`** and type mapping to
  `vs-adapter/delta-type-mapping`, rather than growing this feature further. This feature already
  carries nine scenarios spanning log replay, partition values, deletion vectors, column mapping,
  credentials, format dispatch, and Iceberg parity; a protocol gate and a full type-surface mapping are
  each a distinct reason to change and each carries its own normative protocol citations.
* **Only two recorded statements are affected.** The scenario "A Delta type this plan does not map is
  refused at plan time" is REMOVED, because every clause of it either restates a mapping
  `vs-adapter/delta-type-mapping` now owns or asserts the absence of the gate
  `vs-adapter/delta-reader-feature-gating` now adds. The scenario "The Delta reader is reached from
  production pushdown under the Unity Catalog kind" is CHANGED, because its "SHALL still perform NO
  Delta reader-feature gating" clause and its scoped-exception clause are the exception this plan
  closes.
* **Filter-based file pruning stays deferred, unchanged.** Per-file statistics and partition pruning
  remain issue #321, so a filter still narrows the rows the scan emits without narrowing the files it
  reads. This plan touches neither.
* **Apache Iceberg spec check — this delta changes no Iceberg behavior.** It adds a Delta protocol
  gate and widens the Delta type mapping; no code on the Iceberg resolution path changes. The Iceberg
  table spec's Column Projection requirement that "projection must be done using field ids" still
  holds for every Iceberg column, and its ordered resolution rule (1) — the partition-metadata rule —
  remains the deliberate, accurately-scoped trade-off
  `datafusion-scan/scan-execution-field-id-projection` records, neither closed nor widened here.

## Scenarios

<!-- DELTA:REMOVED -->
### Scenario: A Delta type this plan does not map is refused at plan time

* *GIVEN* the recorded rule that only ten Delta primitive types carry an Arrow tag, that every other
  Delta type refuses the whole TABLE with an error citing issue #322 as the tracked gap, and that the
  reader performs NO Delta reader-feature gating
* *WHEN* the Delta format reader resolves a table declaring a type outside those ten
* *THEN* this scenario SHALL be REMOVED, because issue #322 is this plan: its type list is superseded
  by `vs-adapter/delta-type-mapping`, which maps `byte`, `short`, `void`, both interval types,
  out-of-domain `decimal`, and `array` and refuses `binary`, `struct`, `map`, and `variant` per COLUMN
  with a per-type reason; and its no-gating clause is superseded by
  `vs-adapter/delta-reader-feature-gating`
* *AND* its error-text requirement — that the refusal cite issue #322 — SHALL NOT survive, because a
  closed issue cited in a shipped error text reads as an unfixed gap with no owner; the replacement
  reasons cite issue #350 for `binary`, `struct`, and `map` and issue #349 for type widening
<!-- /DELTA:REMOVED -->

<!-- DELTA:CHANGED -->
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
* *AND* the reader SHALL still apply NO filter-based file pruning, because per-file statistics and
  partition pruning remain issue #321, so a filter narrows the rows the scan emits without narrowing
  the files it reads
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
<!-- /DELTA:CHANGED -->
