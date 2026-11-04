# Feature: Delta Table Planning

The adapter resolves a Delta Lake table into the engine's `ScanSpec` at plan time, so file-level sharding, the pushdown wire format, streaming emit, and the memory model are shared with the Iceberg path.

<!-- DELTA:CHANGED -->
## Background

* The adapter plans a Delta table at pushdown by replaying its Delta log from object storage.
* Within one statement, the adapter sends a table's catalog request once and its storage-credential request at most once, and lists its Delta log once. Each reference to the table reads the log with its own filter and gets its own file list.
* The format that the catalog reports for a table decides how a pushdown plans it.
* Delta never writes a partition column's value into the data file, so the adapter passes each file's partition values to the scan (`scan-read-path/scan-execution-partition-values`).
* Pushdown under the Unity Catalog kind plans a Delta scan (`vs-adapter/catalog-kind-selection`). Deletion vectors, partition values, and column mapping apply at scan time (`scan-read-path/scan-execution-delta-deletion-vectors`, `scan-read-path/scan-execution-partition-values`).
* File pruning on Delta log statistics runs at plan time (`delta/delta-file-pruning`), and the pushdown SQL carries no per-file statistics.
* Reader-feature gating is `delta/delta-reader-feature-gating`. Type mapping is `delta/delta-type-mapping`.
* A Delta table's logical schema, partition columns, and refused columns are decided once at `createVirtualSchema` from Unity Catalog's copy: each column's `type_json` (the Delta `StructField` JSON), the `delta.columnMapping.mode` table property, and each column's `partition_index`. They are recorded in each column's note (`vs-adapter/column-source-notes`), and `createVirtualSchema` reads no Delta log. A pushdown reads the gated Delta log for the feature gate, active files, deletion vectors, and partition values, checks the declaration against it (`delta/delta-table-planning-declaration-checks`), and takes no column type from it.
* Delta protocol "Column Mapping": the physical name is static and the display name can change. In `name` mode readers resolve columns, partition values, and statistics by physical name, and in `id` mode by `field_id`. A recorded binding key therefore names the same column after a source-side rename. Under `none` mode a declared name is the column's identity.
* The protocol honors `delta.columnMapping.mode` only when the table's protocol supports the `columnMapping` feature. `createVirtualSchema` reads no protocol, so a pushdown compares the declaration with the mode the table's protocol honors and fails the query on disagreement.
* The scan treats `c IS NULL` as `false` for a non-nullable column. A stale non-nullable declaration would drop matching rows, so a pushdown fails a query whose declared non-nullable column the Delta log marks nullable. A declared nullable column that the log marks non-nullable plans normally.
* Scoped exception: a type change the declaration does not record, or one recorded after the last `REFRESH`, is not validated by the declaration. A data file written under an unsupported change fails the query at scan time, because the scan admits only the widening pairs `scan-types/type-relaxation` proves.
* Scoped exception: a column dropped at the source after the last `REFRESH`, under `name` or `id` mode, reads NULL from files written without it until `REFRESH`. A dropped non-nullable column fails a query that reads such a file (`scan-read-path/scan-execution-field-id-projection-absent-fields`). This mirrors the Iceberg exception in `file-planning/pushdown-planning-file-resolution`.
* The reader-feature gate stays at pushdown. A table the gate refuses is listed at `createVirtualSchema` and fails at query time.
* The scan decodes each percent-encoded `add.path` of the Delta log before it requests the object.
* Every failure returns an error, never a panic, and no error text carries a credential value.
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:REMOVED -->
### Scenario: The format reader is selected at one site and refuses a mismatched pairing

* *GIVEN* the recorded scenario of this title
* *WHEN* this plan is recorded
* *THEN* this scenario SHALL be REMOVED, because it states code structure rather than behavior
* *AND* it SHALL be REPLACED by "A Unity Catalog or Glue table is planned by the format its catalog reports" below
<!-- /DELTA:REMOVED -->

<!-- DELTA:NEW -->
### Scenario: A Unity Catalog or Glue table is planned by the format its catalog reports

* *GIVEN* a pushdown request over a table of a Unity Catalog or Glue virtual schema
* *WHEN* the adapter loads the table
* *THEN* the format that the catalog's table answer reports SHALL select the plan: under Unity Catalog, the Delta tag plans a Delta scan and the Parquet tag plans the Unity Parquet scan of `unity-catalog/unity-parquet-table-planning`; under Glue, the Iceberg tag plans an Iceberg scan from the table's metadata file and the Parquet tag plans the Hive Parquet scan of `glue/glue-table-planning`
* *AND* a format that the kind does not plan, Iceberg under Unity Catalog or Delta under Glue, SHALL fail the query with an error that names the table and the reported format, because a table planned as another format surfaces a missing log or a wrong schema instead of a clear format refusal
* *AND* the adapter SHALL apply this refusal to the format that the pushdown's own table lookup reports, even when `createVirtualSchema` listed the table under a format the kind plans
<!-- /DELTA:NEW -->

<!-- DELTA:CHANGED -->
### Scenario: The Delta reader is reached from production pushdown under the Unity Catalog kind

* *GIVEN* a virtual schema created with `CATALOG_KIND` set to `UNITY_CATALOG`, and a query against one
  of its Delta tables
* *WHEN* the adapter handles the resulting pushdown request
* *THEN* the adapter SHALL plan a Delta scan for the table whose Unity Catalog table answer reports the Delta format, and SHALL return
  scan-driving SQL over that table's files
* *AND* the scan SHALL reconstruct the table's partition columns from each file's partition values, as
  `scan-read-path/scan-execution-partition-values` specifies
* *AND* the adapter SHALL PRUNE the file list by the request's filter, on partition values and on per-file statistics alike (`delta/delta-file-pruning`), so a filter narrows the files the scan reads as well as the rows it emits
* *AND* the adapter SHALL GATE the Delta reader protocol and reader-feature set (`delta/delta-reader-feature-gating`), so a table whose reader features this engine does not implement is not query-reachable
* *AND* the adapter SHALL refuse a request that reads or emits a column whose Delta type this engine
  cannot render faithfully, per column rather than per table
  (`delta/delta-type-mapping`), so a table carrying one struct column stays queryable on its
  other columns
* *AND* every error on this path MUST be returned as an error value, never raised
  as a panic, and MUST NOT contain any vended or static credential value
<!-- /DELTA:CHANGED -->
