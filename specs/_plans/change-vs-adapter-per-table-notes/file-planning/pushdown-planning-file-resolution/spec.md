<!-- DELTA:CHANGED -->
# Feature: Pushdown File Resolution

Resolves a table's identity and current file state once per pushdown, before any SQL is built. The adapter recovers the target table's catalog identifier from the involved table's identifier note (`vs-adapter/create-virtual-schema`) and passes it to the format reader. The reader returns the data-file list, each file's byte size, the table root, and each file's delete references, so the scan UDF never discovers files, delete files, or sizes itself.
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
## Background

* The resolve-once orchestration (the identifier-note read, one resolve, one `ScanSpec` build) is the same for every table format (`file-planning/pushdown-format-neutral-resolution`). Only the reader behind the seam is format-specific. This feature owns the Iceberg reader: the multi-level `TableIdent` build, the snapshot read, each file's byte size from the manifest, and merge-on-read positional-delete and deletion-vector resolution. `delta/delta-table-planning` owns the Delta reader.
* The table's logical schema comes from its column notes (`vs-adapter/column-source-notes`), never from the reader. `createVirtualSchema` builds each note from the table's `current_schema()`: the Iceberg field id, the name at that time, the Arrow type, the nullability, and the `initial-default`. Pushdown reads the current schema only to translate the filter for pruning (`file-planning/pushdown-file-pruning`), to refuse a recorded `date` promotion, and to check declared nullability.
* Each per-shard file entry carries the file path and its byte size. Delete support adds per-file delete references only, with no serialized Iceberg schema and no bound predicate.
* A `loadTable` response with no table `location` is rejected at the resolve-once seam, above the vended/static storage split, so every path that needs a table root (including each join side) fails identically. The `createVirtualSchema` path reads no location, so the rejection does not apply there.
* Iceberg spec, Table Metadata: `location` is `_required_` in v1, v2, and v3, so a response without it is malformed. Only v4 makes it optional.
* A `loadTable` body can carry `"location": ""` or omit the key. This guard rejects the empty value. An omitted key fails earlier, at deserialization, because the `iceberg` crate declares `location` as a required string, and the error names the unparseable response instead of the field. The two shapes differ only in message specificity.
* The REST `warehouse` is a routing identifier and denotes no object store: a bare AWS account id under Glue, or a warehouse name or UUID under Lakekeeper. It is never a substitute for an absent table location.
* After this rejection an empty table root is unreachable from a `loadTable` response. The adapter's empty-root path relativization and the empty-table-root clauses of `datafusion-scan/scan-execution-file-metadata` stay, because they are a total-function property of the wire encoding.
* Iceberg spec, Column Projection: "Columns in Iceberg data files are selected by field id. The table schema's column names and order may change after a data file is written, and projection must be done using field ids." Schema Evolution: "Renaming an existing field must change the name, but not the field ID", and "The `initial-default` must be set when a field is added and cannot change". A declared field id therefore names the same column for the table's life. A rename between refreshes leaves the declaration correct under its old name.
* Scoped exception: a column dropped at the source between refreshes. Schema Evolution: "Deleting a field removes it from the current schema." The pushdown plans against the declared schema, which still holds the dropped field id. Until `REFRESH`, the declared column reads the values that files written before the drop store, and NULL or its `initial-default` from files written after. No row is misread, but the column shows data the current schema no longer exposes. A dropped column declared required with no `initial-default` instead fails a query that reads a file written after the drop, with the error `scan-read-path/scan-execution-field-id-projection-absent-fields` records. `REFRESH` removes the column.
* Each note fixes its column's `nullable` flag at `createVirtualSchema`, and a catalog can later relax a required field to optional. DataFusion rewrites `c IS NULL` to `false` for a non-nullable field, so a stale `nullable: false` would drop rows stored as NULL without an error. The Iceberg reader therefore fails a query whose declared non-nullable column the current schema marks optional, looked up by the declared field id. A declared optional column that the current schema marks required plans normally, because no row is lost.
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Pushdown derives the scanned Iceberg table from the involved virtual table

* *GIVEN* a virtual schema created over a namespace containing multiple Iceberg tables, each virtual table carrying the identifier note recorded at create or refresh time
* *AND* a `pushdown` request whose `involvedTables[0].name` is the Exasol (uppercased, `__`-flattened) name of one of those tables
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL read the Iceberg identifier from the `name` member of the involved table's identifier note, `involvedTables[0].adapterNotes`, ignore any other member, and never list the catalog or derive the identifier from the virtual table name
* *AND* the adapter SHALL resolve the data-file list and build the scan-driving SQL for exactly that one Iceberg table, loading it from the catalog under that identifier
* *AND* an involved table without a readable identifier note (absent, not a JSON object, or without a string `name`) SHALL fail the request with an error that names the virtual table and `ALTER VIRTUAL SCHEMA ... REFRESH`, never scanning a different or stale table
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: Pushdown resolves the file list once and builds a scan-driving query

* *GIVEN* a virtual schema over a namespace whose tables are backed by SeaweedFS
* *AND* a query that projects a subset of columns from one of those tables
* *WHEN* Exasol sends the corresponding `pushdown` request
* *THEN* the adapter SHALL determine the target Iceberg table from the involved table's identifier note, resolve that table's Iceberg snapshot, data-file list, and each file's byte size exactly once, and take the table's logical schema from its column notes, carrying, per column, its declared `field_id`, name, Arrow type, nullability, and `initial-default`
* *AND* the adapter MUST NOT build the logical schema from the table's `current_schema()` at pushdown
* *AND* the adapter SHALL return a JSON response of type `pushdown` containing SQL that invokes the `LAKEHOUSE_SCAN` SCALAR EMIT UDF, carrying the logical schema AND the table root in the shard-invariant common spec spliced ONCE as the scalar scan's first-argument literal, and the resolved data-file list flowed through the nested `LAKEHOUSE_DISTRIBUTE_FILES` distributor as the per-shard argument, where each per-shard entry carries the file path together with its resolved byte size
* *AND* the outer scalar scan select MUST NOT be wrapped in a `SELECT * FROM (...)` materialization boundary
* *AND* the adapter MUST NOT require the scan UDF to discover files itself, and MUST NOT require the scan UDF to re-fetch any file's size
* *AND* the resolve-once orchestration this scenario describes, identifier-note read, one resolve, one `ScanSpec` build, SHALL be reached identically by every table format, with the format reader supplying the file list, byte sizes, and table root, and the column notes supplying the logical schema (`file-planning/pushdown-format-neutral-resolution`); only the reader behind the seam is format-specific
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: Pushdown resolves multi-level namespace identifiers into the iceberg TableIdent

* *GIVEN* an identifier note whose `name` member is a multi-level Iceberg identifier such as `prod.finance.orders`
* *WHEN* the adapter resolves that identifier to load the table from the catalog
* *THEN* the adapter SHALL split the identifier into all namespace segments and the trailing table name, building the iceberg `TableIdent` from a multi-segment `NamespaceIdent` rather than treating only the first segment as the namespace
* *AND* both the SigV4-signed and the unsigned catalog paths SHALL build the identifier the same way so multi-level namespaces load correctly under either path
<!-- /DELTA:CHANGED -->
