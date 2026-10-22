# Feature: Pushdown File Resolution

Resolves a table's identity and current file state once per pushdown, before any SQL is built. The adapter recovers the target table from the involved-table name via the persisted `TABLE_MAP` and passes it to the format reader. The reader returns the data-file list, each file's byte size, the table root, and each file's delete references, so the scan UDF never discovers files, delete files, or sizes itself.

## Background

* The resolve-once orchestration (the `TABLE_MAP` lookup, one resolve, one `ScanSpec` build) is the same for every table format (`file-planning/pushdown-format-neutral-resolution`). Only the reader behind the seam is format-specific. This feature owns the Iceberg reader: the multi-level `TableIdent` build, the snapshot read, each file's byte size from the manifest, and merge-on-read positional-delete and deletion-vector resolution. `delta/delta-table-planning` owns the Delta reader.
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

## Scenarios

### Scenario: Pushdown derives the scanned Iceberg table from the involved virtual table

* *GIVEN* a virtual schema created over a namespace containing multiple Iceberg tables, whose `adapterNotes` carry the `TABLE_MAP` recorded at create time
* *AND* a `pushdown` request whose `involvedTables[0].name` is the Exasol (uppercased, `__`-flattened) name of one of those tables
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL read `TABLE_MAP` back from `schemaMetadataInfo.adapterNotes` and look up the involved virtual table name to recover its original-cased fully-qualified Iceberg identifier
* *AND* the adapter SHALL resolve the data-file list and build the scan-driving SQL for exactly that one Iceberg table, loading it from the catalog under that identifier
* *AND* a `pushdown` request whose involved virtual table name is absent from `TABLE_MAP` SHALL fail with an error naming the unknown virtual table, never silently scanning a different or stale table

### Scenario: Pushdown resolves the file list once and builds a scan-driving query

* *GIVEN* a virtual schema over a namespace whose tables are backed by SeaweedFS
* *AND* a query that projects a subset of columns from one of those tables
* *WHEN* Exasol sends the corresponding `pushdown` request
* *THEN* the adapter SHALL determine the target Iceberg table from the schema-metadata mapping, resolve that table's Iceberg snapshot, data-file list, and each file's byte size exactly once, and take the table's logical schema from its column notes, carrying, per column, its declared `field_id`, name, Arrow type, nullability, and `initial-default`
* *AND* the adapter MUST NOT build the logical schema from the table's `current_schema()` at pushdown
* *AND* the adapter SHALL return a JSON response of type `pushdown` containing SQL that invokes the `LAKEHOUSE_SCAN` SCALAR EMIT UDF, carrying the logical schema AND the table root in the shard-invariant common spec spliced ONCE as the scalar scan's first-argument literal, and the resolved data-file list flowed through the nested `LAKEHOUSE_DISTRIBUTE_FILES` distributor as the per-shard argument, where each per-shard entry carries the file path together with its resolved byte size
* *AND* the outer scalar scan select MUST NOT be wrapped in a `SELECT * FROM (...)` materialization boundary
* *AND* the adapter MUST NOT require the scan UDF to discover files itself, and MUST NOT require the scan UDF to re-fetch any file's size
* *AND* the resolve-once orchestration this scenario describes, `TABLE_MAP` lookup, one resolve, one `ScanSpec` build, SHALL be reached identically by every table format, with the format reader supplying the file list, byte sizes, and table root, and the column notes supplying the logical schema (`file-planning/pushdown-format-neutral-resolution`); only the reader behind the seam is format-specific

### Scenario: Pushdown resolves multi-level namespace identifiers into the iceberg TableIdent

* *GIVEN* a `TABLE_MAP` entry whose value is a multi-level Iceberg identifier such as `prod.finance.orders`
* *WHEN* the adapter resolves that identifier to load the table from the catalog
* *THEN* the adapter SHALL split the identifier into all namespace segments and the trailing table name, building the iceberg `TableIdent` from a multi-segment `NamespaceIdent` rather than treating only the first segment as the namespace
* *AND* both the SigV4-signed and the unsigned catalog paths SHALL build the identifier the same way so multi-level namespaces load correctly under either path

### Scenario: Delete-file references are carried in the per-shard files argument

* *GIVEN* a virtual schema over an Iceberg merge-on-read table backed by SeaweedFS, where `plan_files` associates each data file with its applicable Parquet positional-delete files (at `file` or `partition` granularity) or its deletion vector
* *WHEN* Exasol sends the corresponding pushdown request
* *THEN* the adapter SHALL resolve the data-file list, each file's byte size, and each file's associated positional-delete files and deletion vector exactly once, at the same resolve-once seam, and MUST NOT require the scan UDF to discover delete files itself
* *AND* the adapter SHALL carry each data file's associated positional-delete file references (path, byte size, delete content type) and deletion vector (the same keys plus the blob's offset and length and its cardinality) in the per-shard files argument alongside the data-file entry, keeping the wire surface minimal: no serialized Iceberg schema and no bound predicate are added for delete support
* *AND* the shard-invariant common spec (logical schema, projection, filter, LIMIT, credentials, table root) SHALL be unchanged by delete support, so a delete-free table produces a byte-identical common spec to before this feature

### Scenario: File resolution rejects a loadTable response that carries no table location

* *GIVEN* a `pushdown` request for a table whose `loadTable` response carries an EMPTY table metadata `location`
* *AND* a CONNECTION that supplies a non-empty `warehouse`
* *WHEN* the adapter resolves the file list for that table, under EITHER value of `use_vended_credentials`
* *THEN* the adapter SHALL return a user error naming the EMPTY table `location`, from ONE check that runs BEFORE the vended/static storage split, so both values of `use_vended_credentials` report the IDENTICAL error text
* *AND* that error text MUST be path-independent: it MUST NOT frame the failure as a vended-storage-backend resolution failure, because on the non-vended path the static storage backend resolves normally and only the table location is missing
* *AND* the adapter MUST NOT substitute the CONNECTION's `warehouse`, `endpoint`, or any other CONNECTION-derived value for the empty location, because the REST `warehouse` is a routing identifier, a bare AWS account id on the Glue path, a warehouse name on the Lakekeeper path, that denotes no object store
* *AND* the adapter MUST NOT go on to resolve the effective storage, build the Iceberg table, or plan any file, so a malformed response costs zero object-storage access
* *AND* that error MUST be returned as an error value, never raised as a panic, because a panic inside a UDF is an abnormal VM exit that makes the engine SIGKILL every sibling VM of the statement part
* *AND* that error MUST NOT contain any credential value
* *AND* every join side SHALL inherit this rejection through the same file-resolution call, so a join whose dimension side carries no location fails at plan time with the same error
* *AND* the `createVirtualSchema` schema-resolution path SHALL be unchanged, because it reads only the table's `current_schema()` and reads no table location, so nothing there can be substituted for one
* *AND* the empty-table-root branch SHALL be retained on both sides of the wire, the adapter's path relativization and the three empty-table-root clauses of `datafusion-scan/scan-execution-file-metadata` (two normative `SHALL` clauses and one descriptive Background bullet), because this rejection makes an empty root unreachable from a `loadTable` response, which makes that branch unreachable rather than dead, and it MUST NOT be deleted or converted into a second error
* *AND* a `loadTable` body that OMITS the `location` key SHALL also be rejected as a user error, at deserialization rather than by this guard, and MUST NOT be reported by any message that substitutes the `warehouse`; that message names the unparseable response rather than the `location` field, which this feature records as a known diagnostic-specificity difference rather than leaving it unstated

### Scenario: A storage read the object store denies fails the pushdown with the store's denial

* *GIVEN* a virtual schema over an Iceberg table whose CONNECTION storage identity the object store denies, for example a base AWS identity that names no role (`connection/connection-credentials-assume-role`)
* *WHEN* a query over that table makes the adapter read the table's manifests from storage while it resolves the file list
* *THEN* the query SHALL fail with an error that reports the object store's access-denied response
* *AND* the error MUST NOT contain any credential value of the CONNECTION

### Scenario: A column renamed at the source after the last REFRESH reads under its declared name

* *GIVEN* an Iceberg table with columns `id` (field id 1) and `score` (field id 2) holding rows in one data file, a virtual schema created over it, and the source column `score` afterwards renamed to `rating` with no `REFRESH`
* *WHEN* a query selects `ID, SCORE`
* *THEN* the query SHALL return each row's `score` value under `SCORE`, because the scan binds the declared field id 2, which the rename kept
* *AND* the virtual table SHALL NOT declare `RATING` until `ALTER VIRTUAL SCHEMA ... REFRESH` runs, after which the same values read under `RATING`

### Scenario: A column dropped at the source after the last REFRESH reads what older data files store

* *GIVEN* an Iceberg table with columns `id` and an optional column `note`, one data file written while `note` existed, a virtual schema created over it, and afterwards `note` dropped from the source schema and a second data file written without it, with no `REFRESH`
* *WHEN* a query selects `ID, NOTE`
* *THEN* the rows of the first data file SHALL carry their stored `note` values and the rows of the second SHALL carry NULL, per the scoped exception this feature's Background records
* *AND* the query MUST NOT fail, and MUST NOT bind `NOTE` to any other column
* *AND* after `ALTER VIRTUAL SCHEMA ... REFRESH` the virtual table SHALL NOT declare `NOTE`
* *AND* had `note` been required with no `initial-default`, the query SHALL instead fail on the second data file with the required-absent error `scan-read-path/scan-execution-field-id-projection-absent-fields` records, until `REFRESH`

### Scenario: A declared required column that the current schema marks optional fails the query

* *GIVEN* an Iceberg table with a required column `qty` (field id 2), a virtual schema created over it, and afterwards `qty` made optional at the source and a data file written that stores NULL in `qty`, with no `REFRESH`
* *WHEN* `SELECT ID FROM <table> WHERE QTY IS NULL` runs, and separately a query that reads only `ID`
* *THEN* each query SHALL fail before any file is planned with an error naming the virtual table and the column `QTY`, stating that the declaration marks it non-nullable while the current schema allows NULL, and naming `ALTER VIRTUAL SCHEMA ... REFRESH` as the fix
* *AND* the reader SHALL find the column in the current schema by its declared field id, not by its name
* *AND* after `REFRESH` the first query SHALL return the row whose `qty` is NULL
