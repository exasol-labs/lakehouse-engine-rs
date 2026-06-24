# Feature: Pushdown Planning

Translates an Exasol query against the virtual schema into a pushdown plan for a single involved table: it derives the scanned Iceberg table from `involvedTables[0].name` via the create-time `TABLE_MAP`, resolves that table's Iceberg data-file list once (signing catalog requests with AWS SigV4 and applying vended S3 credentials when enabled), captures projection, filter, LIMIT, and any supported aggregate, and emits the SQL that drives the DataFusion scan SET UDF over exactly those files.

## Background

Each pushdown request concerns exactly one virtual table — Exasol issues a separate single-table pushdown per table, including for JOINs (which are not advertised; Exasol joins the per-table result sets itself). The `TABLE_MAP` recorded in `schemaMetadata.adapterNotes` at create time is handed back in `schemaMetadataInfo.adapterNotes` and maps each Exasol table name to its original-cased Iceberg identifier.

## Scenarios

<!-- DELTA:NEW -->
### Pushdown derives the scanned Iceberg table from the involved virtual table

* *GIVEN* a virtual schema created over a namespace containing multiple Iceberg tables, whose `adapterNotes` carry the `TABLE_MAP` recorded at create time
* *AND* a `pushdown` request whose `involvedTables[0].name` is the Exasol (uppercased, `__`-flattened) name of one of those tables
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL read `TABLE_MAP` back from `schemaMetadataInfo.adapterNotes` and look up the involved virtual table name to recover its original-cased fully-qualified Iceberg identifier
* *AND* the adapter SHALL resolve the data-file list and build the scan-driving SQL for exactly that one Iceberg table, carrying its identifier in the per-shard `CatalogProps.table`
* *AND* a `pushdown` request whose involved virtual table name is absent from `TABLE_MAP` SHALL fail with an error naming the unknown virtual table, never silently scanning a different or stale table
<!-- /DELTA:NEW -->

<!-- DELTA:CHANGED -->
### Pushdown resolves the file list once and builds a scan-driving query

* *GIVEN* a virtual schema over a namespace whose tables are backed by MinIO
* *AND* a query that projects a subset of columns from one of those tables
* *WHEN* Exasol sends the corresponding `pushdown` request
* *THEN* the adapter SHALL determine the target Iceberg table from `involvedTables[0].name` via the `TABLE_MAP` and resolve that table's Iceberg snapshot and data-file list exactly once
* *AND* the adapter SHALL return a JSON response of type `pushdown` containing SQL that invokes the scan SET UDF and passes the resolved data-file list as an explicit argument
* *AND* the adapter MUST NOT require the scan UDF to discover files itself
<!-- /DELTA:CHANGED -->

<!-- DELTA:NEW -->
### Pushdown resolves multi-level namespace identifiers into the iceberg TableIdent

* *GIVEN* a `TABLE_MAP` entry whose value is a multi-level Iceberg identifier such as `prod.finance.orders`
* *WHEN* the adapter resolves that identifier to load the table from the catalog
* *THEN* the adapter SHALL split the identifier into all namespace segments and the trailing table name, building the iceberg `TableIdent` from a multi-segment `NamespaceIdent` rather than treating only the first segment as the namespace
* *AND* both the SigV4-signed and the unsigned catalog paths SHALL build the identifier the same way so multi-level namespaces load correctly under either path
<!-- /DELTA:NEW -->
