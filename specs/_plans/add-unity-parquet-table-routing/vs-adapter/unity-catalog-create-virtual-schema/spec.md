<!-- DELTA:CHANGED -->
# Feature: Unity Catalog Create Virtual Schema

Enumerates a Unity Catalog namespace during createVirtualSchema and returns one virtual table per plannable base table: a listed entry whose Unity Catalog `table_type` is `MANAGED` or `EXTERNAL` and whose `data_source_format` is `DELTA` or `PARQUET`. Every other listed entry (a view, another format, or any other `table_type`) is excluded from the returned virtual tables and warned. Enumeration runs on the SAME kind-agnostic listing pipeline the Iceberg REST kind uses. The admission decision lives inside the Unity Catalog client, so `data_source_format` never crosses the shared trait boundary. Mapping each `catalog.schema.table` identifier to an Exasol table name and each Unity Catalog column to an Exasol column type is sufficient to list the namespace and expose queryable column metadata. Full type fidelity is the format reader's concern at plan time. This path reads only Unity Catalog metadata and no object storage, so it resolves no snapshot, no file list, and no Parquet footer.
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
## Background

The configured namespace is the `catalog.schema` supplied as the `NAMESPACE` virtual-schema property, dot-split into segments. The adapter obtains the namespace's tables by calling the shared trait's list operation on the constructed Unity Catalog client, and the pipeline that consumes the result is the same code the Iceberg REST kind runs. That pipeline reads neutral table metadata and does not know, or ask, how each client sourced its columns. Inside the Unity Catalog client, every listed table's column metadata comes from the single paginated `GET /tables` list sweep, which returns each table's columns inline by default (verified live against `demo_sales_catalog.sales`). That client MUST NOT issue a per-table `GET /tables/{full_name}` to obtain columns, so enumerating a schema costs one paginated sweep rather than an N+1 fan-out. Each enumerated table's Exasol name is the namespace segments below the configured namespace plus the table name, joined with `__` and uppercased through the SAME single case-fold site the Iceberg createVirtualSchema path uses. The shared pipeline applies one column-name fold for both kinds, so no code path declares a differently-cased name. The Exasol-name-to-Unity-Catalog-identifier map is recorded in the response `adapterNotes.TABLE_MAP`. Unity Catalog reports each column's type as a Spark type (for example `LONG`, `STRING`, `INT`, and the parameterized `DECIMAL(p,s)`), which the neutral column carries as a source-tagged type descriptor holding the FULL parameterized type: the type name plus precision and scale from the wire `type_precision`/`type_scale`. The adapter's single type-mapping home maps that descriptor to an Exasol type reusing the Arrow-to-Exasol convention. Any type without a clean scalar Exasol equivalent is declared `VARCHAR(2000000)` rather than failing the enumeration.

* The Unity Catalog client makes the admission decision (`vs-adapter/unity-catalog-client`) and routes every excluded entry into `CatalogListing.skipped`. The shared `build_listing_virtual_tables` pipeline stays kind-agnostic.
* Excluded entries are warned on the SAME shared skip-warn loop the Iceberg REST kind uses: one `udf_log!(ctx, warn, ...)` line per skipped entry, rendered from the neutral skip reason on the entry rather than from a per-kind branch. The Iceberg path's warning stays byte-identical.
* A Delta table and a Parquet table declare their columns identically, from the same Unity Catalog column types, because this path reads no object storage.
* Shallow clones are admitted by the base-table rule with no shallow-clone-specific handling, because Unity Catalog surfaces a shallow clone as a `MANAGED` or `EXTERNAL` table with `data_source_format` `DELTA`. That a real shallow clone's wire shape matches this rule is an assumption not verified live.
* The `DECIMAL` guard's predicate and the timestamp declaration rule each have ONE owner, `datafusion-scan/type-mapping`. This feature consumes them and does not restate them. `vs-adapter/create-virtual-schema` owns the single `ctx.database_version()` read.
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Create virtual schema enumerates every table in the configured Unity Catalog namespace

* *GIVEN* a Unity Catalog reachable through the CONNECTION named by `CATALOG_CONNECTION` and a createVirtualSchema request whose `CATALOG_KIND` is `UNITY_CATALOG` and whose `NAMESPACE` property names a `catalog.schema`
* *WHEN* Exasol sends the createVirtualSchema request
* *THEN* the adapter SHALL list every table in that schema by calling the shared `CatalogClient` list operation on the constructed Unity Catalog client, and SHALL return one virtual table per listed PLANNABLE BASE table: an entry whose `table_type` is `MANAGED` or `EXTERNAL` AND whose `data_source_format` is `DELTA` or `PARQUET`
* *AND* a `PARQUET` table SHALL declare every column Unity Catalog lists for it, its partition columns included, in the catalog's column order and mapped through the same type-mapping home as a Delta table's columns
* *AND* the adapter SHALL exclude from the returned virtual tables every other listed entry (a view, a base table whose `data_source_format` is neither `DELTA` nor `PARQUET`, or an entry of any other `table_type`), and MUST NOT record an excluded entry in `TABLE_MAP`, so no unplannable entry becomes a queryable virtual table
<!-- /DELTA:CHANGED -->

<!-- DELTA:NEW -->
### Scenario: Enumeration names tables through the shared fold and stops at catalog metadata

* *GIVEN* the same Unity Catalog namespace enumeration
* *WHEN* Exasol sends the createVirtualSchema request
* *THEN* the adapter SHALL name each returned virtual table by flattening the segments below the configured namespace plus the table name with `__` and uppercasing the result through the shared case-fold site
* *AND* the adapter SHALL source every listed entry's columns, `table_type`, and `data_source_format` from the single paginated `GET /tables` list sweep, issuing no per-table `GET /tables/{full_name}`, reading no Delta transaction log, reading no Parquet footer, and resolving no snapshot, because this path stops at catalog metadata
* *AND* the namespace property SHALL be the SAME `NAMESPACE` property the Iceberg REST kind reads, so neither kind carries a namespace property of its own
<!-- /DELTA:NEW -->

<!-- DELTA:REMOVED -->
### Scenario: Create virtual schema excludes every non-Delta-base entry and warns per exclusion

* *GIVEN* a Unity Catalog namespace whose `GET /tables` list sweep returns a `VIEW` entry carrying columns, no `storage_location`, and a null `data_source_format`; a `MANAGED` entry whose `data_source_format` is a non-`DELTA` value such as `ICEBERG`, `CSV`, `PARQUET`, or `JSON`; and an entry whose `table_type` is neither `MANAGED`, `EXTERNAL`, nor `VIEW`
* *WHEN* the adapter enumerates that namespace during createVirtualSchema
* *THEN* the adapter SHALL exclude all three entries from the returned virtual tables and from `TABLE_MAP`, and SHALL complete createVirtualSchema successfully with only the Delta base tables in the namespace
* *AND* the adapter SHALL write one `udf_log!(ctx, warn, ...)` line per excluded entry naming the excluded `catalog.schema.table` identifier and the disqualifying reason — the entry's `table_type` for a view or other-type entry, or its `data_source_format` for a non-`DELTA` base table
* *AND* none of those warning lines MUST contain the resolved bearer token, any OAuth client secret, or any other credential value
* *AND* a namespace whose every listed entry is excluded SHALL yield a createVirtualSchema response with an empty table list and an empty `TABLE_MAP`, rather than an error
<!-- /DELTA:REMOVED -->

<!-- DELTA:NEW -->
### Scenario: Create virtual schema excludes every entry that is not a plannable base table and warns per exclusion

* *GIVEN* a Unity Catalog namespace whose `GET /tables` list sweep returns three entries: a `VIEW` entry carrying columns, no `storage_location`, and a null `data_source_format`, a `MANAGED` entry whose `data_source_format` is neither `DELTA` nor `PARQUET` (such as `ICEBERG`, `CSV`, or `JSON`), and an entry whose `table_type` is neither `MANAGED`, `EXTERNAL`, nor `VIEW`
* *WHEN* the adapter enumerates that namespace during createVirtualSchema
* *THEN* the adapter SHALL exclude all three entries from the returned virtual tables and from `TABLE_MAP`, and SHALL complete createVirtualSchema successfully with only the plannable base tables in the namespace
* *AND* the adapter SHALL write one `udf_log!(ctx, warn, ...)` line per excluded entry naming the excluded `catalog.schema.table` identifier and the disqualifying reason: the entry's `table_type` for a view or other-type entry, or its `data_source_format` for an unplannable base table
* *AND* none of those warning lines MUST contain the resolved bearer token, any OAuth client secret, or any other credential value
* *AND* a namespace whose every listed entry is excluded SHALL yield a createVirtualSchema response with an empty table list and an empty `TABLE_MAP`, rather than an error
<!-- /DELTA:NEW -->
