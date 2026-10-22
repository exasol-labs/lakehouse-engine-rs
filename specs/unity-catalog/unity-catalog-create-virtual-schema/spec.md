# Feature: Unity Catalog Create Virtual Schema

Enumerates a Unity Catalog namespace during createVirtualSchema and returns one virtual table per plannable base table: a listed entry whose `table_type` is `MANAGED` or `EXTERNAL` and whose `data_source_format` is `DELTA` or `PARQUET`. Every other entry (a view, another format, or any other `table_type`) is excluded and warned. Enumeration runs on the same kind-agnostic listing pipeline as the Iceberg REST kind and reads only Unity Catalog metadata, so it resolves no snapshot, no file list, and no Parquet footer.

## Background

* The configured namespace is the `catalog.schema` in the `NAMESPACE` virtual-schema property, dot-split into segments. The adapter calls the shared list operation on the Unity Catalog client, and the pipeline that consumes the result reads neutral table metadata without knowing how each client sourced its columns.
* Every listed table's column metadata and `properties` come from the single paginated `GET /tables` list sweep. During the full listing the client issues no per-table `GET /tables/{full_name}`, so enumerating a schema costs one paginated sweep.
* An Exasol table name is the namespace segments below the configured namespace plus the table name, joined with `__` and uppercased through the one case-fold site the Iceberg path also uses. The adapter records each virtual table's catalog identifier in that table's identifier note per `vs-adapter/create-virtual-schema`.
* Unity Catalog reports a column's Spark type in `type_json`. Real Unity clients leave `type_precision` and `type_scale` unset. The neutral column carries `type_json` verbatim, and the adapter's single type-mapping home parses it. A column whose type has no clean scalar Exasol equivalent, or whose `type_json` is absent or unparseable, is declared `VARCHAR(2000000)` instead of failing the enumeration.
* The Unity Catalog client makes the admission decision (`unity-catalog/unity-catalog-client`) and routes every excluded entry into `CatalogListing.skipped`. The shared `build_listing_virtual_tables` pipeline stays kind-agnostic. The shared skip-warn loop writes one `udf_log!(ctx, warn, ...)` line per skipped entry from the neutral skip reason.
* A Delta table and a Parquet table declare their columns identically from the Unity Catalog column types. Each column's note (`vs-adapter/column-source-notes`) records its `type_json`, and for a Delta table also the column-mapping mode read from the table's `properties`.
* An unusable column-mapping annotation refuses its column, never the table or the statement. A Delta column whose `type_json` lacks the annotation its table's mode requires or carries it malformed, and every column of a table whose mode property holds a value outside `none`, `id`, and `name`, is listed with a note whose refusal states the problem (`delta/delta-type-mapping`).
* This path does not read the Delta log's protocol, so a Delta table that the reader-feature gate refuses is listed like any other and fails at query time. The pushdown gate is the authority (`delta/delta-reader-feature-gating`).
* Shallow clones are admitted by the base-table rule with no special handling, because Unity Catalog reports them as `MANAGED` or `EXTERNAL` with `data_source_format` `DELTA`.
* The `DECIMAL` guard predicate and the timestamp declaration rule have one owner, `scan-types/type-mapping`, and `vs-adapter/create-virtual-schema` owns the single `ctx.database_version()` read. This feature consumes them and does not restate them.

## Scenarios

### Scenario: Create virtual schema enumerates every table in the configured Unity Catalog namespace

* *GIVEN* a Unity Catalog reachable through the CONNECTION named by `CATALOG_CONNECTION` and a createVirtualSchema request whose `CATALOG_KIND` is `UNITY_CATALOG` and whose `NAMESPACE` property names a `catalog.schema`
* *WHEN* Exasol sends the createVirtualSchema request
* *THEN* the adapter SHALL list every table in that schema by calling the shared `CatalogClient` list operation on the constructed Unity Catalog client, and SHALL return one virtual table per listed PLANNABLE BASE table: an entry whose `table_type` is `MANAGED` or `EXTERNAL` AND whose `data_source_format` is `DELTA` or `PARQUET`
* *AND* a `PARQUET` table SHALL declare every column Unity Catalog lists for it, its partition columns included, in the catalog's column order and mapped through the same type-mapping home as a Delta table's columns
* *AND* the adapter SHALL exclude from the returned virtual tables every other listed entry (a view, a base table whose `data_source_format` is neither `DELTA` nor `PARQUET`, or an entry of any other `table_type`), so no identifier note is recorded for an excluded entry and no unplannable entry becomes a queryable virtual table

### Scenario: Enumeration names tables through the shared fold and stops at catalog metadata

* *GIVEN* the same Unity Catalog namespace enumeration
* *WHEN* Exasol sends the createVirtualSchema request
* *THEN* the adapter SHALL name each returned virtual table by flattening the segments below the configured namespace plus the table name with `__` and uppercasing the result through the shared case-fold site
* *AND* the adapter SHALL source every listed entry's columns, table `properties`, `table_type`, and `data_source_format` from the single paginated `GET /tables` list sweep, issuing no per-table `GET /tables/{full_name}`, reading no Delta transaction log, reading no Parquet footer, and resolving no snapshot, because this path stops at catalog metadata
* *AND* the namespace property SHALL be the SAME `NAMESPACE` property the Iceberg REST kind reads, so neither kind carries a namespace property of its own

### Scenario: Create virtual schema includes managed and external Delta base tables, including a shallow clone

* *GIVEN* a Unity Catalog namespace whose `GET /tables` list sweep returns a `MANAGED` table with `data_source_format` `DELTA`, an `EXTERNAL` table with `data_source_format` `DELTA`, and a shallow clone that Unity Catalog reports as a `MANAGED` or `EXTERNAL` table with `data_source_format` `DELTA`
* *WHEN* the adapter enumerates that namespace during createVirtualSchema
* *THEN* the adapter SHALL return a virtual table for each of the three entries, because each satisfies the Delta-base rule
* *AND* the adapter SHALL apply NO shallow-clone-specific handling, because a shallow clone is admitted by the same `table_type` and `data_source_format` rule as any other base table
* *AND* each returned virtual table SHALL carry its columns mapped to Exasol types exactly as any Delta base table, and SHALL carry its catalog identifier in the `name` member of its identifier note

### Scenario: Both catalog kinds enumerate through one shared listing pipeline

* *GIVEN* one createVirtualSchema request resolving `CatalogKind::UnityCatalogNative` and one resolving `CatalogKind::IcebergRest`, each over a namespace holding the same table names with the same column names
* *WHEN* the adapter handles each request
* *THEN* the adapter SHALL run the SAME listing pipeline for both, one code path that reads neutral table metadata through the shared trait, flattens and case-folds names, maps each column through the single type-mapping home, records each table's identifier note, and assembles the response
* *AND* that pipeline MUST NOT name or match `CatalogKind` and MUST NOT branch on which client it holds, so the flattening rule, the collision rule, the case-fold, and the `adapterNotes` shape are provably identical across the two kinds rather than identical by coincidence
* *AND* each client SHALL remain free to source its columns its own way, the Unity Catalog client from its single inline sweep, the Iceberg REST client from its per-table load, because that choice is hidden inside the client and never reaches the pipeline

### Scenario: Create virtual schema lists the namespace with no per-table get-table call

* *GIVEN* a Unity Catalog namespace whose `GET /tables` list sweep returns every table's columns inline across one or more pages
* *WHEN* the adapter enumerates that namespace during createVirtualSchema
* *THEN* the adapter SHALL obtain every table's columns from the list sweep alone, issuing only the paginated `GET /tables` requests and zero `GET /tables/{full_name}` requests
* *AND* the get-table request count SHALL stay at zero regardless of the number of listed tables, because the list response already carries each table's columns inline

### Scenario: Create virtual schema excludes every entry that is not a plannable base table and warns per exclusion

* *GIVEN* a Unity Catalog namespace whose `GET /tables` list sweep returns three entries: a `VIEW` entry carrying columns, no `storage_location`, and a null `data_source_format`, a `MANAGED` entry whose `data_source_format` is neither `DELTA` nor `PARQUET` (such as `ICEBERG`, `CSV`, or `JSON`), and an entry whose `table_type` is neither `MANAGED`, `EXTERNAL`, nor `VIEW`
* *WHEN* the adapter enumerates that namespace during createVirtualSchema
* *THEN* the adapter SHALL exclude all three entries from the returned virtual tables, so no identifier note is recorded for them, and SHALL complete createVirtualSchema successfully with only the plannable base tables in the namespace
* *AND* the adapter SHALL write one `udf_log!(ctx, warn, ...)` line per excluded entry naming the excluded `catalog.schema.table` identifier and the disqualifying reason: the entry's `table_type` for a view or other-type entry, or its `data_source_format` for an unplannable base table
* *AND* none of those warning lines MUST contain the resolved bearer token, any OAuth client secret, or any other credential value
* *AND* a namespace whose every listed entry is excluded SHALL yield a createVirtualSchema response with an empty table list, rather than an error

### Scenario: Unity Catalog Spark column types map to Exasol types sufficient for listing

* *GIVEN* a Unity Catalog table whose columns declare in their `type_json` the Spark types `boolean`, `integer`, `long`, `double`, `string`, `date`, `timestamp`, and `decimal(p,s)` whose `p` and `s` fall inside Exasol's `DECIMAL` domain, `1 ≤ p ≤ 36` and `s ≤ p`
* *AND* each column's `type_precision` and `type_scale` are 0 or absent
* *WHEN* the adapter builds the virtual table's column list
* *THEN* the adapter SHALL declare `boolean` as `BOOLEAN`, `integer` as `DECIMAL(10,0)`, `long` as `DECIMAL(20,0)`, `double` as `DOUBLE PRECISION`, `string` as `VARCHAR(2000000)`, `date` as `DATE`, and `decimal(p,s)` as `DECIMAL(p,s)`, reusing the project's Arrow-to-Exasol convention
* *AND* the adapter SHALL declare `timestamp` and `timestamp_ntz` as `TIMESTAMP(6)` on an Exasol version of 2025.x or later and as the bare string `TIMESTAMP` on 8.x, reading BOTH the version rule and both declaration strings from the single owner `scan-types/type-mapping` specifies, so a Delta timestamp column and an Iceberg timestamp column are declared at the same precision by construction, and this feature carries no copy of either literal
* *AND* the adapter SHALL read each column's type from its `type_json` and MUST NOT read `type_name`, `type_precision`, or `type_scale`
* *AND* the adapter SHALL reach that mapping through the SAME exhaustive match over the neutral column's source-tagged type descriptor that maps an Iceberg column, so the two catalog kinds have ONE Exasol type-mapping home and a third source type is a build failure there
* *AND* the adapter SHALL declare each column name uppercased through the shared case-fold site, so an unquoted column reference in user SQL resolves against the declared name

### Scenario: An incompatible Unity Catalog column type is declared as VARCHAR rather than failing

* *GIVEN* a Unity Catalog table whose columns include an array, map, struct, binary, interval, or variant type, or a `DECIMAL` whose precision and scale fall outside Exasol's `DECIMAL` domain, a precision above 36, or a scale exceeding its own precision, or a column whose `type_json` is absent or unparseable
* *WHEN* the adapter builds the virtual table's column list
* *THEN* the adapter SHALL declare each such column as `VARCHAR(2000000)` rather than aborting the enumeration
* *AND* the adapter SHALL resolve the `DECIMAL` cases through the SINGLE shared guard `scan-types/type-mapping` owns, and MUST NOT carry a Unity-local copy of the precision/scale predicate, so a Unity `DECIMAL(0,0)` and an Iceberg `decimal(0,0)` are declared identically by construction rather than by coincidence
* *AND* the adapter MUST NOT declare an Exasol type Exasol rejects, in particular MUST NOT declare `DECIMAL(0,0)` or a `DECIMAL(p,s)` with `s > p`, because such a declaration fails `createVirtualSchema` outright, which is the failure this VARCHAR fallback exists to prevent
* *AND* the adapter SHALL treat this listing-sufficient mapping as a deliberate boundary, deferring reader-feature gating and full Delta type fidelity to the pushdown gate (`delta/delta-reader-feature-gating`), so the listing never produces an untyped or silently-dropped column

### Scenario: Create virtual schema fails clearly when the Unity Catalog is unreachable

* *GIVEN* a createVirtualSchema request under the Unity Catalog kind whose CONNECTION address cannot be reached or whose namespace cannot be listed
* *WHEN* Exasol sends the createVirtualSchema request
* *THEN* the adapter SHALL return an error describing that the Unity Catalog could not be reached or the namespace could not be listed
* *AND* the error message MUST NOT contain the resolved bearer token, any OAuth client secret, or any other credential value

### Scenario: Create virtual schema records each table's Unity Catalog identifier in its identifier note

* *GIVEN* a createVirtualSchema request under the Unity Catalog kind that enumerates one or more tables
* *WHEN* the adapter builds the createVirtualSchema response
* *THEN* each virtual table's identifier note SHALL name the table's original-cased `catalog.schema.table` identifier
* *AND* when two distinct Unity Catalog identifiers flatten to the same Exasol name, the adapter SHALL return an error naming that Exasol name rather than dropping or overwriting a table
