# Feature: Unity Catalog Create Virtual Schema

Enumerates a Unity Catalog namespace during createVirtualSchema and returns one virtual table per Delta base table — a listed entry whose Unity Catalog `table_type` is `MANAGED` or `EXTERNAL` and whose `data_source_format` is `DELTA`. Every other listed entry — a view, a non-`DELTA` format, or any other `table_type` — is excluded from the returned virtual tables and warned. Enumeration runs on the SAME kind-agnostic listing pipeline the Iceberg REST kind uses; the Delta-base decision lives inside the Unity Catalog client, so `data_source_format` never crosses the shared trait boundary.

## Background

* This delta scopes the Unity Catalog createVirtualSchema listing to Delta base tables only (plan `change-unity-listing-delta-base-filter`, correcting issue #318). It restores the original #318 planning intent — report only Delta-format base tables — which was lost during planning and never recorded, so the shipped code reports every listed entry with no filter and does not deserialize `data_source_format`.
* This delta SUPERSEDES the recorded feature-description and Background clause that the adapter "return one virtual table per listed table", and the recorded Background clause that "A listed entry may be a VIEW, which carries a column list but no storage location; the listing path lists it with its columns". The adapter now returns one virtual table per Delta base table; a view, a non-`DELTA`-format table, or an entry of any other `table_type` is excluded and warned.
* This delta SUPERSEDES the recorded scenario "Create virtual schema lists a Unity Catalog view with its columns and no storage location", which asserted a VIEW is listed with its columns. Its inverse — a VIEW is excluded and warned — is covered by the new scenario "Create virtual schema excludes every non-Delta-base entry and warns per exclusion".
* The Delta-base decision is made INSIDE the Unity Catalog client (see `vs-adapter/unity-catalog-client`), which deserializes `data_source_format` and routes every excluded entry into `CatalogListing.skipped`. The shared `build_listing_virtual_tables` pipeline stays kind-agnostic and UNTOUCHED, and `data_source_format` stays a Unity-wire-private field that never appears in a neutral type.
* Excluded entries are warned on the SAME shared skip-warn loop the Iceberg REST kind uses. The loop writes one `udf_log!(ctx, warn, ...)` line per skipped entry, rendered from the neutral skip reason carried on the entry rather than from a per-kind branch. The Iceberg path's warning stays byte-identical, per `vs-adapter/catalog-kind-selection`; the Unity path's warning names the excluded identifier and its disqualifying `table_type` or `data_source_format`.
* The Iceberg REST listing path is unaffected: it still skips only tables the catalog reports as not loadable (HTTP 404), and its skipped-table warning stays byte-identical.
* Shallow clones are INCLUDED by the base-table rule with no shallow-clone-specific handling, because Unity Catalog surfaces a shallow clone as a `MANAGED` or `EXTERNAL` table with `data_source_format` `DELTA`. That the wire shape of a real shallow clone matches the base-table rule is an assumption not yet verified live; it is recorded as a tracked assumption rather than a silent claim (see decision log).
* No other scenario changes. The Spark-type-to-Exasol mapping, the incompatible-type-to-VARCHAR boundary, the no-per-table-get-table guarantee, the both-kinds-one-pipeline guarantee, `TABLE_MAP`/`adapterNotes` recording, and the unreachable-catalog error all stay as recorded.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Create virtual schema enumerates every table in the configured Unity Catalog namespace

* *GIVEN* a Unity Catalog reachable through the CONNECTION named by `CATALOG_CONNECTION` and a createVirtualSchema request whose `CATALOG_KIND` is `UNITY_CATALOG` and whose `ICEBERG_NAMESPACE` property names a `catalog.schema`
* *WHEN* Exasol sends the createVirtualSchema request
* *THEN* the adapter SHALL list every table in that schema by calling the shared `CatalogClient` list operation on the constructed Unity Catalog client, and SHALL return one virtual table per listed DELTA BASE table — an entry whose `table_type` is `MANAGED` or `EXTERNAL` AND whose `data_source_format` is `DELTA`
* *AND* the adapter SHALL exclude from the returned virtual tables every other listed entry — a view, a base table whose `data_source_format` is not `DELTA`, or an entry of any other `table_type` — and MUST NOT record an excluded entry in `TABLE_MAP`, so no non-Delta or non-base entry becomes a queryable virtual table
* *AND* the adapter SHALL name each returned virtual table by flattening the segments below the configured namespace plus the table name with `__` and uppercasing the result through the shared case-fold site
* *AND* the adapter SHALL source every listed entry's columns, `table_type`, and `data_source_format` from the single paginated `GET /tables` list sweep — issuing no per-table `GET /tables/{full_name}`, reading no Delta transaction log, and resolving no snapshot — because this path stops at catalog metadata
<!-- /DELTA:CHANGED -->

<!-- DELTA:NEW -->
### Scenario: Create virtual schema includes managed and external Delta base tables, including a shallow clone

* *GIVEN* a Unity Catalog namespace whose `GET /tables` list sweep returns a `MANAGED` table with `data_source_format` `DELTA`, an `EXTERNAL` table with `data_source_format` `DELTA`, and a shallow clone that Unity Catalog reports as a `MANAGED` or `EXTERNAL` table with `data_source_format` `DELTA`
* *WHEN* the adapter enumerates that namespace during createVirtualSchema
* *THEN* the adapter SHALL return a virtual table for each of the three entries, because each satisfies the Delta-base rule
* *AND* the adapter SHALL apply NO shallow-clone-specific handling, because a shallow clone is admitted by the same `table_type` and `data_source_format` rule as any other base table
* *AND* each returned virtual table SHALL carry its columns mapped to Exasol types exactly as any Delta base table, and SHALL appear in `TABLE_MAP`
<!-- /DELTA:NEW -->

<!-- DELTA:REMOVED -->
### Scenario: Create virtual schema lists a Unity Catalog view with its columns and no storage location

* *GIVEN* a Unity Catalog namespace whose `GET /tables` list sweep returns a VIEW entry carrying a `columns[]` array but no `storage_location` and a null `data_source_format`
* *WHEN* the adapter enumerates that namespace during createVirtualSchema
* *THEN* the adapter SHALL return a virtual table for the view with its columns mapped to Exasol types, exactly as for a table entry
* *AND* the adapter MUST NOT fail the enumeration or drop the view because its neutral metadata carries no storage location, because the absent location matters only to the deferred scan/vending path (#319/#320)
<!-- /DELTA:REMOVED -->

<!-- DELTA:NEW -->
### Scenario: Create virtual schema excludes every non-Delta-base entry and warns per exclusion

* *GIVEN* a Unity Catalog namespace whose `GET /tables` list sweep returns a `VIEW` entry carrying columns, no `storage_location`, and a null `data_source_format`; a `MANAGED` entry whose `data_source_format` is a non-`DELTA` value such as `ICEBERG`, `CSV`, `PARQUET`, or `JSON`; and an entry whose `table_type` is neither `MANAGED`, `EXTERNAL`, nor `VIEW`
* *WHEN* the adapter enumerates that namespace during createVirtualSchema
* *THEN* the adapter SHALL exclude all three entries from the returned virtual tables and from `TABLE_MAP`, and SHALL complete createVirtualSchema successfully with only the Delta base tables in the namespace
* *AND* the adapter SHALL write one `udf_log!(ctx, warn, ...)` line per excluded entry naming the excluded `catalog.schema.table` identifier and the disqualifying reason — the entry's `table_type` for a view or other-type entry, or its `data_source_format` for a non-`DELTA` base table
* *AND* none of those warning lines MUST contain the resolved bearer token, any OAuth client secret, or any other credential value
* *AND* a namespace whose every listed entry is excluded SHALL yield a createVirtualSchema response with an empty table list and an empty `TABLE_MAP`, rather than an error
<!-- /DELTA:NEW -->
