# Feature: Unity Catalog Native REST Client

A thin bespoke client in `crates/lakehouse-catalog` that talks to the native Unity Catalog REST API to enumerate tables and load a table's metadata. Its listing operation now filters to Delta base tables: it returns one neutral table per entry whose `table_type` is `MANAGED` or `EXTERNAL` and whose `data_source_format` is `DELTA`, and routes every other listed entry into the skipped set with a neutral reason. It implements the shared `CatalogClient` trait, so the engine reaches it through the same operations it uses for the Iceberg REST catalog and never sees a Unity Catalog wire type.

## Background

* This delta scopes the client's list operation to Delta base tables (plan `change-unity-listing-delta-base-filter`, correcting issue #318). The client deserializes `data_source_format` — which #318 deliberately dropped — and makes the Delta/base admission decision itself, before returning neutral tables, so the shared listing pipeline stays kind-agnostic.
* This delta SUPERSEDES the recorded clause of scenario "The client lists tables in a configured catalog and schema" that the client returns "one fully-populated neutral table per listed entry ... together with an EMPTY set of skipped identifiers, because every listed Unity Catalog entry is returned with its columns and none is dropped". The skipped set is no longer always empty: a view, a non-`DELTA`-format base table, or an entry of any other `table_type` is dropped into it.
* This delta SUPERSEDES the same scenario's clause that "when a listed entry is a VIEW the client SHALL return it carrying its columns but with NO storage location". A VIEW list entry still deserializes without failing, but is now routed into the skipped set rather than returned as a neutral table.
* The crate-private wire type `TableInfo` gains `data_source_format` as an optional field (serde default; a VIEW carries it as null), alongside the already-optional `storage_location`. `data_source_format` stays crate-private and MUST NOT appear in any neutral type the engine can name.
* The neutral `CatalogListing.skipped` element changes shape from a bare identifier to an identifier paired with a neutral skip reason. This shared skipped set is also used by the Iceberg REST client, which supplies its own "not a loadable Iceberg table" reason; the Iceberg skip semantics and its skipped-table warning stay byte-identical, per `vs-adapter/catalog-kind-selection`. The reason is a neutral value carried on the skipped entry, not a per-kind branch in the shared pipeline.
* No other scenario changes. The `omit_columns` prohibition, `page_token`/`next_page_token` pagination, the credential-safe transport/HTTP-failure behavior, the single-table `GET /tables/{full_name}` scan-path load, the OSS/Databricks identical request shape, and the shared-trait neutrality guarantee all stay as recorded.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: The client lists tables in a configured catalog and schema

* *GIVEN* a `UnityCatalogSession` and a configured Unity Catalog namespace of the form `catalog.schema`
* *WHEN* the adapter enumerates the tables in that namespace through the shared trait
* *THEN* the client SHALL issue `GET /tables?catalog_name={catalog}&schema_name={schema}` on the session and SHALL return one fully-populated neutral table per listed entry that is a DELTA BASE TABLE — an entry whose `table_type` is `MANAGED` or `EXTERNAL` AND whose `data_source_format` is `DELTA` — carrying its identifier, table type, storage location, and its ordered columns (each column carrying its name and its FULL parameterized Unity Catalog Spark type — type name plus precision and scale, sufficient to declare `DECIMAL(p,s)`) all from the single list response
* *AND* the client SHALL place every other listed entry — a `VIEW`, a base table whose `data_source_format` is not `DELTA`, or an entry of any other `table_type` — into the returned skipped set rather than returning it as a neutral table, so the skipped set is NOT always empty and a non-Delta or non-base entry never reaches the listing pipeline as a table
* *AND* the crate-private wire type SHALL model `data_source_format` as optional (a `VIEW` carries it as null), and `data_source_format` SHALL remain a crate-private wire field that MUST NOT appear in any neutral type the engine can name, because the Delta-base decision is owned inside the client
* *AND* the client MUST NOT set the `omit_columns` request parameter, SHALL follow `page_token`/`next_page_token` pagination so every page's entries are classified and no listed table is truncated, and SHALL address every request under the `{host}/api/2.1/unity-catalog` base path — never the Iceberg-REST compatibility endpoint or the `delta/v1` Delta Tables API
<!-- /DELTA:CHANGED -->

<!-- DELTA:NEW -->
### Scenario: The client returns managed and external Delta base tables including a shallow clone

* *GIVEN* a `UnityCatalogSession` and a `GET /tables` response listing a `MANAGED` entry with `data_source_format` `DELTA`, an `EXTERNAL` entry with `data_source_format` `DELTA`, and a shallow clone Unity Catalog reports as a `MANAGED` or `EXTERNAL` entry with `data_source_format` `DELTA`
* *WHEN* the adapter enumerates the namespace through the shared trait
* *THEN* the client SHALL return one neutral table for each of the three entries, each carrying `CatalogTableType::Table`, its storage location, and its ordered columns
* *AND* the client SHALL apply NO shallow-clone-specific handling, because a shallow clone satisfies the same `table_type` and `data_source_format` rule as any other base table
* *AND* the returned skipped set SHALL be empty, because every listed entry is a Delta base table
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: The client routes a view, a non-Delta-format table, and any other table type into the skipped set with a reason

* *GIVEN* a `UnityCatalogSession` and a `GET /tables` response listing a `VIEW` entry with a null `data_source_format`, a `MANAGED` entry whose `data_source_format` is a non-`DELTA` value such as `ICEBERG` or `CSV`, and an entry whose `table_type` is none of `MANAGED`, `EXTERNAL`, or `VIEW`
* *WHEN* the adapter enumerates the namespace through the shared trait
* *THEN* the client SHALL return NO neutral table for any of the three entries and SHALL place each into the returned skipped set
* *AND* each skipped entry SHALL pair the neutral `catalog.schema.table` identifier with a neutral reason naming the disqualifying cause — the entry's `table_type` for the view and the other-type entry, or its `data_source_format` for the non-`DELTA` base table
* *AND* the client SHALL compare `table_type` and `data_source_format` against the uppercase Unity Catalog vocabulary (`MANAGED`, `EXTERNAL`, `VIEW`, `DELTA`), matching the case Unity Catalog emits on the wire
* *AND* the reason MUST NOT contain the resolved bearer token, any OAuth client secret, or any other credential value
<!-- /DELTA:NEW -->
