# Feature: Unity Catalog Native REST Client

A thin bespoke client in `crates/lakehouse-catalog` that talks to the native Unity Catalog REST API to enumerate tables and load a table's metadata. Its listing operation filters to Delta base tables: it returns one neutral table per entry whose `table_type` is `MANAGED` or `EXTERNAL` and whose `data_source_format` is `DELTA`, and routes every other listed entry into the skipped set with a neutral reason. It implements the shared `CatalogClient` trait, so the engine reaches it through the same operations it uses for the Iceberg REST catalog and never sees a Unity Catalog wire type. It reuses the crate's existing `reqwest` client and `serde` types and targets the standard API — never the Iceberg-REST compatibility endpoint and never the `delta/v1` Delta Tables API. One client serves both OSS Unity Catalog and Databricks-managed Unity Catalog, because the standard API is identical on both, so there is no Databricks-specific code path.

## Background

The client is built once per request as a `UnityCatalogSession` holding one pooled `reqwest::Client`, the resolved base URL, and the resolved authentication strategy; the strategy is applied to every request by the Unity Catalog Authentication feature and is not re-derived per call. All list endpoints paginate through `page_token`/`next_page_token`, and the client follows every page before returning a complete result. `GET /tables` returns a fully-populated `TableInfo` per table by default — including its inline `columns[]` array (each column with its name and its FULL parameterized Unity Catalog Spark type — the type name plus precision and scale from `type_precision`/`type_scale`, or `type_text` — in declared position order, so a `DECIMAL(p,s)` column carries its `p` and `s`, not the bare `DECIMAL`), `storage_location`, and `table_id` — so the single paginated list sweep is the column source for the createVirtualSchema listing path, verified live against `demo_sales_catalog.sales`. The client MUST NOT set the list request's `omit_columns` parameter, which suppresses the inline `columns[]` and would force a per-table `GET /tables/{full_name}` to recover them. `GET /tables/{full_name}` is retained as the scan-path single-table load, not the listing path's column source. A listed VIEW entry is the exception to the fully-populated shape: it carries its `columns[]` but omits `storage_location` and carries a null `data_source_format`, so the crate-private wire type models both as optional and the list method deserializes a VIEW entry without failing. The verified endpoints and their response fields are recorded in `SPIKE_UC_CLIENT.md`, exercised against a live Databricks-managed workspace (`GET /catalogs`, `GET /schemas`, `GET /tables`, `GET /tables/{full_name}`) and against the OSS fixture (#325). The client's session fields stay private, its auth strategy stays crate-private, and its wire types stay crate-private, so making the type reachable from the engine exposes no request internals and no Unity-specific shape.

* This delta (plan `change-unity-listing-delta-base-filter`, correcting issue #318) scopes the client's list operation to Delta base tables. The client deserializes `data_source_format` — which #318 deliberately dropped — and makes the Delta/base admission decision itself, before returning neutral tables, so the shared listing pipeline stays kind-agnostic.
* This delta SUPERSEDES the recorded clause of scenario "The client lists tables in a configured catalog and schema" that the client returns "one fully-populated neutral table per listed entry ... together with an EMPTY set of skipped identifiers, because every listed Unity Catalog entry is returned with its columns and none is dropped". The skipped set is no longer always empty: a view, a non-`DELTA`-format base table, or an entry of any other `table_type` is dropped into it.
* This delta SUPERSEDES the same scenario's clause that "when a listed entry is a VIEW the client SHALL return it carrying its columns but with NO storage location". A VIEW list entry still deserializes without failing, but is now routed into the skipped set rather than returned as a neutral table.
* The crate-private wire type `TableInfo` gains `data_source_format` as an optional field (serde default; a VIEW carries it as null), alongside the already-optional `storage_location`. `data_source_format` stays crate-private and MUST NOT appear in any neutral type the engine can name.
* The neutral `CatalogListing.skipped` element changes shape from a bare identifier to an identifier paired with a neutral skip reason. This shared skipped set is also used by the Iceberg REST client, which supplies its own "not a loadable Iceberg table" reason; the Iceberg skip semantics and its skipped-table warning stay byte-identical, per `vs-adapter/catalog-kind-selection`. The reason is a neutral value carried on the skipped entry, not a per-kind branch in the shared pipeline.

## Scenarios

### Scenario: The Unity Catalog session is reached only through the shared catalog-client trait

* *GIVEN* a `UnityCatalogSession` and the shared `CatalogClient` trait the Iceberg REST catalog client also implements
* *WHEN* the engine adapter enumerates a namespace or loads one table's metadata under the Unity Catalog kind
* *THEN* `UnityCatalogSession` SHALL implement `CatalogClient`, and the engine SHALL reach both operations THROUGH that trait, so the adapter runs the same pipeline it runs for the Iceberg REST kind
* *AND* both operations SHALL return the catalog-NEUTRAL metadata types the trait declares, converting each deserialized Unity Catalog wire entry into a neutral table whose identifier carries the `catalog`/`schema` namespace segments and the table name, whose storage location is absent when the entry omits it, and whose ordered columns each carry the column name in its ORIGINAL case plus a source-tagged Unity Catalog type descriptor
* *AND* the Unity Catalog wire types MUST NOT appear in any signature the engine can name, because the engine consuming a Unity-specific shape is exactly the per-kind branch the shared trait exists to remove
* *AND* the client MUST NOT map a column type to an Exasol type, because that mapping belongs to the engine's single type-mapping home and this crate MUST NOT name the Exasol delivery mechanism

### Scenario: The client lists tables in a configured catalog and schema

* *GIVEN* a `UnityCatalogSession` and a configured Unity Catalog namespace of the form `catalog.schema`
* *WHEN* the adapter enumerates the tables in that namespace through the shared trait
* *THEN* the client SHALL issue `GET /tables?catalog_name={catalog}&schema_name={schema}` on the session and SHALL return one fully-populated neutral table per listed entry that is a DELTA BASE TABLE — an entry whose `table_type` is `MANAGED` or `EXTERNAL` AND whose `data_source_format` is `DELTA` — carrying its identifier, table type, storage location, and its ordered columns (each column carrying its name and its FULL parameterized Unity Catalog Spark type — type name plus precision and scale, sufficient to declare `DECIMAL(p,s)`) all from the single list response
* *AND* the client SHALL place every other listed entry — a `VIEW`, a base table whose `data_source_format` is not `DELTA`, or an entry of any other `table_type` — into the returned skipped set rather than returning it as a neutral table, so the skipped set is NOT always empty and a non-Delta or non-base entry never reaches the listing pipeline as a table
* *AND* the crate-private wire type SHALL model `data_source_format` as optional (a `VIEW` carries it as null), and `data_source_format` SHALL remain a crate-private wire field that MUST NOT appear in any neutral type the engine can name, because the Delta-base decision is owned inside the client
* *AND* the client MUST NOT set the `omit_columns` request parameter, SHALL follow `page_token`/`next_page_token` pagination so every page's entries are classified and no listed table is truncated, and SHALL address every request under the `{host}/api/2.1/unity-catalog` base path — never the Iceberg-REST compatibility endpoint or the `delta/v1` Delta Tables API

### Scenario: The client returns managed and external Delta base tables including a shallow clone

* *GIVEN* a `UnityCatalogSession` and a `GET /tables` response listing a `MANAGED` entry with `data_source_format` `DELTA`, an `EXTERNAL` entry with `data_source_format` `DELTA`, and a shallow clone Unity Catalog reports as a `MANAGED` or `EXTERNAL` entry with `data_source_format` `DELTA`
* *WHEN* the adapter enumerates the namespace through the shared trait
* *THEN* the client SHALL return one neutral table for each of the three entries, each carrying `CatalogTableType::Table`, its storage location, and its ordered columns
* *AND* the client SHALL apply NO shallow-clone-specific handling, because a shallow clone satisfies the same `table_type` and `data_source_format` rule as any other base table
* *AND* the returned skipped set SHALL be empty, because every listed entry is a Delta base table

### Scenario: The client routes a view, a non-Delta-format table, and any other table type into the skipped set with a reason

* *GIVEN* a `UnityCatalogSession` and a `GET /tables` response listing a `VIEW` entry with a null `data_source_format`, a `MANAGED` entry whose `data_source_format` is a non-`DELTA` value such as `ICEBERG` or `CSV`, and an entry whose `table_type` is none of `MANAGED`, `EXTERNAL`, or `VIEW`
* *WHEN* the adapter enumerates the namespace through the shared trait
* *THEN* the client SHALL return NO neutral table for any of the three entries and SHALL place each into the returned skipped set
* *AND* each skipped entry SHALL pair the neutral `catalog.schema.table` identifier with a neutral reason naming the disqualifying cause — the entry's `table_type` for the view and the other-type entry, or its `data_source_format` for the non-`DELTA` base table
* *AND* the client SHALL compare `table_type` and `data_source_format` against the uppercase Unity Catalog vocabulary (`MANAGED`, `EXTERNAL`, `VIEW`, `DELTA`), matching the case Unity Catalog emits on the wire
* *AND* the reason MUST NOT contain the resolved bearer token, any OAuth client secret, or any other credential value

### Scenario: The client retrieves a table's metadata including its columns

* *GIVEN* a `UnityCatalogSession` and a fully-qualified table identifier naming a catalog, a schema, and a table
* *WHEN* a caller loads that single table's metadata through the shared trait
* *THEN* the client SHALL issue `GET /tables/{catalog.schema.table}` on the session and return one neutral table carrying its table type, storage location, and an ordered list of columns, each column carrying its name and its full parameterized Unity Catalog Spark type (type name plus precision and scale, sufficient to declare `DECIMAL(p,s)`)
* *AND* the returned column order SHALL preserve the order the Unity Catalog response declares, so downstream column mapping keeps the table's declared schema order
* *AND* the createVirtualSchema listing path MUST NOT call this single-table load for column metadata — it reads columns inline from the `GET /tables` list sweep — so this load SHALL serve only the scan-path single-table metadata source used by #319/#320, and in this plan it is exercised by the shared trait-contract tests rather than by a production caller

### Scenario: The client follows pagination across every result page

* *GIVEN* a `UnityCatalogSession` and a list endpoint whose first response carries a non-empty `next_page_token`
* *WHEN* the adapter enumerates that endpoint
* *THEN* the client SHALL re-issue the request with `page_token` set to the returned `next_page_token` and SHALL continue until a response carries no `next_page_token`
* *AND* the client SHALL return the concatenation of every page's entries in page order
* *AND* the client MUST NOT return only the first page, because a truncated enumeration would silently hide tables from the virtual schema

### Scenario: The client surfaces a transport or HTTP-status failure as a clear, credential-safe error

* *GIVEN* a `UnityCatalogSession` whose next request fails with a transport error, a non-success HTTP status, or an unparseable response body
* *WHEN* the client issues a list or get-table request
* *THEN* the client SHALL return an error describing that the Unity Catalog request failed and naming the request kind
* *AND* the error message MUST NOT contain the resolved bearer token, any OAuth client secret, or any other credential value
* *AND* the client SHALL return the failure as an error value rather than panicking, because a panic inside a UDF is an abnormal VM exit

### Scenario: One session serves both OSS and Databricks-managed Unity Catalog

* *GIVEN* two `UnityCatalogSession` instances, one whose base URL is a Databricks workspace host and one whose base URL is the local OSS fixture host
* *WHEN* the adapter lists tables and retrieves a table's metadata through each
* *THEN* the client SHALL issue identical request shapes against both, differing only in base URL and in the resolved authentication strategy
* *AND* the client MUST NOT branch its request construction on whether the host is Databricks-managed, because the standard Unity Catalog API is identical on both
