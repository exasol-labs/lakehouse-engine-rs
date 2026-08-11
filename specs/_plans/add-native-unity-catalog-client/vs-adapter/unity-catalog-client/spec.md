# Feature: Unity Catalog Native REST Client

A thin bespoke client in `crates/lakehouse-catalog` that talks to the native Unity Catalog REST API under `{host}/api/2.1/unity-catalog` to enumerate catalogs, schemas, and tables and to retrieve a table's metadata (identifier, table type, data-source format, storage location, and columns). It implements the shared `CatalogClient` trait, so the engine reaches it through the same operations it uses for the Iceberg REST catalog and never sees a Unity Catalog wire type. It reuses the crate's existing `reqwest` client and `serde` types and targets the standard API — never the Iceberg-REST compatibility endpoint and never the `delta/v1` Delta Tables API. One client serves both OSS Unity Catalog and Databricks-managed Unity Catalog, because the standard API is identical on both, so there is no Databricks-specific code path.

## Background

The client is built once per request as a `UnityCatalogSession` holding one pooled `reqwest::Client`, the resolved base URL, and the resolved authentication strategy; the strategy is applied to every request by the Unity Catalog Authentication feature and is not re-derived per call. All list endpoints paginate through `page_token`/`next_page_token`, and the client follows every page before returning a complete result. `GET /tables` returns a fully-populated `TableInfo` per table by default — including its inline `columns[]` array (each column with its name and its FULL parameterized Unity Catalog Spark type — the type name plus precision and scale from `type_precision`/`type_scale`, or `type_text` — in declared position order, so a `DECIMAL(p,s)` column carries its `p` and `s`, not the bare `DECIMAL`), `storage_location`, and `table_id` — so the single paginated list sweep is the column source for the createVirtualSchema listing path, verified live against `demo_sales_catalog.sales`. The client MUST NOT set the list request's `omit_columns` parameter, which suppresses the inline `columns[]` and would force a per-table `GET /tables/{full_name}` to recover them. `GET /tables/{full_name}` is retained as the scan-path single-table load, not the listing path's column source. A listed VIEW entry is the exception to the fully-populated shape: it carries its `columns[]` but omits `storage_location` and carries a null `data_source_format`, so the crate-private wire type models both as optional and the list method deserializes a VIEW entry without failing. The verified endpoints and their response fields are recorded in `SPIKE_UC_CLIENT.md`, exercised against a live Databricks-managed workspace (`GET /catalogs`, `GET /schemas`, `GET /tables`, `GET /tables/{full_name}`) and against the OSS fixture (#325). The client's session fields stay private, its auth strategy stays crate-private, and its wire types stay crate-private, so making the type reachable from the engine exposes no request internals and no Unity-specific shape.

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
* *THEN* the client SHALL issue `GET /tables?catalog_name={catalog}&schema_name={schema}` on the session and return one fully-populated neutral table per listed entry — carrying its identifier, table type, storage location, and its ordered columns (each column carrying its name and its FULL parameterized Unity Catalog Spark type — type name plus precision and scale, sufficient to declare `DECIMAL(p,s)` — in declared position order), all from the single list response — together with an EMPTY set of skipped identifiers, because every listed Unity Catalog entry is returned with its columns and none is dropped as unloadable
* *AND* when a listed entry is a VIEW the client SHALL return it carrying its columns but with NO storage location, so the crate-private wire type models both `storage_location` and `data_source_format` as optional and the list method deserializes a VIEW list entry without failing
* *AND* the client MUST NOT set the `omit_columns` request parameter (which would suppress the inline `columns[]` and force a per-table `GET /tables/{full_name}` to recover them), and SHALL follow `page_token`/`next_page_token` pagination so every page's entries are returned fully-populated and no listed table is truncated
* *AND* the client SHALL address every request under the `{host}/api/2.1/unity-catalog` base path derived from the CONNECTION address, and MUST NOT contact the Iceberg-REST compatibility endpoint or the `delta/v1` Delta Tables API

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
