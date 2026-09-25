# Feature: Unity Catalog Native REST Client

A thin bespoke client in `crates/lakehouse-catalog` that talks to the native Unity Catalog REST API to enumerate tables and load a table's metadata. Its listing operation filters to plannable base tables: it returns one neutral table per entry whose `table_type` is `MANAGED` or `EXTERNAL` and whose `data_source_format` is `DELTA` or `PARQUET`, and routes every other listed entry into the skipped set with a neutral reason. It implements the shared `CatalogClient` trait, so the engine reaches it through the same operations it uses for the Iceberg REST catalog and never sees a Unity Catalog wire type. It reuses the crate's existing `reqwest` client and `serde` types and targets the standard API, never the Iceberg-REST compatibility endpoint and never the `delta/v1` Delta Tables API. One client serves both OSS Unity Catalog and Databricks-managed Unity Catalog, because the standard API is identical on both, so there is no Databricks-specific code path.

The Parquet-admission additions — the wire-contract, admission, and single-table-load scenarios for a `PARQUET` base table — are recorded in `vs-adapter/unity-catalog-client-parquet-admission`, split out once this feature's own scenario count crossed this library's per-spec organization threshold.

## Background

The client is built once per request as a `UnityCatalogSession` holding one pooled `reqwest::Client`, the resolved base URL, and the resolved authentication strategy. The Unity Catalog Authentication feature applies that strategy to every request, and the client does not re-derive it per call. All list endpoints paginate through `page_token`/`next_page_token`, and the client follows every page before returning a complete result. `GET /tables` returns a fully-populated `TableInfo` per table by default, including its `storage_location`, its `table_id`, and its inline `columns[]` array in declared position order. Each column carries its name, its FULL parameterized Unity Catalog Spark type (the type name plus precision and scale from `type_precision`/`type_scale`, so a `DECIMAL(p,s)` column carries its `p` and `s`), its Spark `StructField` JSON as `type_json`, and its `partition_index`. The single paginated list sweep is therefore the column source for the createVirtualSchema listing path, verified live against `demo_sales_catalog.sales`. The client MUST NOT set the list request's `omit_columns` parameter, which suppresses the inline `columns[]` and would force a per-table `GET /tables/{full_name}` to recover them. `GET /tables/{full_name}` is the scan-path single-table load, not the listing path's column source. A listed VIEW entry carries its `columns[]` but omits `storage_location` and carries a null `data_source_format`, so the crate-private wire type models both as optional. The verified endpoints and their response fields are recorded in `SPIKE_UC_CLIENT.md`, exercised against a live Databricks-managed workspace and against the OSS fixture (#325). The client's session fields, its auth strategy, and its wire types stay crate-private, so the engine reaches no request internals and no Unity-specific shape.

* The client deserializes `data_source_format` and makes the listing-admission decision itself. The raw string stays crate-private. A closed neutral format tag crosses the boundary, because the engine's format dispatch reads it.
* A skipped entry pairs its identifier with a neutral skip reason. The Iceberg REST client supplies its own "not a loadable Iceberg table" reason through the same skipped set, and its skip semantics and warning stay byte-identical.
* The credential-vending key is the Unity Catalog `table_id`. It crosses the boundary as an OPAQUE value that no caller outside this crate interprets.
* A column's `partition_index` stays crate-private. It crosses the boundary only as the neutral table's ordered partition-column names.
* No error, log line, or skip reason carries a credential value. The vending key is a table identity, not a secret, and it is still never logged, because it scopes a credential request.

## Scenarios

### Scenario: The Unity Catalog session is reached only through the shared catalog-client trait

* *GIVEN* a `UnityCatalogSession` and the shared `CatalogClient` trait the Iceberg REST catalog client also implements
* *WHEN* the engine adapter enumerates a namespace or loads one table's metadata under the Unity Catalog kind
* *THEN* `UnityCatalogSession` SHALL implement `CatalogClient`, and the engine SHALL reach both operations THROUGH that trait, so the adapter runs the same pipeline it runs for the Iceberg REST kind
* *AND* both operations SHALL return the catalog-NEUTRAL metadata types the trait declares, converting each deserialized Unity Catalog wire entry into a neutral table whose identifier carries the `catalog`/`schema` namespace segments and the table name, whose storage location is absent when the entry omits it, whose table FORMAT is a closed neutral tag, whose credential-vending key is the catalog-assigned key or absent, whose partition columns are the names of the columns carrying a `partition_index`, ordered by it, and whose ordered columns each carry the column name in its ORIGINAL case plus a source-tagged Unity Catalog type descriptor holding the type name, precision, scale, and the column's `type_json` or its absence
* *AND* the Unity Catalog wire types MUST NOT appear in any signature the engine can name, because the engine consuming a Unity-specific shape is exactly the per-kind branch the shared trait exists to remove

### Scenario: The catalog-client trait keeps Unity Catalog's type mapping, format tag, and vending key opaque to the engine

* *GIVEN* the same `UnityCatalogSession` and shared `CatalogClient` trait
* *WHEN* the engine adapter enumerates a namespace or loads one table's metadata under the Unity Catalog kind
* *THEN* the client MUST NOT map a column type to an Exasol type, because that mapping belongs to the engine's single type-mapping home and this crate MUST NOT name the Exasol delivery mechanism
* *AND* the neutral format tag SHALL be a closed enum naming exactly the formats the engine can plan, so a format outside it is a deserialization-time refusal rather than a value the engine matches non-exhaustively
* *AND* the neutral credential-vending key SHALL be documented as OPAQUE, a value a caller hands back to the client that produced it and never parses, so no consumer outside this crate can derive a Unity Catalog request from it

### Scenario: The client lists tables in a configured catalog and schema

* *GIVEN* a `UnityCatalogSession` and a configured Unity Catalog namespace of the form `catalog.schema`
* *WHEN* the adapter enumerates the tables in that namespace through the shared trait
* *THEN* the client SHALL issue `GET /tables?catalog_name={catalog}&schema_name={schema}` on the session and SHALL return one fully-populated neutral table per listed entry that is a PLANNABLE BASE TABLE (an entry whose `table_type` is `MANAGED` or `EXTERNAL` AND whose `data_source_format` is `DELTA` or `PARQUET`), carrying its identifier, table type, storage location, neutral format tag, credential-vending key when the entry carries one, partition columns, and ordered columns, all from the single list response
* *AND* each neutral table this operation returns SHALL carry the format tag its own `data_source_format` names, `DELTA` mapping to the Delta tag and `PARQUET` to the Parquet tag, so the tag restates the admission outcome rather than re-deciding it
* *AND* the client SHALL place every other listed entry (a `VIEW`, a base table whose `data_source_format` is neither `DELTA` nor `PARQUET`, or an entry of any other `table_type`) into the returned skipped set rather than returning it as a neutral table, so no unplannable entry reaches the listing pipeline as a table

### Scenario: The client returns managed and external Delta base tables including a shallow clone

* *GIVEN* a `UnityCatalogSession` and a `GET /tables` response listing a `MANAGED` entry with `data_source_format` `DELTA`, an `EXTERNAL` entry with `data_source_format` `DELTA`, and a shallow clone Unity Catalog reports as a `MANAGED` or `EXTERNAL` entry with `data_source_format` `DELTA`
* *WHEN* the adapter enumerates the namespace through the shared trait
* *THEN* the client SHALL return one neutral table for each of the three entries, each carrying `CatalogTableType::Table`, its storage location, and its ordered columns
* *AND* the client SHALL apply NO shallow-clone-specific handling, because a shallow clone satisfies the same `table_type` and `data_source_format` rule as any other base table
* *AND* the returned skipped set SHALL be empty, because every listed entry is a Delta base table

### Scenario: The client retrieves a table's metadata including its columns

* *GIVEN* a `UnityCatalogSession` and a fully-qualified table identifier naming a catalog, a schema, and a table
* *WHEN* a caller loads that single table's metadata through the shared trait
* *THEN* the client SHALL issue `GET /tables/{catalog.schema.table}` on the session and return one neutral table carrying its table type, storage location, neutral format tag, credential-vending key, partition columns, and ordered columns, each column carrying its name, its full parameterized Unity Catalog Spark type, and its `type_json`
* *AND* the returned column order SHALL preserve the order the Unity Catalog response declares, so downstream column mapping keeps the table's declared schema order

### Scenario: The single-table load refuses a data source format the crate cannot name

* *GIVEN* a `UnityCatalogSession` and a fully-qualified identifier naming a table whose
  `data_source_format` is absent, or is a value that names neither Delta, Parquet, nor Iceberg, such
  as `CSV`, `JSON`, or `DELTASHARING`
* *WHEN* a caller loads that single table's metadata through the shared trait
* *THEN* the client SHALL return a `UdfError` naming the table and the unrecognized or absent
  `data_source_format` value, and MUST NOT return a neutral table
* *AND* the client MUST NOT default the format tag to Delta or Parquet, because the load applies no
  admission filter and a defaulted tag would route a CSV table into a reader that cannot read it,
  surfacing a missing-log or footer error rather than a clear format refusal
* *AND* the client SHALL map `DELTA` to the Delta tag, `PARQUET` to the Parquet tag, and `ICEBERG`
  to the Iceberg tag, comparing against the uppercase Unity Catalog vocabulary the wire emits, so a
  Unity Catalog UniForm table reporting `ICEBERG` is named accurately rather than refused as
  unrecognized

### Scenario: The single-table load's refusal is a returned error value that never carries a credential

* *GIVEN* the same unrecognized-format identifier
* *WHEN* the client refuses to load it
* *THEN* the error SHALL be returned as a `UdfError` value rather than raised as a panic, because a
  panic inside a UDF is an abnormal VM exit that makes the engine SIGKILL every sibling VM of the
  statement part
* *AND* the error message MUST NOT contain the resolved bearer token, any OAuth client secret, or any
  other credential value

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
