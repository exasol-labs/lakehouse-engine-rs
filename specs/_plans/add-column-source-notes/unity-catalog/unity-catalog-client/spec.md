# Feature: Unity Catalog Native REST Client

A thin bespoke client in `crates/lakehouse-catalog` that talks to the native Unity Catalog REST API to enumerate tables and load a table's metadata. Its listing operation filters to plannable base tables: it returns one neutral table per entry whose `table_type` is `MANAGED` or `EXTERNAL` and whose `data_source_format` is `DELTA` or `PARQUET`, and routes every other listed entry into the skipped set with a neutral reason. It implements the shared `CatalogClient` trait, so the engine reaches it through the same operations it uses for the Iceberg REST catalog and never sees a Unity Catalog wire type. It reuses the crate's existing `reqwest` client and `serde` types and targets the standard API, never the Iceberg-REST compatibility endpoint and never the `delta/v1` Delta Tables API. One client serves both OSS Unity Catalog and Databricks-managed Unity Catalog, because the standard API is identical on both, so there is no Databricks-specific code path.

The Parquet-admission additions — the wire-contract, admission, and single-table-load scenarios for a `PARQUET` base table — are recorded in `unity-catalog/unity-catalog-client-parquet-admission`, split out once this feature's own scenario count crossed this library's per-spec organization threshold.

<!-- DELTA:CHANGED -->
## Background

The client is built once per request as a `UnityCatalogSession` holding one pooled `reqwest::Client`, the resolved base URL, and the resolved authentication strategy. The Unity Catalog Authentication feature applies that strategy to every request, and the client does not re-derive it per call. All list endpoints paginate through `page_token`/`next_page_token`, and the client follows every page before returning a complete result. `GET /tables` returns a fully-populated `TableInfo` per table by default, including its `storage_location`, its `table_id`, and its inline `columns[]` array in declared position order. Each column carries its name, its Spark `StructField` JSON as `type_json`, and its `partition_index`. The client does not read `type_name`, `type_precision`, or `type_scale`, because real Unity clients leave a decimal's precision and scale out of those fields (#463). The single paginated list sweep is therefore the column source for the createVirtualSchema listing path, verified live against `demo_sales_catalog.sales`. The client MUST NOT set the list request's `omit_columns` parameter, which suppresses the inline `columns[]` and would force a per-table `GET /tables/{full_name}` to recover them. `GET /tables/{full_name}` is the scan-path single-table load, not the listing path's column source. A listed VIEW entry carries its `columns[]` but omits `storage_location` and carries a null `data_source_format`, so the crate-private wire type models both as optional. The verified endpoints and their response fields were exercised against a live Databricks-managed workspace and against the OSS fixture (#325). The client's session fields, its auth strategy, and its wire types stay crate-private, so the engine reaches no request internals and no Unity-specific shape.

* The client deserializes `data_source_format` and makes the listing-admission decision itself. The raw string stays crate-private. A closed neutral format tag crosses the boundary, because the engine's format dispatch reads it.
* A skipped entry pairs its identifier with a neutral skip reason. The Iceberg REST client supplies its own "not a loadable Iceberg table" reason through the same skipped set, and its skip semantics and warning stay byte-identical.
* The credential-vending key is the Unity Catalog `table_id`. It crosses the boundary as an OPAQUE value that no caller outside this crate interprets.
* A column's `partition_index` stays crate-private. It crosses the boundary only as the neutral table's ordered partition-column names.
* No error, log line, or skip reason carries a credential value. The vending key is a table identity, not a secret, and it is still never logged, because it scopes a credential request.
* The table-level `properties` map crosses the boundary verbatim as the neutral table's properties, from the same `TableInfo` that supplies the columns. The client interprets no key of it, and a listed entry without `properties` yields an empty map. This is issue #426: the engine reads `delta.columnMapping.mode` from it to declare a Delta table's columns at createVirtualSchema (`unity-catalog/unity-catalog-create-virtual-schema`).
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:NEW -->
### Scenario: The client carries each table's properties verbatim from the list sweep and the single-table load

* *GIVEN* a `UnityCatalogSession` over a Unity Catalog namespace holding a Delta base table registered with `properties` holding `delta.columnMapping.mode` set to `name` and one more key, and a Parquet base table registered with no properties
* *WHEN* the adapter enumerates the namespace through the shared trait, and a caller then loads the Delta table through the single-table load
* *THEN* the neutral Delta table SHALL carry both properties verbatim, keys and values unchanged, and the neutral Parquet table SHALL carry an empty map
* *AND* the single-table load SHALL carry the same map for the same entry, because both operations convert the wire entry through one conversion
* *AND* the client SHALL issue no request beyond the list sweep to obtain properties, and SHALL interpret no property key
<!-- /DELTA:NEW -->
