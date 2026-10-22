# Feature: Unity Catalog Native REST Client

A thin client in `crates/lakehouse-catalog` for the native Unity Catalog REST API. It enumerates tables and loads one table's metadata through the shared `CatalogClient` trait, so the engine never sees a Unity Catalog wire type. Listing returns one neutral table per entry whose `table_type` is `MANAGED` or `EXTERNAL` and whose `data_source_format` is `DELTA` or `PARQUET`, and routes every other entry into the skipped set with a neutral reason. One client serves OSS and Databricks-managed Unity Catalog through the standard API, never the Iceberg-REST compatibility endpoint or the `delta/v1` Delta Tables API.

<!-- DELTA:CHANGED -->
## Background

* The client is built once per request as a `UnityCatalogSession` holding one pooled `reqwest::Client`, the resolved base URL, and the resolved authentication strategy, which `unity-catalog/unity-catalog-auth` applies to every request. The session fields, the strategy, and the wire types stay crate-private.
* List endpoints paginate through `page_token` and `next_page_token`, and the client follows every page.
* `GET /tables` returns a fully populated `TableInfo` per table: `storage_location`, `table_id`, and an inline `columns[]` array in declared order. Each column carries its name, its Spark `StructField` JSON as `type_json`, and its `partition_index`. The client does not read `type_name`, `type_precision`, or `type_scale`, because real Unity clients leave a decimal's precision and scale out of those fields. The single list sweep is the column source for the full createVirtualSchema listing, so the full listing does not set the `omit_columns` parameter. `GET /tables/{full_name}` loads one table, for the scan path and for the load of listed tables.
* The identifier-only listing sets `omit_columns`, `omit_properties`, and `omit_username` to `true`. Databricks and OSS Unity Catalog honor all three and keep each entry's `name`.
* A listed `VIEW` entry omits `storage_location` and carries a null `data_source_format`, so the wire type models both as optional.
* The client deserializes `data_source_format` and makes the admission decision itself. The raw string stays crate-private, and a closed neutral format tag crosses the boundary because the engine's format dispatch reads it.
* A skipped entry pairs its identifier with a neutral skip reason.
* The credential-vending key is the Unity Catalog `table_id`. It crosses the boundary as an opaque value that no caller outside this crate interprets.
* The table-level `properties` map crosses the boundary verbatim as the neutral table's properties, from the same `TableInfo` that supplies the columns. The client interprets no key, and an entry without `properties` yields an empty map. The engine reads `delta.columnMapping.mode` from it (`unity-catalog/unity-catalog-create-virtual-schema`).
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: The Unity Catalog session is reached only through the shared catalog-client trait

* *GIVEN* a `UnityCatalogSession` and the shared `CatalogClient` trait the Iceberg REST catalog client also implements
* *WHEN* the engine adapter enumerates a namespace, lists its identifiers, loads listed tables, or loads one table's metadata under the Unity Catalog kind
* *THEN* `UnityCatalogSession` SHALL implement `CatalogClient`, and the engine SHALL reach every operation THROUGH that trait, so the adapter runs the same pipeline it runs for the Iceberg REST kind
* *AND* every operation SHALL return the catalog-NEUTRAL metadata types the trait declares, converting each deserialized Unity Catalog wire entry into a neutral table whose identifier carries the `catalog`/`schema` namespace segments and the table name, whose storage location is absent when the entry omits it, whose table FORMAT is a closed neutral tag, whose credential-vending key is the catalog-assigned key or absent, whose partition columns are the names of the columns carrying a `partition_index`, ordered by it, and whose ordered columns each carry the column name in its ORIGINAL case plus a source-tagged Unity Catalog type descriptor holding the column's `type_json` or its absence
* *AND* the Unity Catalog wire types MUST NOT appear in any signature the engine can name, because the engine consuming a Unity-specific shape is exactly the per-kind branch the shared trait exists to remove
<!-- /DELTA:CHANGED -->

<!-- DELTA:NEW -->
### Scenario: A scoped refresh lists Unity table names without columns and loads only the requested tables

* *GIVEN* a `UnityCatalogSession`, a configured `catalog.schema` namespace, and a partial refresh per `vs-adapter/refresh-and-set-properties`
* *WHEN* the adapter lists the namespace's identifiers through the shared trait and then loads the tables it selects
* *THEN* the identifier-only listing SHALL issue `GET /tables?catalog_name={catalog}&schema_name={schema}&omit_columns=true&omit_properties=true&omit_username=true`, follow every page, and return one neutral identifier per entry, views and unplannable entries included
* *AND* the load SHALL issue one `GET /tables/{catalog.schema.table}` per given identifier and admit or skip each entry as the scenario "The client lists tables in a configured catalog and schema" does
* *AND* a failed request SHALL fail the load with the credential-safe error of the scenario "The client surfaces a transport or HTTP-status failure as a clear, credential-safe error"
<!-- /DELTA:NEW -->
