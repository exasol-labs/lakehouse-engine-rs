<!-- DELTA:CHANGED -->
# Feature: Unity Catalog Native REST Client

The adapter reads a Unity Catalog through its native REST API. It lists the tables of a catalog and schema, and a pushdown loads one table's metadata. Listing admits one virtual table per entry whose `table_type` is a base-table type and whose `data_source_format` is `DELTA` or `PARQUET`, and skips every other entry with a reason. The adapter serves OSS and Databricks-managed Unity Catalog through the standard API, never the Iceberg-REST compatibility endpoint or the `delta/v1` Delta Tables API.
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
## Background

* Every Unity Catalog request carries the authentication of `unity-catalog/unity-catalog-auth`. A `createVirtualSchema`, `refresh`, or `setProperties` request uses one connection pool, and the pushdown requests of one statement on a virtual schema share one.
* List endpoints paginate through `page_token` and `next_page_token`, and the adapter follows every page.
* `GET /tables` returns a fully populated `TableInfo` per table: `storage_location`, `table_id`, and an inline `columns[]` array in declared order. Each column carries its name, its Spark `StructField` JSON as `type_json`, and its `partition_index`. The adapter ignores `type_name`, `type_precision`, and `type_scale`, because real Unity clients leave a decimal's precision and scale out of those fields. The full createVirtualSchema listing takes its columns from this one list sweep, so it does not set the `omit_columns` parameter. `GET /tables/{full_name}` loads one table, for a pushdown and for the load of listed tables.
* The identifier-only listing sets `omit_columns`, `omit_properties`, and `omit_username` to `true`. Databricks and OSS Unity Catalog honor all three and keep each entry's `name`.
* The base-table types are `MANAGED`, `EXTERNAL`, and Databricks' shallow-clone types `MANAGED_SHALLOW_CLONE` and `EXTERNAL_SHALLOW_CLONE` (#332). A listed `VIEW` entry omits `storage_location` and carries a null `data_source_format`, and the listing still succeeds.
* An entry's `data_source_format` decides its admission, and a pushdown plans a loaded table by the format that its `GET /tables/{full_name}` answer reports (`delta/delta-table-planning`). A skipped entry is recorded in `SKIPPED_TABLES` with its identifier and a skip reason.
* A credential-vending request names the table by its Unity Catalog `table_id`.
* The adapter takes a table's `properties` from the same `TableInfo` that supplies its columns and reads only `delta.columnMapping.mode` from them (`unity-catalog/unity-catalog-create-virtual-schema`). An entry without `properties` lists as a table with no properties.
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:REMOVED -->
### Scenario: The Unity Catalog session is reached only through the shared catalog-client trait

* *GIVEN* the recorded scenario of this title
* *WHEN* this plan is recorded
* *THEN* this scenario SHALL be REMOVED, because it states code structure, which `specs/architecture.md` owns, rather than behavior
<!-- /DELTA:REMOVED -->
