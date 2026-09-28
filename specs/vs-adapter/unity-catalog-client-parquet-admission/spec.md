# Feature: Unity Catalog Native REST Client — Parquet Base Table Admission

Extends `vs-adapter/unity-catalog-client`'s listing and single-table load with the wire contract,
the admission rule, and the skip routing needed to admit a Unity Catalog table whose
`data_source_format` is `PARQUET`, alongside the `DELTA` tables the client already admits.

This is the sibling of `vs-adapter/unity-catalog-client`, split out once that feature's own
scenario count crossed this library's per-spec organization threshold. The parent feature owns the
client's shape, its trait implementation, its pagination, and its transport-error handling; this
feature owns the Parquet-specific admission entries this plan
(`add-unity-parquet-table-routing`, issue #409) adds.

## Background

* The client compares `table_type` and `data_source_format` against the uppercase Unity Catalog
  vocabulary (`MANAGED`, `EXTERNAL`, `VIEW`, `DELTA`, `PARQUET`), matching the case Unity Catalog
  emits on the wire.
* A column's `type_json` and `partition_index` are wire fields `vs-adapter/unity-catalog-client`
  already declares crate-private except through the neutral table and neutral column shapes.

## Scenarios

### Scenario: The list request stays within the Unity Catalog wire contract

* *GIVEN* the same `UnityCatalogSession` and configured namespace
* *WHEN* the adapter enumerates the tables in that namespace through the shared trait
* *THEN* the crate-private wire type SHALL model `data_source_format` as optional (a `VIEW` carries it as null), and the raw `data_source_format` STRING SHALL remain a crate-private wire field that MUST NOT appear in any neutral type the engine can name
* *AND* the client MUST NOT set the `omit_columns` request parameter, SHALL follow `page_token`/`next_page_token` pagination so every page's entries are classified and no listed table is truncated, and SHALL address every request under the `{host}/api/2.1/unity-catalog` base path, never the Iceberg-REST compatibility endpoint or the `delta/v1` Delta Tables API

### Scenario: The client admits a Parquet base table and reports its partition columns

* *GIVEN* a `UnityCatalogSession` and a `GET /tables` response listing an `EXTERNAL` entry with `data_source_format` `PARQUET` whose columns are `id` (`partition_index` null), `year` (`partition_index` 1), and `region` (`partition_index` 0), each carrying its `type_json`, the shape the live OSS fixture returns for a Parquet external table
* *WHEN* the adapter enumerates the namespace through the shared trait
* *THEN* the client SHALL return one neutral table carrying the Parquet format tag, its storage location, its credential-vending key, and the partition columns `region` then `year`
* *AND* the returned columns SHALL keep the declared column order `id`, `year`, `region`, each carrying its `type_json` verbatim
* *AND* a column whose `type_json` is absent on the wire SHALL carry an absent `type_json` rather than a synthesized one, so the reader that needs it refuses the column instead of reading a guess

### Scenario: The client routes a view, an unplannable format, and any other table type into the skipped set with a reason

* *GIVEN* a `UnityCatalogSession` and a `GET /tables` response listing a `VIEW` entry with a null `data_source_format`, a `MANAGED` entry whose `data_source_format` is neither `DELTA` nor `PARQUET` (such as `ICEBERG` or `CSV`), a `MANAGED` entry whose `data_source_format` is the lowercase `parquet`, and an entry whose `table_type` is none of `MANAGED`, `EXTERNAL`, or `VIEW`
* *WHEN* the adapter enumerates the namespace through the shared trait
* *THEN* the client SHALL return NO neutral table for any of the four entries and SHALL place each into the returned skipped set
* *AND* each skipped entry SHALL pair the neutral `catalog.schema.table` identifier with a neutral reason naming the disqualifying cause: the entry's `table_type` for the view and the other-type entry, or its `data_source_format` verbatim for each unplannable base table
* *AND* the client SHALL compare `table_type` and `data_source_format` against the uppercase Unity Catalog vocabulary (`MANAGED`, `EXTERNAL`, `VIEW`, `DELTA`, `PARQUET`), matching the case Unity Catalog emits on the wire
* *AND* the reason MUST NOT contain the resolved bearer token, any OAuth client secret, or any other credential value

### Scenario: The single-table load serves only the scan path and applies no admission filter

* *GIVEN* the same `UnityCatalogSession` and fully-qualified table identifier
* *WHEN* a caller loads that single table's metadata through the shared trait
* *THEN* the createVirtualSchema listing path MUST NOT call this single-table load for column metadata, because it reads columns inline from the `GET /tables` list sweep, so this load SHALL serve only the scan-path single-table metadata source, whose production callers are the Unity Catalog format readers of `vs-adapter/delta-table-planning` and `vs-adapter/unity-parquet-table-planning`
* *AND* this load SHALL apply NO listing-admission filter, because it loads the one table its caller named rather than choosing which tables to admit, so the format tag it returns is the caller's ONLY signal of which reader can plan the table
* *AND* the returned credential-vending key SHALL be absent rather than empty when the response carries none, so a caller that requires one fails naming the table instead of POSTing an empty scope
