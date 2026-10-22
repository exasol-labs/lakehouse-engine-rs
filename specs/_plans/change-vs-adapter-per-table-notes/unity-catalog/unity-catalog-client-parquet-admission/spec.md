# Feature: Unity Catalog Native REST Client, Parquet Base Table Admission

Extends `unity-catalog/unity-catalog-client`'s listing and single-table load with the wire contract, admission rule, and skip routing for a Unity Catalog table whose `data_source_format` is `PARQUET`, alongside the `DELTA` tables the client already admits.

## Background

* The parent feature owns the client's shape, its trait implementation, its pagination, and its transport-error handling.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: The list request stays within the Unity Catalog wire contract

* *GIVEN* the same `UnityCatalogSession` and configured namespace
* *WHEN* the adapter enumerates the tables in that namespace through the shared trait
* *THEN* the crate-private wire type SHALL model `data_source_format` as optional (a `VIEW` carries it as null), and the raw `data_source_format` STRING SHALL remain a crate-private wire field that MUST NOT appear in any neutral type the engine can name
* *AND* the full listing MUST NOT set the `omit_columns` request parameter, which only the identifier-only listing of `unity-catalog/unity-catalog-client` sets, and every listing SHALL follow `page_token`/`next_page_token` pagination so every page's entries are classified and no listed table is truncated, and SHALL address every request under the `{host}/api/2.1/unity-catalog` base path, never the Iceberg-REST compatibility endpoint or the `delta/v1` Delta Tables API
<!-- /DELTA:CHANGED -->
