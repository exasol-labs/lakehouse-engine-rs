<!-- DELTA:CHANGED -->
# Feature: Pushdown File Resolution

Resolves a table's identity and current file state before any SQL is built. The adapter recovers the target table's catalog identifier from the involved table's identifier note (`vs-adapter/create-virtual-schema`) and loads the table once per statement (`file-planning/pushdown-format-neutral-resolution`). Each table reference then resolves the data-file list, each file's byte size, the table root, and each file's delete references, so the scan UDF never discovers files, delete files, or sizes itself.
<!-- /DELTA:CHANGED -->

## Background

## Scenarios

### Scenario: Pushdown derives the scanned Iceberg table from the involved virtual table

* *GIVEN* a virtual schema created over a namespace containing multiple Iceberg tables, each virtual table carrying the identifier note recorded at create or refresh time
* *AND* a `pushdown` request whose `involvedTables[0].name` is the Exasol (uppercased, `__`-flattened) name of one of those tables
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL read the Iceberg identifier from the `name` member of the involved table's identifier note, `involvedTables[0].adapterNotes`, ignore any other member, and never list the catalog or derive the identifier from the virtual table name
* *AND* the adapter SHALL resolve the data-file list and build the scan-driving SQL for exactly that one Iceberg table, loading it from the catalog under that identifier
* *AND* an involved table without a readable identifier note (absent, not a JSON object, or without a string `name`) SHALL fail the request with an error that names the virtual table and `ALTER VIRTUAL SCHEMA ... REFRESH`, never scanning a different or stale table
