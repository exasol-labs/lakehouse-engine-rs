# Feature: Column Source Notes

When a virtual schema is created, refreshed, or has its properties set, the adapter records each virtual column's declared source shape in that column's own `adapterNotes`, for every catalog kind. Every pushdown takes the table's logical schema, partition columns, and refused columns from those notes, and no format reader derives a schema at pushdown.

## Background

* The Exasol Virtual Schema protocol accepts an `adapterNotes` string on each `columns[]` entry of `schemaMetadata.tables[]`. Exasol persists it in `SYS.EXA_ALL_VIRTUAL_COLUMNS.ADAPTER_NOTES` and returns it on every pushdown in `involvedTables[].columns[].adapterNotes`, for a single-table request and for each join leg.
* A column note is one JSON object string with up to three members. `sourceType` is the column's logical field in the scan spec's own JSON encoding (`field_id`, `name`, `arrow_type`, `nullable`, `initial_default`, `nested`, `physical_name`). `partition` is the column's 0-based position among the table's ordered partition columns, present only on a partition column. `refused` is the reason a pushdown refuses the column, present only on a refused column. The format is the same for every catalog kind.
* `sourceType.name` keeps the source's own spelling, because the scan binds an identity-bound column by that name. The Exasol column name is its uppercase fold.
* A column's note and its Exasol `dataType` are written in the same response, so both go stale together when the source evolves without a `REFRESH`. The note also fixes the binding key, the nullability, the nested members, and the partition position. A stale non-nullable declaration fails the query rather than drop NULL rows. `REFRESH` rewrites the note and the `dataType` together.
* A pushdown still loads per query what is not the schema: Iceberg table metadata and manifests, the Delta snapshot, the Unity or Glue table response, and the direct-storage file listing.
* Exasol persists a column note of at most 2,000,000 characters (sqlCode `04000`, "Maximum adapterNotes length exceeded (2000000)"). The limit counts characters, not bytes, and applies to `createVirtualSchema`, `refresh`, and `setProperties`. One adapter constant holds it. A wide nested column produces the longest notes, because `sourceType.nested` names every member recursively.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Refresh and set properties rebuild every column note from the source

* *GIVEN* an Iceberg virtual table created over a table with columns `id` and `score`, whose source column `score` is then renamed to `rating` and gains a sibling column `extra`
* *WHEN* Exasol sends a `refresh` request that carries no `requestedTables`, a `refresh` request whose `requestedTables` names the table, and separately a `setProperties` request
* *THEN* each response SHALL declare `ID`, `RATING`, and `EXTRA`, and each column's note SHALL hold the shape the source declares now, so the note of `RATING` carries the source name `rating` and the field id that `score` carried
* *AND* the adapter SHALL build every note from the re-enumerated source and MUST NOT copy or patch a note from the previous response
<!-- /DELTA:CHANGED -->
