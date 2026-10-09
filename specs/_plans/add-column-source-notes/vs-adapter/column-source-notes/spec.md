# Feature: Column Source Notes

Records each virtual column's declared source shape in that column's own `adapterNotes` when a virtual schema is created, refreshed, or has its properties set, for every catalog kind. Every pushdown then takes the table's logical schema, its partition columns, and its refused columns from those notes. The declared shape is decided once, at `createVirtualSchema`, `refresh`, or `setProperties`, and no format reader derives a schema at pushdown.

## Background

* The Exasol Virtual Schema protocol accepts an `adapterNotes` string on each `columns[]` entry of `schemaMetadata.tables[]`. Exasol persists it in `SYS.EXA_ALL_VIRTUAL_COLUMNS.ADAPTER_NOTES` and returns it on every pushdown in `involvedTables[].columns[].adapterNotes`, for a single-table request and for each join leg (verified live on Exasol 2025.1.16 with the direct-storage spike of #426).
* A column note is one JSON object string with up to three members. `sourceType` is the column's logical field in the scan spec's own JSON encoding (`field_id`, `name`, `arrow_type`, `nullable`, `initial_default`, `nested`, `physical_name`). `partition` is the column's 0-based position among the table's ordered partition columns, present only on a partition column. `refused` is the reason a pushdown refuses the column, present only on a refused column. The format is the same for every catalog kind.
* `sourceType.name` keeps the source's own spelling, because the scan binds an identity-bound column by that name. The Exasol column name is the uppercase fold of it.
* A column's note and its Exasol `dataType` are written in the same response. When the source evolves without a `REFRESH`, the declared type is already stale, and the note goes stale at the same moment. The note also fixes four things the Exasol `dataType` does not carry, and each can drift from the source until `REFRESH`: the binding key, the nullability, the nested members, and the partition position. A stale non-nullable declaration fails the query rather than drop NULL rows (`file-planning/pushdown-planning-file-resolution`, `delta/delta-table-planning`). `REFRESH` heals the note and the `dataType` together.
* A pushdown still loads, per query, what is not the schema: Iceberg table metadata and manifests (snapshot, files, deletes, name mapping), the Delta snapshot (reader-feature gate, the check that the declaration agrees with the log, active files, deletion vectors), the Unity or Glue table response (location, vended credentials, partitions), and the direct-storage file listing.
* Exasol accepts a column note up to a per-column limit, the largest `adapterNotes` length Exasol persists for one column. The limit is measured live against the local Docker images of Exasol 2025.x and 8.29.x and is held by one adapter constant. A wide nested column produces the longest notes, because `sourceType.nested` names every member recursively.
* A virtual schema created before this feature carries no column notes. No migration path and no fallback exist: `ALTER VIRTUAL SCHEMA ... REFRESH` writes the notes. #425 applies the same policy to table-level notes.

## Scenarios

### Scenario: Every declared column carries its source shape in its adapterNotes

* *GIVEN* virtual schemas created over an Iceberg REST namespace, a Glue database holding an Iceberg table and a Parquet table, a Unity Catalog schema holding a Delta table and a Parquet table, and a direct-storage base path
* *WHEN* `createVirtualSchema` completes for each
* *THEN* every column of every returned virtual table SHALL carry an `adapterNotes` string that parses as one JSON object, readable from `SYS.EXA_ALL_VIRTUAL_COLUMNS.ADAPTER_NOTES`
* *AND* that object's `sourceType` SHALL be the column's logical field in the scan spec's own JSON encoding: its source name in the source's spelling, its Arrow type tag, its nullability, its nested member descriptor when the column nests, its Iceberg `initial-default` when one is defined, and the one binding key its format selects (an Iceberg field id, a Delta field id or physical name, or none)
* *AND* the object SHALL have the same members for every catalog kind, and MUST NOT carry a catalog-specific type descriptor such as a Unity `type_json` or a Glue Hive type string
* *AND* the adapter SHALL write each column's note in the same response that declares the column's Exasol `dataType`
* *AND* no note SHALL contain a credential value

### Scenario: A partition column's note carries its position among the partition columns

* *GIVEN* a direct-storage table holding `year=2026/month=09/p1.parquet`, and a Unity Parquet table whose catalog lists `region` (`partition_index` 1) before `year` (`partition_index` 0)
* *WHEN* `createVirtualSchema` completes and a query over each table is planned
* *THEN* the note of `year` SHALL carry `partition` `0` and the note of `month` SHALL carry `partition` `1` in the direct-storage table, and the note of `year` SHALL carry `partition` `0` and the note of `region` SHALL carry `partition` `1` in the Unity Parquet table
* *AND* the note of a column that is no partition column SHALL carry no `partition` member
* *AND* the pushdown SHALL order the table's partition columns by those positions, so the Unity Parquet scan lists `year` before `region` whatever the column order is

### Scenario: A refused column's note records its refusal and the column stays declared

* *GIVEN* an Iceberg table with columns `id int` and `b binary`, and a direct-storage directory whose files carry an `id` column and an unannotated `BYTE_ARRAY` column `legacy_name`
* *WHEN* `createVirtualSchema` completes and the pushdown plans `SELECT b`, `SELECT legacy_name`, and `SELECT id` on each table
* *THEN* `B` and `LEGACY_NAME` SHALL be declared `VARCHAR(2000000)`, and each note SHALL carry `refused` holding the reason `vs-adapter/binary-column-refusal` specifies for that column
* *AND* the requests that read `b` or `legacy_name` SHALL fail at plan time with that recorded reason, and `SELECT id` SHALL succeed
* *AND* the pushdown SHALL omit a refused column from the scan's logical schema, so a gate miss fails with an unresolved column rather than a silent NULL column

### Scenario: Pushdown takes the declared schema from the column notes for every kind

* *GIVEN* a virtual table whose columns carry notes, under any catalog kind
* *WHEN* the adapter plans a single-table pushdown over it, or a join in which it is one leg
* *THEN* the adapter SHALL take the table's logical schema, its ordered partition columns, and its refused columns from the involved table's column notes, in the column order of `involvedTables[].columns`
* *AND* no format reader SHALL derive a logical field, a partition column, or a refusal at pushdown from a catalog response, an Iceberg table schema, a Delta log schema, or a Parquet footer
* *AND* each join leg SHALL read the notes of its own involved table, so two legs over different tables never share a declaration
* *AND* a table whose every column is refused SHALL be refused as a whole with the recorded reasons, per `delta/delta-type-mapping`

### Scenario: A column without a readable note fails the pushdown naming the fix

* *GIVEN* a pushdown request whose involved table carries a column with no `adapterNotes`, and a second request whose column note is not a JSON object with a `sourceType` member
* *WHEN* the adapter plans each request
* *THEN* the adapter SHALL fail each request with an error naming the virtual table and the column, stating that the column's declaration note is missing or unreadable, and stating that `ALTER VIRTUAL SCHEMA ... REFRESH` rebuilds it
* *AND* the adapter MUST NOT derive the column's shape from the source as a fallback

### Scenario: Refresh and set properties rebuild every column note from the source

* *GIVEN* an Iceberg virtual table created over a table with columns `id` and `score`, whose source column `score` is then renamed to `rating` and gains a sibling column `extra`
* *WHEN* Exasol sends a `refresh` request, and separately a `setProperties` request
* *THEN* each response SHALL declare `ID`, `RATING`, and `EXTRA`, and each column's note SHALL hold the shape the source declares now, so the note of `RATING` carries the source name `rating` and the field id that `score` carried
* *AND* the adapter SHALL build every note from the re-enumerated source and MUST NOT copy or patch a note from the previous response

### Scenario: A source change after the last REFRESH plans against the declaration

* *GIVEN* a direct-storage table created over files that store column `n` as a 32-bit integer, after which a new file is added that stores `n` as a 64-bit integer and adds a column `extra`
* *WHEN* queries run before and after `ALTER VIRTUAL SCHEMA ... REFRESH`
* *THEN* before the refresh, the virtual table SHALL NOT declare `EXTRA`, and a query reading `n` SHALL fail at scan time naming the storage location, the column, and both types, per `scan-types/type-relaxation`
* *AND* before the refresh, a query that reads only columns whose files agree with the declaration SHALL succeed
* *AND* after the refresh, the virtual table SHALL declare `N` at the wider type and `EXTRA`, and both queries SHALL succeed

### Scenario: A column note longer than the per-column limit fails the statement

* *GIVEN* a table whose column's note is longer than the per-column limit, such as a direct-storage struct column with thousands of members whose names are each hundreds of characters long
* *WHEN* `createVirtualSchema`, `refresh`, or `setProperties` runs
* *THEN* the statement SHALL fail with an error naming the table, the column, the note's length, and the limit
* *AND* the adapter MUST NOT truncate, drop, or shorten a note, because a pushdown cannot plan from a partial declaration
* *AND* a table whose every note fits SHALL be unaffected by the check
