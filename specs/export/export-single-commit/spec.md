# Feature: EXPORT INTO SCRIPT With SINGLE_COMMIT

With `SINGLE_COMMIT = 'true'`, a new table appears only together with all its rows, and a failed export leaves no table behind.

## Background

* Statement and options: export/export-options § Background. Creating a new table: export/export-iceberg-table § Background.
* `SINGLE_COMMIT` means the table appears only with its data. Iceberg REST does it by staged table creation (`stage-create`), so only a `SINGLE_COMMIT` export that creates its table needs a catalog that accepts it.

## Scenarios

### Scenario: SINGLE_COMMIT on a catalog without staged creation fails and creates no table

* *GIVEN* a catalog that refuses staged table creation
* *WHEN* a user exports a source of 1,000 rows with `SINGLE_COMMIT = 'true'`
* *THEN* the export SHALL fail with an error that names the catalog and states that `SINGLE_COMMIT` needs staged table creation
* *AND* the export SHALL NOT create the table
* *AND* the same export without `SINGLE_COMMIT` SHALL succeed

### Scenario: SINGLE_COMMIT creates the table only together with its rows

* *GIVEN* a table that does not exist and a source of 1,000 rows
* *WHEN* a user exports it with `SINGLE_COMMIT = 'true'`
* *THEN* the table SHALL have exactly one snapshot, which holds the 1,000 rows
* *AND* no earlier version of the table's metadata SHALL exist

### Scenario: A failed SINGLE_COMMIT export leaves no table

* *GIVEN* a source of 1,000 rows and a namespace that does not exist
* *WHEN* a user exports it with `SINGLE_COMMIT = 'true'`, and either the source raises an error at its 500th row or the writer fails after it has written its data files and before it commits
* *THEN* the statement SHALL fail, with the source's error when the source failed
* *AND* the catalog SHALL NOT hold the table, and the namespace that the export created SHALL remain
* *AND* the data files that the writer wrote SHALL NOT remain in storage
