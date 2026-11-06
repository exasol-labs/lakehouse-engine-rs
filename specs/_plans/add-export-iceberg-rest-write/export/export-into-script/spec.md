<!-- DELTA:CHANGED -->
# Feature: EXPORT INTO SCRIPT Source Rows

A user exports an Exasol table, a query over virtual schema tables, or a query that joins both, with `EXPORT ... INTO SCRIPT <schema>.LAKEHOUSE_EXPORT`. Exasol runs the same script as the writer over the source rows. The writers write the rows into the new Iceberg table that `TABLE_NAME` names, and Exasol reports the number of written rows as the statement's row count (#566).
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
## Background

* Statement, options, and writer count: export/export-options § Background.
* "Read through a virtual schema" means a `SELECT` over a virtual schema that the same CONNECTION backs and that lists the new table.
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:REMOVED -->
### Scenario: A whole-table export reports every source row

* *GIVEN* an Exasol table with 1,000 rows
* *WHEN* a user runs `EXPORT <table> INTO SCRIPT <schema>.LAKEHOUSE_EXPORT WITH CATALOG_CONNECTION = '<connection>' TABLE_NAME = 'ns.t'`
* *THEN* the statement's row count SHALL be 1,000
* *AND* every source row SHALL reach exactly one writer
<!-- /DELTA:REMOVED -->

<!-- DELTA:NEW -->
### Scenario: A whole-table export writes every source row

* *GIVEN* an Exasol table with 1,000 rows
* *WHEN* a user runs `EXPORT <table> INTO SCRIPT <schema>.LAKEHOUSE_EXPORT WITH CATALOG_CONNECTION = '<connection>' TABLE_NAME = '<namespace>.<new table>'`
* *THEN* the statement's row count SHALL be 1,000
* *AND* the new table, read through a virtual schema, SHALL hold the same 1,000 rows, each exactly once
<!-- /DELTA:NEW -->

<!-- DELTA:CHANGED -->
### Scenario: A filtered query exports only the rows it returns

* *GIVEN* an Exasol table with 1,000 rows
* *WHEN* a user exports a query over that table whose filter matches a strict subset of the rows
* *THEN* the statement's row count SHALL equal the number of rows the query returns
* *AND* the new table, read through a virtual schema, SHALL hold exactly the rows the query returns
<!-- /DELTA:CHANGED -->

<!-- DELTA:REMOVED -->
### Scenario: A column-list export reports every row of the table

* *GIVEN* an Exasol table with several columns and 1,000 rows
* *WHEN* a user exports `<table> (<a subset of its columns>)`
* *THEN* the statement's row count SHALL be 1,000
<!-- /DELTA:REMOVED -->

<!-- DELTA:NEW -->
### Scenario: A column-list export writes only the listed columns

* *GIVEN* an Exasol table with several columns and 1,000 rows
* *WHEN* a user exports `<table> (<a subset of its columns>)`
* *THEN* the statement's row count SHALL be 1,000
* *AND* the new table SHALL have only the listed columns, in list order, and hold all 1,000 rows
<!-- /DELTA:NEW -->

<!-- DELTA:CHANGED -->
### Scenario: A join of a virtual schema table and an Exasol table exports the joined rows

* *GIVEN* a virtual schema table and an Exasol table that share a join key
* *WHEN* a user exports a query that joins the two tables
* *THEN* the statement's row count SHALL equal the row count of the same query run as a `SELECT`
* *AND* the new table, read through a virtual schema, SHALL hold the rows that `SELECT` returns
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: An empty source exports zero rows

* *GIVEN* a query that returns no rows
* *WHEN* a user exports it into a table that does not exist
* *THEN* the statement SHALL succeed with a row count of 0
* *AND* the export SHALL NOT create the table
<!-- /DELTA:CHANGED -->
