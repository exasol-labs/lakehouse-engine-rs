# Feature: EXPORT INTO SCRIPT Row Counts

A user exports an Exasol table, a query over virtual schema tables, or a query that joins both, with `EXPORT ... INTO SCRIPT <schema>.LAKEHOUSE_EXPORT`. Exasol runs the same script as the writer over the source rows, each writer counts the rows it receives, and Exasol reports the sum of the counts as the statement's row count. No data reaches the lakehouse (#565).

## Background

* Statement: `EXPORT {<table> [(<columns>)] | (<query>)} INTO SCRIPT <schema>.LAKEHOUSE_EXPORT WITH CATALOG_CONNECTION = '<connection>' TABLE_NAME = '<namespace>.<table>' [WRITERS_PER_NODE = '<n>'] [SINGLE_COMMIT = '<true|false>']`. `CATALOG_CONNECTION` names the CONNECTION object that the virtual schema uses.
* Writer count: with `SINGLE_COMMIT = 'true'`, one writer runs for the whole cluster. Otherwise the export deals the source rows in turn to G writers, where G is the node count × `WRITERS_PER_NODE`, capped at 300. A source with at least G rows runs exactly G writers, and a source with fewer rows runs one writer per row.

## Scenarios

### Scenario: A whole-table export reports every source row

* *GIVEN* an Exasol table with 1,000 rows
* *WHEN* a user runs `EXPORT <table> INTO SCRIPT <schema>.LAKEHOUSE_EXPORT WITH CATALOG_CONNECTION = '<connection>' TABLE_NAME = 'ns.t'`
* *THEN* the statement's row count SHALL be 1,000
* *AND* every source row SHALL reach exactly one writer

### Scenario: A filtered query exports only the rows it returns

* *GIVEN* an Exasol table with 1,000 rows
* *WHEN* a user exports a query over that table whose filter matches a strict subset of the rows
* *THEN* the statement's row count SHALL equal the number of rows the query returns

### Scenario: A column-list export reports every row of the table

* *GIVEN* an Exasol table with several columns and 1,000 rows
* *WHEN* a user exports `<table> (<a subset of its columns>)`
* *THEN* the statement's row count SHALL be 1,000

### Scenario: A join of a virtual schema table and an Exasol table exports the joined rows

* *GIVEN* a virtual schema table and an Exasol table that share a join key
* *WHEN* a user exports a query that joins the two tables
* *THEN* the statement's row count SHALL equal the row count of the same query run as a `SELECT`

### Scenario: An empty source exports zero rows

* *GIVEN* a query that returns no rows
* *WHEN* a user exports it
* *THEN* the statement SHALL succeed with a row count of 0
