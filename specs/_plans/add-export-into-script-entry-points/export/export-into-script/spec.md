# Feature: EXPORT INTO SCRIPT Entry Points

A user exports an Exasol table, a query over virtual schema tables, or a query that joins both, with `EXPORT ... INTO SCRIPT <schema>.LAKEHOUSE_EXPORT`. The export script checks the statement before any source row is read and spreads the source rows over writer scripts. Each writer counts the rows it receives, and Exasol reports the sum of the counts as the statement's row count. No data reaches the lakehouse (#565).

## Background

* Statement: `EXPORT {<table> [(<columns>)] | (<query>)} INTO SCRIPT <schema>.LAKEHOUSE_EXPORT AT <connection> WITH TABLE_NAME = '<namespace>.<table>' [WRITERS_PER_NODE = '<n>'] [SINGLE_COMMIT = '<true|false>']`. `<connection>` names the CONNECTION object that the virtual schema uses. Exasol folds option names to upper case and accepts only quoted option values.
* `TABLE_NAME` is required. It is a namespace and a table name joined by `.`. The namespace may have several `.`-separated levels, and no part is empty.
* `WRITERS_PER_NODE` is optional: an integer of at least 1, default 1. `SINGLE_COMMIT` is optional: `true` or `false` in any letter case, default `false`. The export refuses `WRITERS_PER_NODE` together with `SINGLE_COMMIT = 'true'`.
* Writer count: with `SINGLE_COMMIT = 'true'`, one writer runs for the whole cluster. Otherwise the export assigns each row at random to one of G writers, where G is the node count × `WRITERS_PER_NODE`, capped at 300. A writer that receives no row does not run, so a source with few rows can run fewer than G writers.
* An exporting user needs the `EXPORT` system privilege, `EXECUTE` on `LAKEHOUSE_EXPORT` and `LAKEHOUSE_EXPORT_WRITER`, the `CONNECTION` privilege on the CONNECTION (Exasol requires it for `AT <connection>`), and `GRANT ACCESS ON CONNECTION <connection> FOR SCRIPT` on each of the two scripts, granted directly or through a role. Both scripts read the CONNECTION.
* The export refuses columns of type GEOMETRY, HASHTYPE, INTERVAL YEAR TO MONTH, INTERVAL DAY TO SECOND, and TIMESTAMP WITH LOCAL TIME ZONE, because Exasol cannot pass them to a UDF. The Iceberg table spec (§ Schemas and Data Types, Primitive Types) defines `timestamptz` ("Timestamp, microsecond precision, with timezone"), `uuid` ("Universally unique identifiers"), `fixed(L)` ("Fixed-length byte array of length L"), and, in format v3, `geometry(C)`. It defines no interval type. Refusing GEOMETRY, HASHTYPE, and TIMESTAMP WITH LOCAL TIME ZONE is a trade-off that Exasol's UDF interface forces. A user casts such a column, for example to VARCHAR or TIMESTAMP, to export it.
* The column type check runs on the rows the source returns, so it does not run for a source that returns no rows.

## Scenarios

### Scenario: A whole-table export reports every source row

* *GIVEN* an Exasol table with 1,000 rows
* *WHEN* a user runs `EXPORT <table> INTO SCRIPT <schema>.LAKEHOUSE_EXPORT AT <connection> WITH TABLE_NAME = 'ns.t'`
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

### Scenario: WRITERS_PER_NODE sets the number of writers on each node

* *GIVEN* a cluster of N nodes and a source with at least 100 rows per planned writer
* *WHEN* a user exports it with `WRITERS_PER_NODE = '<W>'`, where N × W is at most 300
* *THEN* the export SHALL run N × W writers and report the source row count
* *AND* an export with neither `WRITERS_PER_NODE` nor `SINGLE_COMMIT` SHALL run N writers

### Scenario: The writer count never exceeds 300

* *GIVEN* a node count and a `WRITERS_PER_NODE` value whose product exceeds 300
* *WHEN* the export plans its writers
* *THEN* it SHALL plan 300 writers

### Scenario: SINGLE_COMMIT runs one writer for the whole cluster

* *GIVEN* a cluster of one or more nodes and a source with 1,000 rows
* *WHEN* a user exports it with `SINGLE_COMMIT = 'true'`
* *THEN* exactly one writer SHALL run in the whole cluster
* *AND* the statement's row count SHALL be 1,000

### Scenario: WRITERS_PER_NODE together with SINGLE_COMMIT is refused

* *GIVEN* a source whose rows raise an error when they are read
* *WHEN* a user exports it with `WRITERS_PER_NODE = '2'` and `SINGLE_COMMIT = 'true'`
* *THEN* the export SHALL fail before any source row is read
* *AND* the error SHALL name both options

### Scenario: An invalid option fails the export before any data is read

* *GIVEN* a source whose rows raise an error when they are read
* *WHEN* a user exports it without `TABLE_NAME`, with a `TABLE_NAME` that is not `<namespace>.<table>`, with an unknown option, or with a `WRITERS_PER_NODE` or `SINGLE_COMMIT` value outside its domain
* *THEN* the export SHALL fail before any source row is read
* *AND* the error SHALL name the option and its accepted form, and for an unknown option SHALL list the accepted options

### Scenario: An export without a named CONNECTION fails before any data is read

* *GIVEN* a source whose rows raise an error when they are read
* *WHEN* a user exports it with no `AT` clause, or with an inline `AT '<address>' USER '<user>' IDENTIFIED BY '<password>'`
* *THEN* the export SHALL fail before any source row is read, with an error that asks for `AT <connection>` naming the CONNECTION of the virtual schema
* *AND* the error MUST NOT contain the inline password

### Scenario: TRUNCATE, REPLACE, and CREATED BY are refused

* *GIVEN* an export statement that is otherwise valid
* *WHEN* the user adds `TRUNCATE`, `REPLACE`, or `CREATED BY '<sql>'`
* *THEN* the export SHALL fail before any source row is read
* *AND* the error SHALL name the refused clause and state that the export only appends rows

### Scenario: A column of an unsupported type fails the export naming the column and its type

* *GIVEN* a source that returns at least one row and has a column of type GEOMETRY, HASHTYPE, INTERVAL YEAR TO MONTH, INTERVAL DAY TO SECOND, or TIMESTAMP WITH LOCAL TIME ZONE
* *WHEN* a user exports it
* *THEN* the export SHALL fail with an error that names the column and its Exasol type
* *AND* the error SHALL name the column as the source names it: the table column, the column-list entry, or the query's column alias

### Scenario: A user who holds the export privileges exports rows

* *GIVEN* a user who holds the privileges § Background lists through one role, and no other privilege on the CONNECTION
* *WHEN* the user exports a table they may read
* *THEN* the export SHALL succeed and report the table's row count

### Scenario: A missing script-scoped CONNECTION grant fails with a named error

* *GIVEN* a user who holds every export privilege except `GRANT ACCESS ON CONNECTION ... FOR SCRIPT` on one of the two export scripts
* *WHEN* the user exports a table
* *THEN* the export SHALL fail with an error that names the CONNECTION, the script that lacks the grant, and the user
* *AND* when `LAKEHOUSE_EXPORT` lacks the grant, the export SHALL fail before any source row is read

### Scenario: No CONNECTION value appears in the generated SQL, an error, or the script output

* *GIVEN* a CONNECTION whose address, user, and password are known
* *WHEN* an export succeeds, or fails in any way this feature describes
* *THEN* the SQL that `LAKEHOUSE_EXPORT` generates SHALL carry the CONNECTION name and no value of the CONNECTION
* *AND* no error message and no script output SHALL contain the CONNECTION's password

### Scenario: The installer creates the export scripts and prints their grants

* *GIVEN* the install script has uploaded the engine `.so`
* *WHEN* it creates the deployment scripts in the target schema
* *THEN* it SHALL create `LAKEHOUSE_EXPORT` as a RUST SCALAR script and `LAKEHOUSE_EXPORT_WRITER` as a RUST SET script that returns `DECIMAL(18,0)`, both with dynamic input and both referencing the same `.so`
* *AND* the printed next-step template SHALL create one export role that holds the privileges § Background lists for the template's CONNECTION, and SHALL tell the operator to grant that role to each exporting user
* *AND* the template SHALL print the export role lines for every installing user, including `SYS`
