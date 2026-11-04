# Feature: EXPORT INTO SCRIPT Options and Writer Count

The export script checks the statement, its options, and its source column types before any source row is read, and sets the number of writers from `WRITERS_PER_NODE` and `SINGLE_COMMIT`.

## Background

* Statement: `EXPORT {<table> [(<columns>)] | (<query>)} INTO SCRIPT <schema>.LAKEHOUSE_EXPORT AT <connection> WITH TABLE_NAME = '<namespace>.<table>' [WRITERS_PER_NODE = '<n>'] [SINGLE_COMMIT = '<true|false>']`. `<connection>` names the CONNECTION object that the virtual schema uses.
* `TABLE_NAME` is required. It is a namespace and a table name joined by `.`. The namespace may have several `.`-separated levels, and no part is empty.
* `WRITERS_PER_NODE` is optional: an integer of at least 1, default 1. `SINGLE_COMMIT` is optional: `true` or `false` in any letter case, default `false`. The export refuses `WRITERS_PER_NODE` together with `SINGLE_COMMIT = 'true'`.
* Writer count: with `SINGLE_COMMIT = 'true'`, one writer runs for the whole cluster. Otherwise the export deals the source rows in turn to G writers, where G is the node count × `WRITERS_PER_NODE`, capped at 300. A source with at least G rows runs exactly G writers, and a source with fewer rows runs one writer per row.
* The export refuses columns of type GEOMETRY, HASHTYPE, INTERVAL YEAR TO MONTH, INTERVAL DAY TO SECOND, and TIMESTAMP WITH LOCAL TIME ZONE, because Exasol does not pass them to a UDF reliably. The Iceberg table spec (§ Schemas and Data Types, Primitive Types) defines `timestamptz` ("Timestamp, microsecond precision, with timezone"; the counterpart of TIMESTAMP WITH LOCAL TIME ZONE), `uuid` ("Universally unique identifiers") and `fixed(L)` ("Fixed-length byte array of length L"), the Iceberg counterparts of HASHTYPE, and, in format v3, `geometry(C)`. It defines no interval type. Refusing GEOMETRY, HASHTYPE, and TIMESTAMP WITH LOCAL TIME ZONE is a trade-off that Exasol's UDF interface forces. A user casts such a column, for example to VARCHAR or TIMESTAMP, to export it.
* Exasol's own UDF type check races the export's check, so a refused column fails the export with one of two errors: the export's error, which names the column and its type, or Exasol's `[22002] Column type not supported: <type>`, which does not name the column. Which one a run returns is not deterministic. An empty source with such a column may be refused the same way or export 0 rows, so the export makes no promise about it.

## Scenarios

### Scenario: WRITERS_PER_NODE sets the writer count

* *GIVEN* a cluster of N nodes and a source with at least N × W rows
* *WHEN* a user exports it with `WRITERS_PER_NODE = '<W>'`, where N × W is at most 300
* *THEN* the export SHALL run exactly N × W writers and report the source row count
* *AND* an export with neither `WRITERS_PER_NODE` nor `SINGLE_COMMIT` SHALL run exactly N writers
* *AND* a source with fewer than N × W rows SHALL run one writer per row

### Scenario: The writer count never exceeds 300

* *GIVEN* a cluster whose node count × `WRITERS_PER_NODE` exceeds 300
* *WHEN* a user exports a source of at least 300 rows with that `WRITERS_PER_NODE` value
* *THEN* the export SHALL run exactly 300 writers and report the source row count

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
* *THEN* the export SHALL fail before any source row is read, with an error that asks for `AT <connection>`, where `<connection>` is the CONNECTION the virtual schema uses
* *AND* the error MUST NOT contain the inline password

### Scenario: TRUNCATE, REPLACE, and CREATED BY are refused

* *GIVEN* an export statement that is otherwise valid
* *WHEN* the user adds `TRUNCATE`, `REPLACE`, or `CREATED BY '<sql>'`
* *THEN* the export SHALL fail before any source row is read
* *AND* the error SHALL name the refused clause and state that the export only appends rows

### Scenario: A column of an unsupported type is refused

* *GIVEN* a source that returns at least one row and has a column of type GEOMETRY, HASHTYPE, INTERVAL YEAR TO MONTH, INTERVAL DAY TO SECOND, or TIMESTAMP WITH LOCAL TIME ZONE
* *WHEN* a user exports it
* *THEN* the export SHALL fail
* *AND* the error SHALL either name the column as the source names it (the table column, the column-list entry, or the query's column alias) and its Exasol type, or be Exasol's `Column type not supported: <type>`
