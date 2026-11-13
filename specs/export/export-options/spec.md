# Feature: EXPORT INTO SCRIPT Options and Writer Count

The export script checks the statement, and its options before any source row is read, and sets the number of writers from `WRITERS_PER_NODE` and `SINGLE_COMMIT`.

## Background

* Statement: `EXPORT {<table> [(<columns>)] | (<query>)} INTO SCRIPT <schema>.LAKEHOUSE_EXPORT WITH CATALOG_CONNECTION = '<connection>' TABLE_NAME = '<namespace>.<table>' [WRITERS_PER_NODE = '<n>'] [SINGLE_COMMIT = '<true|false>'] [ALLOW_HTTP = '<true|false>']`. `CATALOG_CONNECTION` names the CONNECTION object that the virtual schema uses.
* `CATALOG_CONNECTION` is required: the name of a CONNECTION object. The statement carries no `AT` clause, because with `AT` Exasol checks the `CONNECTION` privilege without passing the script the credentials.
* `TABLE_NAME` is required. It is a namespace and a table name joined by `.`. The namespace may have several `.`-separated levels, and no part is empty.
* `WRITERS_PER_NODE` is optional: an integer of at least 1, default 1. `SINGLE_COMMIT` is optional: `true` or `false` in any letter case, default `false`. The export refuses `WRITERS_PER_NODE` together with `SINGLE_COMMIT = 'true'`.
* `ALLOW_HTTP` is optional: `true` or `false` in any letter case, default `false`. Only `true` lets the export send a request over plain HTTP: to the catalog address, the OAuth2 server, the AWS STS endpoint, the CONNECTION's storage endpoint, or a storage endpoint that the catalog vends.
* Writer count: with `SINGLE_COMMIT = 'true'`, one writer runs for the whole cluster. Otherwise the export deals the source rows in turn to G writers, where G is the node count × `WRITERS_PER_NODE`, capped at 300. A source with at least G rows runs exactly G writers, and a source with fewer rows runs one writer per row.

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
* *WHEN* a user exports it without `TABLE_NAME`, with a `TABLE_NAME` that is not `<namespace>.<table>`, with an unknown option, or with a `WRITERS_PER_NODE`, `SINGLE_COMMIT`, or `ALLOW_HTTP` value outside its domain
* *THEN* the export SHALL fail before any source row is read
* *AND* the error SHALL name the option and its accepted form, and for an unknown option SHALL list the accepted options

### Scenario: An export without CATALOG_CONNECTION fails before any data is read

* *GIVEN* a source whose rows raise an error when they are read
* *WHEN* a user exports it with no `CATALOG_CONNECTION` option, or with an empty one
* *THEN* the export SHALL fail before any source row is read, with an error that asks for `CATALOG_CONNECTION = '<connection>'`

### Scenario: An AT clause is refused

* *GIVEN* a source whose rows raise an error when they are read
* *WHEN* a user exports it with `AT <connection>`, or with an inline `AT '<address>' USER '<user>' IDENTIFIED BY '<password>'`
* *THEN* the export SHALL fail before any source row is read, with an error that points to the `CATALOG_CONNECTION` option
* *AND* the error MUST NOT contain the inline password

### Scenario: TRUNCATE, REPLACE, and CREATED BY are refused

* *GIVEN* an export statement that is otherwise valid
* *WHEN* the user adds `TRUNCATE`, `REPLACE`, or `CREATED BY '<sql>'`
* *THEN* the export SHALL fail before any source row is read
* *AND* the error SHALL name the refused clause and state that the export never truncates or replaces a table

### Scenario: Plain HTTP needs ALLOW_HTTP

* *GIVEN* a CONNECTION whose catalog address or storage endpoint uses `http://`
* *AND* a source whose rows raise an error when they are read
* *WHEN* a user exports it without `ALLOW_HTTP = 'true'`
* *THEN* the export SHALL fail before any source row is read, with an error that names `ALLOW_HTTP`
* *AND* the same export of a readable source with `ALLOW_HTTP = 'true'` SHALL succeed
