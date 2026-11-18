# Feature: EXPORT INTO SCRIPT Options and Writer Count

The export script checks the statement, and its options before any source row is read, and sets the number of writers from `WRITERS_PER_NODE` and `SINGLE_COMMIT`.

<!-- DELTA:CHANGED -->
## Background

* Statement: `EXPORT {<table> [(<columns>)] | (<query>)} INTO SCRIPT <schema>.LAKEHOUSE_EXPORT WITH CATALOG_CONNECTION = '<connection>' TABLE_NAME = '<namespace>.<table>' [WRITERS_PER_NODE = '<n>'] [SINGLE_COMMIT = '<true|false>'] [ALLOW_HTTP = '<true|false>'] [PARTITION_BY = '<fields>']`. `CATALOG_CONNECTION` names the CONNECTION object that the virtual schema uses.
* `CATALOG_CONNECTION` is required: the name of a CONNECTION object. The statement carries no `AT` clause, because with `AT` Exasol checks the `CONNECTION` privilege without passing the script the credentials.
* `TABLE_NAME` is required. It is a namespace and a table name joined by `.`. The namespace may have several `.`-separated levels, and no part is empty.
* `WRITERS_PER_NODE` is optional: an integer of at least 1, default 1. `SINGLE_COMMIT` is optional: `true` or `false` in any letter case, default `false`. The export refuses `WRITERS_PER_NODE` together with `SINGLE_COMMIT = 'true'`.
* `ALLOW_HTTP` is optional: `true` or `false` in any letter case, default `false`. Only `true` lets the export send a request over plain HTTP: to the catalog address, the OAuth2 server, the AWS STS endpoint, the CONNECTION's storage endpoint, or a storage endpoint that the catalog vends.
* `PARTITION_BY` is optional: the partition fields of a table the export creates, as export/export-partitioned-iceberg-table § Background states.
* Writer count: with `SINGLE_COMMIT = 'true'`, one writer runs for the whole cluster. Otherwise the export deals the source rows in turn to G writers, where G is the node count × `WRITERS_PER_NODE`, capped at 300. A source with at least G rows runs exactly G writers, and a source with fewer rows runs one writer per row.
<!-- /DELTA:CHANGED -->

## Scenarios

### Scenario: WRITERS_PER_NODE sets the writer count

* *GIVEN* a cluster of N nodes and a source with at least N × W rows
* *WHEN* a user exports it with `WRITERS_PER_NODE = '<W>'`, where N × W is at most 300
* *THEN* the export SHALL run exactly N × W writers and report the source row count
* *AND* an export with neither `WRITERS_PER_NODE` nor `SINGLE_COMMIT` SHALL run exactly N writers
* *AND* a source with fewer than N × W rows SHALL run one writer per row
