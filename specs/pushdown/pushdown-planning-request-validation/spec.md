# Feature: Pushdown Planning Request Validation

The adapter rejects a `pushdown` request whose types or columns Exasol does not declare, and accepts every request a supported Exasol version sends. The planning rules live in `pushdown/pushdown-planning`.

## Background

* The Background of `pushdown/pushdown-planning` applies: Exasol validates the pushed query's result positionally, and the `selectList` takes one of three forms.
* The adapter fails a malformed request before any catalog request.

## Scenarios

### Scenario: A pushdown request with a missing or invalid Exasol type fails the query

* *GIVEN* a `pushdown` request that lacks `selectListDataTypes`, or whose `selectListDataTypes` entry, involved-table column, or CAST node carries a `dataType` that is incomplete, names a type Exasol does not define, or lies outside Exasol's limits
* *AND* Exasol's limits are DECIMAL precision 1 to 36 with scale 0 to the precision, CHAR size 1 to 2,000, VARCHAR size 1 to 2,000,000, TIMESTAMP precision 0 to 9, INTERVAL leading precision 1 to 9 and fraction 0 to 9, and HASHTYPE size 1 to 1,024 bytes
* *WHEN* the adapter plans the pushdown
* *THEN* the adapter SHALL fail the query with an error that names the missing `selectListDataTypes` or the offending `dataType` object
* *AND* the adapter MUST NOT substitute a default for an absent field, except precision 3 for an absent TIMESTAMP `fractionalSecondsPrecision`, which Exasol 8.x omits, `false` for an absent `withLocalTimeZone`, and UTF8 for an absent `characterSet`, which Exasol sends only for ASCII, and MUST NOT cap or clamp a value or declare `VARCHAR(2000000)` in place of the type
* *AND* a request whose `selectList` holds one or more items SHALL fail the query when `selectListDataTypes` holds a different number of entries
* *AND* a request whose `selectList` is neither an array, absent, nor JSON null SHALL fail the query
* *AND* a request that carries an `aggregationType` beside an empty `selectList` SHALL fail the query, because Exasol sends an aggregating request at least one item

### Scenario: A column the request does not declare fails the request

* *GIVEN* a `pushdown` request with a `column` node, in any clause, that binds no single table of the query, or whose table's `involvedTables` entry does not declare it
* *WHEN* the adapter plans the pushdown
* *THEN* the adapter SHALL fail the query before any catalog request, with an error that names the column
* *AND* a `column` node SHALL bind a table by its `tableName`, and by its `tableAlias` when that table occurs several times, so a node with no `tableName`, a `tableName` the query does not read, or a `tableAlias` that names no occurrence binds none
* *AND* this state SHALL be unreachable for a request Exasol sends, because Exasol stamps a table of the query on every `column` node, single-table requests included, and declares every column a query reads in `involvedTables`

### Scenario: Every request of a supported Exasol version passes the strict type check

* *GIVEN* Exasol 8.29.x or later
* *WHEN* a query over a virtual table sends the adapter a `pushdown` request
* *THEN* the request SHALL carry `selectListDataTypes` with one complete `dataType` per select-list item, and the adapter SHALL plan it without a type error
* *AND* a `dataType` that omits only the fields the scenario "A pushdown request with a missing or invalid Exasol type fails the query" defaults SHALL be valid
* *AND* an absent or JSON-null `selectList`, which Exasol sends for `SELECT *` with one `selectListDataTypes` entry per table column, SHALL skip the count check
* *AND* an empty `selectList` with an empty `selectListDataTypes`, which Exasol sends for `SELECT RANDOM() FROM <vs>.<table>`, SHALL pass the count check
* *AND* the query SHALL return as many rows as native Exasol returns for it
