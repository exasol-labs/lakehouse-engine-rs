# Feature: Pushdown Planning — String Function Argument Types

Keeps every pushed-down Exasol string function and string CAST correct for every argument type.
Exasol converts a non-string argument to text before it applies `UPPER`, `SUBSTR`, `LENGTH`,
`CONCAT`, and the rest of the family, or a `CAST(... AS VARCHAR/CHAR)`. The DataFusion dialect
reproduces that conversion through `exa_to_varchar`
(`sql-comprehension/vs-expression-translator-string-conversion`), which converts every Arrow type to
Exasol's text (`datafusion-scan/scan-execution-exa-to-varchar`). The adapter therefore makes no
string-conversion decision, and every surface that renders DataFusion SQL pushes these functions
down (issues #210 and #227).

## Background

* The adapter reads no column type for a string-function or string-CAST argument and rewrites no
  expression tree for string conversion. The renderer wraps each string-converted argument, and
  `exa_to_varchar` converts it by its Arrow type, so a bare column, a literal, and a computed
  expression convert the same way.
* The conversion therefore holds on every surface that renders DataFusion SQL: the WHERE filter,
  the select list, GROUP BY keys, aggregate arguments, HAVING, ORDER BY, and both join filters.
* A DataFusion-dialect render error routes a surface to its existing fallback. `INSTR` and `LOCATE`
  beyond two arguments use it (#228). The WHERE filter self-applies in the qualified single-table
  wrapper (`vs-adapter/pushdown-declined-filter-self-apply`), a select-list item widens the
  projection to the base row, a grouped request routes to `RequestShape::GroupByWrapper`, and a
  single-group aggregate routes to the row-scan wrapper. Each wrapper renders the original tree in
  the Exasol dialect.
* Once Exasol delegates an advertised string function or `FN_CAST`, it never re-applies it, so
  every shape routed to a wrapper is applied by the adapter's own wrapper SQL.
* The Iceberg and Delta spec check are recorded in `datafusion-scan/scan-execution-exa-to-varchar`,
  with the text exceptions: the session-setting exception #216, the `Float64` value range exception
  #TBD, `date_trunc` over a DATE (#201), and `ROUND`/`TRUNC` over a DECIMAL (#431).

## Scenarios

### Scenario: A string-position VARCHAR or CHAR column argument pushes down unchanged

* *GIVEN* a `pushdown` request carrying a string function or a string CAST whose string-converted argument is a bare `column` node of Exasol type `VARCHAR(n)` or `CHAR(n)`, for example `UPPER(c_varchar)`
* *WHEN* the adapter builds the scan spec
* *THEN* the pushed SQL SHALL render the argument as `exa_to_varchar("C_VARCHAR")`, which DataFusion simplifies to the bare column (`datafusion-scan/scan-execution-exa-to-varchar`)
* *AND* the returned value SHALL equal native Exasol evaluation of the same expression

### Scenario: A string-position DECIMAL column argument renders through Exasol's trimmed decimal-to-string form

* *GIVEN* a `pushdown` request carrying a string function or a string CAST whose string-converted argument is a bare `column` node whose Exasol type begins `DECIMAL`, including an Exasol integer carried as `DECIMAL(p,0)`, for example issue #210's repros `UPPER(c_custkey)`, `TRIM(c_custkey)`, and `LTRIM(c_acctbal)`
* *WHEN* the adapter builds the scan spec
* *THEN* the adapter SHALL NOT rewrite the argument, and `exa_to_varchar` SHALL convert it by its Arrow type: plain digits for an integer, trailing scale zeros removed for a decimal
* *AND* the returned value SHALL equal native Exasol evaluation of the same expression

### Scenario: A string-position DATE column argument pushes down as ISO date text

* *GIVEN* a `pushdown` request carrying a string function or a string CAST whose string-converted argument is a bare `DATE` column, for example issue #210's repro `LOWER(l_shipdate)`
* *WHEN* the adapter builds the scan spec
* *THEN* `exa_to_varchar` SHALL convert the argument to `YYYY-MM-DD` text
* *AND* under a session whose `NLS_DATE_FORMAT` differs from `YYYY-MM-DD` the pushed-down result MAY diverge from native Exasol evaluation: the tracked exception #216

### Scenario: A DOUBLE, BOOLEAN, or TIMESTAMP column argument pushes down with Exasol's text

* *GIVEN* a `pushdown` request carrying a string function or a string CAST over a bare `DOUBLE PRECISION`, `BOOLEAN`, or `TIMESTAMP` column in the WHERE filter, the select list, a GROUP BY key, or an aggregate argument, for example `WHERE UPPER(c_double) = '0.5'`, `CAST(c_bool AS VARCHAR(5))`, `GROUP BY UPPER(c_double)`, or `MAX(CAST(c_ts AS VARCHAR(30)))`
* *WHEN* the adapter builds the pushdown
* *THEN* the adapter SHALL push the expression into the scan like any other string function, and `exa_to_varchar` SHALL convert the argument to Exasol's text, for example `0.5`, `TRUE`, and `2024-05-15 10:00:00.000000`
* *AND* the returned rows SHALL equal native Exasol evaluation under the default session settings

### Scenario: An INSTR or LOCATE call beyond two arguments reaches native Exasol evaluation on every surface

* *GIVEN* a `pushdown` request carrying `INSTR(a, b, start)`, `INSTR(a, b, start, occurrence)`, or `LOCATE(a, b, start)` over any argument types in the WHERE filter, the select list, a GROUP BY key, or an aggregate argument, for example `MAX(INSTR(c_name, '0', 12))`, which Exasol sends with the arguments as written (`C_NAME`, `'0'`, `12`)
* *WHEN* the adapter builds the pushdown
* *THEN* the DataFusion-dialect render error SHALL route each surface to its existing fallback, and the returned value SHALL equal native Exasol evaluation: on TPC-H SF100, `MAX(INSTR(c_name, '0', 12))` returns `12`, where the aggregate path pushed `strpos` without the start position and returned `10` before this change (issue #227)
* *AND* a two-argument `INSTR` or `LOCATE` SHALL keep its pushdown
* *AND* faithful three- and four-argument pushdown SHALL remain the tracked exception #228

### Scenario: A computed string-converted argument converts by its DataFusion type

* *GIVEN* a `pushdown` request whose string-converted argument is a computed expression rather than a bare column, for example `UPPER(c_decimal_a * 2)`, `UPPER(c_double * 2)`, or `CAST(ROUND(c_acctbal / 3, 2) AS VARCHAR(40))`
* *WHEN* the adapter builds the pushdown
* *THEN* the adapter SHALL push the expression down unchanged, and `exa_to_varchar` SHALL convert its result exactly as it converts a column of the same Arrow type
* *AND* the returned value SHALL equal native Exasol evaluation, apart from the `Float64` value range that `datafusion-scan/scan-execution-exa-to-varchar` names as the tracked exception #TBD, and the #201 and #431 text exceptions it records
