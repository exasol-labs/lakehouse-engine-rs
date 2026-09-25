# Feature: Pushdown Planning — Empty Result When All Files Are Pruned

Unchanged by this plan, and reproduced here only because the delta validator requires a description. The recorded text stands as written in `specs/vs-adapter/pushdown-planning-empty-result/spec.md`.

## Background

* Unchanged by this plan. This section is unmarked, so `speq record` ignores it and the recorded Background survives intact. See `specs/vs-adapter/pushdown-planning-empty-result/spec.md`.

## Scenarios

<!-- DELTA:NEW -->
### Scenario: A single-group aggregate that routes to the row-scan wrapper returns one row when all files are pruned

* *GIVEN* a `pushdown` request with no GROUP BY whose select list carries an aggregate that does not decompose, so the shared request classifier resolves `RequestShape::RowScan` and the projection widens, for example `SELECT COUNT(UPPER(c_double)) FROM t` (a string-conversion decline, `vs-adapter/pushdown-planning-string-fn-type-coercion`) or `SELECT MAX(INSTR(c_varchar, 'b', 3)) FROM t` (a DataFusion-dialect render error), and whose WHERE predicate prunes 100% of the table's data files
* *WHEN* the adapter reaches the zero-files short-circuit
* *THEN* the row-scan empty shape for that request SHALL be the qualified single-table wrapper's select list and trailing clauses, rendered in the Exasol dialect over a zero-row derived table that projects each referenced column as `CAST(NULL AS <its Exasol type>)`, so Exasol evaluates the aggregate over zero rows itself
* *AND* the response SHALL produce exactly one row equal to native Exasol evaluation, `0` for `COUNT(UPPER(c_double))` and NULL for `MAX(INSTR(c_varchar, 'b', 3))`, with the column count and types of the non-empty wrapper
* *AND* the response MUST NOT invoke the scan fan-out UDF and MUST NOT reference any resolved data file
* *AND* a row-scan request whose select list carries no aggregate SHALL take the typed empty projection of "Row-scan query with all files pruned returns a typed empty projection"
<!-- /DELTA:NEW -->
