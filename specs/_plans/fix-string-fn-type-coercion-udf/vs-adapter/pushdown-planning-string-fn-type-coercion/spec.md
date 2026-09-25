<!-- DELTA:CHANGED -->
# Feature: Pushdown Planning — String Function Argument Types

Keeps every pushed-down Exasol string function and string CAST correct for every argument type.
Exasol converts a non-string argument to text before it applies `UPPER`, `SUBSTR`, `LENGTH`,
`CONCAT`, and the rest of the family, or a `CAST(... AS VARCHAR/CHAR)`. The DataFusion dialect
reproduces that conversion through `exa_to_varchar` for integers, decimals, and dates
(`sql-comprehension/vs-expression-translator-string-conversion`). DOUBLE, BOOLEAN, and TIMESTAMP
have engine-specific text, so the adapter runs one read-only decline check and routes a request
that converts a column of those types to native Exasol evaluation. The check covers every surface
that renders DataFusion SQL: the WHERE filter, the select list, GROUP BY keys, aggregate arguments,
HAVING, and ORDER BY, on the single-table and the join paths (issues #210 and #227).
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
## Background

* An expression's `column` node carries no `dataType` on the wire. Column Exasol types are read
  from the involved tables' column metadata through `column_exa_type`, which uppercases the name
  before it matches (`vs-adapter/pushdown-col-types-consolidation`).
* The check inspects only the arguments `vs_expression::string_converted_args` returns, the table
  the renderer wraps. The adapter holds no copy of that table.
* The check classifies an inspected bare `column` argument by its Exasol type family
  (`classify_exa_type`). `Character`, `Date`, and `Decimal` pass. `Other` — DOUBLE PRECISION,
  BOOLEAN, TIMESTAMP, and every other type — declines, and an unresolvable name declines
  fail-safe. An Exasol integer arrives as `DECIMAL(p,0)` and passes.
* The check reads the tree and never rewrites it. The tree it accepts is the tree the renderer
  renders, so every text match in grouped planning sees one rendering.
* The check walks every curated child-bearing field through the shared post-order primitive
  (`vs-adapter/pushdown-module-dedup-consolidation`). A string function under a comparison
  predicate, a CASE branch, or another function is therefore inspected.
* The check has one owner, `string_conversion_declined` in `pushdown/support.rs`, and two
  consumers. `apply_type_rewrites` runs it on each WHERE filter and each select-list item, which
  reaches the single-table surfaces and both join surfaces. `classify_request_shape` runs it on
  `groupBy`, `selectList`, `having`, and `orderBy`, which reaches the grouped and single-group
  aggregate shapes on the non-empty and the empty-result paths alike.
* A decline reuses each surface's existing fallback. The WHERE filter self-applies in the qualified
  single-table wrapper (`vs-adapter/pushdown-declined-filter-self-apply`), a select-list item widens
  the projection to the base row, a grouped request routes to `RequestShape::GroupByWrapper`, and a
  single-group aggregate request routes to the row-scan wrapper. Each wrapper renders the original
  tree in the Exasol dialect.
* A DataFusion-dialect render error is a second decline trigger on every surface. `INSTR` and
  `LOCATE` beyond two arguments (#228) and a string-converted DOUBLE, fractional, or TIMESTAMP
  literal use it.
* Once Exasol delegates an advertised string function or `FN_CAST`, it never re-applies it, so
  every declined shape above is applied by the adapter's own wrapper SQL.
* Iceberg and Delta spec check: the conversion dispatches on primitive types, and neither spec
  defines a query text form for a value. The Iceberg table spec's Primitive Types table defines
  `boolean` as "True or false", `double` as "64-bit IEEE 754 floating point", `timestamp` as
  "Timestamp, microsecond precision, without timezone", `date` as "Calendar date without timezone
  or time", and `decimal(P,S)` as "Fixed-point decimal; precision P, scale S" with "Scale is fixed,
  precision must be 38 or less". The Delta protocol's `§ Primitive Types` defines `boolean` as
  "`true` or `false`", `double` as "8-byte double-precision floating-point numbers", `date` as "A
  calendar date, represented as a year-month-day triple without a timezone", and `decimal` as a
  "signed decimal number with fixed precision (maximum number of digits) and scale (number of
  digits on right side of dot)". Exasol's conversion is therefore the reference, and no deviation
  exists to fix or track.
* The DATE conversion matches Exasol only under the default `NLS_DATE_FORMAT` (`YYYY-MM-DD`). An
  altered session format is the tracked exception #216.
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: A string-position VARCHAR or CHAR column argument pushes down unchanged

* *GIVEN* a `pushdown` request carrying a string function or a string CAST whose string-converted argument is a bare `column` node of Exasol type `VARCHAR(n)` or `CHAR(n)`, for example `UPPER(c_varchar)`
* *WHEN* the adapter builds the scan spec
* *THEN* the decline check SHALL NOT decline, and the pushed SQL SHALL render the argument as `exa_to_varchar("C_VARCHAR")`, which returns the string unchanged
* *AND* the returned value SHALL equal native Exasol evaluation of the same expression
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: A string-position DECIMAL column argument renders through Exasol's trimmed decimal-to-string form

* *GIVEN* a `pushdown` request carrying a string function or a string CAST whose string-converted argument is a bare `column` node whose Exasol type begins `DECIMAL`, including an Exasol integer carried as `DECIMAL(p,0)` — for example issue #210's repros `UPPER(c_custkey)`, `TRIM(c_custkey)`, and `LTRIM(c_acctbal)`
* *WHEN* the adapter builds the scan spec
* *THEN* the decline check SHALL NOT decline and the adapter SHALL NOT rewrite the argument, so the renderer's `exa_to_varchar` wrapper converts it by its Arrow type: plain digits for an integer, trailing scale zeros removed for a decimal
* *AND* the returned value SHALL equal native Exasol evaluation of the same expression
<!-- /DELTA:CHANGED -->

<!-- DELTA:REMOVED -->
### Scenario: A string-position DATE column argument is wrapped in an explicit CAST to VARCHAR

* *GIVEN* a `pushdown` request whose select list or filter carries a governed string `function_scalar` whose string-position argument is a bare `column` node — for example issue #210's repro `LOWER(l_shipdate)`
* *AND* the column's Exasol type in `involvedTables[0].columns` is `DATE`
* *WHEN* the adapter builds the scan spec
* *THEN* the guard SHALL replace that argument with a `function_scalar_cast` node carrying `dataType` `{"type":"VARCHAR"}`, rendering `CAST(<col> AS VARCHAR)`, exactly the shape `guard_like_subject` already emits for a DATE LIKE subject
* *AND* the emitted match semantics SHALL equal Exasol's implicit DATE-to-VARCHAR conversion under the default `NLS_DATE_FORMAT` of `YYYY-MM-DD`, which is the ISO-8601 text form both engines render for the Iceberg `date` primitive
* *AND* under a session that has altered `NLS_DATE_FORMAT` away from that default the pushed-down result MAY diverge from native Exasol evaluation, because DataFusion's `CAST(Date32 AS VARCHAR)` is unconditionally ISO `YYYY-MM-DD` and the pushdown request carries no session NLS format — the accepted tracked exception #216, not a silent gap
<!-- /DELTA:REMOVED -->

<!-- DELTA:NEW -->
### Scenario: A string-position DATE column argument pushes down as ISO date text

* *GIVEN* a `pushdown` request carrying a string function or a string CAST whose string-converted argument is a bare `DATE` column, for example issue #210's repro `LOWER(l_shipdate)`
* *WHEN* the adapter builds the scan spec
* *THEN* the decline check SHALL NOT decline, and `exa_to_varchar` SHALL convert the argument to `YYYY-MM-DD` text
* *AND* under a session whose `NLS_DATE_FORMAT` differs from `YYYY-MM-DD` the pushed-down result MAY diverge from native Exasol evaluation — the tracked exception #216
<!-- /DELTA:NEW -->

<!-- DELTA:CHANGED -->
### Scenario: A non-coercible resolvable column type in a WHERE-clause string function declines the whole filter

* *GIVEN* a `pushdown` request whose filter carries a string function or a string CAST whose string-converted argument is a bare `column` node of a resolvable Exasol type other than VARCHAR, CHAR, DATE, or DECIMAL — for example `BOOLEAN`, `DOUBLE PRECISION`, or `TIMESTAMP`
* *WHEN* the adapter builds the single-table DataFusion scan-spec filter
* *THEN* the decline check SHALL decline the WHOLE top-level filter, so the scan spec carries no `filter` and the qualified single-table wrapper applies the original predicate tree as its own `WHERE`
* *AND* the adapter SHALL NOT push a text conversion of such an argument, because Exasol's `TRUE`, `1E20`, and space-separated TIMESTAMP text differ from DataFusion's
* *AND* the raw filter tree forwarded to Iceberg file pruning SHALL stay unchanged, and the returned rows SHALL equal native Exasol evaluation
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: A non-coercible resolvable column type in a select-list string function falls back to the full base row

* *GIVEN* a `pushdown` request whose select list carries a string function or a string CAST over a bare `column` node of a resolvable non-coercible type — for example `UPPER(c_double)` or `CAST(c_bool AS VARCHAR(5))`
* *WHEN* the adapter builds the scan-spec projection in `project_columns`
* *THEN* the decline SHALL set the existing `needs_full_fallback` flag and SHALL NOT propagate as an error, so the qualified single-table wrapper renders the item in the Exasol dialect over the base row
* *AND* the same decline reached through the broadcast join's shared use of `project_columns` (`extract_join_projection`) SHALL widen the projection to the disjoint union of both joined tables' columns
* *AND* the returned value SHALL equal native Exasol evaluation of the same expression
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: A string-position argument whose column name does not resolve declines fail-safe

* *GIVEN* a `pushdown` request carrying a string function or a string CAST whose string-converted argument is a bare `column` node whose name is absent from the column metadata, or which carries no `name`
* *WHEN* the adapter builds the scan spec
* *THEN* the decline check SHALL decline, because it cannot prove the argument is convertible
* *AND* the name lookup SHALL go through `column_exa_type`, so a case-mismatched name resolves rather than spuriously declining
<!-- /DELTA:CHANGED -->

<!-- DELTA:REMOVED -->
### Scenario: Only string-position argument indices are coerced

* *GIVEN* a `pushdown` request carrying a governed string `function_scalar` that mixes string-position and numeric-position arguments over bare columns — `SUBSTR(str_col, int_col, int_col)`, `REPEAT(str_col, int_col)`, `LEFT(str_col, int_col)`, `RIGHT(str_col, int_col)`, or `LPAD(str_col, int_col, pad_col)`
* *WHEN* the adapter builds the scan spec
* *THEN* the guard SHALL resolve string-position indices per function — all arguments for `CONCAT`, `TRIM`, `LTRIM`, `RTRIM`, `REPLACE`, and `TRANSLATE`; index 0 only for `LOWER`, `UPPER`, `ASCII`, `INITCAP`, `REVERSE`, `LENGTH`, `OCTET_LENGTH`, `UNICODE`, `SUBSTR`, `REPEAT`, `LEFT`, and `RIGHT`; indices 0 and 2 for `LPAD` and `RPAD` when index 2 is present
* *AND* the guard SHALL leave every non-string-position argument untouched, so a numeric length or offset argument is neither coerced to text nor able to trigger a decline
* *AND* a `LPAD`/`RPAD` call carrying only two arguments SHALL coerce index 0 only, without indexing past the end of the argument list
<!-- /DELTA:REMOVED -->

<!-- DELTA:REMOVED -->
### Scenario: CHR and UNICODECHR are excluded from the guard

* *GIVEN* a `pushdown` request carrying `CHR(<column>)` or `UNICODECHR(<column>)`
* *WHEN* the adapter builds the scan spec
* *THEN* the guard SHALL treat neither function as a governed string function, leaving its single argument unchanged and never declining on it, because that argument is a genuine integer codepoint rather than a string-position argument
* *AND* the guard SHALL still recurse into the argument, so a governed string function nested inside `CHR`/`UNICODECHR` is still coerced
<!-- /DELTA:REMOVED -->

<!-- DELTA:NEW -->
### Scenario: The decline check inspects only the arguments vs-expression converts to text

* *GIVEN* a `pushdown` request whose string functions mix string-converted and other arguments over bare columns — `SUBSTR(c_varchar, c_double, c_double)`, `LPAD(c_varchar, c_double)`, `REPEAT(c_varchar, c_double)` — or carry `CHR(c_double)` or `UNICODECHR(c_double)`
* *WHEN* the adapter runs the decline check
* *THEN* the check SHALL inspect exactly the arguments `vs_expression::string_converted_args` returns, so a length, offset, count, or codepoint argument of any type SHALL NOT trigger a decline
* *AND* the check SHALL still reach a string function nested inside such an argument, so `CHR(LENGTH(c_double))` declines on the argument of `LENGTH`
<!-- /DELTA:NEW -->

<!-- DELTA:REMOVED -->
### Scenario: INSTR and LOCATE coerce their first two arguments and decline beyond two

* *GIVEN* a `pushdown` request carrying `INSTR(a, b)` or `LOCATE(a, b)` where either bare-column argument is a non-string column — for example issue #210's repro `INSTR(c_custkey, '1')`
* *WHEN* the adapter builds the scan spec
* *THEN* the guard SHALL treat indices 0 and 1 as string-position for both functions, coercing or declining each independently
* *AND* the index assignment SHALL be independent of the translator's render-time argument reorder, because `vs-expression` renders Exasol `INSTR(string, substring)` as `strpos(arg0, arg1)` and Exasol `LOCATE(substring, string)` as `strpos(arg1, arg0)` — the reorder swaps which rendered slot each argument fills, never which arguments are string-position
* *AND* the previously hard-failing `Function 'strpos' requires String, but received Int64` planning error SHALL no longer occur for this shape
* *AND* an `INSTR` or `LOCATE` call carrying MORE than two arguments — `INSTR(a, b, start)`, `INSTR(a, b, start, occurrence)`, or `LOCATE(a, b, start)` — SHALL instead make the guard return `None`, declining the whole tree for EVERY argument type including all-VARCHAR, because `vs-expression` reads only `args[0]` and `args[1]` and drops the rest (issue #228): coercing index 0 would let an incompletely rendered call plan successfully, converting today's loud DataFusion type error into a silently wrong position, and it SHALL therefore also correct the pre-existing wrong result for an all-string `INSTR(c_varchar, 'b', 3)`, which pushed down as `strpos("C_VARCHAR", 'b')` and ignored the start position
* *AND* that beyond-two decline SHALL be reached at the broadcast join's combined WHERE filter and at the N-scan fallback's per-leg WHERE filter as well as at the single-table WHERE filter and the select-list projection, each routing the decline through its OWN already-existing self-application outcome — REPLACING this feature's recorded out-of-scope bullet naming the join per-leg WHERE-filter path as a deferred surface (issue #223 slice 2, wired by issue #215)
* *AND* narrowing #228's exposure this way SHALL NOT be recorded as closing #228, whose root cause is the `crates/vs-expression` rendering defect this delta does not touch
<!-- /DELTA:REMOVED -->

<!-- DELTA:NEW -->
### Scenario: An INSTR or LOCATE call beyond two arguments reaches native Exasol evaluation on every surface

* *GIVEN* a `pushdown` request carrying `INSTR(a, b, start)`, `INSTR(a, b, start, occurrence)`, or `LOCATE(a, b, start)` over any argument types in the WHERE filter, the select list, a GROUP BY key, or an aggregate argument — for example `MAX(INSTR(c_varchar, 'b', 3))`
* *WHEN* the adapter builds the pushdown
* *THEN* the DataFusion-dialect render error SHALL route each surface to its existing fallback: the WHERE filter self-applies, the select list widens, a grouped request routes to `RequestShape::GroupByWrapper`, and a single-group aggregate declines to the wrapper
* *AND* the returned value SHALL equal native Exasol evaluation, so `INSTR('abcabc', 'b', 3)` yields `5` on every surface, not the `2` a two-argument `strpos` returns
* *AND* faithful three- and four-argument pushdown SHALL remain the tracked exception #228
<!-- /DELTA:NEW -->

<!-- DELTA:REMOVED -->
### Scenario: A non-bare-column string-position argument is left unchanged as a tracked exception

* *GIVEN* a `pushdown` request carrying a governed string `function_scalar` whose string-position argument is NOT a bare `column` node — a literal such as `'x'`, a computed expression such as `c_decimal_a * 2`, or another already-string-valued function call such as the inner `TRIM` of `UPPER(TRIM(c_varchar))`
* *WHEN* the adapter builds the scan spec
* *THEN* the guard SHALL leave that argument unchanged and SHALL NOT decline on it, because its Exasol type is not resolvable from `involvedTables[0].columns`
* *AND* a numeric-valued computed argument MAY still hard-fail the DataFusion scan exactly as before this change — an accepted, accurately-scoped tracked exception (#223), not a silent gap
* *AND* post-order recursion SHALL still reach a governed string function nested inside such an argument, so `UPPER(TRIM(c_decimal_a))` coerces the inner `TRIM`'s DECIMAL argument even though `UPPER`'s own argument is not a bare column
<!-- /DELTA:REMOVED -->

<!-- DELTA:NEW -->
### Scenario: A computed string-converted argument converts by its DataFusion type

* *GIVEN* a `pushdown` request whose string-converted argument is a computed expression rather than a bare column — for example `UPPER(c_decimal_a * 2)` or `UPPER(c_double * 2)`
* *WHEN* the adapter builds the scan spec
* *THEN* the decline check SHALL NOT decline on the computed argument, because its type is not resolvable from the column metadata
* *AND* `exa_to_varchar` SHALL convert an integer, decimal, or date result exactly as it converts a column, so `UPPER(c_decimal_a * 2)` yields Exasol's trimmed text
* *AND* a DOUBLE, BOOLEAN, or TIMESTAMP result SHALL fail the query at planning time with an error naming `exa_to_varchar` — the tracked exception #223, not a silent gap
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: The decline check reaches GROUP BY keys, aggregate arguments, HAVING, and ORDER BY

* *GIVEN* a `pushdown` request whose `groupBy`, `selectList`, `having`, or `orderBy` carries a string function or a string CAST over a bare DOUBLE, BOOLEAN, or TIMESTAMP column — for example `GROUP BY UPPER(c_double)`, `MAX(UPPER(c_double))`, `HAVING MAX(CAST(c_bool AS VARCHAR(5))) = 'TRUE'`, or `ORDER BY UPPER(c_ts)`
* *WHEN* `classify_request_shape` decides the request shape, on the non-empty dispatch path and on the empty-result path
* *THEN* the check SHALL run before any grouped or single-group decomposition, and a GROUP BY request SHALL route to `RequestShape::GroupByWrapper` while any other request SHALL route to `RequestShape::RowScan`, whose aggregate select list widens to the qualified single-table wrapper
* *AND* no partial-aggregate scan spec SHALL carry such an argument
* *AND* the returned rows SHALL equal native Exasol evaluation on both paths, a single-group aggregate on the empty-result path through the one-row wrapper shape of `vs-adapter/pushdown-planning-empty-result` (issue #227)
<!-- /DELTA:NEW -->
