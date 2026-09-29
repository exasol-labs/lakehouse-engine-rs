<!-- DELTA:CHANGED -->
# Feature: Pushdown Planning — String Function Argument Types

Keeps every pushed-down Exasol string function and string CAST correct for every argument type.
Exasol converts a non-string argument to text before it applies `UPPER`, `SUBSTR`, `LENGTH`,
`CONCAT`, and the rest of the family, or a `CAST(... AS VARCHAR/CHAR)`. The DataFusion dialect
reproduces that conversion through `exa_to_varchar`
(`sql-comprehension/vs-expression-translator-string-conversion`), which converts every Arrow type to
Exasol's text (`datafusion-scan/scan-execution-exa-to-varchar`). The adapter therefore makes no
string-conversion decision, and every surface that renders DataFusion SQL pushes these functions
down (issues #210 and #227).
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
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
* The Iceberg and Delta spec check, the session-setting exception #216, and the `Float64` value
  range exception #TBD are recorded in `datafusion-scan/scan-execution-exa-to-varchar`.
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: A string-position VARCHAR or CHAR column argument pushes down unchanged

* *GIVEN* a `pushdown` request carrying a string function or a string CAST whose string-converted argument is a bare `column` node of Exasol type `VARCHAR(n)` or `CHAR(n)`, for example `UPPER(c_varchar)`
* *WHEN* the adapter builds the scan spec
* *THEN* the pushed SQL SHALL render the argument as `exa_to_varchar("C_VARCHAR")`, which DataFusion simplifies to the bare column (`datafusion-scan/scan-execution-exa-to-varchar`)
* *AND* the returned value SHALL equal native Exasol evaluation of the same expression
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: A string-position DECIMAL column argument renders through Exasol's trimmed decimal-to-string form

* *GIVEN* a `pushdown` request carrying a string function or a string CAST whose string-converted argument is a bare `column` node whose Exasol type begins `DECIMAL`, including an Exasol integer carried as `DECIMAL(p,0)`, for example issue #210's repros `UPPER(c_custkey)`, `TRIM(c_custkey)`, and `LTRIM(c_acctbal)`
* *WHEN* the adapter builds the scan spec
* *THEN* the adapter SHALL NOT rewrite the argument, and `exa_to_varchar` SHALL convert it by its Arrow type: plain digits for an integer, trailing scale zeros removed for a decimal
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
* *THEN* `exa_to_varchar` SHALL convert the argument to `YYYY-MM-DD` text
* *AND* under a session whose `NLS_DATE_FORMAT` differs from `YYYY-MM-DD` the pushed-down result MAY diverge from native Exasol evaluation: the tracked exception #216
<!-- /DELTA:NEW -->

<!-- DELTA:REMOVED -->
### Scenario: A non-coercible resolvable column type in a WHERE-clause string function declines the whole filter

* *GIVEN* a `pushdown` request whose filter carries a governed string `function_scalar` whose string-position argument is a bare `column` node
* *AND* the column's Exasol type in `involvedTables[0].columns` is resolvable but is none of VARCHAR, CHAR, DATE, or DECIMAL — for example `BOOLEAN`, `DOUBLE PRECISION`, or `TIMESTAMP`
* *WHEN* the adapter builds the single-table DataFusion scan-spec filter
* *THEN* the guard SHALL return `None`, declining pushdown of the WHOLE top-level filter so no `filter` is emitted in the common spec
* *AND* the adapter SHALL route the request to the qualified single-table wrapper and render the ORIGINAL predicate tree as that wrapper's own `WHERE` — REPLACING the recorded "and Exasol evaluates the entire predicate natively", which assumed an Exasol-side re-check of a delegated predicate that does not occur
* *AND* the guard SHALL NOT inject a CAST for such an argument, because DataFusion's text rendering of BOOLEAN (`true`) and TIMESTAMP (`T`-separated) diverges from Exasol's (`TRUE`, space-separated) and would silently change which rows match
* *AND* a decline reached at any nesting depth SHALL propagate to the top-level filter and SHALL apply ONLY to the JSON tree fed to `render_df_filter_safe`, leaving the raw filter tree forwarded to Iceberg file pruning unchanged — REPLACING the recorded "mirroring the all-or-nothing untranslatable-predicate backstop that `like_subject_type_guard` already uses", which named a backstop that does not exist; the all-or-nothing SCOPE is retained, its named justification is not
* *AND* the returned rows SHALL equal native Exasol evaluation of the same query
<!-- /DELTA:REMOVED -->

<!-- DELTA:REMOVED -->
### Scenario: A non-coercible resolvable column type in a select-list string function falls back to the full base row

* *GIVEN* a `pushdown` request whose select list carries a governed string `function_scalar` whose string-position argument is a bare `column` node of a resolvable non-coercible type — for example `UPPER(c_double)`
* *WHEN* the adapter builds the scan-spec projection in `project_columns`
* *THEN* the `None` decline SHALL set the existing `needs_full_fallback` flag, projecting the full base column set so Exasol post-processes the expression itself
* *AND* the decline SHALL NOT propagate as an error out of `project_columns`, because the full-row fallback is the established correctness backstop for a select-list item the adapter cannot push
* *AND* the returned value SHALL equal native Exasol evaluation of the same expression, which the hard-failing pre-change pushdown never produced
* *AND* the same decline reached through the broadcast join's shared use of `project_columns` (`extract_join_projection`) SHALL set `needs_full_fallback` over the disjoint UNION of both joined tables' columns, so the join wrapper projects every column of both sides and Exasol post-processes the expression, with no error and no per-leg SQL change
<!-- /DELTA:REMOVED -->

<!-- DELTA:NEW -->
### Scenario: A DOUBLE, BOOLEAN, or TIMESTAMP column argument pushes down with Exasol's text

* *GIVEN* a `pushdown` request carrying a string function or a string CAST over a bare `DOUBLE PRECISION`, `BOOLEAN`, or `TIMESTAMP` column in the WHERE filter, the select list, a GROUP BY key, or an aggregate argument, for example `WHERE UPPER(c_double) = '0.5'`, `CAST(c_bool AS VARCHAR(5))`, `GROUP BY UPPER(c_double)`, or `MAX(CAST(c_ts AS VARCHAR(30)))`
* *WHEN* the adapter builds the pushdown
* *THEN* the adapter SHALL push the expression into the scan like any other string function, and `exa_to_varchar` SHALL convert the argument to Exasol's text, for example `0.5`, `TRUE`, and `2024-05-15 10:00:00.000000`
* *AND* the returned rows SHALL equal native Exasol evaluation under the default session settings
<!-- /DELTA:NEW -->

<!-- DELTA:REMOVED -->
### Scenario: A string-position argument whose column name does not resolve declines fail-safe

* *GIVEN* a `pushdown` request whose select list or filter carries a governed string `function_scalar` whose string-position argument is a bare `column` node
* *AND* the column's name is NOT found in `involvedTables[0].columns`, or the node carries no `name`
* *WHEN* the adapter builds the scan spec
* *THEN* the guard SHALL return `None`, because it cannot prove the argument is a string and an unproven non-string argument would hard-fail the DataFusion scan
* *AND* the name lookup SHALL uppercase the argument's column name before matching, so a case-mismatched name resolves rather than spuriously declining
* *AND* that normalization SHALL be owned by exactly ONE helper, `column_exa_type` (`pushdown/support.rs`), which every type-rewrite guard calls rather than reimplementing — so this clause names an owner instead of asserting that the guard MIRRORS one. The superseded form said the lookup mirrors `extract_all_column_types`'s uppercasing, which stopped being true when issue #265 rewired this guard onto the shared helper
<!-- /DELTA:REMOVED -->

<!-- DELTA:REMOVED -->
### Scenario: Only string-position argument indices are coerced

* *GIVEN* a `pushdown` request carrying a governed string `function_scalar` that mixes string-position and numeric-position arguments over bare columns — `SUBSTR(str_col, int_col, int_col)`, `REPEAT(str_col, int_col)`, `LEFT(str_col, int_col)`, `RIGHT(str_col, int_col)`, or `LPAD(str_col, int_col, pad_col)`
* *WHEN* the adapter builds the scan spec
* *THEN* the guard SHALL resolve string-position indices per function — all arguments for `CONCAT`, `TRIM`, `LTRIM`, `RTRIM`, `REPLACE`, and `TRANSLATE`; index 0 only for `LOWER`, `UPPER`, `ASCII`, `INITCAP`, `REVERSE`, `LENGTH`, `OCTET_LENGTH`, `UNICODE`, `SUBSTR`, `REPEAT`, `LEFT`, and `RIGHT`; indices 0 and 2 for `LPAD` and `RPAD` when index 2 is present
* *AND* the guard SHALL leave every non-string-position argument untouched, so a numeric length or offset argument is neither coerced to text nor able to trigger a decline
* *AND* a `LPAD`/`RPAD` call carrying only two arguments SHALL coerce index 0 only, without indexing past the end of the argument list
<!-- /DELTA:REMOVED -->

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

* *GIVEN* a `pushdown` request carrying `INSTR(a, b, start)`, `INSTR(a, b, start, occurrence)`, or `LOCATE(a, b, start)` over any argument types in the WHERE filter, the select list, a GROUP BY key, or an aggregate argument, for example `MAX(INSTR(c_name, '0', 12))`, which Exasol sends as `INSTR(C_NAME,'0',12,1)`
* *WHEN* the adapter builds the pushdown
* *THEN* the DataFusion-dialect render error SHALL route each surface to its existing fallback, and the returned value SHALL equal native Exasol evaluation: on TPC-H SF100, `MAX(INSTR(c_name, '0', 12))` returns `12`, where the aggregate path pushed `strpos` without the start position and returned `10` before this change (issue #227)
* *AND* a two-argument `INSTR` or `LOCATE` SHALL keep its pushdown
* *AND* faithful three- and four-argument pushdown SHALL remain the tracked exception #228
<!-- /DELTA:NEW -->

<!-- DELTA:REMOVED -->
### Scenario: CHR and UNICODECHR are excluded from the guard

* *GIVEN* a `pushdown` request carrying `CHR(<column>)` or `UNICODECHR(<column>)`
* *WHEN* the adapter builds the scan spec
* *THEN* the guard SHALL treat neither function as a governed string function, leaving its single argument unchanged and never declining on it, because that argument is a genuine integer codepoint rather than a string-position argument
* *AND* the guard SHALL still recurse into the argument, so a governed string function nested inside `CHR`/`UNICODECHR` is still coerced
<!-- /DELTA:REMOVED -->

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

* *GIVEN* a `pushdown` request whose string-converted argument is a computed expression rather than a bare column, for example `UPPER(c_decimal_a * 2)`, `UPPER(c_double * 2)`, or `CAST(ROUND(c_acctbal / 3, 2) AS VARCHAR(40))`
* *WHEN* the adapter builds the pushdown
* *THEN* the adapter SHALL push the expression down unchanged, and `exa_to_varchar` SHALL convert its result exactly as it converts a column of the same Arrow type
* *AND* the returned value SHALL equal native Exasol evaluation, apart from the `Float64` value range that `datafusion-scan/scan-execution-exa-to-varchar` names as the tracked exception #TBD
<!-- /DELTA:NEW -->
