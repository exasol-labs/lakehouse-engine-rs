# Feature: Pushdown Planning, String Function Argument Type Coercion

The adapter type-checks the string-position arguments of every pushed-down Exasol string function. It passes string arguments through, rewraps DATE arguments in an explicit CAST-to-VARCHAR, rewraps DECIMAL arguments in the `decimal_to_varchar_exasol` node, and declines pushdown for every other resolvable type so Exasol evaluates the expression natively.

## Background

* Exasol converts a numeric or DATE argument to VARCHAR implicitly before applying a string function. DataFusion does not, so a pushed-down string function over a non-string column fails the scan (`Function 'upper' requires String, but received Int64`, SQL state 22002).
* The guard is `string_function_arg_type_guard` in the adapter (`pushdown/support.rs`), because `vs-expression` is a syntactic translator with no column-type context. A `column` node carries no `dataType` on the wire. The adapter reads column Exasol types from `involvedTables[0].columns` through `extract_all_column_types`, which uppercases column names.
* The governed functions are `CONCAT`, `LOWER`, `UPPER`, `SUBSTR`, `TRIM`, `LTRIM`, `RTRIM`, `REPLACE`, `REPEAT`, `REVERSE`, `LPAD`, `RPAD`, `ASCII`, `INITCAP`, `LEFT`, `RIGHT`, `TRANSLATE`, `LENGTH`, `OCTET_LENGTH`, `UNICODE`, `INSTR`, and `LOCATE`. `CHR` and `UNICODECHR` are excluded because their sole argument is an integer codepoint.
* String-position argument indices are per function, because `SUBSTR(str, start, length)` and `LPAD(str, length, pad)` also take numeric arguments that must not become text.
* The per-function argument table has three outcomes: not governed (leave the node, never decline), coerce (a set of string-position indices), and decline (governed but unrenderable at this arity). `INSTR` and `LOCATE` with more than two arguments are the decline case, because `vs-expression` renders only `args[0]` and `args[1]`.
* The guard returns `Option<Json>`: `Some(tree)` renders the possibly rewritten tree, `None` declines. It recurses through the shared post-order rewrite primitive over the curated child-bearing fields `expressions`, `arguments`, `results`, `expression`, `pattern`, `left`, `right`, `basis`. A filter-side string function sits under a comparison predicate (`UPPER(c) = 'X'` has the function under `left`).
* A WHERE-clause decline routes the request to the qualified single-table wrapper, which renders the original predicate tree as its own `WHERE` (`pushdown/pushdown-declined-filter-self-apply`). A select-list decline sets the full-base-row fallback flag, and Exasol computes the item over the returned columns.
* The select-list path is shared with the broadcast join: `extract_join_projection` calls `project_columns` against the union of both joined tables' columns, and `joins/mod.rs`'s empty-side path calls it against the union of every involved table's columns. A decline there projects the full union.
* The guard also runs on the broadcast join's combined WHERE filter and the N-scan fallback's per-leg WHERE filter; see `pushdown-joins/pushdown-planning-join-filter-type-coercion`.
* An Exasol integer is `DECIMAL(p,0)` on the wire, so the DECIMAL branch covers integer columns. The trim is a no-op at scale 0.
* The DECIMAL branch reuses the `decimal_to_varchar_exasol` node and `wrap_decimal_to_varchar` helper of `pushdown-types/pushdown-planning-decimal-string-format`. The DATE branch reuses the `function_scalar_cast` shape of `guard_like_subject`.
* The DATE CAST matches Exasol only under the default `NLS_DATE_FORMAT` (`YYYY-MM-DD`), which is also DataFusion's `Date32`-to-`Utf8` form.
* The decline branch is a correctness fix, not only a crash fix. The Iceberg spec assigns no text form to `boolean`, `double`, or `timestamp`. Exasol renders BOOLEAN as `TRUE`/`FALSE` and DataFusion as `true`/`false`, and Exasol's TIMESTAMP text is space-separated where DataFusion's is `T`-separated.
* `pushdown-types/pushdown-planning-string-fn-type-coercion-composition` covers this guard's order relative to `like_subject_type_guard` and `rewrite_decimal_stringifications`.
* Out of scope: a string-position argument that is a literal or computed expression, the grouped-aggregate render path (every `groupBy` element and non-aggregate select item), the aggregate-argument render path, `CHR`/`UNICODECHR` over a non-numeric argument, and rendering of `INSTR`'s optional third and fourth arguments and `LOCATE`'s optional third argument, which the translator drops.

## Scenarios

### Scenario: A string-position VARCHAR or CHAR column argument pushes down unchanged

* *GIVEN* a `pushdown` request whose select list or filter carries a governed string `function_scalar`, for example `UPPER(c_varchar)`, whose string-position argument is a bare `column` node
* *AND* the column's Exasol type in `involvedTables[0].columns` is `VARCHAR(n)` or `CHAR(n)`
* *WHEN* the adapter builds the scan spec
* *THEN* the guard SHALL return the node unchanged, injecting neither a CAST nor a `decimal_to_varchar_exasol` wrapper, because DataFusion needs no coercion for a string argument
* *AND* the rendered SQL SHALL be identical to its form without this guard, so the common VARCHAR case keeps its existing pushdown

### Scenario: A string-position DECIMAL column argument renders through Exasol's trimmed decimal-to-string form

* *GIVEN* a `pushdown` request whose select list or filter carries a governed string `function_scalar` whose string-position argument is a bare `column` node, for example `UPPER(c_custkey)`, `TRIM(c_custkey)`, and `LTRIM(c_acctbal)`
* *AND* the column's Exasol type in `involvedTables[0].columns` begins `DECIMAL`, which on the wire includes Exasol integers carried as `DECIMAL(p,0)`
* *WHEN* the adapter builds the scan spec
* *THEN* the guard SHALL replace that argument with a `{"type":"decimal_to_varchar_exasol","arguments":[<column>]}` node via the existing `wrap_decimal_to_varchar` helper, so the argument reaches the string function as Exasol's trailing-zero-trimmed text rather than as a hard type error
* *AND* the guard SHALL NOT emit a plain `CAST(<col> AS VARCHAR)` for a DECIMAL argument and SHALL NOT reimplement decimal formatting, because DataFusion's fixed-declared-scale rendering diverges from Exasol's trimmed form and would turn a hard failure into a silently wrong result, the node and helper that `pushdown-types/pushdown-planning-decimal-string-format` renders through `format_decimal_exasol_style` are the single owner of that conversion
* *AND* the previously hard-failing `Function 'upper' requires String, but received Int64` planning error SHALL no longer occur for this shape

### Scenario: A string-position DATE column argument is wrapped in an explicit CAST to VARCHAR

* *GIVEN* a `pushdown` request whose select list or filter carries a governed string `function_scalar` whose string-position argument is a bare `column` node, for example `LOWER(l_shipdate)`
* *AND* the column's Exasol type in `involvedTables[0].columns` is `DATE`
* *WHEN* the adapter builds the scan spec
* *THEN* the guard SHALL replace that argument with a `function_scalar_cast` node carrying `dataType` `{"type":"VARCHAR"}`, rendering `CAST(<col> AS VARCHAR)`, exactly the shape `guard_like_subject` already emits for a DATE LIKE subject
* *AND* the emitted match semantics SHALL equal Exasol's implicit DATE-to-VARCHAR conversion under the default `NLS_DATE_FORMAT` of `YYYY-MM-DD`, which is the ISO-8601 text form both engines render for the Iceberg `date` primitive
* *AND* under a session that has altered `NLS_DATE_FORMAT` away from that default the pushed-down result MAY diverge from native Exasol evaluation, because DataFusion's `CAST(Date32 AS VARCHAR)` is unconditionally ISO `YYYY-MM-DD` and the pushdown request carries no session NLS format, an accepted tracked exception, not a silent gap

### Scenario: A non-coercible resolvable column type in a WHERE-clause string function declines the whole filter

* *GIVEN* a `pushdown` request whose filter carries a governed string `function_scalar` whose string-position argument is a bare `column` node
* *AND* the column's Exasol type in `involvedTables[0].columns` is resolvable but is none of VARCHAR, CHAR, DATE, or DECIMAL, for example `BOOLEAN`, `DOUBLE PRECISION`, or `TIMESTAMP`
* *WHEN* the adapter builds the single-table DataFusion scan-spec filter
* *THEN* the guard SHALL return `None`, declining pushdown of the WHOLE top-level filter so no `filter` is emitted in the common spec
* *AND* the adapter SHALL route the request to the qualified single-table wrapper and render the ORIGINAL predicate tree as that wrapper's own `WHERE`, REPLACING the recorded "and Exasol evaluates the entire predicate natively", which assumed an Exasol-side re-check of a delegated predicate that does not occur
* *AND* the guard SHALL NOT inject a CAST for such an argument, because DataFusion's text rendering of BOOLEAN (`true`) and TIMESTAMP (`T`-separated) diverges from Exasol's (`TRUE`, space-separated) and would silently change which rows match
* *AND* a decline reached at any nesting depth SHALL propagate to the top-level filter and SHALL apply ONLY to the JSON tree fed to `render_df_filter_safe`, leaving the raw filter tree forwarded to Iceberg file pruning unchanged, REPLACING the recorded "mirroring the all-or-nothing untranslatable-predicate backstop that `like_subject_type_guard` already uses", which named a backstop that does not exist; the all-or-nothing SCOPE is retained, its named justification is not
* *AND* the returned rows SHALL equal native Exasol evaluation of the same query

### Scenario: A non-coercible resolvable column type in a select-list string function falls back to the full base row

* *GIVEN* a `pushdown` request whose select list carries a governed string `function_scalar` whose string-position argument is a bare `column` node of a resolvable non-coercible type, for example `UPPER(c_double)`
* *WHEN* the adapter builds the scan-spec projection in `project_columns`
* *THEN* the `None` decline SHALL set the existing `needs_full_fallback` flag, projecting the full base column set so Exasol post-processes the expression itself
* *AND* the decline SHALL NOT propagate as an error out of `project_columns`, because the full-row fallback is the established correctness backstop for a select-list item the adapter cannot push
* *AND* the returned value SHALL equal native Exasol evaluation of the same expression, which the hard-failing pushdown without this guard never produced
* *AND* the same decline reached through the broadcast join's shared use of `project_columns` (`extract_join_projection`) SHALL set `needs_full_fallback` over the disjoint UNION of both joined tables' columns, so the join wrapper projects every column of both sides and Exasol post-processes the expression, with no error and no per-leg SQL change

### Scenario: A string-position argument whose column name does not resolve declines fail-safe

* *GIVEN* a `pushdown` request whose select list or filter carries a governed string `function_scalar` whose string-position argument is a bare `column` node
* *AND* the column's name is NOT found in `involvedTables[0].columns`, or the node carries no `name`
* *WHEN* the adapter builds the scan spec
* *THEN* the guard SHALL return `None`, because it cannot prove the argument is a string and an unproven non-string argument would hard-fail the DataFusion scan
* *AND* the name lookup SHALL uppercase the argument's column name before matching, so a case-mismatched name resolves rather than spuriously declining
* *AND* that normalization SHALL be owned by exactly ONE helper, `column_exa_type` (`pushdown/support.rs`), which every type-rewrite guard calls rather than reimplementing, so this clause names an owner instead of asserting that the guard MIRRORS one.

### Scenario: Only string-position argument indices are coerced

* *GIVEN* a `pushdown` request carrying a governed string `function_scalar` that mixes string-position and numeric-position arguments over bare columns, `SUBSTR(str_col, int_col, int_col)`, `REPEAT(str_col, int_col)`, `LEFT(str_col, int_col)`, `RIGHT(str_col, int_col)`, or `LPAD(str_col, int_col, pad_col)`
* *WHEN* the adapter builds the scan spec
* *THEN* the guard SHALL resolve string-position indices per function, all arguments for `CONCAT`, `TRIM`, `LTRIM`, `RTRIM`, `REPLACE`, and `TRANSLATE`; index 0 only for `LOWER`, `UPPER`, `ASCII`, `INITCAP`, `REVERSE`, `LENGTH`, `OCTET_LENGTH`, `UNICODE`, `SUBSTR`, `REPEAT`, `LEFT`, and `RIGHT`; indices 0 and 2 for `LPAD` and `RPAD` when index 2 is present
* *AND* the guard SHALL leave every non-string-position argument untouched, so a numeric length or offset argument is neither coerced to text nor able to trigger a decline
* *AND* a `LPAD`/`RPAD` call carrying only two arguments SHALL coerce index 0 only, without indexing past the end of the argument list

### Scenario: INSTR and LOCATE coerce their first two arguments and decline beyond two

* *GIVEN* a `pushdown` request carrying `INSTR(a, b)` or `LOCATE(a, b)` where either bare-column argument is a non-string column, for example `INSTR(c_custkey, '1')`
* *WHEN* the adapter builds the scan spec
* *THEN* the guard SHALL treat indices 0 and 1 as string-position for both functions, coercing or declining each independently
* *AND* the index assignment SHALL be independent of the translator's render-time argument reorder, because `vs-expression` renders Exasol `INSTR(string, substring)` as `strpos(arg0, arg1)` and Exasol `LOCATE(substring, string)` as `strpos(arg1, arg0)`, the reorder swaps which rendered slot each argument fills, never which arguments are string-position
* *AND* the previously hard-failing `Function 'strpos' requires String, but received Int64` planning error SHALL no longer occur for this shape
* *AND* an `INSTR` or `LOCATE` call carrying MORE than two arguments, `INSTR(a, b, start)`, `INSTR(a, b, start, occurrence)`, or `LOCATE(a, b, start)`, SHALL instead make the guard return `None`, declining the whole tree for EVERY argument type including all-VARCHAR, because `vs-expression` reads only `args[0]` and `args[1]` and drops the rest: coercing index 0 would let an incompletely rendered call plan successfully, converting today's loud DataFusion type error into a silently wrong position, and it SHALL therefore also correct the wrong result for an all-string `INSTR(c_varchar, 'b', 3)`, which pushed down as `strpos("C_VARCHAR", 'b')` and ignored the start position
* *AND* that beyond-two decline SHALL be reached at the broadcast join's combined WHERE filter and at the N-scan fallback's per-leg WHERE filter as well as at the single-table WHERE filter and the select-list projection, each routing the decline through its OWN already-existing self-application outcome
* *AND* this decline SHALL narrow the exposure without fixing the root cause, which is the `vs-expression` rendering defect that drops arguments beyond the second

### Scenario: CHR and UNICODECHR are excluded from the guard

* *GIVEN* a `pushdown` request carrying `CHR(<column>)` or `UNICODECHR(<column>)`
* *WHEN* the adapter builds the scan spec
* *THEN* the guard SHALL treat neither function as a governed string function, leaving its single argument unchanged and never declining on it, because that argument is a genuine integer codepoint rather than a string-position argument
* *AND* the guard SHALL still recurse into the argument, so a governed string function nested inside `CHR`/`UNICODECHR` is still coerced

### Scenario: A non-bare-column string-position argument is left unchanged as a tracked exception

* *GIVEN* a `pushdown` request carrying a governed string `function_scalar` whose string-position argument is NOT a bare `column` node, a literal such as `'x'`, a computed expression such as `c_decimal_a * 2`, or another already-string-valued function call such as the inner `TRIM` of `UPPER(TRIM(c_varchar))`
* *WHEN* the adapter builds the scan spec
* *THEN* the guard SHALL leave that argument unchanged and SHALL NOT decline on it, because its Exasol type is not resolvable from `involvedTables[0].columns`
* *AND* a numeric-valued computed argument MAY still hard-fail the DataFusion scan as it does without this guard, an accepted, accurately-scoped tracked exception, not a silent gap
* *AND* post-order recursion SHALL still reach a governed string function nested inside such an argument, so `UPPER(TRIM(c_decimal_a))` coerces the inner `TRIM`'s DECIMAL argument even though `UPPER`'s own argument is not a bare column
