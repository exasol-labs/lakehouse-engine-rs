# Feature: Pushdown Planning — String Function Argument Type Coercion

Makes every pushed-down Exasol string scalar function type-aware in its string-position arguments. Exasol implicitly converts a numeric or DATE argument to VARCHAR before applying `UPPER`/`LOWER`/`TRIM`/`INSTR`/`LOCATE` and the rest of the family; DataFusion performs no such coercion, so a pushed-down string function over a non-string column hard-failed the scan at execution time (`F-UDF-CL-RUST-9001 … Function 'upper' requires String, but received Int64`, SQL state 22002, issue #210). This feature resolves which argument INDICES of each function sit in string position, dispatches each such bare-column argument on its Exasol type read from `involvedTables[0].columns`, and rewrites the expression JSON before rendering: string arguments pass through unchanged, DATE arguments are rewrapped in an explicit CAST-to-VARCHAR, DECIMAL arguments are rewrapped in the `decimal_to_varchar_exasol` node that reproduces Exasol's trimmed number-to-string form, and every other resolvable type declines pushdown so Exasol evaluates the expression natively.

Scope: the two render surfaces that issue #211 already covers — the select-list projection (`project_columns`) and the single-table WHERE-clause filter tree fed to `render_df_filter_safe` in `handle_pushdown`. `project_columns` is not single-table: the broadcast join reaches it through `extract_join_projection`, so the join SELECT list is in scope as well (see Background). The governed functions are `CONCAT`, `LOWER`, `UPPER`, `SUBSTR`, `TRIM`, `LTRIM`, `RTRIM`, `REPLACE`, `REPEAT`, `REVERSE`, `LPAD`, `RPAD`, `ASCII`, `INITCAP`, `LEFT`, `RIGHT`, `TRANSLATE`, `LENGTH`, `OCTET_LENGTH`, `UNICODE`, `INSTR`, and `LOCATE`. `CHR` and `UNICODECHR` are deliberately excluded: their sole argument is a genuine integer codepoint, not a string-position argument, so pushing them unchanged is correct.

Out of scope, each an accurately-scoped tracked exception rather than a silent gap:

* A string-position argument that is a literal or a computed expression rather than a bare `column` node — its Exasol type is not resolvable from `involvedTables[0].columns` (issue #223's existing scope, same convention as `vs-adapter/pushdown-planning-decimal-string-format`).
* The broadcast-join PER-LEG WHERE-clause filter path (`pushdown/joins/sql_builders.rs`), a render surface distinct from the join SELECT list (issue #223). The join SELECT list is NOT out of scope — `project_columns` reaches it.
* The ENTIRE grouped-aggregate render path — every `groupBy` element AND every non-aggregate select-list item. `detect_group_by_aggregates` renders both with bare `render_expression` and matches them by rendered-SQL string equality, and `handle_pushdown` consumes that grouped SQL directly; the grouped arm's `projection` is inert, so `project_columns` never runs against it. `SELECT UPPER(c_custkey), COUNT(*) … GROUP BY UPPER(c_custkey)` therefore still hard-fails, whether or not the key also appears in the select list (issue #227).
* The aggregate-argument render path — `parse_agg_item`'s `arg_column_or_expr` renders an aggregate's argument expression with no type guard, so `MAX(UPPER(c_custkey))` and `COUNT(UPPER(c_custkey))` still hard-fail (issue #227).
* `CHR`/`UNICODECHR` over a non-numeric argument, the mirror-image type-blindness of an integer-position argument.
* A faithful rendering of `INSTR`'s optional third (start position) and fourth (occurrence) arguments and `LOCATE`'s optional third (start position) argument, which the translator silently drops today — a wrong-result defect in argument arity, not in argument typing (issue #228). This feature does not render those arguments; it DECLINES pushdown for any `INSTR`/`LOCATE` call carrying more than two arguments, so the two wired surfaces return Exasol's native result instead of a position computed from a truncated rendering.

## Background

* An expression argument's `column` node carries no `dataType` on the wire; column Exasol types are read from `involvedTables[0].columns` via `extract_all_column_types`, which uppercases every column name.
* The type dispatch and rewrite happen in the adapter (`pushdown/support.rs`), not in `vs-expression`, because `vs-expression` is a pure syntactic JSON-to-SQL translator with no external column-type context and is shared with a sibling VS-adapter project.
* The select-list projection path is shared with the broadcast join: `extract_join_projection` (`pushdown/joins/rendering.rs`) calls `project_columns` against the disjoint union of BOTH joined tables' columns, and `joins/mod.rs`'s empty-side path calls it against the union of every involved table's columns. The new guard therefore reaches the broadcast-join SELECT list through that shared function. Unlike `vs-adapter/pushdown-planning-decimal-string-format`'s rewriter, which never declines, this guard CAN decline — and a decline there sets `needs_full_fallback`, projecting the full union of both joined tables' columns so Exasol post-processes the expression. That is correct and safe (it is the join path's established fallback shape, already reachable today via any unsupported select-list item), but it was undisclosed and untested before this feature, so this feature adds one join-projection test rather than resting on the predecessor's shared-function argument.
* String-position argument indices are per-function, because a single function mixes string-position and numeric-position arguments: `SUBSTR(str, start, length)` and `LPAD(str, length, pad)` both carry a genuinely numeric argument that MUST NOT be coerced to text.
* The per-function argument table is arity-aware and has three outcomes, not two: NOT-GOVERNED (leave the node unchanged, never decline), COERCE (a set of string-position indices), and DECLINE (governed, but unrenderable at this arity). `INSTR` and `LOCATE` carrying more than two arguments are the DECLINE case, because `vs-expression` renders only `args[0]` and `args[1]` for them and drops the rest.
* An Exasol integer is carried on the wire as `DECIMAL(p,0)`; the DECIMAL branch therefore also covers integer columns, and the trailing-zero trim is a no-op on a scale-0 value.
* `string_function_arg_type_guard` returns `Option<Json>`: `Some(tree)` means render this (possibly rewritten) tree, `None` means decline. It recurses through the shared post-order rewrite primitive (`vs-adapter/pushdown-module-structure`) rather than its own copied traversal, over every child-bearing field of this codebase's expression grammar — the array fields `expressions` / `arguments` / `results` and the single-child fields `expression` / `pattern` / `left` / `right` / `basis`. That reach is still load-bearing for the same reason: a filter-side string function sits under a comparison predicate (`UPPER(c) = 'X'` is `predicate_equal` with the function under `left`), a position no junction-only traversal reaches.
* `like_subject_type_guard` is no longer junction-only: both guards now traverse the identical curated field set through the same shared primitive, so the difference between them is confined to their per-node decision, not their reach. Where a governed string function is used as a LIKE subject, the operative reason `like_subject_type_guard` leaves it alone is that its per-node decision declines to act on a non-bare-`column` subject — not that it cannot reach the node. `vs-adapter/pushdown-planning-string-fn-type-coercion-composition` verifies this guard's ordering against `like_subject_type_guard` and `rewrite_decimal_stringifications`.
* The DECIMAL branch reuses the existing `decimal_to_varchar_exasol` node and `wrap_decimal_to_varchar` helper introduced by `vs-adapter/pushdown-planning-decimal-string-format`; the DATE branch reuses the same `function_scalar_cast` shape as `guard_like_subject`. Neither formatting rule is reimplemented.
* The DATE CAST-to-VARCHAR is Exasol-faithful only under the default `NLS_DATE_FORMAT` (`YYYY-MM-DD`), which is also DataFusion's unconditional `Date32`-to-`Utf8` form; an altered session format is the accepted tracked exception #216, already recorded for `vs-adapter/pushdown-planning-like-type-coercion`.
* Apache Iceberg's own primitive types are what make the decline branch a correctness fix rather than only a crash fix. The Iceberg table spec's Primitive Types table defines `boolean` as "True or false", `double` as "64-bit IEEE 754 floating point", and `timestamp` as "Timestamp, microsecond precision, without timezone" — none of which the spec assigns a text form. Each engine picks its own: Exasol renders BOOLEAN as `TRUE`/`FALSE` and DataFusion as `true`/`false`, and Exasol's TIMESTAMP text form is space-separated where DataFusion's is `T`-separated. Declining is therefore the only branch that cannot silently change a result.
* Iceberg's `date` is "Calendar date without timezone or time" and its `decimal(P,S)` is "Fixed-point decimal; precision P, scale S" with the requirement "Scale is fixed, precision must be 38 or less" — so a decimal value's trailing scale digits are an artifact of the fixed S, not data, and trimming them for the string form changes presentation only. Iceberg `string` is "Arbitrary-length character sequences" encoded UTF-8, which both engines render identically, so the passthrough branch is safe.

## Scenarios

### Scenario: A string-position VARCHAR or CHAR column argument pushes down unchanged

* *GIVEN* a `pushdown` request whose select list or filter carries a governed string `function_scalar` — for example `UPPER(c_varchar)` — whose string-position argument is a bare `column` node
* *AND* the column's Exasol type in `involvedTables[0].columns` is `VARCHAR(n)` or `CHAR(n)`
* *WHEN* the adapter builds the scan spec
* *THEN* the guard SHALL return the node unchanged, injecting neither a CAST nor a `decimal_to_varchar_exasol` wrapper, because DataFusion needs no coercion for a string argument
* *AND* the rendered SQL SHALL be identical to its pre-change form, so the common VARCHAR case keeps its existing pushdown

### Scenario: A string-position DECIMAL column argument renders through Exasol's trimmed decimal-to-string form

* *GIVEN* a `pushdown` request whose select list or filter carries a governed string `function_scalar` whose string-position argument is a bare `column` node — for example issue #210's repros `UPPER(c_custkey)`, `TRIM(c_custkey)`, and `LTRIM(c_acctbal)`
* *AND* the column's Exasol type in `involvedTables[0].columns` begins `DECIMAL`, which on the wire includes Exasol integers carried as `DECIMAL(p,0)`
* *WHEN* the adapter builds the scan spec
* *THEN* the guard SHALL replace that argument with a `{"type":"decimal_to_varchar_exasol","arguments":[<column>]}` node via the existing `wrap_decimal_to_varchar` helper, so the argument reaches the string function as Exasol's trailing-zero-trimmed text rather than as a hard type error
* *AND* the guard SHALL NOT emit a plain `CAST(<col> AS VARCHAR)` for a DECIMAL argument and SHALL NOT reimplement decimal formatting, because DataFusion's fixed-declared-scale rendering diverges from Exasol's trimmed form and would turn a hard failure into a silently wrong result — the node and helper that `vs-adapter/pushdown-planning-decimal-string-format` renders through `format_decimal_exasol_style` are the single owner of that conversion
* *AND* the previously hard-failing `Function 'upper' requires String, but received Int64` planning error SHALL no longer occur for this shape

### Scenario: A string-position DATE column argument is wrapped in an explicit CAST to VARCHAR

* *GIVEN* a `pushdown` request whose select list or filter carries a governed string `function_scalar` whose string-position argument is a bare `column` node — for example issue #210's repro `LOWER(l_shipdate)`
* *AND* the column's Exasol type in `involvedTables[0].columns` is `DATE`
* *WHEN* the adapter builds the scan spec
* *THEN* the guard SHALL replace that argument with a `function_scalar_cast` node carrying `dataType` `{"type":"VARCHAR"}`, rendering `CAST(<col> AS VARCHAR)`, exactly the shape `guard_like_subject` already emits for a DATE LIKE subject
* *AND* the emitted match semantics SHALL equal Exasol's implicit DATE-to-VARCHAR conversion under the default `NLS_DATE_FORMAT` of `YYYY-MM-DD`, which is the ISO-8601 text form both engines render for the Iceberg `date` primitive
* *AND* under a session that has altered `NLS_DATE_FORMAT` away from that default the pushed-down result MAY diverge from native Exasol evaluation, because DataFusion's `CAST(Date32 AS VARCHAR)` is unconditionally ISO `YYYY-MM-DD` and the pushdown request carries no session NLS format — the accepted tracked exception #216, not a silent gap

### Scenario: A non-coercible resolvable column type in a WHERE-clause string function declines the whole filter

* *GIVEN* a `pushdown` request whose filter carries a governed string `function_scalar` whose string-position argument is a bare `column` node
* *AND* the column's Exasol type in `involvedTables[0].columns` is resolvable but is none of VARCHAR, CHAR, DATE, or DECIMAL — for example `BOOLEAN`, `DOUBLE PRECISION`, or `TIMESTAMP`
* *WHEN* the adapter builds the single-table DataFusion scan-spec filter
* *THEN* the guard SHALL return `None`, declining pushdown of the WHOLE top-level filter so no `filter` is emitted in the common spec and Exasol evaluates the entire predicate natively
* *AND* the guard SHALL NOT inject a CAST for such an argument, because DataFusion's text rendering of BOOLEAN (`true`) and TIMESTAMP (`T`-separated) diverges from Exasol's (`TRUE`, space-separated) and would silently change which rows match
* *AND* a decline reached at any nesting depth SHALL propagate to the top-level filter, mirroring the all-or-nothing untranslatable-predicate backstop that `like_subject_type_guard` already uses, and SHALL apply ONLY to the JSON tree fed to `render_df_filter_safe`, leaving the raw filter tree forwarded to Iceberg file pruning unchanged

### Scenario: A non-coercible resolvable column type in a select-list string function falls back to the full base row

* *GIVEN* a `pushdown` request whose select list carries a governed string `function_scalar` whose string-position argument is a bare `column` node of a resolvable non-coercible type — for example `UPPER(c_double)`
* *WHEN* the adapter builds the scan-spec projection in `project_columns`
* *THEN* the `None` decline SHALL set the existing `needs_full_fallback` flag, projecting the full base column set so Exasol post-processes the expression itself
* *AND* the decline SHALL NOT propagate as an error out of `project_columns`, because the full-row fallback is the established correctness backstop for a select-list item the adapter cannot push
* *AND* the returned value SHALL equal native Exasol evaluation of the same expression, which the hard-failing pre-change pushdown never produced
* *AND* the same decline reached through the broadcast join's shared use of `project_columns` (`extract_join_projection`) SHALL set `needs_full_fallback` over the disjoint UNION of both joined tables' columns, so the join wrapper projects every column of both sides and Exasol post-processes the expression, with no error and no per-leg SQL change

### Scenario: A string-position argument whose column name does not resolve declines fail-safe

* *GIVEN* a `pushdown` request whose select list or filter carries a governed string `function_scalar` whose string-position argument is a bare `column` node
* *AND* the column's name is NOT found in `involvedTables[0].columns`, or the node carries no `name`
* *WHEN* the adapter builds the scan spec
* *THEN* the guard SHALL return `None`, because it cannot prove the argument is a string and an unproven non-string argument would hard-fail the DataFusion scan
* *AND* the name lookup SHALL uppercase the argument's column name before matching, mirroring `extract_all_column_types`'s uppercasing, so a case-mismatched name resolves rather than spuriously declining

### Scenario: Only string-position argument indices are coerced

* *GIVEN* a `pushdown` request carrying a governed string `function_scalar` that mixes string-position and numeric-position arguments over bare columns — `SUBSTR(str_col, int_col, int_col)`, `REPEAT(str_col, int_col)`, `LEFT(str_col, int_col)`, `RIGHT(str_col, int_col)`, or `LPAD(str_col, int_col, pad_col)`
* *WHEN* the adapter builds the scan spec
* *THEN* the guard SHALL resolve string-position indices per function — all arguments for `CONCAT`, `TRIM`, `LTRIM`, `RTRIM`, `REPLACE`, and `TRANSLATE`; index 0 only for `LOWER`, `UPPER`, `ASCII`, `INITCAP`, `REVERSE`, `LENGTH`, `OCTET_LENGTH`, `UNICODE`, `SUBSTR`, `REPEAT`, `LEFT`, and `RIGHT`; indices 0 and 2 for `LPAD` and `RPAD` when index 2 is present
* *AND* the guard SHALL leave every non-string-position argument untouched, so a numeric length or offset argument is neither coerced to text nor able to trigger a decline
* *AND* a `LPAD`/`RPAD` call carrying only two arguments SHALL coerce index 0 only, without indexing past the end of the argument list

### Scenario: INSTR and LOCATE coerce their first two arguments and decline beyond two

* *GIVEN* a `pushdown` request carrying `INSTR(a, b)` or `LOCATE(a, b)` where either bare-column argument is a non-string column — for example issue #210's repro `INSTR(c_custkey, '1')`
* *WHEN* the adapter builds the scan spec
* *THEN* the guard SHALL treat indices 0 and 1 as string-position for both functions, coercing or declining each independently
* *AND* the index assignment SHALL be independent of the translator's render-time argument reorder, because `vs-expression` renders Exasol `INSTR(string, substring)` as `strpos(arg0, arg1)` and Exasol `LOCATE(substring, string)` as `strpos(arg1, arg0)` — the reorder swaps which rendered slot each argument fills, never which arguments are string-position
* *AND* the previously hard-failing `Function 'strpos' requires String, but received Int64` planning error SHALL no longer occur for this shape
* *AND* an `INSTR` or `LOCATE` call carrying MORE than two arguments — `INSTR(a, b, start)`, `INSTR(a, b, start, occurrence)`, or `LOCATE(a, b, start)` — SHALL instead make the guard return `None`, declining the whole tree for EVERY argument type including all-VARCHAR, because `vs-expression` reads only `args[0]` and `args[1]` and drops the rest (issue #228): coercing index 0 would let an incompletely rendered call plan successfully, converting today's loud DataFusion type error into a silently wrong position, and it SHALL therefore also correct the pre-existing wrong result for an all-string `INSTR(c_varchar, 'b', 3)`, which pushed down as `strpos("C_VARCHAR", 'b')` and ignored the start position

### Scenario: CHR and UNICODECHR are excluded from the guard

* *GIVEN* a `pushdown` request carrying `CHR(<column>)` or `UNICODECHR(<column>)`
* *WHEN* the adapter builds the scan spec
* *THEN* the guard SHALL treat neither function as a governed string function, leaving its single argument unchanged and never declining on it, because that argument is a genuine integer codepoint rather than a string-position argument
* *AND* the guard SHALL still recurse into the argument, so a governed string function nested inside `CHR`/`UNICODECHR` is still coerced

### Scenario: A non-bare-column string-position argument is left unchanged as a tracked exception

* *GIVEN* a `pushdown` request carrying a governed string `function_scalar` whose string-position argument is NOT a bare `column` node — a literal such as `'x'`, a computed expression such as `c_decimal_a * 2`, or another already-string-valued function call such as the inner `TRIM` of `UPPER(TRIM(c_varchar))`
* *WHEN* the adapter builds the scan spec
* *THEN* the guard SHALL leave that argument unchanged and SHALL NOT decline on it, because its Exasol type is not resolvable from `involvedTables[0].columns`
* *AND* a numeric-valued computed argument MAY still hard-fail the DataFusion scan exactly as before this change — an accepted, accurately-scoped tracked exception (#223), not a silent gap
* *AND* post-order recursion SHALL still reach a governed string function nested inside such an argument, so `UPPER(TRIM(c_decimal_a))` coerces the inner `TRIM`'s DECIMAL argument even though `UPPER`'s own argument is not a bare column
