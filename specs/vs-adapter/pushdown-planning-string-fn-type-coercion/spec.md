# Feature: Pushdown Planning — String Function Argument Type Coercion

Makes every pushed-down Exasol string scalar function type-aware in its string-position arguments. Exasol implicitly converts a numeric or DATE argument to VARCHAR before applying `UPPER`/`LOWER`/`TRIM`/`INSTR`/`LOCATE` and the rest of the family; DataFusion performs no such coercion, so a pushed-down string function over a non-string column hard-failed the scan at execution time (`F-UDF-CL-RUST-9001 … Function 'upper' requires String, but received Int64`, SQL state 22002, issue #210). This feature resolves which argument INDICES of each function sit in string position, dispatches each such bare-column argument on its Exasol type read from `involvedTables[0].columns`, and rewrites the expression JSON before rendering: string arguments pass through unchanged, DATE arguments are rewrapped in an explicit CAST-to-VARCHAR, DECIMAL arguments are rewrapped in the `decimal_to_varchar_exasol` node that reproduces Exasol's trimmed number-to-string form, and every other resolvable type declines pushdown so Exasol evaluates the expression natively.

Scope: the two render surfaces that issue #211 already covers — the select-list projection (`project_columns`) and the single-table WHERE-clause filter tree fed to `render_df_filter_safe` in `handle_pushdown`. `project_columns` is not single-table: the broadcast join reaches it through `extract_join_projection`, so the join SELECT list is in scope as well (see Background). The governed functions are `CONCAT`, `LOWER`, `UPPER`, `SUBSTR`, `TRIM`, `LTRIM`, `RTRIM`, `REPLACE`, `REPEAT`, `REVERSE`, `LPAD`, `RPAD`, `ASCII`, `INITCAP`, `LEFT`, `RIGHT`, `TRANSLATE`, `LENGTH`, `OCTET_LENGTH`, `UNICODE`, `INSTR`, and `LOCATE`. `CHR` and `UNICODECHR` are deliberately excluded: their sole argument is a genuine integer codepoint, not a string-position argument, so pushing them unchanged is correct.

Out of scope, each an accurately-scoped tracked exception rather than a silent gap:

* A string-position argument that is a literal or a computed expression rather than a bare `column` node — its Exasol type is not resolvable from `involvedTables[0].columns` (issue #223's existing scope, same convention as `vs-adapter/pushdown-planning-decimal-string-format`).
* The ENTIRE grouped-aggregate render path — every `groupBy` element AND every non-aggregate select-list item. `detect_group_by_aggregates` renders both with bare `render_expression` and matches them by rendered-SQL string equality, and `handle_pushdown` consumes that grouped SQL directly; the grouped arm's `projection` is inert, so `project_columns` never runs against it. `SELECT UPPER(c_custkey), COUNT(*) … GROUP BY UPPER(c_custkey)` therefore still hard-fails, whether or not the key also appears in the select list (issue #227).
* The aggregate-argument render path — `parse_agg_item`'s `arg_column_or_expr` renders an aggregate's argument expression with no type guard, so `MAX(UPPER(c_custkey))` and `COUNT(UPPER(c_custkey))` still hard-fail (issue #227).
* `CHR`/`UNICODECHR` over a non-numeric argument, the mirror-image type-blindness of an integer-position argument.
* A faithful rendering of `INSTR`'s optional third (start position) and fourth (occurrence) arguments and `LOCATE`'s optional third (start position) argument, which the translator silently drops today — a wrong-result defect in argument arity, not in argument typing (issue #228). This feature does not render those arguments; it DECLINES pushdown for any `INSTR`/`LOCATE` call carrying more than two arguments, so the two wired surfaces return Exasol's native result instead of a position computed from a truncated rendering.

## Background

* This delta corrects what a WHERE-clause decline MEANS. The guard's decline scope, its dispatch
  table, and its traversal are unchanged; only the caller's handling of a decline changes. A
  declined filter is no longer omitted and left to a non-existent Exasol backstop — the request
  routes to the qualified single-table wrapper, which renders the ORIGINAL predicate tree as its own
  `WHERE`. See `vs-adapter/pushdown-declined-filter-self-apply`.
* The `INSTR`/`LOCATE`-with-more-than-two-arguments decline is one route into that corrected
  handling, so the structural fix reaches it as a side effect of fixing the caller. Issue #228,
  which independently asserted the same false backstop for that shape, is NOT closed by this delta:
  it is re-verified and adjudicated on its own, and nothing here should be read as having done so.
* The select-list decline is unaffected. It sets the full-base-row fallback flag, which is a
  different and already-correct mechanism: the adapter returns the columns and Exasol computes the
  item over them, which it does because the adapter never claimed the item.
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
* This delta amends exactly ONE clause of ONE scenario — the lookup-normalization clause of "A string-position argument whose column name does not resolve declines fail-safe" — and nothing else. No argument-index resolution, no per-type dispatch, no traversal, no decline meaning, no `vs-expression` rendering, and no generated SQL changes. The guard's behavior is byte-identical; only the recorded NAME of the normalization's owner changes.
* This delta SUPERSEDES the clause "*AND* the name lookup SHALL uppercase the argument's column name before matching, mirroring `extract_all_column_types`'s uppercasing, so a case-mismatched name resolves rather than spuriously declining". One thing in it ceased to be true. The convention's owner is no longer `extract_all_column_types`: issue #265 extracted the shared `column_exa_type` helper in `pushdown/support.rs`, which owns the node -> uppercased name -> `col_types` scan for all three type-rewrite guards, so `coerce_string_position_arg` no longer mirrors a convention — it calls the one implementation of it.
* A stale OWNER name is the same defect class as a stale line number, which is why this clause is amended rather than excluded. The sibling `vs-adapter/pushdown-planning-like-type-coercion` clause carried both defects, this one carries only the first; leaving it standing would record a mirror relationship the code no longer has. The amended clause names an owning function rather than a mirrored one, and cites no line number.
* The amended clause records the same normalization requirement it always did. A case-mismatched argument column name MUST still resolve rather than spuriously declining, and the fail-safe decline on a genuine miss is unchanged.
* `vs-adapter/pushdown-module-structure` owns `column_exa_type`'s contract — its `Option<&str>` return, its Unicode `to_uppercase` fold, and its exclusion of the node's `type` tag test. The two `col_types` builders' fold divergence produces no join-path miss for any column name the adapter can declare; `vs-adapter/pushdown-module-structure` records the live capture that established this. This feature consumes that contract and SHALL NOT restate it: it records only that the normalization its decline depends on now has one owner, named rather than mirrored.
* Background bullet "An expression argument's `column` node carries no `dataType` on the wire; column Exasol types are read from `involvedTables[0].columns` via `extract_all_column_types`, which uppercases every column name" stays accurate and is NOT amended. `extract_all_column_types` still produces the list and still uppercases; what moves is the CONSUMING lookup's fold, into `column_exa_type`.
* Apache Iceberg spec check: NOT implicated. This delta changes no type mapping, no schema handling, no scan, and no pushdown decision — it renames the owner of a case-normalization step inside the adapter's own column-name lookup. The Iceberg determination this feature already records for its DATE, DECIMAL, and decline dispatch is unaffected and stands unedited.
* This feature's recorded out-of-scope bullet "The broadcast-join PER-LEG WHERE-clause filter path
  (`pushdown/joins/sql_builders.rs`), a render surface distinct from the join SELECT list (issue
  #223)" is REPLACED: that surface is now IN scope. Both join WHERE-filter render surfaces — the
  broadcast join's combined filter and the N-scan fallback's per-leg filter — run the guard through
  the shared type-rewrite pipeline, so the guard's decline and coercion reach them exactly as they
  reach the single-table WHERE filter. See `vs-adapter/pushdown-planning-join-filter-type-coercion`
  (issue #215). Issue #223's slice 2 closes with it; #223's slices 1 (computed-expression arguments)
  and 3 (GROUP-BY-only keys) remain open and out of scope here.
* The guard itself is untouched — no dispatch-table, arity-table, or traversal change. Only its
  reachable surface set grows, and it grows by wiring, not by new guard code.
* Issue #228's exposure NARROWS as a direct consequence: the `INSTR`/`LOCATE`-beyond-two-arguments
  decline now also covers the two join WHERE surfaces, so those surfaces return Exasol's native
  result instead of a position computed from a rendering that silently drops the third and fourth
  arguments. #228 is NOT closed — its root cause is the rendering defect in
  `crates/vs-expression`, untouched here, and any render surface still unwired to the guard remains
  exposed. Nothing in this delta should be read as having adjudicated #228.
* The grouped-aggregate render path, the aggregate-argument render path, `CHR`/`UNICODECHR`, and a
  non-bare-column string-position argument all remain out of scope, unchanged by this delta.

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
* *THEN* the guard SHALL return `None`, declining pushdown of the WHOLE top-level filter so no `filter` is emitted in the common spec
* *AND* the adapter SHALL route the request to the qualified single-table wrapper and render the ORIGINAL predicate tree as that wrapper's own `WHERE` — REPLACING the recorded "and Exasol evaluates the entire predicate natively", which assumed an Exasol-side re-check of a delegated predicate that does not occur
* *AND* the guard SHALL NOT inject a CAST for such an argument, because DataFusion's text rendering of BOOLEAN (`true`) and TIMESTAMP (`T`-separated) diverges from Exasol's (`TRUE`, space-separated) and would silently change which rows match
* *AND* a decline reached at any nesting depth SHALL propagate to the top-level filter and SHALL apply ONLY to the JSON tree fed to `render_df_filter_safe`, leaving the raw filter tree forwarded to Iceberg file pruning unchanged — REPLACING the recorded "mirroring the all-or-nothing untranslatable-predicate backstop that `like_subject_type_guard` already uses", which named a backstop that does not exist; the all-or-nothing SCOPE is retained, its named justification is not
* *AND* the returned rows SHALL equal native Exasol evaluation of the same query

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
* *AND* the name lookup SHALL uppercase the argument's column name before matching, so a case-mismatched name resolves rather than spuriously declining
* *AND* that normalization SHALL be owned by exactly ONE helper, `column_exa_type` (`pushdown/support.rs`), which every type-rewrite guard calls rather than reimplementing — so this clause names an owner instead of asserting that the guard MIRRORS one. The superseded form said the lookup mirrors `extract_all_column_types`'s uppercasing, which stopped being true when issue #265 rewired this guard onto the shared helper

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
* *AND* that beyond-two decline SHALL be reached at the broadcast join's combined WHERE filter and at the N-scan fallback's per-leg WHERE filter as well as at the single-table WHERE filter and the select-list projection, each routing the decline through its OWN already-existing self-application outcome — REPLACING this feature's recorded out-of-scope bullet naming the join per-leg WHERE-filter path as a deferred surface (issue #223 slice 2, wired by issue #215)
* *AND* narrowing #228's exposure this way SHALL NOT be recorded as closing #228, whose root cause is the `crates/vs-expression` rendering defect this delta does not touch

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
