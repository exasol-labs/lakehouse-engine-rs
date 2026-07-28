# Feature: Pushdown Planning — Decimal String Formatting

Makes pushed-down DECIMAL→string conversions reproduce Exasol's shortest-form formatting. Exasol trims trailing scale zeros when it converts a DECIMAL to text (`2912.00`→`'2912'`, `-272.60`→`'-272.6'`); DataFusion's `CAST(decimal AS VARCHAR)` and its implicit decimal→utf8 coercion both render the full declared scale, so a pushed-down expression that stringifies a DECIMAL column silently returned different results (issue #211). A stringified column's `dataType` never crosses the wire; column Exasol types exist only in `involvedTables[0].columns`. Mirroring the LIKE type-coercion fix (`vs-adapter/pushdown-planning-like-type-coercion`), this feature dispatches on the column's Exasol type in the adapter and rewrites the expression JSON before rendering: a stringification of a bare DECIMAL column is rewrapped in the adapter-synthesized `decimal_to_varchar_exasol` node (rendered via `format_decimal_exasol_style`, see `sql-comprehension/vs-expression-translator-scalar-ops`); every other case is left unchanged.

Scope: the single-table scan paths for the three stringifications confirmed to silently coerce rather than hard-fail on a DECIMAL argument — explicit `CAST(<col> AS VARCHAR/CHAR)`, and implicit `CONCAT`/`LENGTH` over a bare DECIMAL column — in BOTH the select-list projection (`project_columns`) and the WHERE-clause filter tree (`handle_pushdown`'s single filter render). A single shared recursive rewriter (`rewrite_decimal_stringifications`) walks each tree and rewrites the bare DECIMAL column wherever it is directly stringified, at any nesting depth. Out of scope, tracked in issue #223: a stringified argument that is a computed expression rather than a bare column, the broadcast-join per-leg filter path, a GROUP-BY key absent from the select list, and every other string function that hard-fails on a DECIMAL argument (issue #210).

## Background

* A stringified `column` node carries no `dataType` on the wire; column Exasol types are read from `involvedTables[0].columns` via `extract_all_column_types`.
* The type dispatch and rewrite happen in the adapter (`pushdown/support.rs`), not in `vs-expression`, because `vs-expression` is a pure syntactic JSON-to-SQL translator with no external column-type context and is shared with a sibling VS-adapter project.
* A DECIMAL column is any column whose Exasol type begins `DECIMAL`, which on the wire includes Exasol integers (carried as `DECIMAL(p,0)`; Exasol has no distinct integer type).
* `rewrite_decimal_stringifications` is a recursive tree walk applied to any expression or predicate node. At a stringifier node (`function_scalar_cast` to VARCHAR/CHAR, `function_scalar` named `CONCAT`, `function_scalar` named `LENGTH`) it wraps each directly-stringified argument that is a bare DECIMAL column in a `decimal_to_varchar_exasol` node; at every other node it recurses into child expressions without wrapping, so a DECIMAL column in a non-stringifying context (arithmetic, a comparison operand) is never wrapped. Nesting is handled by the recursion itself: Exasol renders `a || b || c` as nested `CONCAT(a, CONCAT(b, c))`, so a DECIMAL column reached only through an inner `CONCAT` is rewritten when the walk descends into that inner node.
* The filter rewrite is composed after `like_subject_type_guard` and applies ONLY to the JSON tree fed to the DataFusion filter renderer (`render_df_filter_safe`); the raw filter tree forwarded to Iceberg file pruning is left untouched. The rewrite never declines a filter — it only rewrites stringification points in place.
* The select-list projection path is shared with the broadcast join (`project_columns` resolves a projected column's type from whichever involved table owns it), so a join SELECT-list decimal stringification is structurally covered by the same rewriter; this plan adds no join-specific test, and the broadcast-join per-leg FILTER path (`joins/sql_builders.rs`) is a separate render surface left out of scope (#223).
* Apache Iceberg carries decimals as the `decimal(P, S)` primitive with a fixed scale S ("Fixed-point decimal; precision P, scale S. Scale is fixed and precision must be 38 or less" — Iceberg table spec, Primitive Types), so a stored value's trailing-zero digits in that scale are a formatting artifact of S, not data; trimming them for the string form changes presentation only, never the decimal value, matching Exasol's own conversion.

## Scenarios

### Scenario: Explicit CAST of a DECIMAL column to VARCHAR renders the trimmed form

* *GIVEN* a `pushdown` request whose select list carries a `function_scalar_cast` item with target `dataType` `VARCHAR` or `CHAR` whose single argument is a bare `column` node
* *AND* the column's Exasol type in `involvedTables[0].columns` is `DECIMAL(p,s)`
* *WHEN* the adapter builds the scan-spec projection
* *THEN* the adapter SHALL replace the `function_scalar_cast` node with a `decimal_to_varchar_exasol` node wrapping the same `column` argument before rendering, so the projected SQL trims trailing scale zeros to Exasol's shortest form
* *AND* the `project_columns` select-list dispatch SHALL recognize the top-level `decimal_to_varchar_exasol` node as a renderable scalar item and route it through the expression translator, NOT into the full-row fallback
* *AND* the projected EMITS column type SHALL remain the item's declared `selectListDataTypes` text type, unchanged by the rewrite

### Scenario: Implicit CONCAT over a DECIMAL column renders the trimmed form, including nested concatenation

* *GIVEN* a `pushdown` request whose select list carries a `function_scalar` item named `CONCAT` that stringifies a bare DECIMAL `column` node, whether that column is a direct argument of the top `CONCAT` node or of an inner `CONCAT` produced by chained `||` (Exasol renders `id || '-' || c_decimal_a` as nested `CONCAT("ID", CONCAT('-', "C_DECIMAL_A"))`)
* *WHEN* the adapter builds the scan-spec projection
* *THEN* the recursive rewriter SHALL descend through nested `CONCAT` arguments and replace each bare DECIMAL-column argument with a `decimal_to_varchar_exasol` node, leaving every non-DECIMAL argument and the surrounding `CONCAT` structure unchanged
* *AND* the rewriter SHALL NOT wrap a DECIMAL column that appears under a `CONCAT` argument only through a non-stringifying node — for example `c_decimal_a * 2` as a `CONCAT` argument stays a computed expression (a tracked exception, #223), not a wrapped column
* *AND* the rendered projection SHALL concatenate the trimmed decimal text in place, so `id || '-' || c_decimal_a` over `30.00` yields `4-30`, not `4-30.00`

### Scenario: Implicit LENGTH over a DECIMAL column renders the trimmed form

* *GIVEN* a `pushdown` request whose select list carries a `function_scalar` item named `LENGTH` whose single argument is a bare DECIMAL `column` node
* *WHEN* the adapter builds the scan-spec projection
* *THEN* the adapter SHALL replace the DECIMAL-column argument with a `decimal_to_varchar_exasol` node before rendering, so the length is measured over the Exasol-trimmed string form
* *AND* the projected `character_length` over `30.00` SHALL yield `2`, not `5`, aligning the pushed-down length with Exasol's native `LENGTH` over the DECIMAL

### Scenario: WHERE-clause stringification of a DECIMAL column renders the trimmed form

* *GIVEN* a `pushdown` request whose filter stringifies a bare DECIMAL column via `CAST(... AS VARCHAR/CHAR)`, `CONCAT`, or `LENGTH` — for example the filter `LENGTH(c_acctbal) > 5` (issue #211's headline COUNT-divergence repro)
* *WHEN* the adapter builds the single-table DataFusion scan-spec filter
* *THEN* the adapter SHALL apply `rewrite_decimal_stringifications` to the filter tree, after `like_subject_type_guard` and before `render_df_filter_safe`, wrapping each directly-stringified bare DECIMAL column in a `decimal_to_varchar_exasol` node so the predicate matches over the Exasol-trimmed string form
* *AND* the rewrite SHALL apply ONLY to the JSON tree fed to `render_df_filter_safe`, leaving the raw filter tree forwarded to Iceberg file pruning unchanged
* *AND* the rewrite SHALL NOT decline the filter and SHALL compose with a preceding `like_subject_type_guard` decline (a declined filter is never rewritten because it is no longer pushed), so the pushed count for `LENGTH(c_acctbal) > 5` matches native Exasol evaluation

### Scenario: A DECIMAL column in a non-stringifying filter context is left unchanged

* *GIVEN* a `pushdown` request whose filter references a bare DECIMAL column in a non-stringifying position — for example the comparison `c_decimal_a > 5` or the arithmetic `c_decimal_a * 2 = 10`
* *WHEN* the adapter builds the DataFusion scan-spec filter
* *THEN* the rewriter SHALL leave the DECIMAL column unchanged, injecting no `decimal_to_varchar_exasol` node, because the column is not being converted to string there
* *AND* the rendered filter SHALL be identical to its pre-change form

### Scenario: CAST, CONCAT, or LENGTH over a non-DECIMAL column is left unchanged

* *GIVEN* a `pushdown` request whose select list or filter stringifies a bare `column` whose Exasol type in `involvedTables[0].columns` is NOT `DECIMAL` (for example `VARCHAR`, `DATE`, or `DOUBLE`)
* *WHEN* the adapter builds the scan spec
* *THEN* `rewrite_decimal_stringifications` SHALL leave the stringification unchanged, injecting no `decimal_to_varchar_exasol` node, because only DECIMAL stringification diverges through this fix
* *AND* a `function_scalar_cast` to VARCHAR or CHAR over such a column SHALL render exactly as it did before this change end to end, because no other guard governs that node shape
* *AND* a `CONCAT` or `LENGTH` over such a column MAY instead be rewritten or declined at the wired surfaces by `vs-adapter/pushdown-planning-string-fn-type-coercion`, which governs every string function's string-position arguments and runs first — a DATE argument is rewrapped as `CAST(<col> AS VARCHAR)` and a `DOUBLE`/`BOOLEAN`/`TIMESTAMP` argument declines to native Exasol evaluation, so the end-to-end rendering of `CONCAT`/`LENGTH` over a non-DECIMAL column is that feature's contract, NOT this scenario's

### Scenario: A stringified computed expression is left unchanged as a tracked exception

* *GIVEN* a `pushdown` request that stringifies a NON-bare-column argument via `CAST(... AS VARCHAR/CHAR)`, `CONCAT`, or `LENGTH` — for example the computed expression `c_acctbal * 2` — in either the select list or the filter
* *WHEN* the adapter builds the scan spec
* *THEN* the rewriter SHALL leave the stringification unchanged, because the argument's Exasol type is not resolvable from `involvedTables[0].columns`
* *AND* a DECIMAL-valued computed argument MAY still render with divergent DataFusion formatting — an accepted, accurately-scoped tracked exception (#223), not a silent gap
