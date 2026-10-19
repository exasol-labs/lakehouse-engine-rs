# Feature: Pushdown Planning, Decimal String Formatting

Pushed-down DECIMAL-to-string conversions reproduce Exasol's shortest-form formatting. The adapter dispatches on the column's Exasol type and rewrites a stringification of a bare DECIMAL column into the adapter-synthesized `decimal_to_varchar_exasol` node. It leaves every other case unchanged.

## Background

* Exasol trims trailing scale zeros when it converts a DECIMAL to text (`2912.00` becomes `'2912'`, `-272.60` becomes `'-272.6'`). DataFusion's `CAST(decimal AS VARCHAR)` and its implicit decimal-to-utf8 coercion keep the full declared scale. Trimming changes presentation only, because an Iceberg `decimal(P, S)` has a fixed scale.
* A stringified `column` node carries no `dataType` on the wire. The adapter reads column Exasol types from `involvedTables[0].columns` through `extract_all_column_types`.
* The dispatch and rewrite live in the adapter (`pushdown/support.rs`), because `vs-expression` is a syntactic JSON-to-SQL translator with no column-type context. `format_decimal_exasol_style` renders the synthesized node; see `sql-comprehension/vs-expression-translator-scalar-ops`.
* A DECIMAL column is any column whose Exasol type begins `DECIMAL`. Exasol integers are `DECIMAL(p,0)` on the wire.
* The covered stringifications are explicit `CAST(<col> AS VARCHAR/CHAR)` and implicit `CONCAT` and `LENGTH` over a bare DECIMAL column, in the select-list projection (`project_columns`) and in the filter tree.
* `rewrite_decimal_stringifications` decides per node. At a stringifier node (`function_scalar_cast` to VARCHAR/CHAR, `function_scalar` named `CONCAT` or `LENGTH`) it wraps each directly-stringified bare DECIMAL column. At every other node it changes nothing. The shared post-order rewrite primitive (`pushdown/pushdown-module-structure`) does the recursion. Exasol renders `a || b || c` as nested `CONCAT(a, CONCAT(b, c))`, so the traversal must reach inner `CONCAT` nodes.
* The rewriter never declines a filter or a select-list item.
* The filter rewrite runs after `like_subject_type_guard` and `string_function_arg_type_guard` and before `render_df_filter_safe`. It applies only to the JSON tree fed to the DataFusion filter renderer, not to the raw filter tree forwarded to Iceberg file pruning.
* The rewrite covers the single-table path, the broadcast join's combined filter, and the N-scan fallback's per-leg filter; see `pushdown-joins/pushdown-planning-join-filter-type-coercion`. The select-list path is shared with the broadcast join.
* Before the join filter surfaces were wired, a join `WHERE` filter that stringified a DECIMAL column matched DataFusion's full-scale form (`2912.00`) instead of `2912` and returned wrong rows without an error.
* The string-function guard runs before the decimal rewrite at every surface, so a coerced argument is never double-wrapped; see `pushdown-types/pushdown-planning-string-fn-type-coercion-composition`.
* Out of scope: a stringified argument that is a computed expression, a GROUP-BY key absent from the select list, and string functions that hard-fail on a DECIMAL argument.

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
* *THEN* the shared post-order traversal SHALL descend through nested `CONCAT` arguments and the rewriter's per-node decision SHALL replace each bare DECIMAL-column argument with a `decimal_to_varchar_exasol` node, leaving every non-DECIMAL argument and the surrounding `CONCAT` structure unchanged
* *AND* the rewriter SHALL NOT wrap a DECIMAL column that appears under a `CONCAT` argument only through a non-stringifying node, for example `c_decimal_a * 2` as a `CONCAT` argument stays a computed expression (a tracked exception), not a wrapped column
* *AND* the rendered projection SHALL concatenate the trimmed decimal text in place, so `id || '-' || c_decimal_a` over `30.00` yields `4-30`, not `4-30.00`

### Scenario: Implicit LENGTH over a DECIMAL column renders the trimmed form

* *GIVEN* a `pushdown` request whose select list carries a `function_scalar` item named `LENGTH` whose single argument is a bare DECIMAL `column` node
* *WHEN* the adapter builds the scan-spec projection
* *THEN* the adapter SHALL replace the DECIMAL-column argument with a `decimal_to_varchar_exasol` node before rendering, so the length is measured over the Exasol-trimmed string form
* *AND* the projected `character_length` over `30.00` SHALL yield `2`, not `5`, aligning the pushed-down length with Exasol's native `LENGTH` over the DECIMAL

### Scenario: WHERE-clause stringification of a DECIMAL column renders the trimmed form

* *GIVEN* a `pushdown` request whose filter stringifies a bare DECIMAL column via `CAST(... AS VARCHAR/CHAR)`, `CONCAT`, or `LENGTH`, for example the filter `LENGTH(c_acctbal) > 5`
* *WHEN* the adapter builds the DataFusion scan-spec filter for the single-table path, the broadcast join's combined filter, or an N-scan fallback side's per-leg filter
* *THEN* the adapter SHALL apply `rewrite_decimal_stringifications` to the filter tree, after `like_subject_type_guard` and `string_function_arg_type_guard` and before `render_df_filter_safe`, wrapping each directly-stringified bare DECIMAL column in a `decimal_to_varchar_exasol` node so the predicate matches over the Exasol-trimmed string form
* *AND* the rewrite SHALL apply ONLY to the JSON tree fed to `render_df_filter_safe`, leaving the raw filter tree forwarded to Iceberg file pruning unchanged
* *AND* the rewrite SHALL NOT decline the filter and SHALL compose with a preceding guard decline (a declined filter is never rewritten because it is no longer pushed), so the pushed count for `LENGTH(c_acctbal) > 5` matches native Exasol evaluation
* *AND* the DECIMAL column's Exasol type SHALL be resolved from the column metadata of the table that OWNS the column, the union of both involved tables' columns at the broadcast surface, that side's own columns at an N-scan per-leg surface
* *AND* a join WHERE filter that stringifies a DECIMAL column SHALL therefore no longer return rows matched against DataFusion's full-scale decimal text, which was a silent wrong answer rather than an error

### Scenario: A DECIMAL column in a non-stringifying filter context is left unchanged

* *GIVEN* a `pushdown` request whose filter references a bare DECIMAL column in a non-stringifying position, for example the comparison `c_decimal_a > 5` or the arithmetic `c_decimal_a * 2 = 10`
* *WHEN* the adapter builds the DataFusion scan-spec filter
* *THEN* the rewriter SHALL leave the DECIMAL column unchanged, injecting no `decimal_to_varchar_exasol` node, because the column is not being converted to string there
* *AND* the rendered filter SHALL be identical to the form without this rewrite

### Scenario: CAST, CONCAT, or LENGTH over a non-DECIMAL column is left unchanged

* *GIVEN* a `pushdown` request whose select list or filter stringifies a bare `column` whose Exasol type in `involvedTables[0].columns` is NOT `DECIMAL` (for example `VARCHAR`, `DATE`, or `DOUBLE`)
* *WHEN* the adapter builds the scan spec
* *THEN* `rewrite_decimal_stringifications` SHALL leave the stringification unchanged, injecting no `decimal_to_varchar_exasol` node, because only DECIMAL stringification diverges through this fix
* *AND* a `function_scalar_cast` to VARCHAR or CHAR over such a column SHALL render without a DECIMAL rewrite end to end, because no other guard governs that node shape
* *AND* a `CONCAT` or `LENGTH` over such a column MAY instead be rewritten or declined at the wired surfaces by `pushdown-types/pushdown-planning-string-fn-type-coercion`, which governs every string function's string-position arguments and runs first, a DATE argument is rewrapped as `CAST(<col> AS VARCHAR)` and a `DOUBLE`/`BOOLEAN`/`TIMESTAMP` argument declines to native Exasol evaluation, so the end-to-end rendering of `CONCAT`/`LENGTH` over a non-DECIMAL column is that feature's contract, NOT this scenario's

### Scenario: A stringified computed expression is left unchanged as a tracked exception

* *GIVEN* a `pushdown` request that stringifies a NON-bare-column argument via `CAST(... AS VARCHAR/CHAR)`, `CONCAT`, or `LENGTH`, for example the computed expression `c_acctbal * 2`, in either the select list or the filter
* *WHEN* the adapter builds the scan spec
* *THEN* the rewriter SHALL leave the stringification unchanged, because the argument's Exasol type is not resolvable from `involvedTables[0].columns`
* *AND* a DECIMAL-valued computed argument MAY still render with divergent DataFusion formatting, an accepted, accurately-scoped tracked exception, not a silent gap
