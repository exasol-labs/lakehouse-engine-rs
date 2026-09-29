<!-- DELTA:CHANGED -->
# Feature: Pushdown Planning — Decimal String Formatting

Makes every pushed-down DECIMAL→string conversion reproduce Exasol's shortest form. Exasol trims
trailing scale zeros when it converts a DECIMAL to text (`2912.00`→`'2912'`,
`-272.60`→`'-272.6'`). DataFusion's `CAST(decimal AS VARCHAR)` and its implicit decimal→utf8
coercion keep the full declared scale, so their text differs from Exasol's (issue #211). The
DataFusion dialect routes every string-converted argument through `exa_to_varchar`, which trims a
DECIMAL by its Arrow type, on every surface that renders DataFusion SQL and for a computed DECIMAL
argument as well as a bare column (issue #227).
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
## Background

* The stringifications this feature covers are the string CAST (`CAST(<x> AS VARCHAR/CHAR)`) and
  the implicit conversions of `CONCAT` (Exasol's `||`) and `LENGTH`, the two string functions that
  convert silently rather than fail.
* `sql-comprehension/vs-expression-translator-string-conversion` owns which positions convert: every
  `CONCAT` level of Exasol's nested `a || b || c`, and no non-stringifying position such as
  arithmetic, a comparison operand, or a CAST to a non-string target.
* `datafusion-scan/scan-execution-exa-to-varchar` owns the trim, its DECIMAL scenario pins the text,
  and its Background records the Iceberg and Delta spec check and the 37- and 38-digit decimal
  trade-off.
* An Exasol integer arrives as `DECIMAL(p,0)`. The trim is a no-op on it.
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:REMOVED -->
### Scenario: Explicit CAST of a DECIMAL column to VARCHAR renders the trimmed form

* *GIVEN* a `pushdown` request whose select list carries a `function_scalar_cast` item with target `dataType` `VARCHAR` or `CHAR` whose single argument is a bare `column` node
* *AND* the column's Exasol type in `involvedTables[0].columns` is `DECIMAL(p,s)`
* *WHEN* the adapter builds the scan-spec projection
* *THEN* the adapter SHALL replace the `function_scalar_cast` node with a `decimal_to_varchar_exasol` node wrapping the same `column` argument before rendering, so the projected SQL trims trailing scale zeros to Exasol's shortest form
* *AND* the `project_columns` select-list dispatch SHALL recognize the top-level `decimal_to_varchar_exasol` node as a renderable scalar item and route it through the expression translator, NOT into the full-row fallback
* *AND* the projected EMITS column type SHALL remain the item's declared `selectListDataTypes` text type, unchanged by the rewrite
<!-- /DELTA:REMOVED -->

<!-- DELTA:REMOVED -->
### Scenario: Implicit CONCAT over a DECIMAL column renders the trimmed form, including nested concatenation

* *GIVEN* a `pushdown` request whose select list carries a `function_scalar` item named `CONCAT` that stringifies a bare DECIMAL `column` node, whether that column is a direct argument of the top `CONCAT` node or of an inner `CONCAT` produced by chained `||` (Exasol renders `id || '-' || c_decimal_a` as nested `CONCAT("ID", CONCAT('-', "C_DECIMAL_A"))`)
* *WHEN* the adapter builds the scan-spec projection
* *THEN* the shared post-order traversal SHALL descend through nested `CONCAT` arguments and the rewriter's per-node decision SHALL replace each bare DECIMAL-column argument with a `decimal_to_varchar_exasol` node, leaving every non-DECIMAL argument and the surrounding `CONCAT` structure unchanged
* *AND* the rewriter SHALL NOT wrap a DECIMAL column that appears under a `CONCAT` argument only through a non-stringifying node — for example `c_decimal_a * 2` as a `CONCAT` argument stays a computed expression (a tracked exception, #223), not a wrapped column
* *AND* the rendered projection SHALL concatenate the trimmed decimal text in place, so `id || '-' || c_decimal_a` over `30.00` yields `4-30`, not `4-30.00`, byte-identical to its pre-refactor output
<!-- /DELTA:REMOVED -->

<!-- DELTA:REMOVED -->
### Scenario: Implicit LENGTH over a DECIMAL column renders the trimmed form

* *GIVEN* a `pushdown` request whose select list carries a `function_scalar` item named `LENGTH` whose single argument is a bare DECIMAL `column` node
* *WHEN* the adapter builds the scan-spec projection
* *THEN* the adapter SHALL replace the DECIMAL-column argument with a `decimal_to_varchar_exasol` node before rendering, so the length is measured over the Exasol-trimmed string form
* *AND* the projected `character_length` over `30.00` SHALL yield `2`, not `5`, aligning the pushed-down length with Exasol's native `LENGTH` over the DECIMAL
<!-- /DELTA:REMOVED -->

<!-- DELTA:REMOVED -->
### Scenario: WHERE-clause stringification of a DECIMAL column renders the trimmed form

* *GIVEN* a `pushdown` request whose filter stringifies a bare DECIMAL column via `CAST(... AS VARCHAR/CHAR)`, `CONCAT`, or `LENGTH` — for example the filter `LENGTH(c_acctbal) > 5` (issue #211's headline COUNT-divergence repro)
* *WHEN* the adapter builds the DataFusion scan-spec filter for the single-table path, the broadcast join's combined filter, or an N-scan fallback side's per-leg filter
* *THEN* the adapter SHALL apply `rewrite_decimal_stringifications` to the filter tree, after `like_subject_type_guard` and `string_function_arg_type_guard` and before `render_df_filter_safe`, wrapping each directly-stringified bare DECIMAL column in a `decimal_to_varchar_exasol` node so the predicate matches over the Exasol-trimmed string form
* *AND* the rewrite SHALL apply ONLY to the JSON tree fed to `render_df_filter_safe`, leaving the raw filter tree forwarded to Iceberg file pruning unchanged
* *AND* the rewrite SHALL NOT decline the filter and SHALL compose with a preceding guard decline (a declined filter is never rewritten because it is no longer pushed), so the pushed count for `LENGTH(c_acctbal) > 5` matches native Exasol evaluation
* *AND* the DECIMAL column's Exasol type SHALL be resolved from the column metadata of the table that OWNS the column — the union of both involved tables' columns at the broadcast surface, that side's own columns at an N-scan per-leg surface — REPLACING this feature's recorded scope statement deferring the join per-leg filter path to issue #223 (slice 2, wired by issue #215)
* *AND* a join WHERE filter that stringifies a DECIMAL column SHALL therefore no longer return rows matched against DataFusion's full-scale decimal text, which was a silent wrong answer rather than an error
<!-- /DELTA:REMOVED -->

<!-- DELTA:REMOVED -->
### Scenario: A DECIMAL column in a non-stringifying filter context is left unchanged

* *GIVEN* a `pushdown` request whose filter references a bare DECIMAL column in a non-stringifying position — for example the comparison `c_decimal_a > 5` or the arithmetic `c_decimal_a * 2 = 10`
* *WHEN* the adapter builds the DataFusion scan-spec filter
* *THEN* the rewriter SHALL leave the DECIMAL column unchanged, injecting no `decimal_to_varchar_exasol` node, because the column is not being converted to string there
* *AND* the rendered filter SHALL be identical to its pre-change form
<!-- /DELTA:REMOVED -->

<!-- DELTA:REMOVED -->
### Scenario: CAST, CONCAT, or LENGTH over a non-DECIMAL column is left unchanged

* *GIVEN* a `pushdown` request whose select list or filter stringifies a bare `column` whose Exasol type in `involvedTables[0].columns` is NOT `DECIMAL` (for example `VARCHAR`, `DATE`, or `DOUBLE`)
* *WHEN* the adapter builds the scan spec
* *THEN* `rewrite_decimal_stringifications` SHALL leave the stringification unchanged, injecting no `decimal_to_varchar_exasol` node, because only DECIMAL stringification diverges through this fix
* *AND* a `function_scalar_cast` to VARCHAR or CHAR over such a column SHALL render exactly as it did before this change end to end, because no other guard governs that node shape
* *AND* a `CONCAT` or `LENGTH` over such a column MAY instead be rewritten or declined at the wired surfaces by `vs-adapter/pushdown-planning-string-fn-type-coercion`, which governs every string function's string-position arguments and runs first — a DATE argument is rewrapped as `CAST(<col> AS VARCHAR)` and a `DOUBLE`/`BOOLEAN`/`TIMESTAMP` argument declines to native Exasol evaluation, so the end-to-end rendering of `CONCAT`/`LENGTH` over a non-DECIMAL column is that feature's contract, NOT this scenario's
<!-- /DELTA:REMOVED -->

<!-- DELTA:REMOVED -->
### Scenario: A stringified computed expression is left unchanged as a tracked exception

* *GIVEN* a `pushdown` request that stringifies a NON-bare-column argument via `CAST(... AS VARCHAR/CHAR)`, `CONCAT`, or `LENGTH` — for example the computed expression `c_acctbal * 2` — in either the select list or the filter
* *WHEN* the adapter builds the scan spec
* *THEN* the rewriter SHALL leave the stringification unchanged, because the argument's Exasol type is not resolvable from `involvedTables[0].columns`
* *AND* a DECIMAL-valued computed argument MAY still render with divergent DataFusion formatting — an accepted, accurately-scoped tracked exception (#223), not a silent gap
<!-- /DELTA:REMOVED -->

<!-- DELTA:NEW -->
### Scenario: A DECIMAL stringification in a GROUP BY key or an aggregate argument renders the trimmed form

* *GIVEN* issue #227's repros over a `DECIMAL(12,2)` column holding `0.50` and `100.10`: `SELECT CAST(c_acctbal AS VARCHAR(20)) k, COUNT(*) ... GROUP BY CAST(c_acctbal AS VARCHAR(20))` and `SELECT MAX(CAST(c_acctbal AS VARCHAR(20))) ...`
* *WHEN* the adapter pushes the grouped or the single-group aggregate down
* *THEN* the group keys SHALL be `0.5` and `100.1` and the maximum SHALL be `100.1`, equal to native Exasol, NOT `0.50` and `100.10`
* *AND* the select-list key SHALL match its GROUP BY key by rendered text, so the request decomposes into the grouped partial/merge scan
<!-- /DELTA:NEW -->
