# Feature: Pushdown Planning — Decimal String Formatting

Makes pushed-down DECIMAL→string conversions reproduce Exasol's shortest-form formatting. Exasol trims trailing scale zeros when it converts a DECIMAL to text (`2912.00`→`'2912'`, `-272.60`→`'-272.6'`); DataFusion's `CAST(decimal AS VARCHAR)` and its implicit decimal→utf8 coercion both render the full declared scale, so a pushed-down expression that stringifies a DECIMAL column silently returned different results (issue #211). A stringified column's `dataType` never crosses the wire; column Exasol types exist only in `involvedTables[0].columns`. Mirroring the LIKE type-coercion fix (`vs-adapter/pushdown-planning-like-type-coercion`), this feature dispatches on the column's Exasol type in the adapter and rewrites the expression JSON before rendering: a stringification of a bare DECIMAL column is rewrapped in the adapter-synthesized `decimal_to_varchar_exasol` node (rendered via `format_decimal_exasol_style`, see `sql-comprehension/vs-expression-translator-scalar-ops`); every other case is left unchanged.

Scope: the single-table scan paths for the three stringifications confirmed to silently coerce rather than hard-fail on a DECIMAL argument — explicit `CAST(<col> AS VARCHAR/CHAR)`, and implicit `CONCAT`/`LENGTH` over a bare DECIMAL column — in BOTH the select-list projection (`project_columns`) and the WHERE-clause filter tree (`handle_pushdown`'s single filter render). A single shared recursive rewriter (`rewrite_decimal_stringifications`) walks each tree and rewrites the bare DECIMAL column wherever it is directly stringified, at any nesting depth. Out of scope, tracked in issue #223: a stringified argument that is a computed expression rather than a bare column, the broadcast-join per-leg filter path, and a GROUP-BY key absent from the select list.

## Background

* This delta reconciles ONE scenario with the string-function argument type coercion added by `vs-adapter/pushdown-planning-string-fn-type-coercion` (issue #210). Every other scenario of this feature, and `rewrite_decimal_stringifications` itself, is unchanged.
* Issue #210 is no longer an out-of-scope follow-up of this feature: the remaining string functions that hard-fail on a DECIMAL argument are now governed by `vs-adapter/pushdown-planning-string-fn-type-coercion`, which reuses this feature's `decimal_to_varchar_exasol` node and `wrap_decimal_to_varchar` helper rather than reimplementing decimal formatting.
* At both wired surfaces the string-function guard runs BEFORE `rewrite_decimal_stringifications`, so a bare DECIMAL column under `CONCAT` or `LENGTH` is already wrapped by the time this feature's rewriter sees it. The rewriter's `CONCAT`/`LENGTH` arms therefore become an idempotent backstop at those surfaces and remain the sole handler for a `function_scalar_cast`-to-string over a DECIMAL column, which the string-function guard deliberately does not touch.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: CAST, CONCAT, or LENGTH over a non-DECIMAL column is left unchanged

* *GIVEN* a `pushdown` request whose select list or filter stringifies a bare `column` whose Exasol type in `involvedTables[0].columns` is NOT `DECIMAL` (for example `VARCHAR`, `DATE`, or `DOUBLE`)
* *WHEN* the adapter builds the scan spec
* *THEN* `rewrite_decimal_stringifications` SHALL leave the stringification unchanged, injecting no `decimal_to_varchar_exasol` node, because only DECIMAL stringification diverges through this fix
* *AND* a `function_scalar_cast` to VARCHAR or CHAR over such a column SHALL render exactly as it did before this change end to end, because no other guard governs that node shape
* *AND* a `CONCAT` or `LENGTH` over such a column MAY instead be rewritten or declined at the wired surfaces by `vs-adapter/pushdown-planning-string-fn-type-coercion`, which governs every string function's string-position arguments and runs first — a DATE argument is rewrapped as `CAST(<col> AS VARCHAR)` and a `DOUBLE`/`BOOLEAN`/`TIMESTAMP` argument declines to native Exasol evaluation, so the end-to-end rendering of `CONCAT`/`LENGTH` over a non-DECIMAL column is that feature's contract, NOT this scenario's
<!-- /DELTA:CHANGED -->
