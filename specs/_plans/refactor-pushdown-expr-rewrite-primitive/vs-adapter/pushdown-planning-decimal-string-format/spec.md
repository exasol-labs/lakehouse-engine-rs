# Feature: Pushdown Planning — Decimal String Formatting

Makes pushed-down DECIMAL→string conversions reproduce Exasol's shortest-form formatting. Exasol trims trailing scale zeros when it converts a DECIMAL to text (`2912.00`→`'2912'`, `-272.60`→`'-272.6'`); DataFusion's `CAST(decimal AS VARCHAR)` and its implicit decimal→utf8 coercion both render the full declared scale, so a pushed-down expression that stringifies a DECIMAL column silently returned different results (issue #211). A stringified column's `dataType` never crosses the wire; column Exasol types exist only in `involvedTables[0].columns`. Mirroring the LIKE type-coercion fix (`vs-adapter/pushdown-planning-like-type-coercion`), this feature dispatches on the column's Exasol type in the adapter and rewrites the expression JSON before rendering: a stringification of a bare DECIMAL column is rewrapped in the adapter-synthesized `decimal_to_varchar_exasol` node (rendered via `format_decimal_exasol_style`, see `sql-comprehension/vs-expression-translator-scalar-ops`); every other case is left unchanged.

Scope: the single-table scan paths for the three stringifications confirmed to silently coerce rather than hard-fail on a DECIMAL argument — explicit `CAST(<col> AS VARCHAR/CHAR)`, and implicit `CONCAT`/`LENGTH` over a bare DECIMAL column — in BOTH the select-list projection (`project_columns`) and the WHERE-clause filter tree (`handle_pushdown`'s single filter render). A single shared per-node rewriter (`rewrite_decimal_stringifications`), applied across each tree by the shared post-order traversal (`vs-adapter/pushdown-module-structure`), rewrites the bare DECIMAL column wherever it is directly stringified, at any nesting depth. Out of scope, tracked in issue #223: a stringified argument that is a computed expression rather than a bare column, the broadcast-join per-leg filter path, a GROUP-BY key absent from the select list, and every other string function that hard-fails on a DECIMAL argument (issue #210).

## Background

* This delta reconciles TWO Background/scope claims with the shared post-order rewrite primitive (`vs-adapter/pushdown-module-structure`). No scenario of this feature changes, and no rendered output changes: the rewriter's per-node stringifier decisions, its three stringifier shapes, and its post-order nesting behavior are all unaffected.
* `rewrite_decimal_stringifications` no longer owns a traversal. It contributes the per-node stringifier decision — `function_scalar_cast` to VARCHAR/CHAR, `function_scalar` named `CONCAT`, `function_scalar` named `LENGTH`, each over a bare DECIMAL column argument — and delegates recursion to the shared post-order primitive, which walks the curated child-bearing field set and applies that decision to each node after its children.
* Post-order nesting behavior is unchanged and still load-bearing for the same reason: Exasol renders `a || b || c` as nested `CONCAT(a, CONCAT(b, c))`, so a DECIMAL column reached only through an inner `CONCAT` is still rewritten because the shared traversal descends into that inner node before the outer node's own check runs.
* The rewriter remains infallible (`&Json -> Json`). It composes with the primitive as the never-declining case and gains no decline path, so a DECIMAL column in a non-stringifying context is still left untouched rather than dropping the enclosing filter or select-list item.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Implicit CONCAT over a DECIMAL column renders the trimmed form, including nested concatenation

* *GIVEN* a `pushdown` request whose select list carries a `function_scalar` item named `CONCAT` that stringifies a bare DECIMAL `column` node, whether that column is a direct argument of the top `CONCAT` node or of an inner `CONCAT` produced by chained `||` (Exasol renders `id || '-' || c_decimal_a` as nested `CONCAT("ID", CONCAT('-', "C_DECIMAL_A"))`)
* *WHEN* the adapter builds the scan-spec projection
* *THEN* the shared post-order traversal SHALL descend through nested `CONCAT` arguments and the rewriter's per-node decision SHALL replace each bare DECIMAL-column argument with a `decimal_to_varchar_exasol` node, leaving every non-DECIMAL argument and the surrounding `CONCAT` structure unchanged
* *AND* the rewriter SHALL NOT wrap a DECIMAL column that appears under a `CONCAT` argument only through a non-stringifying node — for example `c_decimal_a * 2` as a `CONCAT` argument stays a computed expression (a tracked exception, #223), not a wrapped column
* *AND* the rendered projection SHALL concatenate the trimmed decimal text in place, so `id || '-' || c_decimal_a` over `30.00` yields `4-30`, not `4-30.00`, byte-identical to its pre-refactor output
<!-- /DELTA:CHANGED -->
