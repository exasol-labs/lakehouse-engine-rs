# Feature: Pushdown Planning — String Function Argument Type Coercion

Makes every pushed-down Exasol string scalar function type-aware in its string-position arguments. Exasol implicitly converts a numeric or DATE argument to VARCHAR before applying `UPPER`/`LOWER`/`TRIM`/`INSTR`/`LOCATE` and the rest of the family; DataFusion performs no such coercion, so a pushed-down string function over a non-string column hard-failed the scan at execution time (`F-UDF-CL-RUST-9001 … Function 'upper' requires String, but received Int64`, SQL state 22002, issue #210). This feature resolves which argument INDICES of each function sit in string position, dispatches each such bare-column argument on its Exasol type read from `involvedTables[0].columns`, and rewrites the expression JSON before rendering: string arguments pass through unchanged, DATE arguments are rewrapped in an explicit CAST-to-VARCHAR, DECIMAL arguments are rewrapped in the `decimal_to_varchar_exasol` node that reproduces Exasol's trimmed number-to-string form, and every other resolvable type declines pushdown so Exasol evaluates the expression natively.

## Background

* This delta reconciles ONE Background claim and ONE scenario clause with the shared post-order rewrite primitive (`vs-adapter/pushdown-module-structure`) and the LIKE guard's migration onto it. Every other scenario of this feature is unchanged, as is every argument-index, coercion, and decline rule it specifies.
* `string_function_arg_type_guard` recurses through the shared post-order primitive rather than its own copied traversal. The curated child-bearing field set is unchanged — the array fields `expressions` / `arguments` / `results` and the single-child fields `expression` / `pattern` / `left` / `right` / `basis` — and is still load-bearing for the same reason: a filter-side string function sits under a comparison predicate (`UPPER(c) = 'X'` is `predicate_equal` with the function under `left`), a position no junction-only traversal reaches.
* What is no longer true is the CONTRAST that claim was drawn against: `like_subject_type_guard` is no longer junction-only. Both guards now traverse the identical curated field set through the same primitive, so the difference between them is confined to their per-node decision, not their reach. Where this feature's scenarios previously explained a coercion by this guard's DEEPER reach — notably a governed string function used as a LIKE subject — the operative reason is now the per-node decision alone: the LIKE guard declines to act on a non-bare-`column` subject, not because it cannot reach the node. The reattribution changes no observable output, which is why the scenario clause states only the behavior and leaves this reasoning here.
* The composition order at the wired surfaces is unchanged: this guard still runs BEFORE `rewrite_decimal_stringifications`, so a coerced argument is no longer a bare column by the time the decimal rewriter sees it and cannot be double-wrapped.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: The guard composes with the LIKE type guard and the decimal-stringification rewriter without double coercion

* *GIVEN* a `pushdown` request whose filter is processed by the wired chain `like_subject_type_guard` then `string_function_arg_type_guard` then `rewrite_decimal_stringifications` then `render_df_filter_safe` — for example `LENGTH(c_decimal_a) > 5`
* *WHEN* the adapter builds the single-table DataFusion scan-spec filter
* *THEN* `string_function_arg_type_guard` SHALL wrap the bare DECIMAL argument first, after which `rewrite_decimal_stringifications` SHALL see a `decimal_to_varchar_exasol` node rather than a bare column and leave it alone, so exactly ONE trim wrapper is emitted
* *AND* the rendered filter SHALL carry the same trimmed form issue #211 established, keeping `vs-adapter/pushdown-planning-decimal-string-format`'s WHERE-clause scenario satisfied through the new composition
* *AND* a DATE LIKE subject that `like_subject_type_guard` already rewrapped as `CAST(<col> AS VARCHAR)` SHALL pass through `string_function_arg_type_guard` untouched, because `function_scalar_cast` is not a governed string function
* *AND* for a governed string function used AS a LIKE subject — for example `UPPER(c_decimal_a) LIKE '1%'` — `like_subject_type_guard` SHALL leave the LIKE node unchanged because its subject is not a bare `column` node, while `string_function_arg_type_guard` SHALL coerce the DECIMAL argument inside that subject
<!-- /DELTA:CHANGED -->
