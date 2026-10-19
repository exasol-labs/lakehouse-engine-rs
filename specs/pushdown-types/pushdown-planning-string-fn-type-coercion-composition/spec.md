# Feature: Pushdown Planning, String Function Type Guard Composition

`string_function_arg_type_guard`, `like_subject_type_guard`, and `rewrite_decimal_stringifications` run in that order before `render_df_filter_safe`. This feature checks that the passes compose without double coercion.

## Background

* Both type guards traverse the same curated field set through the shared post-order rewrite primitive (`pushdown/pushdown-module-structure`), so they differ only in their per-node decision. `like_subject_type_guard` leaves a governed string function used as a LIKE subject alone because the subject is not a bare `column` node.
* The DECIMAL branch of `string_function_arg_type_guard` reuses the `decimal_to_varchar_exasol` node and `wrap_decimal_to_varchar` helper from `pushdown-types/pushdown-planning-decimal-string-format`. The DATE branch reuses the `function_scalar_cast` shape of `guard_like_subject`.
* One named pipeline function in `pushdown/support.rs` owns the three-pass order. The three passes are private to `support.rs`, so the compiler enforces the order. `render_df_filter_safe` lives in the `vs-expression` crate and runs after the pipeline function.

## Scenarios

### Scenario: The guard composes with the LIKE type guard and the decimal-stringification rewriter without double coercion

* *GIVEN* a `pushdown` request whose filter is processed by the filter type-rewrite pipeline function that owns the ordered pass list `like_subject_type_guard` then `string_function_arg_type_guard` then `rewrite_decimal_stringifications`, whose result is then passed to `render_df_filter_safe` — for example `LENGTH(c_decimal_a) > 5`
* *WHEN* the adapter builds the single-table DataFusion scan-spec filter
* *THEN* `string_function_arg_type_guard` SHALL wrap the bare DECIMAL argument first, after which `rewrite_decimal_stringifications` SHALL see a `decimal_to_varchar_exasol` node rather than a bare column and leave it alone, so exactly ONE trim wrapper is emitted
* *AND* the rendered filter SHALL carry the same trimmed form issue #211 established, keeping `pushdown-types/pushdown-planning-decimal-string-format`'s WHERE-clause scenario satisfied through the new composition
* *AND* a DATE LIKE subject that `like_subject_type_guard` already rewrapped as `CAST(<col> AS VARCHAR)` SHALL pass through `string_function_arg_type_guard` untouched, because `function_scalar_cast` is not a governed string function
* *AND* for a governed string function used AS a LIKE subject — for example `UPPER(c_decimal_a) LIKE '1%'` — `like_subject_type_guard` SHALL leave the LIKE node unchanged because its subject is not a bare `column` node, while `string_function_arg_type_guard` SHALL coerce the DECIMAL argument inside that subject
* *AND* the test that pins this no-op interaction SHALL invoke the same pipeline function `handle_pushdown` invokes, rather than re-deriving the pass sequence, so a future reordering of the passes cannot leave the test passing against a stale hand-written copy of the old order
