# Feature: Pushdown Planning — String Function Type Guard Composition

Verifies that `string_function_arg_type_guard` (`vs-adapter/pushdown-planning-string-fn-type-coercion`)
composes correctly with the two other filter-side rewrite passes it is wired next to —
`like_subject_type_guard` (`vs-adapter/pushdown-planning-like-type-coercion`) and
`rewrite_decimal_stringifications` (`vs-adapter/pushdown-planning-decimal-string-format`) — in the
chain `like_subject_type_guard` → `string_function_arg_type_guard` → `rewrite_decimal_stringifications`
→ `render_df_filter_safe`. Split out of `pushdown-planning-string-fn-type-coercion` as a pure
file-organization move (no content change) once that feature crossed the per-spec scenario
threshold.

## Background

* `like_subject_type_guard` is no longer junction-only: both guards now traverse the identical curated field set through the same shared post-order rewrite primitive (`vs-adapter/pushdown-module-structure`), so the difference between them is confined to their per-node decision, not their reach. Where a governed string function is used as a LIKE subject, the operative reason `like_subject_type_guard` leaves it alone is that its per-node decision declines to act on a non-bare-`column` subject — not that it cannot reach the node.
* The DECIMAL branch of `string_function_arg_type_guard` reuses the existing `decimal_to_varchar_exasol` node and `wrap_decimal_to_varchar` helper introduced by `vs-adapter/pushdown-planning-decimal-string-format`; the DATE branch reuses the same `function_scalar_cast` shape as `guard_like_subject`. Neither formatting rule is reimplemented.
* The three-pass order this feature verifies is now owned by one named pipeline function in `pushdown/support.rs` rather than spelled out at its call site (`vs-adapter/pushdown-module-structure`, issue #259). The passes and their order are unchanged; only their owner is. Rendered SQL is byte-identical.
* All three passes are now private to `pushdown/support.rs` and reachable only through that pipeline function, so no module outside `support` can sequence them itself. This is what makes the order enforced by the compiler rather than by comment. Inside `support` the passes remain directly callable, which is how this feature's own unit tests exercise each pass in isolation.
* The pipeline function does NOT absorb `render_df_filter_safe`. Rendering lives in the `vs-expression` crate and stays a separate step at the call site, so this feature's chain is now "pipeline function, then render".

## Scenarios

### Scenario: The guard composes with the LIKE type guard and the decimal-stringification rewriter without double coercion

* *GIVEN* a `pushdown` request whose filter is processed by the filter type-rewrite pipeline function that owns the ordered pass list `like_subject_type_guard` then `string_function_arg_type_guard` then `rewrite_decimal_stringifications`, whose result is then passed to `render_df_filter_safe` — for example `LENGTH(c_decimal_a) > 5`
* *WHEN* the adapter builds the single-table DataFusion scan-spec filter
* *THEN* `string_function_arg_type_guard` SHALL wrap the bare DECIMAL argument first, after which `rewrite_decimal_stringifications` SHALL see a `decimal_to_varchar_exasol` node rather than a bare column and leave it alone, so exactly ONE trim wrapper is emitted
* *AND* the rendered filter SHALL carry the same trimmed form issue #211 established, keeping `vs-adapter/pushdown-planning-decimal-string-format`'s WHERE-clause scenario satisfied through the new composition
* *AND* a DATE LIKE subject that `like_subject_type_guard` already rewrapped as `CAST(<col> AS VARCHAR)` SHALL pass through `string_function_arg_type_guard` untouched, because `function_scalar_cast` is not a governed string function
* *AND* for a governed string function used AS a LIKE subject — for example `UPPER(c_decimal_a) LIKE '1%'` — `like_subject_type_guard` SHALL leave the LIKE node unchanged because its subject is not a bare `column` node, while `string_function_arg_type_guard` SHALL coerce the DECIMAL argument inside that subject
* *AND* the test that pins this no-op interaction SHALL invoke the same pipeline function `handle_pushdown` invokes, rather than re-deriving the pass sequence, so a future reordering of the passes cannot leave the test passing against a stale hand-written copy of the old order
