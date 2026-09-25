<!-- DELTA:CHANGED -->
# Feature: Pushdown Planning — Type-Rewrite Pipeline Composition

Verifies that the two passes of the type-rewrite pipeline compose with the renderer's
`exa_to_varchar` wrapping (`sql-comprehension/vs-expression-translator-string-conversion`), so each
string conversion happens exactly once. The passes are `like_subject_type_guard`
(`vs-adapter/pushdown-planning-like-type-coercion`) and the string-conversion decline check
(`vs-adapter/pushdown-planning-string-fn-type-coercion`).
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
## Background

* `apply_type_rewrites` in `pushdown/support.rs` owns the pass order: `like_subject_type_guard`,
  then the decline check. Both passes are private to `support`. Rendering stays a separate step at
  the call site.
* `like_subject_type_guard` rewraps a DATE `LIKE` subject as a `function_scalar_cast` to
  `{"type":"VARCHAR"}`. The renderer treats that node as a string CAST and wraps its source.
* Neither pass wraps a string-converted argument. The renderer alone applies `exa_to_varchar`, so
  the adapter cannot produce a double conversion.
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:REMOVED -->
### Scenario: The guard composes with the LIKE type guard and the decimal-stringification rewriter without double coercion

* *GIVEN* a `pushdown` request whose filter is processed by the filter type-rewrite pipeline function that owns the ordered pass list `like_subject_type_guard` then `string_function_arg_type_guard` then `rewrite_decimal_stringifications`, whose result is then passed to `render_df_filter_safe` — for example `LENGTH(c_decimal_a) > 5`
* *WHEN* the adapter builds the single-table DataFusion scan-spec filter
* *THEN* `string_function_arg_type_guard` SHALL wrap the bare DECIMAL argument first, after which `rewrite_decimal_stringifications` SHALL see a `decimal_to_varchar_exasol` node rather than a bare column and leave it alone, so exactly ONE trim wrapper is emitted
* *AND* the rendered filter SHALL carry the same trimmed form issue #211 established, keeping `vs-adapter/pushdown-planning-decimal-string-format`'s WHERE-clause scenario satisfied through the new composition
* *AND* a DATE LIKE subject that `like_subject_type_guard` already rewrapped as `CAST(<col> AS VARCHAR)` SHALL pass through `string_function_arg_type_guard` untouched, because `function_scalar_cast` is not a governed string function
* *AND* for a governed string function used AS a LIKE subject — for example `UPPER(c_decimal_a) LIKE '1%'` — `like_subject_type_guard` SHALL leave the LIKE node unchanged because its subject is not a bare `column` node, while `string_function_arg_type_guard` SHALL coerce the DECIMAL argument inside that subject
* *AND* the test that pins this no-op interaction SHALL invoke the same pipeline function `handle_pushdown` invokes, rather than re-deriving the pass sequence, so a future reordering of the passes cannot leave the test passing against a stale hand-written copy of the old order
<!-- /DELTA:REMOVED -->

<!-- DELTA:NEW -->
### Scenario: The LIKE subject guard and the string-conversion check compose without double conversion

* *GIVEN* `pushdown` filters processed by `apply_type_rewrites` and then rendered by `render_df_filter_safe` — `c_date LIKE '2024%'`, `LENGTH(c_decimal_a) > 5`, and `UPPER(c_decimal_a) LIKE '1%'`
* *WHEN* the adapter builds the single-table DataFusion scan-spec filter
* *THEN* the DATE subject SHALL render as `CAST(exa_to_varchar("C_DATE") AS VARCHAR)`, one conversion, which the decline check accepts because the CAST's source is a DATE column
* *AND* `LENGTH(c_decimal_a) > 5` SHALL render exactly one `exa_to_varchar` wrapper, around the bare column
* *AND* for `UPPER(c_decimal_a) LIKE '1%'` the LIKE guard SHALL leave the node unchanged, because its subject is not a bare column, while the renderer wraps the DECIMAL argument of `UPPER`
* *AND* the test SHALL call `apply_type_rewrites` itself rather than a re-derived pass sequence
<!-- /DELTA:NEW -->
