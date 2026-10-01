# Feature: Pushdown Planning — Type-Rewrite Pipeline Composition

Verifies that the type-rewrite pipeline's one pass, `like_subject_type_guard`
(`vs-adapter/pushdown-planning-like-type-coercion`), composes with the renderer's `exa_to_varchar`
wrapping (`sql-comprehension/vs-expression-translator-string-conversion`), so each string
conversion happens exactly once.

## Background

* `apply_type_rewrites` in `pushdown/support.rs` runs one pass, `like_subject_type_guard`, which is
  private to `support`. Rendering stays a separate step at the call site.
* `like_subject_type_guard` rewraps a DATE `LIKE` subject as a `function_scalar_cast` to
  `{"type":"VARCHAR"}`. The renderer treats that node as a string CAST and wraps its source.
* No adapter pass wraps a string-converted argument. The renderer alone applies `exa_to_varchar`, so
  the adapter cannot produce a double conversion.

## Scenarios

### Scenario: The LIKE subject guard and the renderer's conversion compose without double conversion

* *GIVEN* `pushdown` filters processed by `apply_type_rewrites` and then rendered by `render_df_filter_safe`: `c_date LIKE '2024%'`, `LENGTH(c_decimal_a) > 5`, and `UPPER(c_decimal_a) LIKE '1%'`
* *WHEN* the adapter builds the single-table DataFusion scan-spec filter
* *THEN* the DATE subject SHALL render as `CAST(exa_to_varchar("C_DATE") AS VARCHAR)`, and `LENGTH(c_decimal_a) > 5` SHALL render exactly one `exa_to_varchar` call, around the bare column
* *AND* for `UPPER(c_decimal_a) LIKE '1%'` the LIKE guard SHALL leave the node unchanged, because its subject is not a bare column, while the renderer wraps the DECIMAL argument of `UPPER`
* *AND* the test SHALL call `apply_type_rewrites` itself rather than a re-derived pass sequence
