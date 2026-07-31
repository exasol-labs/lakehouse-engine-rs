# Feature: Pushdown Planning — Decimal String Formatting

Makes pushed-down DECIMAL→string conversions reproduce Exasol's shortest-form (trailing-zero
trimmed) formatting, so a pushed expression that stringifies a DECIMAL column matches what Exasol
would have computed.

## Background

* This feature's recorded scope statement deferring "the broadcast-join per-leg filter path" to issue
  #223 is REPLACED: that surface is now IN scope. Both join WHERE-filter render surfaces — the
  broadcast join's combined filter and the N-scan fallback's per-leg filter — run
  `rewrite_decimal_stringifications` through the shared type-rewrite pipeline, so a DECIMAL
  stringification in a join WHERE filter renders in the same trimmed form the single-table WHERE
  filter has produced since issue #211. See
  `vs-adapter/pushdown-planning-join-filter-type-coercion` (issue #215). Issue #223's slice 2 closes
  with it; #223's slices 1 (computed-expression arguments) and 3 (GROUP-BY-only keys) remain open and
  out of scope here.
* This closes a SILENT-wrong-answer exposure at the join surfaces, not a hard failure: unlike the
  LIKE and string-function guards, the decimal rewriter never declines — before this wiring a join
  WHERE filter stringifying a DECIMAL column planned and executed successfully but matched against
  DataFusion's full-scale form (`2912.00`) rather than Exasol's trimmed form (`2912`), so the join
  returned the wrong rows with no error.
* The rewriter itself is untouched: same per-node stringifier decision, same shared post-order
  traversal, same infallibility. Only its reachable surface set grows, and it grows by wiring.
* The pass ORDER is inherited from the pipeline unchanged — the string-function guard still runs
  before the decimal rewrite at every surface, so a coerced argument is never double-wrapped
  (`vs-adapter/pushdown-planning-string-fn-type-coercion-composition`).

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: WHERE-clause stringification of a DECIMAL column renders the trimmed form

* *GIVEN* a `pushdown` request whose filter stringifies a bare DECIMAL column via `CAST(... AS VARCHAR/CHAR)`, `CONCAT`, or `LENGTH` — for example the filter `LENGTH(c_acctbal) > 5` (issue #211's headline COUNT-divergence repro)
* *WHEN* the adapter builds the DataFusion scan-spec filter for the single-table path, the broadcast join's combined filter, or an N-scan fallback side's per-leg filter
* *THEN* the adapter SHALL apply `rewrite_decimal_stringifications` to the filter tree, after `like_subject_type_guard` and `string_function_arg_type_guard` and before `render_df_filter_safe`, wrapping each directly-stringified bare DECIMAL column in a `decimal_to_varchar_exasol` node so the predicate matches over the Exasol-trimmed string form
* *AND* the rewrite SHALL apply ONLY to the JSON tree fed to `render_df_filter_safe`, leaving the raw filter tree forwarded to Iceberg file pruning unchanged
* *AND* the rewrite SHALL NOT decline the filter and SHALL compose with a preceding guard decline (a declined filter is never rewritten because it is no longer pushed), so the pushed count for `LENGTH(c_acctbal) > 5` matches native Exasol evaluation
* *AND* the DECIMAL column's Exasol type SHALL be resolved from the column metadata of the table that OWNS the column — the union of both involved tables' columns at the broadcast surface, that side's own columns at an N-scan per-leg surface — REPLACING this feature's recorded scope statement deferring the join per-leg filter path to issue #223 (slice 2, wired by issue #215)
* *AND* a join WHERE filter that stringifies a DECIMAL column SHALL therefore no longer return rows matched against DataFusion's full-scale decimal text, which was a silent wrong answer rather than an error
<!-- /DELTA:CHANGED -->
