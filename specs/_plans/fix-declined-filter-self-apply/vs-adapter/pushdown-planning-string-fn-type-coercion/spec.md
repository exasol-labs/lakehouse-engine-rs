# Feature: Pushdown Planning — String-Function Type Coercion

Makes the string-position arguments of pushed-down string functions type-aware on the select-list
projection and the single-table WHERE-clause filter, so a non-string argument is coerced, declined,
or passed through rather than hard-failing the DataFusion scan.

## Background

* This delta corrects what a WHERE-clause decline MEANS. The guard's decline scope, its dispatch
  table, and its traversal are unchanged; only the caller's handling of a decline changes. A
  declined filter is no longer omitted and left to a non-existent Exasol backstop — the request
  routes to the qualified single-table wrapper, which renders the ORIGINAL predicate tree as its own
  `WHERE`. See `vs-adapter/pushdown-declined-filter-self-apply`.
* The `INSTR`/`LOCATE`-with-more-than-two-arguments decline is one route into that corrected
  handling, so the structural fix reaches it as a side effect of fixing the caller. Issue #228,
  which independently asserted the same false backstop for that shape, is NOT closed by this delta:
  it is re-verified and adjudicated on its own, and nothing here should be read as having done so.
* The select-list decline is unaffected. It sets the full-base-row fallback flag, which is a
  different and already-correct mechanism: the adapter returns the columns and Exasol computes the
  item over them, which it does because the adapter never claimed the item.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: A non-coercible resolvable column type in a WHERE-clause string function declines the whole filter

* *GIVEN* a `pushdown` request whose filter carries a governed string `function_scalar` whose string-position argument is a bare `column` node
* *AND* the column's Exasol type in `involvedTables[0].columns` is resolvable but is none of VARCHAR, CHAR, DATE, or DECIMAL — for example `BOOLEAN`, `DOUBLE PRECISION`, or `TIMESTAMP`
* *WHEN* the adapter builds the single-table DataFusion scan-spec filter
* *THEN* the guard SHALL return `None`, declining pushdown of the WHOLE top-level filter so no `filter` is emitted in the common spec
* *AND* the adapter SHALL route the request to the qualified single-table wrapper and render the ORIGINAL predicate tree as that wrapper's own `WHERE` — REPLACING the recorded "and Exasol evaluates the entire predicate natively", which assumed an Exasol-side re-check of a delegated predicate that does not occur
* *AND* the guard SHALL NOT inject a CAST for such an argument, because DataFusion's text rendering of BOOLEAN (`true`) and TIMESTAMP (`T`-separated) diverges from Exasol's (`TRUE`, space-separated) and would silently change which rows match
* *AND* a decline reached at any nesting depth SHALL propagate to the top-level filter and SHALL apply ONLY to the JSON tree fed to `render_df_filter_safe`, leaving the raw filter tree forwarded to Iceberg file pruning unchanged — REPLACING the recorded "mirroring the all-or-nothing untranslatable-predicate backstop that `like_subject_type_guard` already uses", which named a backstop that does not exist; the all-or-nothing SCOPE is retained, its named justification is not
* *AND* the returned rows SHALL equal native Exasol evaluation of the same query
<!-- /DELTA:CHANGED -->
