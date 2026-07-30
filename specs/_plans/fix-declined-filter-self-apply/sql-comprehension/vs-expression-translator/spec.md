# Feature: VS Expression Translator

Translates Exasol Virtual-Schema pushdown-request expression JSON into SQL, in two dialects: a
DataFusion dialect parsed by the scan UDF's SQL frontend, and an Exasol dialect spliced into outer
wrapper SQL that Exasol's own core engine parses.

## Background

* This delta SUPERSEDES the preceding Background statement "Every one of those four sites reads raw pushdown-request JSON. A WHERE-clause predicate on a single-table scan is NOT among them: `build_qualified_single_table_fallback_sql` (`adapter/pushdown/joins/sql_builders.rs`) applies that filter inside the scan through `fan_out_spec.filter`, which the DataFusion trio renders. An Exasol-dialect node therefore reaches Exasol's parser only as a select item, a GROUP BY key, a HAVING operand, an ORDER BY element, or an N-scan cross-side residual, and an acceptance test for any Exasol-dialect rendering MUST use one of those positions." A single-table WHERE-clause predicate IS now among them. `build_qualified_single_table_fallback_sql` applies the filter inside the scan through `fan_out_spec.filter` only when the DataFusion trio renders it; when the DataFusion render DECLINES, the same wrapper renders the predicate in its own outer `WHERE` through `render_df_filter_qualified`. An Exasol-dialect node therefore reaches Exasol's parser as a select item, a GROUP BY key, a HAVING operand, an ORDER BY element, an N-scan cross-side residual, or a DECLINED single-table WHERE predicate, and an acceptance test for any Exasol-dialect rendering MUST use one of those positions.
* `render_df_filter_qualified`'s consumer entry in the Exasol-dialect consumer table gains that second wrapper SQL position: "the outer WHERE residual of the N-scan join wrapper, and the outer WHERE of the qualified single-table wrapper when the single-table DataFusion filter render declined".
* No translator entry point changes. Distinguishing a declined filter from a trivially-true one needs no new API: `render_expression_safe` does not suppress a trivially-true result, so it returns `None` for exactly the declined case, and the trivially-true rule stays owned by this crate.
* The `render_df_filter_safe` and `render_df_filter_exasol_safe` doc comments MUST NOT state that the adapter omits an unrenderable filter and Exasol keeps it as a correctness backstop. Both return `None` for two distinguishable reasons — trivially true, and unrenderable — and only the first is safe for a caller to omit. What a `None` MEANS belongs to the caller, and the callers differ (see `vs-adapter/pushdown-declined-filter-self-apply`).

## Scenarios

<!-- DELTA:NEW -->
### Scenario: A declined single-table WHERE predicate is an Exasol-dialect wrapper position

* *GIVEN* a single-table WHERE predicate whose DataFusion-dialect render declines while its Exasol-dialect render succeeds
* *WHEN* the adapter renders that predicate for the qualified single-table wrapper's outer `WHERE`
* *THEN* the Exasol trio SHALL be the renderer used, reached through `render_df_filter_qualified`, so the fragment is parsed by Exasol's core engine and carries length-qualified CAST targets
* *AND* the recorded set of wrapper positions an Exasol-dialect node can reach SHALL include this one, so an acceptance test for Exasol-dialect rendering MAY use a declined single-table WHERE predicate
* *AND* no translator entry point SHALL be added, because `render_expression_safe` already returns `None` for exactly the declined case and the trivially-true rule stays owned by this crate
<!-- /DELTA:NEW -->

