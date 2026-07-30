# Feature: Pushdown Planning — Aggregate Extensions

Extends aggregate pushdown with the statistical-aggregate merge SQL and routes a HAVING the
adapter cannot render over the partial/merge decomposition to the qualified single-table wrapper.

## Background

* This delta SUPERSEDES the preceding Background bullet "This delta does NOT adjudicate the WHERE-filter backstop. An untranslatable WHERE predicate is genuinely omitted from the scan spec (`vs_expression::render_df_filter_safe` returns `None`), a distinct mechanism with its own capability story; the Background statement about filter and select-list expressions stands as recorded (see `vs-adapter/pushdown-planning-selectlist-expressions`). Only the HAVING claim is corrected." The WHERE-filter backstop is now adjudicated and the deferred claim was wrong: an untranslatable WHERE predicate omitted from the scan spec is applied by nobody and returns extra rows, verified live. The correct handling is the SAME one this delta already chose for HAVING — route the request to the qualified single-table wrapper and render the clause as ordinary Exasol SQL over materialized rows. See `vs-adapter/pushdown-declined-filter-self-apply`.
* The two clauses now share one destination, so the wrapper is the single place a clause the partial/merge decomposition cannot express is evaluated. A declined WHERE renders as the wrapper's `WHERE`, ahead of the aggregate; a declined HAVING renders as the wrapper's `HAVING`, after it.

## Scenarios

<!-- DELTA:NEW -->
### Scenario: A declined WHERE filter under an aggregate request is applied ahead of the aggregate

* *GIVEN* an aggregate or `groupBy` `pushdown` request whose WHERE filter the DataFusion-bound render declines
* *WHEN* the adapter routes the request to the qualified single-table wrapper
* *THEN* the wrapper SHALL render the declined predicate as its `WHERE`, positioned between the raw sharded fan-out and the aggregate select list, GROUP BY, and HAVING it renders
* *AND* the per-shard fan-out SHALL carry no aggregate, no group keys, and no filter, so no shard aggregates unfiltered rows
* *AND* the returned aggregate value SHALL equal native Exasol evaluation of the same query, not the aggregate over the unfiltered row set the omission returned
<!-- /DELTA:NEW -->

