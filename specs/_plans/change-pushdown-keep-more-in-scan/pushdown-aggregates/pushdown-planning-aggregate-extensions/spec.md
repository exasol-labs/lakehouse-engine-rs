# Feature: Pushdown Planning, Aggregate Extensions (HAVING & Statistical Aggregates)

The adapter pushes HAVING and the statistical aggregates (STDDEV and VARIANCE families) down over the partial/merge decomposition, and routes aggregates that do not decompose to the qualified single-table wrapper.

## Background

* An aggregate is pushed down only when it decomposes into a shard-associative partial/merge plan. Otherwise the adapter routes the request to the qualified single-table wrapper, because Exasol never re-aggregates a declined pushdown.
* Exasol does not re-apply a HAVING or a WHERE predicate that the adapter advertised and then omitted, so omission returns wrong rows.
* The qualified single-table wrapper evaluates any clause the partial/merge decomposition cannot express, as ordinary Exasol SQL over materialized rows. A declined WHERE renders as the wrapper's `WHERE`, ahead of the aggregate. A declined HAVING renders as the wrapper's `HAVING`, after it.
* The statistical family accepts a bare column argument only. An expression argument declines the aggregate pushdown, because the scan would render an aggregate plan with no source column and fail.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: A declined WHERE filter under an aggregate request is applied ahead of the aggregate

* *GIVEN* an aggregate or `groupBy` `pushdown` request whose WHERE filter the DataFusion-bound render declines
* *WHEN* the adapter routes the request to the qualified single-table wrapper
* *THEN* the wrapper SHALL render the declined predicate as its `WHERE`, positioned between the raw sharded fan-out and the aggregate select list, GROUP BY, and HAVING it renders
* *AND* the per-shard fan-out SHALL carry no aggregate and no group keys, and its `filter` SHALL hold only the conjuncts the DataFusion-bound render renders (`pushdown/pushdown-declined-filter-self-apply`), so no shard aggregates unfiltered rows
* *AND* the returned aggregate value SHALL equal native Exasol evaluation of the same query, not the aggregate over the unfiltered row set the omission returned
<!-- /DELTA:CHANGED -->
