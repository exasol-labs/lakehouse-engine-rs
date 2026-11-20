# Feature: Pushdown Declined-Filter Self-Application

Guarantees that the adapter evaluates every WHERE predicate it accepts. The adapter applies, in its own returned SQL, any predicate it cannot push to DataFusion.

## Background

* Exasol does not re-apply a predicate whose capability the adapter advertised, and the response has no field to hand one back. An omitted predicate returns extra unfiltered rows.
* A conjunct declines when the scan cannot compute it (`pushdown-types/pushdown-planning-scan-text-conversion`). Exasol drops a `WHERE` that folds to `TRUE`, so it never sends a trivially-true filter.
* The adapter self-applies a declined conjunct by rendering its original request tree in the Exasol dialect, which renders every predicate Exasol sends (`sql-comprehension/vs-expression-translator`), so Exasol applies its own conversions with the session's settings.
* File pruning reads the original request filter at the single-table scan and at each join side, so it keeps every conjunct, renderable or not, and removes only files that cannot match.
* `pushdown-joins/pushdown-planning-join-fallback` owns the N-scan leg-eligibility rule.

## Scenarios

<!-- DELTA:REMOVED -->
### Scenario: A broadcast-eligible join whose filter declines takes the N-scan fallback

* *GIVEN* a broadcast-eligible join whose filter holds a conjunct the scan translator declines
* *WHEN* the adapter renders the join
* *THEN* this behavior SHALL be removed; the join keeps the broadcast and Exasol's `WHERE` applies the conjunct, per "A broadcast join applies a declined filter conjunct in Exasol's WHERE"
<!-- /DELTA:REMOVED -->

<!-- DELTA:NEW -->
### Scenario: A broadcast join applies a declined filter conjunct in Exasol's WHERE

* *GIVEN* an inner join `pushdown` request with a broadcast pair (`pushdown-joins/pushdown-planning-join`), of two legs or more
* *AND* its filter holds a top-level conjunct over the pair's legs that the scan translator declines, such as `TO_CHAR(c.signup_date) LIKE '2024%'`
* *WHEN* the adapter builds the pushdown SQL
* *THEN* the adapter SHALL keep the broadcast pair
* *AND* the pair's scan spec SHALL carry the conjunction of the conjuncts the scan translator renders as its `filter`, and Exasol's `WHERE` over the fan-outs SHALL carry each declined conjunct, rendered from its original request tree in the Exasol dialect, so each conjunct applies exactly once
* *AND* the pair's fan-out SHALL emit every column a declined conjunct reads
* *AND* the pair's join block SHALL carry no post-join limit and no post-join ordering, and Exasol SHALL apply the request's aggregation, `ORDER BY`, `LIMIT`, and `OFFSET` after its `WHERE`
* *AND* a two-leg request whose ordering or window the broadcast cannot serve (`pushdown-joins/pushdown-planning-join-ordering`) SHALL take the unified unaccelerated fallback, as it does without a declined conjunct
* *AND* the returned rows SHALL equal native Exasol evaluation of the same query, for a two-leg row scan, a two-leg `ORDER BY` with `LIMIT`, a two-leg aggregate, and a join of three legs
<!-- /DELTA:NEW -->

<!-- DELTA:CHANGED -->
### Scenario: A single-table WHERE filter splits at AND between the scan and the wrapper

* *GIVEN* a single-table `pushdown` request whose `filter` is an `AND` of conjuncts, at least one of which the DataFusion-bound render declines and at least one of which it renders, such as `WHERE c_decimal LIKE '1%' AND id > 1` over a `DECIMAL(9,2)` column
* *WHEN* the adapter builds the pushdown SQL
* *THEN* the adapter SHALL split the filter into its top-level conjuncts, flattening nested `AND` nodes and keeping each `OR` or `NOT` node whole, and apply each conjunct exactly once, in the scan or in the wrapper's `WHERE`
* *AND* the per-shard scan spec of the qualified single-table wrapper's fan-out SHALL carry the conjunction of the rendered conjuncts as its `filter`, and the wrapper's `WHERE` SHALL carry the conjunction of the declined conjuncts, each rendered from its original request tree in the Exasol dialect
* *AND* the fan-out's columns SHALL follow "A declined filter does not widen the wrapper's projection", so a column that only a rendered conjunct names is not emitted
* *AND* the result SHALL equal native Exasol evaluation of the same query, for a row scan, an aggregate, a `GROUP BY`, a `COUNT(DISTINCT)`, and an `ORDER BY` with `LIMIT`
* *AND* a join with a broadcast pair whose filter holds a declined conjunct SHALL split the same way, as "A broadcast join applies a declined filter conjunct in Exasol's WHERE" states
<!-- /DELTA:CHANGED -->
