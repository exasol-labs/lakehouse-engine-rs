# Feature: Pushdown Declined-Filter Self-Application

Guarantees that the adapter evaluates every WHERE predicate it accepts. The adapter applies, in its own returned SQL, any predicate it cannot push to DataFusion.

<!-- DELTA:CHANGED -->
## Background

* Exasol does not re-apply a predicate whose capability the adapter advertised, and the pushdown response has no field to hand one back, so an omitted predicate returns extra unfiltered rows.
* A conjunct declines when the scan translator declines it (`pushdown-types/pushdown-planning-scan-text-conversion`). The single-table path and the broadcast-join path screen the WHERE filter through `ScanTranslator::split_where`, and the N-scan legs through `screen_leg_filter`. Both split a filter that does not render whole into its top-level `AND` conjuncts and screen each one.
* The adapter self-applies a declined conjunct by rendering its original request tree in the Exasol dialect, which renders every predicate Exasol sends (`sql-comprehension/vs-expression-translator`), so Exasol applies its own conversions with the session's settings.
* File pruning reads the original request filter, so it keeps every conjunct, renderable or not, and removes only files that cannot match.
* `pushdown-joins/pushdown-planning-join-fallback` owns the N-scan leg-eligibility rule.
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: A declined single-table WHERE filter is applied in the adapter's own outer WHERE

* *GIVEN* a single-table `pushdown` request carrying a non-null `filter` whose capability the adapter advertises, for example `WHERE SECOND(c_ts, 3) > 1`, delegated because `FN_SECOND` is advertised
* *AND* the DataFusion-bound render declines every top-level conjunct of that filter
* *WHEN* the adapter builds the pushdown SQL
* *THEN* the adapter SHALL route the request to the qualified single-table wrapper and render the ORIGINAL request filter tree as that wrapper's own `WHERE`, table-qualified against the wrapper's single subquery alias
* *AND* the per-shard scan spec SHALL carry NO `filter`, so the predicate is applied exactly once
* *AND* the returned SQL SHALL evaluate the predicate, so the result SHALL equal native Exasol evaluation of the same query rather than the unfiltered row set the omission returned
* *AND* the adapter MUST NOT return SQL that omits the predicate, because nothing else would apply it
<!-- /DELTA:CHANGED -->

<!-- DELTA:NEW -->
### Scenario: A single-table WHERE filter splits at AND between the scan and the wrapper

* *GIVEN* a single-table `pushdown` request whose `filter` is an `AND` of conjuncts, at least one of which the DataFusion-bound render declines and at least one of which it renders, such as `WHERE c_decimal LIKE '1%' AND id > 1` over a `DECIMAL(9,2)` column
* *WHEN* the adapter builds the pushdown SQL
* *THEN* the adapter SHALL split the filter into its top-level conjuncts, flattening nested `AND` nodes and keeping each `OR` or `NOT` node whole, and render each conjunct for DataFusion once
* *AND* the per-shard scan spec of the qualified single-table wrapper's fan-out SHALL carry the conjunction of the rendered conjuncts as its `filter`
* *AND* the wrapper's `WHERE` SHALL carry the conjunction of the declined conjuncts, each rendered from its original request tree in the Exasol dialect, so each conjunct SHALL be applied exactly once
* *AND* the result SHALL equal native Exasol evaluation of the same query, for a row scan, an aggregate, a `GROUP BY`, a `COUNT(DISTINCT)`, and an `ORDER BY` with `LIMIT`
* *AND* a broadcast-eligible join whose filter holds a declined conjunct SHALL still take the N-scan fallback, as "A broadcast-eligible join whose filter declines takes the N-scan fallback" states
<!-- /DELTA:NEW -->
