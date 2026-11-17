# Feature: Pushdown Declined-Filter Self-Application

Guarantees that the adapter evaluates every WHERE predicate it accepts. The adapter applies, in its own returned SQL, any predicate it cannot push to DataFusion.

## Background

* Exasol does not re-apply a predicate whose capability the adapter advertised, and the response has no field to hand one back. An omitted predicate returns extra unfiltered rows.
* A conjunct declines when the scan cannot compute it (`pushdown-types/pushdown-planning-scan-text-conversion`). Exasol drops a `WHERE` that folds to `TRUE`, so it never sends a trivially-true filter.
* The adapter self-applies a declined conjunct by rendering its original request tree in the Exasol dialect, which renders every predicate Exasol sends (`sql-comprehension/vs-expression-translator`), so Exasol applies its own conversions with the session's settings.
* File pruning reads the original request filter at the single-table scan and at each join side, so it keeps every conjunct, renderable or not, and removes only files that cannot match.
* `pushdown-joins/pushdown-planning-join-fallback` owns the N-scan leg-eligibility rule.

## Scenarios

### Scenario: A declined single-table WHERE filter is applied in the adapter's own outer WHERE

* *GIVEN* a single-table `pushdown` request carrying a non-null `filter` whose capability the adapter advertises, for example `WHERE SECOND(c_ts, 3) > 1`, delegated because `FN_SECOND` is advertised
* *AND* the DataFusion-bound render declines every top-level conjunct of that filter
* *WHEN* the adapter builds the pushdown SQL
* *THEN* the adapter SHALL route the request to the qualified single-table wrapper and render the ORIGINAL request filter tree as that wrapper's own `WHERE`, table-qualified against the wrapper's single subquery alias
* *AND* the per-shard scan spec SHALL carry NO `filter`, so the predicate is applied exactly once
* *AND* the returned SQL SHALL evaluate the predicate, so the result SHALL equal native Exasol evaluation of the same query rather than the unfiltered row set the omission returned
* *AND* the adapter MUST NOT return SQL that omits the predicate, because nothing else would apply it

### Scenario: A declined filter is applied before aggregation, grouping, and truncation

* *GIVEN* a single-table `pushdown` request whose filter declines and which ALSO carries an aggregate, a `groupBy`, a `COUNT(DISTINCT)`, an `orderBy`, or a `limit`
* *WHEN* the adapter builds the pushdown SQL
* *THEN* the self-applied `WHERE` SHALL sit between the raw sharded fan-out and every aggregate, GROUP BY, HAVING, ORDER BY, and LIMIT clause the wrapper renders, so the predicate restricts the rows those clauses consume
* *AND* the fan-out SHALL remain aggregate-free, sort-free, and LIMIT-free, so no shard aggregates or truncates unfiltered rows
* *AND* the adapter MUST NOT apply the predicate to already-aggregated or already-truncated output
* *AND* the returned aggregate value, group set, and row window SHALL each equal native Exasol evaluation of the same query

### Scenario: A filter that renders keeps the wrapper-free fast path unchanged

* *GIVEN* a single-table `pushdown` request whose filter renders successfully for DataFusion
* *WHEN* the adapter builds the pushdown SQL
* *THEN* the emitted SQL SHALL be byte-identical to its pre-change output, carrying the rendered filter in the per-shard common spec and NO outer `SELECT … FROM (…)` materialization boundary
* *AND* no golden-SQL fixture covering a rendering filter SHALL change
* *AND* the decline path's wrapper SHALL therefore add no cost to any request the adapter can push

### Scenario: A declined filter does not widen the wrapper's projection

* *GIVEN* a single-table `pushdown` request whose filter holds a declined conjunct
* *WHEN* the adapter builds the qualified single-table wrapper
* *THEN* the wrapper's fan-out SHALL emit only the columns that the request's select list, the wrapper's other clauses, and the declined conjuncts name, and no column because a conjunct declined
* *AND* the wrapper's returned column count, order, and declared types SHALL equal what the request's `selectList` declares: the full base row in table-column order for an absent or JSON-null `selectList`, and one column for an empty one, so the route never trips Exasol's positional `04000` validation
* *AND* for an absent or JSON-null `selectList`, the fan-out SHALL emit the full base row, because Exasol validates the result positionally against the whole row

### Scenario: A broadcast-eligible join whose filter declines takes the N-scan fallback

* *GIVEN* an inner equi-join `pushdown` request that meets every other broadcast condition, two legs, an equi-condition, a byte size at or below the broadcast threshold
* *AND* the request carries a non-null `filter` whose DataFusion-bound render declines against the two legs' column metadata keyed by (relation alias, column)
* *WHEN* the adapter renders the join pushdown
* *THEN* the adapter SHALL decline the broadcast plan and fall through to the unified unaccelerated N-scan fallback, which applies the predicate itself, honouring the recorded broadcast contract that a filter the translator cannot render is served by the fallback
* *AND* the adapter MUST NOT emit a broadcast plan whose scan spec carries no filter, because the broadcast SQL has no outer `WHERE` in which the predicate could be applied
* *AND* the decline SHALL be a clean `Ok(None)`-shaped fall-through, NOT an error, matching the unrenderable-condition and widened-projection declines already on that path
* *AND* the returned rows SHALL equal native Exasol evaluation of the same join

### Scenario: An N-scan side-local conjunct whose DataFusion render declines becomes a residual conjunct

* *GIVEN* an N-scan unaccelerated join over N ≥ 2 involved tables whose WHERE filter carries a top-level conjunct that references only ONE table and whose DataFusion-bound render declines against THAT SIDE's own column metadata
* *WHEN* the adapter partitions the filter's top-level conjuncts between the per-leg fan-outs and the outer wrapper's `WHERE`
* *THEN* that conjunct SHALL be classified as RESIDUAL and rendered into the outer wrapper's `WHERE` table-qualified, NOT pushed into its side's fan-out leg
* *AND* the partition SHALL remain total and disjoint: a conjunct is pushed into a leg if and only if it is side-local to exactly one table AND the scan translator renders it against that side's own column metadata; every other conjunct is residual, so no conjunct is dropped and none is applied twice
* *AND* the screen SHALL run PER SIDE and PER CONJUNCT AFTER attribution, because two N-scan sides MAY declare the same column name with different Exasol types and only the owning side's metadata resolves it correctly
* *AND* a side-local conjunct the scan translator renders SHALL still be pushed into its leg exactly as before, so per-leg row-group pruning and row filtering are unchanged for every conjunct the adapter can push
* *AND* each side-local conjunct SHALL be rendered exactly once, against its side's column types, and the filter each leg receives SHALL be the conjunction of the accepted conjuncts' rendered SQL, so no second render can decline it
* *AND* the side-local predicate each side forwards to Iceberg manifest pruning SHALL still carry the declined conjunct in RAW form, because the screen governs only what a leg renders and pruning only ever removes files that provably cannot match
* *AND* the residual conjunct the outer wrapper renders SHALL be the RAW conjunct in the Exasol dialect, so Exasol applies its own conversions with the session's settings
* *AND* the returned rows SHALL equal native Exasol evaluation of the same join

### Scenario: An absent filter is distinguished from a declined filter at every site

* *GIVEN* a `pushdown` request carrying no `filter` key, or a `filter` whose value is JSON null
* *WHEN* the adapter builds the pushdown SQL at the single-table, broadcast-join, or N-scan per-leg site
* *THEN* every site SHALL treat the filter as ABSENT: no scan-spec filter, no outer `WHERE`, no wrapper route, and no broadcast decline
* *AND* no site SHALL infer "absent" from a `None` render result alone, because a present-but-declined filter produces the same `None` and requires the opposite handling
* *AND* the emitted SQL for a filterless request SHALL be byte-identical to its pre-change output at all three sites

### Scenario: A single-table WHERE filter splits at AND between the scan and the wrapper

* *GIVEN* a single-table `pushdown` request whose `filter` is an `AND` of conjuncts, at least one of which the DataFusion-bound render declines and at least one of which it renders, such as `WHERE c_decimal LIKE '1%' AND id > 1` over a `DECIMAL(9,2)` column
* *WHEN* the adapter builds the pushdown SQL
* *THEN* the adapter SHALL split the filter into its top-level conjuncts, flattening nested `AND` nodes and keeping each `OR` or `NOT` node whole, and apply each conjunct exactly once, in the scan or in the wrapper's `WHERE`
* *AND* the per-shard scan spec of the qualified single-table wrapper's fan-out SHALL carry the conjunction of the rendered conjuncts as its `filter`, and the wrapper's `WHERE` SHALL carry the conjunction of the declined conjuncts, each rendered from its original request tree in the Exasol dialect
* *AND* the fan-out's columns SHALL follow "A declined filter does not widen the wrapper's projection", so a column that only a rendered conjunct names is not emitted
* *AND* the result SHALL equal native Exasol evaluation of the same query, for a row scan, an aggregate, a `GROUP BY`, a `COUNT(DISTINCT)`, and an `ORDER BY` with `LIMIT`
* *AND* a broadcast-eligible join whose filter holds a declined conjunct SHALL still take the N-scan fallback, as "A broadcast-eligible join whose filter declines takes the N-scan fallback" states
