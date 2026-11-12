# Feature: Pushdown Declined-Filter Self-Application

Guarantees that the adapter evaluates every WHERE predicate it accepts. The adapter applies, in its own returned SQL, any predicate it cannot push to DataFusion.

## Background

* Exasol does not re-apply a predicate whose capability the adapter advertised, and the response has no field to hand one back. An omitted predicate returns extra unfiltered rows.
* Three sites render the DataFusion-bound WHERE filter: the single-table path (`handle_pushdown`) and the broadcast-join path (`render_broadcast_join`) through `ScanTranslator::split_where`, and the N-scan legs one conjunct at a time through `screen_leg_filter`. A missing scan filter covers two outcomes: no filter and a declined filter. Exasol drops a `WHERE` that folds to `TRUE`, so it never sends a trivially-true filter. Only the declined filter must not be omitted, so each site returns it beside the scan filter.
* A filter declines when the scan translator declines it (`pushdown-types/pushdown-planning-scan-text-conversion`).
* `render_scan_filter` returns `Err` only for a declined filter and `Ok(None)` for a trivially-true one. `crates/vs-expression` owns the trivially-true rule, and no call site re-tests it.
* Self-application renders the original request filter tree, so Exasol applies its own conversions with the session's settings.
* The adapter self-applies a declined predicate through the non-suppressing Exasol render, which renders every predicate Exasol sends (`sql-comprehension/vs-expression-translator`), so a declined predicate is never applied nowhere.
* `handle_pushdown` computes the decline classification once per path and passes it down.
* The self-applied predicate must precede any aggregation, grouping or truncation the request carries. The single-table path therefore routes a declined filter through the qualified single-table wrapper, whose raw fan-out has no aggregate, sort or LIMIT.
* On the decline path the fan-out carries no filter, so projection width is the only remaining lever. A `SELECT *` cannot narrow, because Exasol validates its result positionally against the full base row.
* Iceberg file pruning keeps every conjunct, renderable or not, at both pruning inputs: the single-table scan-resolution tree and each join side's side-local predicate. Pruning only removes files that cannot match.
* The N-scan partition screens per side and per conjunct after attribution, because it needs the owning side's column metadata. `pushdown-joins/pushdown-planning-join-fallback` owns the leg-eligibility rule.

## Scenarios

### Scenario: A declined single-table WHERE filter is applied in the adapter's own outer WHERE

* *GIVEN* a single-table `pushdown` request carrying a non-null `filter` whose capability the adapter advertises, for example `WHERE SECOND(c_ts, 3) > 1`, delegated because `FN_SECOND` is advertised
* *AND* the DataFusion-bound render of that filter declines
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

* *GIVEN* a single-table `pushdown` request whose filter declines
* *WHEN* the adapter derives the wrapper's inner-scan projection
* *THEN* the projection SHALL be decided by the request's `selectList` alone, never by the fact that a filter declined
* *AND* a request carrying a non-empty `selectList` SHALL keep the referenced-column narrowing, projecting only the columns the wrapper's rendered clauses name, its select list's and the self-applied predicate's
* *AND* a request carrying NO select list, a `SELECT *`, which Exasol sends as an omitted `selectList` key, SHALL project the FULL base row in table-column order at BOTH the inner scan and the wrapper's outer select list, because Exasol validates the pushdown result positionally against the whole row
* *AND* the "no select list" test SHALL treat an absent key, JSON null, an empty array, and a non-array value alike, so a later change in Exasol's wire form cannot reintroduce a positional mismatch

### Scenario: A broadcast-eligible join whose filter declines takes the N-scan fallback

* *GIVEN* an inner equi-join `pushdown` request that meets every other broadcast condition, two legs, an equi-condition, no Exasol postprocessing, a byte size at or below the broadcast threshold
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
