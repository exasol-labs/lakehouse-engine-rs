# Feature: Pushdown Declined-Filter Self-Application

Guarantees that the adapter evaluates every WHERE predicate it accepts. The adapter applies, in its own returned SQL, any predicate it cannot push to DataFusion.

## Background

* Exasol does not re-apply a predicate whose capability the adapter advertised, and the response has no field to hand one back. An omitted predicate returns extra unfiltered rows.
* Three sites render the DataFusion-bound WHERE filter, and each returns `Option<String>`: the single-table path (`handle_pushdown`), the broadcast-join path (`render_broadcast_join`) and the N-scan per-leg path (`build_side_fan_out_sql`). `None` covers three outcomes: no filter, a filter that renders trivially true, and a declined filter. Only the declined filter must not be omitted.
* A filter declines when an adapter type-rewrite guard returns `None` or when the DataFusion-dialect renderer cannot express a node.
* `render_expression_safe` does not suppress a trivially-true result, so it returns `None` only for a declined filter. `crates/vs-expression` owns the trivially-true rule, and no call site re-tests it.
* Self-application renders the original request filter tree, not the type-rewritten tree. Exasol applies its own implicit coercions, including non-string-to-VARCHAR, which DataFusion refuses.
* A predicate unrenderable under both the DataFusion and the Exasol dialect is applied nowhere. The adapter returns a clean error, the same refusal as `pushdown/pushdown-planning-selectlist-expressions`. A self-applied predicate whose Exasol render is trivially true emits no clause and is not an error, so the adapter decides the error from a non-suppressing render.
* `handle_pushdown` computes the decline classification once per path and passes it down.
* The self-applied predicate must precede any aggregation, grouping or truncation the request carries. The single-table path therefore routes a declined filter through the qualified single-table wrapper, whose raw fan-out has no aggregate, sort or LIMIT.
* On the decline path the fan-out carries no filter, so projection width is the only remaining lever. A `SELECT *` cannot narrow, because Exasol validates its result positionally against the full base row.
* Iceberg file pruning keeps every conjunct, renderable or not, at both pruning inputs: the single-table scan-resolution tree and each join side's side-local predicate. Pruning only removes files that cannot match.
* The N-scan partition runs the type screen per side and per conjunct after attribution, because it needs the owning side's column metadata. `pushdown-joins/pushdown-planning-join-fallback` owns the leg-eligibility rule.

## Scenarios

### Scenario: A declined single-table WHERE filter is applied in the adapter's own outer WHERE

* *GIVEN* a single-table `pushdown` request carrying a non-null `filter` whose capability the adapter advertises, for example `WHERE SECOND(c_ts, 3) > 1`, delegated because `FN_SECOND` is advertised
* *AND* the DataFusion-bound render of that filter declines, either because a type-rewrite guard returned `None` or because the DataFusion dialect cannot express a node in the tree
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

### Scenario: A trivially-true filter is still omitted with no wrapper

* *GIVEN* a single-table `pushdown` request whose filter renders to exactly `TRUE` or `NULL`
* *WHEN* the adapter builds the pushdown SQL
* *THEN* the adapter SHALL omit the filter from the scan spec and SHALL NOT route the request to the wrapper, because a no-op predicate restricts nothing and omitting it cannot change a result
* *AND* the emitted SQL SHALL be byte-identical to its pre-change output
* *AND* the trivially-true rule SHALL stay owned by `crates/vs-expression`, so no adapter site SHALL test a rendered fragment against the literal strings `TRUE` or `NULL`

### Scenario: A declined filter does not widen the wrapper's projection

* *GIVEN* a single-table `pushdown` request whose filter declines
* *WHEN* the adapter derives the wrapper's inner-scan projection
* *THEN* the projection SHALL be decided by the request's `selectList` alone, never by the fact that a filter declined
* *AND* a request carrying a non-empty `selectList` SHALL keep the referenced-column narrowing, projecting only the columns the wrapper's rendered clauses name, its select list's and the self-applied predicate's
* *AND* a request carrying NO select list, a `SELECT *`, which Exasol sends as an omitted `selectList` key, SHALL project the FULL base row in table-column order at BOTH the inner scan and the wrapper's outer select list, because Exasol validates the pushdown result positionally against the whole row
* *AND* the "no select list" test SHALL treat an absent key, JSON null, an empty array, and a non-array value alike, so a later change in Exasol's wire form cannot reintroduce a positional mismatch

### Scenario: A broadcast-eligible join whose filter declines takes the N-scan fallback

* *GIVEN* an inner equi-join `pushdown` request that meets every other broadcast condition, two involved tables, an equi-condition, disjoint column names, no Exasol postprocessing, a byte size at or below the broadcast threshold
* *AND* the request carries a non-null `filter` whose DataFusion-bound render declines, either because a type-rewrite guard returned `None` when run against the union of the two involved tables' column metadata or because the DataFusion dialect cannot express a node in the tree
* *WHEN* the adapter renders the join pushdown
* *THEN* the adapter SHALL decline the broadcast plan and fall through to the unified unaccelerated N-scan fallback, which applies the predicate itself, honouring the recorded broadcast contract that a filter the translator cannot render is served by the fallback
* *AND* the adapter MUST NOT emit a broadcast plan whose scan spec carries no filter, because the broadcast SQL has no outer `WHERE` in which the predicate could be applied
* *AND* the decline SHALL be a clean `Ok(None)`-shaped fall-through, NOT an error, matching the disjoint-schema, unrenderable-condition, and widened-projection declines already on that path
* *AND* both trigger classes SHALL reach this ONE outcome, so the adapter SHALL NOT grow a second broadcast-decline route for a type decline, REPLACING the recorded "whose DataFusion-bound render declines", which named only the dialect trigger because the broadcast site ran no type screen
* *AND* a filter the type-rewrite pipeline REWRITES rather than declines SHALL NOT trigger this decline, and SHALL keep the broadcast plan with the REWRITTEN tree carried in the common spec
* *AND* the returned rows SHALL equal native Exasol evaluation of the same join

### Scenario: An N-scan side-local conjunct whose DataFusion render declines becomes a residual conjunct

* *GIVEN* an N-scan unaccelerated join over N ≥ 2 involved tables whose WHERE filter carries a top-level conjunct that references only ONE table and whose DataFusion-bound render declines, either because the DataFusion dialect cannot express a node in it or because a type-rewrite guard returned `None` when run against THAT SIDE's own column metadata
* *WHEN* the adapter partitions the filter's top-level conjuncts between the per-leg fan-outs and the outer wrapper's `WHERE`
* *THEN* that conjunct SHALL be classified as RESIDUAL and rendered into the outer wrapper's `WHERE` table-qualified, NOT pushed into its side's fan-out leg
* *AND* the partition SHALL remain total and disjoint: a conjunct is pushed into a leg if and only if it is side-local to exactly one table AND the type-rewrite pipeline run against that side's own column metadata accepts it AND the DataFusion dialect can render that pipeline's REWRITTEN form of it; every other conjunct is residual, so no conjunct is dropped and none is applied twice, REPLACING the recorded two-condition rule "side-local to exactly one table AND the DataFusion dialect can render it", whose purely syntactic second condition admitted a conjunct DataFusion cannot type-check
* *AND* the renderability condition SHALL be evaluated on the REWRITTEN conjunct, because that is the tree the leg renders; a conjunct the pipeline ACCEPTS but whose REWRITTEN form is unrenderable SHALL become RESIDUAL in RAW form and MUST NOT fall out of both halves, which would apply it nowhere and return extra rows with no error, the same defect #279 found at the broadcast site, which `classify_where_filter`'s `(Some(raw), Some(tree)) if !datafusion_renderable(tree)` arm already prevents there
* *AND* the type screen SHALL run PER SIDE and PER CONJUNCT AFTER attribution, because two N-scan sides MAY declare the same column name with different Exasol types and only the owning side's metadata resolves it correctly
* *AND* a side-local conjunct that DOES render and IS type-accepted SHALL still be pushed into its leg exactly as before, so per-leg row-group pruning and row filtering are unchanged for every conjunct the adapter can push
* *AND* the filter each leg receives SHALL be pre-screened by that partition as type-accepted AND, in its REWRITTEN form, DataFusion-renderable, and SHALL BE that REWRITTEN tree, so the leg's own render cannot decline
* *AND* if a side's re-formed accepted-conjunct tree does not itself survive the pipeline, OR survives but is not DataFusion-renderable, that side's ENTIRE side-local set SHALL become residual, so no conjunct is ever applied nowhere
* *AND* the side-local predicate each side forwards to Iceberg manifest pruning SHALL still carry the declined conjunct in RAW form, because the screen governs only what a leg renders and pruning only ever removes files that provably cannot match
* *AND* the residual conjunct the outer wrapper renders SHALL be the RAW conjunct in the Exasol dialect, which is renderable by construction for a type decline because Exasol applies the implicit non-string-to-VARCHAR coercion DataFusion refuses
* *AND* the returned rows SHALL equal native Exasol evaluation of the same join

### Scenario: A predicate unrenderable under both dialects returns a clean error

* *GIVEN* a `pushdown` request whose filter carries a node no dialect can express, for example a `CAST` to `INTERVAL`, `GEOMETRY`, `HASHTYPE`, or `TIMESTAMP WITH LOCAL TIME ZONE`, delegated because `FN_CAST` is advertised
* *WHEN* the adapter attempts to self-apply the predicate in its outer `WHERE`
* *THEN* the adapter SHALL return a client-facing error naming the unrenderable predicate, at both the single-table wrapper and the N-scan wrapper
* *AND* the adapter MUST NOT return SQL that omits the predicate, so the query SHALL fail rather than return rows the predicate would have excluded
* *AND* the error SHALL redact no more and no less than the existing wrapper refusal site already redacts, because this is a new route to an existing outcome and not a new failure mode
* *AND* Exasol SHALL NOT re-plan on that error, so the failure is final and visible to the client

### Scenario: An absent filter is distinguished from a declined filter at every site

* *GIVEN* a `pushdown` request carrying no `filter` key, or a `filter` whose value is JSON null
* *WHEN* the adapter builds the pushdown SQL at the single-table, broadcast-join, or N-scan per-leg site
* *THEN* every site SHALL treat the filter as ABSENT: no scan-spec filter, no outer `WHERE`, no wrapper route, and no broadcast decline
* *AND* no site SHALL infer "absent" from a `None` render result alone, because a present-but-declined filter produces the same `None` and requires the opposite handling
* *AND* the emitted SQL for a filterless request SHALL be byte-identical to its pre-change output at all three sites
