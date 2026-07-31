# Feature: Pushdown Declined-Filter Self-Application

Guarantees every WHERE predicate the adapter accepts is evaluated, by self-applying in the adapter's
own returned SQL any predicate it cannot push to DataFusion.

## Background

* This delta changes no MECHANISM here. Both join scenarios describe an OUTCOME — broadcast forfeits
  its plan, an N-scan side-local conjunct becomes residual — and this delta only widens the set of
  TRIGGERS that reaches those outcomes to include a type-rewrite decline at the two join WHERE
  surfaces, which previously ran no type screen at all. See
  `vs-adapter/pushdown-planning-join-filter-type-coercion` (issue #215).
* The single-table scenario already names both trigger classes explicitly ("either because a
  type-rewrite guard returned `None` or because the DataFusion dialect cannot express a node in the
  tree"). The two join scenarios named only the second, which was accurate only because the join
  sites ran no type guard. Making the trigger set explicit at all three sites removes an asymmetry
  a reader would otherwise have to infer.
* The N-scan partition's leg-eligibility rule gains a THIRD condition and, with it, a per-side
  dimension the recorded two-condition rule did not have: the type screen needs the OWNING side's
  own column metadata, so it runs per side and per conjunct AFTER attribution, not over the combined
  pre-attribution set. `vs-adapter/pushdown-planning-join-fallback` owns that rule's full statement.
* The both-dialects-unrenderable clean-error backstop is unaffected and needs no new route: a
  type-declined predicate is by construction renderable in the Exasol dialect — Exasol applies the
  implicit non-string-to-VARCHAR coercion DataFusion refuses, which is exactly why the outer `WHERE`
  is a safe home for it.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: A broadcast-eligible join whose filter declines takes the N-scan fallback

* *GIVEN* an inner equi-join `pushdown` request that meets every other broadcast condition — two involved tables, an equi-condition, disjoint column names, no Exasol postprocessing, a byte size at or below the broadcast threshold
* *AND* the request carries a non-null `filter` whose DataFusion-bound render declines, either because a type-rewrite guard returned `None` when run against the union of the two involved tables' column metadata or because the DataFusion dialect cannot express a node in the tree
* *WHEN* the adapter renders the join pushdown
* *THEN* the adapter SHALL decline the broadcast plan and fall through to the unified unaccelerated N-scan fallback, which applies the predicate itself, honouring the recorded broadcast contract that a filter the translator cannot render is served by the fallback
* *AND* the adapter MUST NOT emit a broadcast plan whose scan spec carries no filter, because the broadcast SQL has no outer `WHERE` in which the predicate could be applied
* *AND* the decline SHALL be a clean `Ok(None)`-shaped fall-through, NOT an error, matching the disjoint-schema, unrenderable-condition, and widened-projection declines already on that path
* *AND* both trigger classes SHALL reach this ONE outcome, so the adapter SHALL NOT grow a second broadcast-decline route for a type decline — REPLACING the recorded "whose DataFusion-bound render declines", which named only the dialect trigger because the broadcast site ran no type screen (issue #215)
* *AND* a filter the type-rewrite pipeline REWRITES rather than declines SHALL NOT trigger this decline, and SHALL keep the broadcast plan with the REWRITTEN tree carried in the common spec
* *AND* the returned rows SHALL equal native Exasol evaluation of the same join
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: An N-scan side-local conjunct whose DataFusion render declines becomes a residual conjunct

* *GIVEN* an N-scan unaccelerated join over N ≥ 2 involved tables whose WHERE filter carries a top-level conjunct that references only ONE table and whose DataFusion-bound render declines, either because the DataFusion dialect cannot express a node in it or because a type-rewrite guard returned `None` when run against THAT SIDE's own column metadata
* *WHEN* the adapter partitions the filter's top-level conjuncts between the per-leg fan-outs and the outer wrapper's `WHERE`
* *THEN* that conjunct SHALL be classified as RESIDUAL and rendered into the outer wrapper's `WHERE` table-qualified, NOT pushed into its side's fan-out leg
* *AND* the partition SHALL remain total and disjoint: a conjunct is pushed into a leg if and only if it is side-local to exactly one table AND the type-rewrite pipeline run against that side's own column metadata accepts it AND the DataFusion dialect can render that pipeline's REWRITTEN form of it; every other conjunct is residual, so no conjunct is dropped and none is applied twice — REPLACING the recorded two-condition rule "side-local to exactly one table AND the DataFusion dialect can render it", whose purely syntactic second condition admitted a conjunct DataFusion cannot type-check (issue #215)
* *AND* the renderability condition SHALL be evaluated on the REWRITTEN conjunct, because that is the tree the leg renders; a conjunct the pipeline ACCEPTS but whose REWRITTEN form is unrenderable SHALL become RESIDUAL in RAW form and MUST NOT fall out of both halves, which would apply it nowhere and return extra rows with no error — the same defect #279 found at the broadcast site, which `classify_where_filter`'s `(Some(raw), Some(tree)) if !datafusion_renderable(tree)` arm already prevents there
* *AND* the type screen SHALL run PER SIDE and PER CONJUNCT AFTER attribution, because two N-scan sides MAY declare the same column name with different Exasol types and only the owning side's metadata resolves it correctly
* *AND* a side-local conjunct that DOES render and IS type-accepted SHALL still be pushed into its leg exactly as before, so per-leg row-group pruning and row filtering are unchanged for every conjunct the adapter can push
* *AND* the filter each leg receives SHALL be pre-screened by that partition as type-accepted AND, in its REWRITTEN form, DataFusion-renderable, and SHALL BE that REWRITTEN tree, so the leg's own render cannot decline
* *AND* if a side's re-formed accepted-conjunct tree does not itself survive the pipeline, OR survives but is not DataFusion-renderable, that side's ENTIRE side-local set SHALL become residual, so no conjunct is ever applied nowhere
* *AND* the side-local predicate each side forwards to Iceberg manifest pruning SHALL still carry the declined conjunct in RAW form, because the screen governs only what a leg renders and pruning only ever removes files that provably cannot match
* *AND* the residual conjunct the outer wrapper renders SHALL be the RAW conjunct in the Exasol dialect, which is renderable by construction for a type decline because Exasol applies the implicit non-string-to-VARCHAR coercion DataFusion refuses
* *AND* the returned rows SHALL equal native Exasol evaluation of the same join
<!-- /DELTA:CHANGED -->
