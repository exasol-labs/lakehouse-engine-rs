# Feature: Pushdown Declined-Filter Self-Application

Guarantees every WHERE predicate the adapter accepts is evaluated, by self-applying in the adapter's
own returned SQL any predicate it cannot push to DataFusion.

## Background

* Exasol's pushdown response envelope carries exactly two fields, `type` and `sql`. There is no
  residual, partial-pushdown, or "I did not handle this" field, and capability negotiation is
  whole-capability and whole-schema, fixed at DDL time. The adapter cannot hand a predicate back.
* Exasol splits the query BEFORE sending the pushdown request, using the capabilities response
  alone: it post-processes only what the adapter did NOT advertise. A predicate whose capability
  the adapter advertised is removed from Exasol's own plan and never re-checked.
* Three sites render the DataFusion-bound WHERE filter and each returns `Option<String>`: the
  single-table path (`handle_pushdown`), the broadcast-join path (`render_broadcast_join`), and the
  N-scan per-leg path (`build_side_fan_out_sql`). `None` conflates three outcomes — no filter in the
  request, a filter that renders trivially true, and a filter that declined. The first two are safe
  to omit; the third is not, and omitting it returned extra unfiltered rows.
* A decline arises from two independent sources: the adapter's own type-rewrite guards
  (`apply_type_rewrites`, single-table path only) returning `None`, and the DataFusion-dialect
  renderer failing on a node it cannot express. Both are in scope; this feature names neither
  source, only the outcome.
* Declined and trivially-true are distinguishable with the translator's existing entry points and
  need no new renderer: `render_expression_safe` does not suppress a trivially-true result, so it
  returns `None` for exactly the declined case. The trivially-true rule stays owned by
  `crates/vs-expression` and is never re-tested at a call site.
* Self-application renders the ORIGINAL request filter tree, not the type-rewritten one. The
  rewrites exist to make a predicate safe for DataFusion; Exasol evaluates the predicate with its
  own implicit coercions and needs the tree as sent.
* A predicate unrenderable under BOTH dialects can be applied nowhere. That returns a clean
  adapter error, never a result. This is not a new failure mode: it is the same refusal site every
  existing route into the qualified wrapper already reaches, and the same
  correctness-over-availability outcome `vs-adapter/pushdown-planning-selectlist-expressions`
  records for a select-list node untranslatable under both dialects.
* A self-applied predicate whose Exasol render is TRIVIALLY TRUE is a third outcome, not that error:
  it emits no clause. The WHERE-filter renderers suppress a trivially-true result to nothing exactly
  as they report an unrenderable one, so the error MUST be decided by a NON-SUPPRESSING render. Taking
  it from a WHERE-filter renderer's empty result would re-create, one dialect over, the same
  three-way conflation this feature exists to remove.
* The decline classification has ONE owner per path. On the single-table path `handle_pushdown`
  computes it once, beside the scan filter it already derives from the same request filter and column
  types, and passes the result down; no downstream site recomputes renderability. This mirrors
  `crates/vs-expression` owning the trivially-true rule — one decision, one site, nothing to drift.
* Self-application MUST place the predicate ahead of any aggregation, grouping, or truncation the
  request also carries. Wrapping already-aggregated or already-truncated output in a WHERE would
  filter the wrong rows, so the single-table path routes a declined filter through the qualified
  single-table wrapper, whose raw fan-out is aggregate-free, sort-free, and LIMIT-free.
* The wrapper-free single-table fast path is unchanged for every request whose filter renders. A
  materialization boundary appears only on the decline path, which is rare and already slower.
* Iceberg-level file pruning is unaffected and stays sound at BOTH pruning inputs — the single-table
  `resolve_file_list` tree and each join side's side-local predicate. Both keep every conjunct,
  renderable or not: pruning only ever removes files that provably cannot match, and the predicate is
  still evaluated — in the wrapper's WHERE rather than in the scan.
* Apache Iceberg spec check: checked, not engaged. This feature changes only where a SQL predicate
  is evaluated between Exasol's engine and the node-local DataFusion scan. It reads no manifest
  field, evaluates no column bound, and touches no schema-resolution, field-id, or type-mapping
  surface, so no normative Iceberg requirement applies and there is no deviation to fix or track.

## Scenarios

### Scenario: A declined single-table WHERE filter is applied in the adapter's own outer WHERE

* *GIVEN* a single-table `pushdown` request carrying a non-null `filter` whose capability the adapter advertises — for example `WHERE SECOND(c_ts, 3) > 1`, delegated because `FN_SECOND` is advertised
* *AND* the DataFusion-bound render of that filter declines, either because a type-rewrite guard returned `None` or because the DataFusion dialect cannot express a node in the tree
* *WHEN* the adapter builds the pushdown SQL
* *THEN* the adapter SHALL route the request to the qualified single-table wrapper and render the ORIGINAL request filter tree as that wrapper's own `WHERE`, table-qualified against the wrapper's single subquery alias
* *AND* the per-shard scan spec SHALL carry NO `filter`, so the predicate is applied exactly once
* *AND* the returned SQL SHALL evaluate the predicate, so the result SHALL equal native Exasol evaluation of the same query rather than the unfiltered row set the omission returned
* *AND* the adapter MUST NOT return SQL that omits the predicate, because Exasol removed it from its own plan when it delegated it and re-applies nothing

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

### Scenario: A broadcast-eligible join whose filter declines takes the N-scan fallback

* *GIVEN* an inner equi-join `pushdown` request that meets every other broadcast condition — two involved tables, an equi-condition, disjoint column names, no Exasol postprocessing, a byte size at or below the broadcast threshold
* *AND* the request carries a non-null `filter` whose DataFusion-bound render declines
* *WHEN* the adapter renders the join pushdown
* *THEN* the adapter SHALL decline the broadcast plan and fall through to the unified unaccelerated N-scan fallback, which applies the predicate itself, honouring the recorded broadcast contract that a filter the translator cannot render is served by the fallback
* *AND* the adapter MUST NOT emit a broadcast plan whose scan spec carries no filter, because the broadcast SQL has no outer `WHERE` in which the predicate could be applied
* *AND* the decline SHALL be a clean `Ok(None)`-shaped fall-through, NOT an error, matching the disjoint-schema, unrenderable-condition, and widened-projection declines already on that path
* *AND* the returned rows SHALL equal native Exasol evaluation of the same join

### Scenario: An N-scan side-local conjunct whose DataFusion render declines becomes a residual conjunct

* *GIVEN* an N-scan unaccelerated join over N ≥ 2 involved tables whose WHERE filter carries a top-level conjunct that references only ONE table and whose DataFusion-bound render declines
* *WHEN* the adapter partitions the filter's top-level conjuncts between the per-leg fan-outs and the outer wrapper's `WHERE`
* *THEN* that conjunct SHALL be classified as RESIDUAL and rendered into the outer wrapper's `WHERE` table-qualified, NOT pushed into its side's fan-out leg
* *AND* the partition SHALL remain total and disjoint: a conjunct is pushed into a leg if and only if it is side-local to exactly one table AND the DataFusion dialect can render it; every other conjunct is residual, so no conjunct is dropped and none is applied twice
* *AND* a side-local conjunct that DOES render SHALL still be pushed into its leg exactly as before, so per-leg row-group pruning and row filtering are unchanged for every conjunct the adapter can push
* *AND* the filter each leg receives SHALL be pre-screened as DataFusion-renderable by that partition, so the leg's own render cannot decline
* *AND* the side-local predicate each side forwards to Iceberg manifest pruning SHALL still carry the declined conjunct, because the screen governs only what a leg renders and pruning only ever removes files that provably cannot match
* *AND* the returned rows SHALL equal native Exasol evaluation of the same join

### Scenario: A predicate unrenderable under both dialects returns a clean error

* *GIVEN* a `pushdown` request whose filter carries a node no dialect can express — for example a `CAST` to `INTERVAL`, `GEOMETRY`, `HASHTYPE`, or `TIMESTAMP WITH LOCAL TIME ZONE`, delegated because `FN_CAST` is advertised
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
