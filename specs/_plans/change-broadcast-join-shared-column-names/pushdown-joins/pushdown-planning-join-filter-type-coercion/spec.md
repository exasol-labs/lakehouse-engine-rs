# Feature: Pushdown Planning, Join Filter Type Coercion

Applies the pushdown type-rewrite pipeline to the broadcast join's combined filter and the N-scan fallback's per-leg filter. A predicate whose column types DataFusion does not coerce is rewritten, or declined into the join path's self-application route, instead of failing the scan.

<!-- DELTA:CHANGED -->
## Background

* Both join sites run `apply_type_rewrites`, the ordered pipeline (`like_subject_type_guard`, `string_function_arg_type_guard`, `rewrite_decimal_stringifications`) owned by `pushdown-types/pushdown-planning-string-fn-type-coercion-composition`. Guard decisions are inherited from the single-table WHERE surface.
* The two surfaces screen against different column-type universes:
  * Broadcast: both legs' columns, keyed by the pair (relation alias, column) (`pushdown-joins/pushdown-planning-join`). A qualified column resolves to its own leg's Exasol type even when the two legs share a column name.
  * N-scan: each side's own columns, applied after conjunct attribution to a table. Two sides may declare the same column name with different Exasol types.
* A type decline takes the outcome each surface already has for a syntactically unrenderable filter. Broadcast forfeits the broadcast plan to the N-scan fallback, because broadcast SQL has no outer `WHERE`. An N-scan side-local conjunct becomes a residual conjunct in the qualified outer `WHERE`. `pushdown/pushdown-declined-filter-self-apply` owns both outcomes.
* The DATE CAST-to-VARCHAR arm carries the altered-session `NLS_DATE_FORMAT` tracked exception at both join surfaces, as on the single-table surface.
* The leg-eligibility screen governs the rewritten tree, because that is the tree the leg renders. A conjunct whose rewritten form DataFusion cannot render becomes residual in raw form.
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: A broadcast-join filter over a non-string LIKE subject declines the broadcast plan

* *GIVEN* a broadcast-eligible inner equi-join `pushdown` request meeting every other broadcast condition, whose `filter` carries a `predicate_like` whose subject is a bare `column` node
* *AND* the column's Exasol type in its own leg's column metadata is a non-string, non-DATE type, `DECIMAL(p,s)` (including an Exasol integer, which the wire carries as `DECIMAL(p,0)`), `DOUBLE PRECISION`, `BOOLEAN`, or `TIMESTAMP`, or the column's name does not resolve in that metadata at all
* *WHEN* the adapter renders the broadcast join pushdown
* *THEN* the type-rewrite pipeline SHALL decline the filter, and the adapter SHALL decline the WHOLE broadcast plan and fall through to the unified unaccelerated N-scan fallback, which self-applies the predicate in its own qualified outer `WHERE`
* *AND* the decline SHALL be the SAME clean `Ok(None)`-shaped fall-through the syntactically-unrenderable-filter decline takes, NOT an error and NOT a new route, see `pushdown/pushdown-declined-filter-self-apply`
* *AND* the adapter MUST NOT emit a broadcast plan carrying the predicate rendered bare, which DataFusion's `type_coercion` planner rejects, failing the scan with no result
* *AND* the adapter MUST NOT emit a broadcast plan whose scan spec simply OMITS the predicate, because broadcast SQL carries no outer `WHERE` in which anything could apply it
* *AND* the returned rows SHALL equal native Exasol evaluation of the same join
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: A broadcast-join filter over a DATE LIKE subject keeps the broadcast plan

* *GIVEN* a broadcast-eligible inner equi-join `pushdown` request whose `filter` carries a `predicate_like` whose subject is a bare `column` node
* *AND* the column's Exasol type in its own leg's column metadata is `DATE`
* *WHEN* the adapter renders the broadcast join pushdown
* *THEN* the adapter SHALL rewrap the subject as an explicit CAST-to-VARCHAR node before rendering, exactly as the single-table WHERE surface does, and SHALL carry the REWRITTEN tree's rendered fragment in the common spec
* *AND* the broadcast plan SHALL remain eligible, so a DATE-column LIKE SHALL NOT cost the broadcast optimization, the CAST arm is a rewrite, not a decline
* *AND* the adapter SHALL render the pipeline's REWRITTEN tree and MUST NOT render the raw tree, because rendering the raw tree after a successful rewrite would silently discard the coercion and reintroduce the hard scan failure
* *AND* the emitted match semantics and the altered-session `NLS_DATE_FORMAT` tracked exception SHALL be identical to the single-table WHERE surface's, because the identical CAST node is emitted
* *AND* the raw filter tree forwarded to Iceberg file pruning SHALL be left unchanged
<!-- /DELTA:CHANGED -->
