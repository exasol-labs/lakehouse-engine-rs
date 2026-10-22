# Feature: Pushdown Planning, Join Filter Type Coercion

Applies the pushdown type-rewrite pipeline to the broadcast join's combined filter and the N-scan fallback's per-leg filter. A predicate whose column types DataFusion does not coerce is rewritten, or declined into the join path's self-application route, instead of failing the scan.

## Background

* Both join sites run `apply_type_rewrites`, the ordered pipeline (`like_subject_type_guard`, `string_function_arg_type_guard`, `rewrite_decimal_stringifications`) owned by `pushdown-types/pushdown-planning-string-fn-type-coercion-composition`. Guard decisions are inherited from the single-table WHERE surface.
* The two surfaces screen against different column-type universes:
  * Broadcast: the union of both involved tables' columns, matched by bare column name, read after `disjoint_schema_guard` has passed. That guard makes a bare name resolve to one Exasol type.
  * N-scan: each side's own columns, applied after conjunct attribution to a table. Two sides may declare the same column name with different Exasol types.
* A type decline takes the outcome each surface already has for a syntactically unrenderable filter. Broadcast forfeits the broadcast plan to the N-scan fallback, because broadcast SQL has no outer `WHERE`. An N-scan side-local conjunct becomes a residual conjunct in the qualified outer `WHERE`. `pushdown/pushdown-declined-filter-self-apply` owns both outcomes.
* The DATE CAST-to-VARCHAR arm carries the altered-session `NLS_DATE_FORMAT` tracked exception at both join surfaces, as on the single-table surface.
* The leg-eligibility screen governs the rewritten tree, because that is the tree the leg renders. A conjunct whose rewritten form DataFusion cannot render becomes residual in raw form.

## Scenarios

### Scenario: A broadcast-join filter over a non-string LIKE subject declines the broadcast plan

* *GIVEN* a broadcast-eligible inner equi-join `pushdown` request meeting every other broadcast condition, whose `filter` carries a `predicate_like` whose subject is a bare `column` node
* *AND* the column's Exasol type in the union of the two involved tables' column metadata is a non-string, non-DATE type, `DECIMAL(p,s)` (including an Exasol integer, which the wire carries as `DECIMAL(p,0)`), `DOUBLE PRECISION`, `BOOLEAN`, or `TIMESTAMP`, or the column's name does not resolve in that union at all
* *WHEN* the adapter renders the broadcast join pushdown
* *THEN* the type-rewrite pipeline SHALL decline the filter, and the adapter SHALL decline the WHOLE broadcast plan and fall through to the unified unaccelerated N-scan fallback, which self-applies the predicate in its own qualified outer `WHERE`
* *AND* the decline SHALL be the SAME clean `Ok(None)`-shaped fall-through the syntactically-unrenderable-filter decline takes, NOT an error and NOT a new route, see `pushdown/pushdown-declined-filter-self-apply`
* *AND* the adapter MUST NOT emit a broadcast plan carrying the predicate rendered bare, which DataFusion's `type_coercion` planner rejects, failing the scan with no result
* *AND* the adapter MUST NOT emit a broadcast plan whose scan spec simply OMITS the predicate, because broadcast SQL carries no outer `WHERE` in which anything could apply it
* *AND* the returned rows SHALL equal native Exasol evaluation of the same join

### Scenario: A broadcast-join filter over a DATE LIKE subject keeps the broadcast plan

* *GIVEN* a broadcast-eligible inner equi-join `pushdown` request whose `filter` carries a `predicate_like` whose subject is a bare `column` node
* *AND* the column's Exasol type in the union of the two involved tables' column metadata is `DATE`
* *WHEN* the adapter renders the broadcast join pushdown
* *THEN* the adapter SHALL rewrap the subject as an explicit CAST-to-VARCHAR node before rendering, exactly as the single-table WHERE surface does, and SHALL carry the REWRITTEN tree's rendered fragment in the common spec
* *AND* the broadcast plan SHALL remain eligible, so a DATE-column LIKE SHALL NOT cost the broadcast optimization, the CAST arm is a rewrite, not a decline
* *AND* the adapter SHALL render the pipeline's REWRITTEN tree and MUST NOT render the raw tree, because rendering the raw tree after a successful rewrite would silently discard the coercion and reintroduce the hard scan failure
* *AND* the emitted match semantics and the altered-session `NLS_DATE_FORMAT` tracked exception SHALL be identical to the single-table WHERE surface's, because the identical CAST node is emitted
* *AND* the raw filter tree forwarded to Iceberg file pruning SHALL be left unchanged

### Scenario: An N-scan side-local conjunct the type pipeline declines becomes a residual conjunct

* *GIVEN* an N-scan unaccelerated join over N ≥ 2 involved tables whose WHERE filter carries a top-level conjunct that is side-local to exactly ONE table, is syntactically renderable for DataFusion, and that the type-rewrite pipeline declines when run against THAT SIDE's own column metadata, for example a `LIKE` over that side's `DECIMAL(20,0)` column
* *WHEN* the adapter partitions the filter's top-level conjuncts between the per-leg fan-outs and the outer wrapper's `WHERE`
* *THEN* the adapter SHALL run the type-rewrite pipeline PER SIDE and PER CONJUNCT, AFTER side-local attribution, against that side's own column metadata
* *AND* a declined conjunct SHALL be reclassified as RESIDUAL and rendered into the outer wrapper's `WHERE` table-qualified in the Exasol dialect, NOT pushed into its side's fan-out leg, reaching the SAME residual bucket a syntactically-unrenderable conjunct reaches
* *AND* the decline SHALL be scoped to the offending CONJUNCT, so every other side-local conjunct of the same side SHALL still be pushed into that leg, because a whole-tree decline would forfeit pushdown for conjuncts the leg can apply
* *AND* the leg MUST NOT receive the predicate rendered bare, which fails the DataFusion scan inside the N-scan fallback with no result at all
* *AND* a conjunct the type pipeline ACCEPTS but whose REWRITTEN form the DataFusion dialect CANNOT render SHALL become residual in RAW form, and MUST NOT be omitted from both the leg and the residual, the leg renders the REWRITTEN tree, so leg-eligibility SHALL require that tree to be renderable, not merely the raw one
* *AND* if a side's re-formed accepted-conjunct tree does not itself survive the pipeline, OR survives but is not renderable for DataFusion, that side's ENTIRE side-local set SHALL become residual, so no conjunct is ever applied nowhere
* *AND* the side-local predicate that side forwards to Iceberg manifest pruning SHALL still carry the declined conjunct in RAW form, because the screen governs only what a LEG renders and pruning only ever removes files that provably cannot match
* *AND* the partition SHALL remain total and disjoint under the added condition, so no conjunct SHALL be dropped and none SHALL be applied twice
* *AND* the returned rows SHALL equal native Exasol evaluation of the same join

### Scenario: An N-scan side-local conjunct the type pipeline rewrites reaches its leg rewritten

* *GIVEN* an N-scan unaccelerated join whose WHERE filter carries a side-local conjunct the pipeline REWRITES rather than declines, for example a `predicate_like` over that side's `DATE` column, rewrapped as CAST-to-VARCHAR
* *WHEN* the adapter builds that side's fan-out leg
* *THEN* the leg's DataFusion scan-spec filter SHALL carry the REWRITTEN tree, so the conjunct KEEPS its per-leg row-group pruning and row filtering rather than becoming residual
* *AND* the conjunct SHALL NOT also appear in the outer wrapper's `WHERE`, so it is applied exactly once
* *AND* the rewrite SHALL apply only to the tree the leg renders, leaving that side's Iceberg manifest-pruning predicate unchanged

### Scenario: Two N-scan sides sharing a column name are each screened against their own side's types

* *GIVEN* an N-scan unaccelerated join over two involved tables that BOTH declare a column of the same name with DIFFERENT Exasol types, for example `KEYCOL` as `VARCHAR(2000000)` on one side and as `DECIMAL(20,0)` on the other
* *AND* the WHERE filter carries one side-local `LIKE` conjunct over `KEYCOL` for EACH side
* *WHEN* the adapter partitions the filter's top-level conjuncts
* *THEN* each conjunct SHALL be screened against the Exasol type declared by the side its `tableName` attributes it to, so the `VARCHAR` side's conjunct SHALL be pushed into its leg unchanged while the `DECIMAL` side's conjunct SHALL become residual
* *AND* the adapter MUST NOT screen either conjunct against a COMBINED cross-side type universe, because a shared name in such a universe resolves to one arbitrary side's type and would either push a non-string LIKE into a leg, a hard scan failure, or forfeit a valid string LIKE's pushdown

### Scenario: A join filter with no type-rewrite trigger emits byte-identical SQL

* *GIVEN* a broadcast-eligible join request and an N-scan fallback request whose WHERE filters carry no `LIKE` over a non-string column, no governed string function over a non-string argument, no `INSTR`/`LOCATE` beyond two arguments, and no DECIMAL stringification
* *WHEN* the adapter renders each pushdown
* *THEN* the emitted SQL SHALL equal the SQL rendered from the untouched filter at both sites, because the type-rewrite pipeline returns an untriggered tree unchanged
* *AND* an ABSENT filter and a TRIVIALLY-TRUE filter SHALL each stay distinguished from a DECLINED one at both join sites, so neither routes to a decline, the distinction owned by `pushdown/pushdown-declined-filter-self-apply`
