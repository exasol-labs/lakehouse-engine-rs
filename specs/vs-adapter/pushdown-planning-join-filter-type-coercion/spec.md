# Feature: Pushdown Planning — Join Filter Type Coercion

Extends the pushdown type-rewrite pipeline to the two JOIN WHERE-filter render surfaces — the
broadcast join's combined filter and the N-scan fallback's per-leg filter — so a pushed-down
predicate whose column types DataFusion will not implicitly coerce is coerced, or declined into the
join path's own already-existing self-application route, instead of being rendered bare and
hard-failing the scan at execution time. Both join sites previously screened a filter PURELY
SYNTACTICALLY ("does this tree render at all") with no column-type awareness whatsoever, so a `LIKE`
over a non-string column, a governed string function over a non-string argument, and a DECIMAL
stringification each reached the scan in a form DataFusion rejects or evaluates differently from
Exasol. Split into its own feature rather than appended to
`vs-adapter/pushdown-planning-like-type-coercion` because that feature sits at the per-spec scenario
threshold and because the two join surfaces share one distinct concern the single-table surfaces do
not have: WHICH column-type universe a surface may screen against.

## Background

* This feature adds NO guard and NO type dispatch. It wires the two join sites to
  `apply_type_rewrites` — the one ordered pipeline (`like_subject_type_guard` →
  `string_function_arg_type_guard` → `rewrite_decimal_stringifications`) owned by
  `vs-adapter/pushdown-planning-string-fn-type-coercion-composition` — so every guard decision,
  dispatch table, and traversal is inherited verbatim from the single-table WHERE surface. Issue
  #215; issue #223's slice 2 ("broadcast-join per-leg FILTER path") closes with it, while #223's
  slices 1 (computed-expression arguments) and 3 (GROUP-BY-only keys) remain open.
* The broadcast site reuses `classify_where_filter`, already the SOLE owner of the
  "rewrite, then decide scan-spec filter vs. self-apply" classification for the single-table path,
  rather than re-deriving that sequence at a second site.
* The two surfaces differ in exactly one respect, and it is the reason this feature exists: the
  column-type UNIVERSE a surface may legitimately screen against.
  * BROADCAST — the UNION of both involved tables' columns, matched by bare column name, read only
    AFTER `disjoint_schema_guard` has passed. That guard is what makes a bare name resolve to
    exactly one Exasol type, and broadcast rendering is side-agnostic bare-name, so a bare-name
    universe is the matching one.
  * N-SCAN — each side's OWN columns, applied AFTER conjunct attribution to a table. The N-scan path
    has NO disjoint-column-name precondition, so two sides MAY declare the same column name with
    different Exasol types; a combined universe would resolve such a name against an arbitrary side.
* This feature introduces no new decline OUTCOME and no new error path. A type decline is routed
  through the outcome each surface already has for a syntactically-unrenderable filter — broadcast
  forfeits the broadcast plan to the N-scan fallback, an N-scan side-local conjunct becomes a
  residual conjunct in the qualified outer `WHERE`. Both outcomes are owned by
  `vs-adapter/pushdown-declined-filter-self-apply`; this feature only widens what triggers them.
* Every decline is therefore SAFE rather than silently lossy, which is what unblocks shipping the
  decline arm at all: before the self-application mechanism existed, a declined join filter was
  omitted from the emitted SQL and applied nowhere, returning extra rows.
* The `NLS_DATE_FORMAT` tracked exception (#216) on the DATE CAST-to-VARCHAR arm and the
  DECIMAL-stringification trade-off (#211) apply unchanged at both join surfaces: the same pipeline
  emits the same nodes, so the same accepted trade-offs carry over. Neither is a new exception.
* Wiring the pipeline makes `string_function_arg_type_guard`'s `INSTR`/`LOCATE`-beyond-two-arguments
  decline newly reachable at the join surfaces, replacing a silently truncated position with the
  native Exasol result. That NARROWS issue #228's exposure to the surfaces still unwired; it does
  NOT close #228, whose root cause is a rendering defect in `crates/vs-expression` that this feature
  does not touch.
* Both join sites receive an already alias-stripped tree, because `handle_pushdown` strips every
  `tableAlias` from the whole pushdown request at one chokepoint before any downstream render
  (issue #193). The guards match a `column` node's `name` alone, so stripping neither helps nor
  hinders them — but it is why a bare-name universe is the correct one at the broadcast site.
* The leg-eligibility screen governs the REWRITTEN tree, because that is the tree the leg renders. A
  conjunct the type pipeline ACCEPTS but whose REWRITTEN form the DataFusion dialect cannot render is
  therefore NOT leg-eligible — it becomes residual in RAW form. Screening only the raw tree for
  renderability and only the rewritten tree for type-acceptance would leave such a conjunct in neither
  the leg nor the residual, applied nowhere: the defect #279 found, at a new site. The single-table
  owner already carries this arm (`classify_where_filter`'s
  `(Some(raw), Some(tree)) if !datafusion_renderable(tree)`), and the join sites inherit it rather than
  diverging from it.
* The "Two N-scan sides sharing a column name" scenario is pinned at the PARTITION level only — it
  makes no claim about what a live query returns, unlike every other scenario here. No E2E fixture
  declares a column name shared across two seed tables at different types, and the claim is about
  WHICH column-type universe the screen consults, which is pure planning-time computation. The live
  row-equality guarantee for the same residual route is carried by the N-scan decline scenario below,
  whose conjunct name does not collide.
* This feature covers the join WHERE-filter surfaces ONLY. The join SELECT-list projection is a
  separate, already-correct surface: the broadcast join reaches `project_columns` through
  `extract_join_projection`, so it has run the pipeline since #211/#210/#207 and its decline widens
  the projection to the disjoint union of every involved table's columns.

## Scenarios

### Scenario: A broadcast-join filter over a non-string LIKE subject declines the broadcast plan

* *GIVEN* a broadcast-eligible inner equi-join `pushdown` request meeting every other broadcast condition, whose `filter` carries a `predicate_like` or `predicate_regexp_like` whose subject is a bare `column` node
* *AND* the column's Exasol type in the union of the two involved tables' column metadata is a non-string, non-DATE type — `DECIMAL(p,s)` (including an Exasol integer, which the wire carries as `DECIMAL(p,0)`), `DOUBLE PRECISION`, `BOOLEAN`, or `TIMESTAMP` — or the column's name does not resolve in that union at all
* *WHEN* the adapter renders the broadcast join pushdown
* *THEN* the type-rewrite pipeline SHALL decline the filter, and the adapter SHALL decline the WHOLE broadcast plan and fall through to the unified unaccelerated N-scan fallback, which self-applies the predicate in its own qualified outer `WHERE`
* *AND* the decline SHALL be the SAME clean `Ok(None)`-shaped fall-through the syntactically-unrenderable-filter decline already takes, NOT an error and NOT a new route — see `vs-adapter/pushdown-declined-filter-self-apply`
* *AND* the adapter MUST NOT emit a broadcast plan carrying the predicate rendered bare, which is what previously reached DataFusion's `type_coercion` planner and failed the scan with no result at all (issue #215)
* *AND* the adapter MUST NOT emit a broadcast plan whose scan spec simply OMITS the predicate, because broadcast SQL carries no outer `WHERE` in which anything could apply it
* *AND* the returned rows SHALL equal native Exasol evaluation of the same join

### Scenario: A broadcast-join filter over a DATE LIKE subject keeps the broadcast plan

* *GIVEN* a broadcast-eligible inner equi-join `pushdown` request whose `filter` carries a `predicate_like` whose subject is a bare `column` node
* *AND* the column's Exasol type in the union of the two involved tables' column metadata is `DATE`
* *WHEN* the adapter renders the broadcast join pushdown
* *THEN* the adapter SHALL rewrap the subject as an explicit CAST-to-VARCHAR node before rendering, exactly as the single-table WHERE surface does, and SHALL carry the REWRITTEN tree's rendered fragment in the common spec
* *AND* the broadcast plan SHALL remain eligible, so a DATE-column LIKE SHALL NOT cost the broadcast optimization — the CAST arm is a rewrite, not a decline
* *AND* the adapter SHALL render the pipeline's REWRITTEN tree and MUST NOT render the raw tree, because rendering the raw tree after a successful rewrite would silently discard the coercion and reintroduce the hard scan failure
* *AND* the emitted match semantics and the altered-session `NLS_DATE_FORMAT` tracked exception (#216) SHALL be identical to the single-table WHERE surface's, because the identical CAST node is emitted
* *AND* the raw filter tree forwarded to Iceberg file pruning SHALL be left unchanged

### Scenario: An N-scan side-local conjunct the type pipeline declines becomes a residual conjunct

* *GIVEN* an N-scan unaccelerated join over N ≥ 2 involved tables whose WHERE filter carries a top-level conjunct that is side-local to exactly ONE table, is syntactically renderable for DataFusion, and that the type-rewrite pipeline declines when run against THAT SIDE's own column metadata — for example a `LIKE` over that side's `DECIMAL(20,0)` column
* *WHEN* the adapter partitions the filter's top-level conjuncts between the per-leg fan-outs and the outer wrapper's `WHERE`
* *THEN* the adapter SHALL run the type-rewrite pipeline PER SIDE and PER CONJUNCT, AFTER side-local attribution, against that side's own column metadata
* *AND* a declined conjunct SHALL be reclassified as RESIDUAL and rendered into the outer wrapper's `WHERE` table-qualified in the Exasol dialect, NOT pushed into its side's fan-out leg — reaching the SAME residual bucket a syntactically-unrenderable conjunct already reaches, not a new one
* *AND* the decline SHALL be scoped to the offending CONJUNCT, so every other side-local conjunct of the same side SHALL still be pushed into that leg, because a whole-tree decline would forfeit pushdown for conjuncts the leg can apply
* *AND* the leg MUST NOT receive the predicate rendered bare, which is what previously failed the DataFusion scan inside the N-scan fallback with no result at all (issue #215)
* *AND* a conjunct the type pipeline ACCEPTS but whose REWRITTEN form the DataFusion dialect CANNOT render SHALL become residual in RAW form, and MUST NOT be omitted from both the leg and the residual — the leg renders the REWRITTEN tree, so leg-eligibility SHALL require that tree to be renderable, not merely the raw one
* *AND* if a side's re-formed accepted-conjunct tree does not itself survive the pipeline, OR survives but is not renderable for DataFusion, that side's ENTIRE side-local set SHALL become residual, so no conjunct is ever applied nowhere
* *AND* the side-local predicate that side forwards to Iceberg manifest pruning SHALL still carry the declined conjunct in RAW form, because the screen governs only what a LEG renders and pruning only ever removes files that provably cannot match
* *AND* the partition SHALL remain total and disjoint under the added condition, so no conjunct SHALL be dropped and none SHALL be applied twice
* *AND* the returned rows SHALL equal native Exasol evaluation of the same join

### Scenario: An N-scan side-local conjunct the type pipeline rewrites reaches its leg rewritten

* *GIVEN* an N-scan unaccelerated join whose WHERE filter carries a side-local conjunct the pipeline REWRITES rather than declines — for example a `predicate_like` over that side's `DATE` column, rewrapped as CAST-to-VARCHAR
* *WHEN* the adapter builds that side's fan-out leg
* *THEN* the leg's DataFusion scan-spec filter SHALL carry the REWRITTEN tree, so the conjunct KEEPS its per-leg row-group pruning and row filtering rather than becoming residual
* *AND* the conjunct SHALL NOT also appear in the outer wrapper's `WHERE`, so it is applied exactly once
* *AND* the rewrite SHALL apply only to the tree the leg renders, leaving that side's Iceberg manifest-pruning predicate unchanged

### Scenario: Two N-scan sides sharing a column name are each screened against their own side's types

* *GIVEN* an N-scan unaccelerated join over two involved tables that BOTH declare a column of the same name with DIFFERENT Exasol types — for example `KEYCOL` as `VARCHAR(2000000)` on one side and as `DECIMAL(20,0)` on the other
* *AND* the WHERE filter carries one side-local `LIKE` conjunct over `KEYCOL` for EACH side
* *WHEN* the adapter partitions the filter's top-level conjuncts
* *THEN* each conjunct SHALL be screened against the Exasol type declared by the side its `tableName` attributes it to, so the `VARCHAR` side's conjunct SHALL be pushed into its leg unchanged while the `DECIMAL` side's conjunct SHALL become residual
* *AND* the adapter MUST NOT screen either conjunct against a COMBINED cross-side type universe, because a shared name in such a universe resolves to one arbitrary side's type and would either push a non-string LIKE into a leg — a hard scan failure — or forfeit a valid string LIKE's pushdown

### Scenario: A join filter with no type-rewrite trigger emits byte-identical SQL

* *GIVEN* a broadcast-eligible join request and an N-scan fallback request whose WHERE filters carry no `LIKE` over a non-string column, no governed string function over a non-string argument, no `INSTR`/`LOCATE` beyond two arguments, and no DECIMAL stringification
* *WHEN* the adapter renders each pushdown
* *THEN* the emitted SQL SHALL be BYTE-IDENTICAL to its pre-change output at both sites, because the type-rewrite pipeline returns an untriggered tree unchanged
* *AND* no existing golden-SQL fixture covering a broadcast join or an N-scan fallback whose filter carries no trigger SHALL change
* *AND* an ABSENT filter and a TRIVIALLY-TRUE filter SHALL each stay distinguished from a DECLINED one at both join sites, so neither routes to a decline — the distinction owned by `vs-adapter/pushdown-declined-filter-self-apply`
* *AND* the wiring SHALL add no cost to any join request the adapter can already push
