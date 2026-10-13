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
`pushdown-types/pushdown-planning-like-type-coercion` because that feature sits at the per-spec scenario
threshold and because the two join surfaces share one distinct concern the single-table surfaces do
not have: WHICH column-type universe a surface may screen against.

## Background

* This feature adds NO guard and NO type dispatch. It wires the two join sites to
  `apply_type_rewrites` — the one ordered pipeline (`like_subject_type_guard` →
  `string_function_arg_type_guard` → `rewrite_decimal_stringifications`) owned by
  `pushdown-types/pushdown-planning-string-fn-type-coercion-composition` — so every guard decision,
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
  `pushdown/pushdown-declined-filter-self-apply`; this feature only widens what triggers them.
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
* This feature covers the join WHERE-filter surfaces ONLY. The join SELECT-list projection is a
  separate, already-correct surface: the broadcast join reaches `project_columns` through
  `extract_join_projection`, so it has run the pipeline since #211/#210/#207 and its decline widens
  the projection to the disjoint union of every involved table's columns.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: A broadcast-join filter over a non-string LIKE subject declines the broadcast plan

* *GIVEN* a broadcast-eligible inner equi-join `pushdown` request meeting every other broadcast condition, whose `filter` carries a `predicate_like` whose subject is a bare `column` node
* *AND* the column's Exasol type in the union of the two involved tables' column metadata is a non-string, non-DATE type — `DECIMAL(p,s)` (including an Exasol integer, which the wire carries as `DECIMAL(p,0)`), `DOUBLE PRECISION`, `BOOLEAN`, or `TIMESTAMP` — or the column's name does not resolve in that union at all
* *WHEN* the adapter renders the broadcast join pushdown
* *THEN* the type-rewrite pipeline SHALL decline the filter, and the adapter SHALL decline the WHOLE broadcast plan and fall through to the unified unaccelerated N-scan fallback, which self-applies the predicate in its own qualified outer `WHERE`
* *AND* the decline SHALL be the SAME clean `Ok(None)`-shaped fall-through the syntactically-unrenderable-filter decline already takes, NOT an error and NOT a new route — see `pushdown/pushdown-declined-filter-self-apply`
* *AND* the adapter MUST NOT emit a broadcast plan carrying the predicate rendered bare, which DataFusion's `type_coercion` planner rejects, failing the scan with no result (issue #215)
* *AND* the adapter MUST NOT emit a broadcast plan whose scan spec simply OMITS the predicate, because broadcast SQL carries no outer `WHERE` in which anything could apply it
* *AND* the returned rows SHALL equal native Exasol evaluation of the same join
<!-- /DELTA:CHANGED -->
