# Feature: Pushdown Planning — Join Filter Type Coercion

Unchanged by this plan, and reproduced here only because the delta validator requires a description. The recorded text stands as written in `specs/vs-adapter/pushdown-planning-join-filter-type-coercion/spec.md`.

<!-- DELTA:CHANGED -->
## Background

* This feature adds NO guard and NO type dispatch. It wires the two join sites to
  `apply_type_rewrites` — the one ordered pipeline (`like_subject_type_guard`, then the
  string-conversion decline check) owned by
  `vs-adapter/pushdown-planning-string-fn-type-coercion-composition` — so every guard decision,
  dispatch table, and traversal is inherited verbatim from the single-table WHERE surface
  (issue #215).
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
* The `NLS_DATE_FORMAT` tracked exception (#216) on the DATE conversions and the DECIMAL trim (#211)
  hold at both join surfaces, because both surfaces render through the same `exa_to_varchar`
  wrapping as the single-table surface.
* An `INSTR`/`LOCATE` call beyond two arguments is a DataFusion-dialect render error, so at both
  join surfaces it takes the unrenderable-filter outcome and returns the native Exasol result
  (issue #228, step 1).
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
  `extract_join_projection`, so it runs the same pipeline, and its decline widens the projection to
  the disjoint union of every involved table's columns.
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: A join filter with no type-rewrite trigger emits byte-identical SQL

* *GIVEN* a broadcast-eligible join request and an N-scan fallback request whose WHERE filters carry no `LIKE` over a non-string column, no string function, and no string CAST
* *WHEN* the adapter renders each pushdown
* *THEN* the type-rewrite pipeline SHALL return each filter tree unchanged and the renderer SHALL wrap no argument, so the emitted SQL at both sites SHALL be BYTE-IDENTICAL to the rendering of the request's own filter tree
* *AND* no existing golden-SQL fixture covering a broadcast join or an N-scan fallback whose filter carries no trigger SHALL change
* *AND* an ABSENT filter and a TRIVIALLY-TRUE filter SHALL each stay distinguished from a DECLINED one at both join sites, so neither routes to a decline — the distinction owned by `vs-adapter/pushdown-declined-filter-self-apply`
* *AND* the wiring SHALL add no cost to any join request the adapter can already push
<!-- /DELTA:CHANGED -->
