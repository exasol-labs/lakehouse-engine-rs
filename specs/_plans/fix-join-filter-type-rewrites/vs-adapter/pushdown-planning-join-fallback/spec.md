# Feature: Pushdown Planning — Join Fallback

Extends pushdown planning with the SINGLE unified renderer that serves every inner join outside the
broadcast contract: each involved table scanned through its own sharded fan-out and reconstructed
into the original inner join by Exasol's core engine.

## Background

* The leg-eligibility test grows a THIRD condition. It was "side-local to exactly one table AND
  syntactically renderable for DataFusion"; it becomes "…AND accepted by the type-rewrite pipeline
  run against THAT SIDE's own column metadata AND whose REWRITTEN form the DataFusion dialect can
  render". Issue #215: the syntactic screen (`datafusion_renderable`) carries no column-type
  awareness, so a side-local LIKE over a non-string column was classified leg-eligible and rendered
  bare into the leg's scan spec — the tree that hard-fails DataFusion's `type_coercion` planner at
  scan execution time.
* The renderability half of that condition MUST target the REWRITTEN tree, not the raw one, because
  the REWRITTEN tree is what the leg renders. Screening the raw tree for renderability and the
  rewritten tree only for type-acceptance would let a conjunct that is type-accepted but
  rewritten-unrenderable fall out of the leg half AND out of the residual half — applied nowhere,
  returning extra rows with no error. That is the defect #279 found at the broadcast site, and the
  single-table owner already carries the arm that prevents it
  (`classify_where_filter`'s `(Some(raw), Some(tree)) if !datafusion_renderable(tree) => (None, Some(raw))`).
  The N-scan site inherits that arm rather than diverging from it.
* The new condition CANNOT be folded into the existing pre-attribution screen, and that is a
  structural constraint rather than an implementation preference. `renderable_only` /
  `declined_only` partition the WHOLE top-level conjunct set with ONE predicate BEFORE
  `side_local_filter` attributes any conjunct to a table, whereas the type screen needs the OWNING
  side's own `col_types` — and unlike broadcast, the N-scan path has NO disjoint-column-name
  precondition, so two sides MAY declare the same column name with different Exasol types. The type
  screen therefore runs PER SIDE, PER CONJUNCT, AFTER attribution, and the leg/residual partition
  is computed in that order: per-side legs first, residual last.
* The screen is per CONJUNCT, not per side-local tree, so one type-declining conjunct does not
  forfeit the other side-local conjuncts' pushdown. This is a deliberate difference from the
  single-table WHERE surface, where a decline is necessarily whole-filter because there is one
  filter and one wrapper; here the partition already exists and can absorb a single conjunct.
* The residual set gains a third disjoint component: the cross-side complement of the leg-eligible
  half, the syntactically-declined half, and now the per-side TYPE-declined conjuncts. All three
  are disjoint by construction — the type-declined set is drawn from conjuncts that are inside the
  leg-eligible half AND side-local to one table, which is exactly the complement of the other two.
* The per-side split is FAIL-CLOSED in BOTH directions: if the re-formed accepted-conjunct tree does
  not itself survive the pipeline, OR survives but is not renderable for DataFusion (either must hold,
  since each of its conjuncts satisfied both, but nothing in the type system forbids it), the WHOLE
  side-local set goes to the residual rather than being silently lost. A conjunct applied nowhere
  returns wrong rows, so the safe direction is always "residual".
* The tree a leg receives is now the pipeline's REWRITTEN tree, not the raw one, so a side-local
  DATE-column LIKE keeps its leg pushdown as `CAST(<col> AS VARCHAR) LIKE …` instead of becoming
  residual. Only what the LEG renders changes; each side's Iceberg manifest-pruning predicate keeps
  every side-local conjunct in its RAW form, unchanged by this delta.
* The residual conjunct rendered into the outer wrapper's `WHERE` is the RAW conjunct, in the
  Exasol dialect, because Exasol applies the implicit non-string-to-VARCHAR coercion DataFusion
  refuses — which is the whole reason the predicate is safe there and not in the leg.
* This delta adds no new residual MECHANISM and no new error path. The residual bucket, its
  qualified render, and its both-dialects-unrenderable error are owned by
  `vs-adapter/pushdown-declined-filter-self-apply`; only the set of triggers that reaches the bucket
  widens. See `vs-adapter/pushdown-planning-like-type-coercion` for the per-surface type dispatch.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Join conditions attach greedily by table-name set and side-local filters push into each leg

* *GIVEN* a unified unaccelerated fallback over N ≥ 2 involved tables with a set of join conditions and a WHERE filter
* *WHEN* the adapter renders the `INNER JOIN … ON` chain
* *THEN* the adapter SHALL attach each join condition to the earliest join point in the left-to-right chain at which every table the condition references is in scope, deciding scope by the SET of `tableName`s the condition touches — NEVER by column name, so shared column names across sides stay correctly qualified
* *AND* a join point at which no not-yet-attached condition becomes resolvable SHALL be rendered with `ON 1=1`
* *AND* a top-level WHERE conjunct SHALL be pushed INTO a side's fan-out leg as a DataFusion filter if and only if it references only that ONE table AND the type-rewrite pipeline run against THAT SIDE's own column metadata accepts it AND the DataFusion dialect can render that pipeline's REWRITTEN form of it — REPLACING the recorded "if and only if it references only that ONE table AND the DataFusion dialect can render it", whose purely syntactic second condition let a side-local LIKE over a non-string column into a leg and hard-fail the scan (issue #215)
* *AND* the renderability condition SHALL be evaluated on the REWRITTEN conjunct, not the raw one, because the leg renders the rewritten tree; a conjunct that is type-accepted but whose REWRITTEN form is unrenderable SHALL become RESIDUAL in raw form and MUST NOT be omitted from both the leg and the residual, which would apply it nowhere and return extra rows with no error
* *AND* the type screen SHALL run PER SIDE and PER CONJUNCT, AFTER side-local attribution, and MUST NOT be folded into the pre-attribution syntactic screen, because two N-scan sides MAY declare the same column name with different Exasol types and only the owning side's metadata resolves it correctly
* *AND* every OTHER conjunct SHALL be RESIDUAL and remain in the outer wrapper's `WHERE`: cross-table, OR-spanning, untagged, column-free, side-local-but-unrenderable, or side-local-but-type-declined
* *AND* the partition SHALL stay total and disjoint under the added condition, so no conjunct is dropped and none is applied twice
* *AND* a type decline SHALL cost only the offending CONJUNCT's leg pushdown, so the same side's other side-local conjuncts SHALL still be pushed into its leg
* *AND* the filter each leg receives SHALL already be screened as type-accepted AND, in its REWRITTEN form, DataFusion-renderable, and SHALL BE that REWRITTEN tree, so the leg's own render cannot decline and no second renderability or type decision exists to drift from the first — REPLACING the recorded "SHALL already be screened as DataFusion-renderable", which named only the syntactic half and screened only the raw tree
* *AND* a side-local conjunct the pipeline REWRITES rather than declines — for example a DATE-column LIKE rewrapped as CAST-to-VARCHAR — SHALL still be pushed into its leg in rewritten form, so it keeps per-leg row-group pruning and row filtering and SHALL NOT also appear in the outer `WHERE`
* *AND* each side's Iceberg manifest-pruning predicate SHALL keep every side-local conjunct in its RAW form, renderable or not and type-accepted or not, so neither a render decline nor a type decline nor a rewrite SHALL change which files are opened
* *AND* the residual conjunct the outer wrapper renders SHALL be the RAW conjunct in the Exasol dialect, which applies the implicit non-string-to-VARCHAR coercion DataFusion refuses
* *AND* a non-empty residual set that the NON-SUPPRESSING qualified Exasol render also cannot express SHALL return the wrapper's existing client-facing error, because the predicate can be applied nowhere and returning rows without it would be wrong
* *AND* a non-empty residual set that renders trivially true SHALL emit no outer `WHERE` and SHALL NOT error, so the error decision MUST NOT be taken from the trivially-true-suppressing render's empty result
* *AND* if the re-formed accepted-conjunct tree of a side does not itself survive the pipeline, OR survives but is not DataFusion-renderable, that side's ENTIRE side-local set SHALL become residual, so no conjunct is ever applied nowhere
* *AND* an N-scan request whose filter triggers no rewrite and no type decline SHALL emit byte-identical SQL to its pre-change output, so no golden-SQL fixture over such a filter changes
* *AND* the returned result SHALL equal the result of the same inner join evaluated on a single node, for any assignment of conditions to join points
<!-- /DELTA:CHANGED -->
