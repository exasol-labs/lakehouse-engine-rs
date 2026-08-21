# Feature: Pushdown Planning — Select-List Expression Pushdown

Extends pushdown planning (`vs-adapter/pushdown-planning`) so a scalar or boolean
expression in the select list — not only a bare column reference — is rendered by the
`crates/vs-expression` translator and pushed into the scan-driving query, and defines the
correctness-safe routing for a request the adapter cannot project item-for-item (a
WIDENED derived projection).

## Background

<!-- DELTA:NEW -->
* **The `function_aggregate` exclusion was enforced at the top level only, and a nested
  aggregate slipped through it (issues #194, #188).** This feature already requires that
  the pushable node-type set be "exactly the set the translator renders MINUS
  `function_aggregate`", with the recorded reason that "a row-scan projection evaluates its
  expressions PER SHARD, so an aggregate pushed as a projection item would compute a
  per-shard result". The projection builder enforced that by node TYPE at the item's root:
  a top-level `function_aggregate` fell into the unknown-node arm and widened, but a
  `function_scalar` (or predicate) item WRAPPING one reached the pushable arm and rendered
  successfully, because `render_expression_safe`'s `function_aggregate` arm renders a nested
  aggregate verbatim as SQL text — deliberately, since the grouped merge substitution
  depends on that arm. `ROUND(SUM(col), 2)` therefore became a `ProjectionItem::Expr`, the
  widening signal stayed `false`, and every shard computed the whole aggregate over its own
  files. Verified live: one correct row against a native schema, four unmerged per-shard
  rows through the virtual schema, no error.
* **This delta's recorded claim that "this delta adds no new exposure to a nested aggregate
  inside a predicate" was true of the predicate arms it added and NOT of the pre-existing
  `function_scalar` arm.** `predicate_equal`, `predicate_less`, `predicate_and`, and
  `predicate_like` did already recurse into a nested aggregate, so the claim held for the
  six node types that delta added. The exposure it did not name is the `function_scalar`
  family, which the pushable set has always carried. This delta closes the whole class at
  once rather than per node type.
* **The guard is depth-insensitive and precedes the node-type match, so it has one owner.**
  Probing the item's whole subtree for a `function_aggregate` before the type dispatch
  subsumes the unknown-node arm's existing handling of a TOP-LEVEL aggregate — that item
  widens either way, so the pre-existing outcome is unchanged — and extends it to every
  depth and every pushable node type in one decision instead of per arm.
* **Widening is the correctness floor, not the intended outcome for these shapes.**
  `vs-adapter/pushdown-planning-single-group-agg-scalar-over-aggregate` decomposes the
  common ungrouped scalar-over-aggregate shape into the partial/merge plan, so it is
  classified as a single-group aggregate and never consults the widening signal. The guard
  is what the shapes that decomposition declines fall back to.
* **No existing `dispatch_golden` fixture changes.** None of the eighteen fixtures under
  `adapter/pushdown/testdata/dispatch_golden/` carries an aggregate inside a row-scan
  projection, so none encodes the pre-guard behaviour; a diff in any of them is a
  regression rather than an expected update.
<!-- /DELTA:NEW -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Scalar select-list expression is pushed into the scan-driving query

* *GIVEN* a query whose select list contains a scalar or boolean expression over table columns (e.g. `UPPER(name)`, `price * qty`, `EXTRACT(YEAR FROM order_date)`, `CAST(id AS VARCHAR(2000000))`, `CASE WHEN qty > 0 THEN 1 ELSE 0 END`, `qty IN (1,2,3)`, `qty BETWEEN 1 AND 3`, `name IS NULL`, `name IS NOT NULL`, `name <> 'x'`, or `name REGEXP_LIKE '^a'`)
* *AND* the adapter advertises `SELECTLIST_EXPRESSIONS` and the capability backing that node type
* *WHEN* Exasol sends the `pushdown` request carrying that select-list expression
* *THEN* the adapter SHALL render each select-list expression node — recognizing the distinct `function_scalar_cast`, `function_scalar_extract`, and `function_scalar_case` node types Exasol emits for CAST, EXTRACT, and CASE (including CASE-expanded NULLIF/ZEROIFNULL), and the boolean predicate node types `predicate_in_constlist`, `predicate_between`, `predicate_is_null`, `predicate_is_not_null`, `predicate_notequal`, and `predicate_like_regexp` alongside the already-recognized `predicate_equal`, `predicate_less`, `predicate_lessequal`, `predicate_like`, `predicate_and`, `predicate_or`, and `predicate_not`, not only the generic `function_scalar` node — to a DataFusion SQL fragment using the VS expression translator (raising mode), and SHALL carry the rendered fragments in the scan spec so the scan UDF projects exactly those expressions rather than triggering the full-base-row fallback that yields a column count Exasol rejects
* *AND* the pushable node-type set SHALL be exactly the set the translator renders MINUS `function_aggregate` — so an aggregate select item reaches the aggregate planner or the wrapper rather than being evaluated per shard as a projection item, and a refused node type is never one that is both renderable and advertised (issue #196)
* *AND* that `function_aggregate` exclusion SHALL be enforced at EVERY DEPTH of a select-list item, not only at the item's root: a select-list item whose subtree contains a `function_aggregate` ANYWHERE — nested inside a `function_scalar`, a `function_scalar_cast`/`_extract`/`_case`, an arithmetic node, or any pushable predicate node — SHALL widen the derived projection exactly as a top-level `function_aggregate` already does, because the translator renders a nested aggregate verbatim as SQL text and the pushable arm would otherwise accept it and evaluate it PER SHARD (issues #194, #188)
* *AND* that subtree probe SHALL be a SINGLE decision applied before the node-type dispatch, so the rule has one owner rather than one guard per pushable arm, and the outcome for a TOP-LEVEL `function_aggregate` item MUST be unchanged — it widened through the unknown-node arm before and widens through the probe now
* *AND* every existing `dispatch_golden` fixture MUST remain byte-identical, because none carries an aggregate inside a row-scan projection and therefore none encodes the pre-guard behaviour
* *AND* the UDF's declared EMITS column list SHALL match the rendered select-list expressions in order and result type, where result types are read from the parallel top-level `selectListDataTypes` array in the pushdown request
* *AND* a select-list item the adapter cannot translate SHALL cause the adapter to fall back to projecting the underlying columns and let Exasol evaluate the expression, rather than producing an incorrect result
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: A widened derived projection routes to a native wrapper on every path

* *GIVEN* a row-scan or inner-join `pushdown` request with at least one select-list item the adapter cannot project item-for-item — an untranslatable expression, an unknown or aggregate node, a select-list item whose subtree carries a nested `function_aggregate` (issues #194, #188), a string-function argument the type guard declines (issue #210), or a rendered item whose declared EMITS type Exasol rejects (issue #218) — so the derived projection is widened to the full base row
* *WHEN* the adapter plans the request
* *THEN* the adapter SHALL decide the routing from the widening signal computed where the widening happens, and MUST NOT decide it by comparing the derived projection's column count against the select-list arity, because those counts COINCIDE whenever the base table's column count equals the select-list item count and the comparison then admits a full-base-row projection whose column types Exasol rejects (`sqlCode 04000`, "Data type mismatch in column number 10 (1-indexed). Expected BOOLEAN, but got DECIMAL(20,0)", verified live)
* *AND* on the single-table dispatch path a widened projection SHALL route to the qualified single-table wrapper for EVERY base-table column count including one equal to the select-list arity, and SHALL do so BEFORE the declined-`ORDER BY` path runs, retiring `(#234)`'s coincidental-arity variant — the variant the count comparison admitted — and not the arity-mismatch variant #234 itself reports, which that comparison already covered
* *AND* the widening signal SHALL be consumed ONLY where it already is — inside the row-scan routing arm, the empty-result row-scan arm, and the broadcast-join eligibility check — so an aggregate tier that classifies the request first takes precedence and a decomposable nested aggregate never widens at all (see `vs-adapter/pushdown-planning-single-group-agg-scalar-over-aggregate`)
* *AND* that wrapper SHALL render the original select list as native Exasol SQL over a sharded raw scan narrowed to the referenced columns for every widening trigger whose select-list items its qualified translator still renders — a nested `function_aggregate` (issues #194, #188), a string-function argument the DataFusion-dialect type guard declines (issue #210), or a rendered item whose declared EMITS type Exasol rejects (issue #218) — and for a select-list node that is UNKNOWN or untranslatable under BOTH dialects SHALL instead return the wrapper's PRE-EXISTING hard error rather than a rendered wrapper, because `qualified_single_table_fallback_pushdown` reaches the same `n_scan_join_select_items` refusal site (via `outer_wrapper_clauses`) as the N-scan join route below; that outcome is correctness-safe — Exasol receives an error, never wrong data — and is already the behaviour of every existing route into that wrapper, so this delta adds a route to an existing outcome rather than a new failure mode
* *AND* on the empty-result path — every data file pruned, single-table or join — the zero-row response for a widened projection SHALL carry one column per select-list item typed from `selectListDataTypes` rather than the full base row, so the empty and non-empty column shapes never diverge
* *AND* on the broadcast-join path a widened projection SHALL be a plain reason the broadcast plan is unavailable — the broadcast planner SHALL decline cleanly and MUST NOT raise an error — so the request falls through to the unified unaccelerated N-scan fallback (see `vs-adapter/pushdown-planning-join-fallback`, "A join outside the broadcast contract is declined safely"); this is the path that makes a NESTED aggregate over a join correct, because the join planner's aggregation-clause check inspects only TOP-LEVEL `function_aggregate` select items and does not see one wrapped in a scalar function
* *AND* that N-scan fallback SHALL render the original select list natively for every widening trigger whose select-list items the wrapper's qualified translator still renders — a string-function argument the type guard declines (issue #210), a rendered item whose declared EMITS type Exasol rejects (issue #218), a top-level `function_aggregate`, or a nested `function_aggregate` — and for a select-list node that is UNKNOWN or untranslatable under BOTH dialects SHALL instead return the N-scan wrapper's pre-existing hard error, exactly as the single-table wrapper entry point above and at the same refusal site; that outcome is correctness-safe — Exasol receives an error, never wrong data — and is already the behaviour of every other route into the N-scan wrapper, so this delta adds a route to an existing outcome rather than a new failure mode
* *AND* the reason those triggers still render SHALL be understood as a DIALECT difference, not a second translator: the wrapper renders through the same shared node dispatch that refused the item, but via the Exasol-dialect entry point (`render_expression_exasol_safe`, reached by `render_expression_qualified`), whereas the widening was decided by the DataFusion-dialect entry point (`render_expression_safe`) — so an item only the DataFusion dialect declines still renders in the wrapper (the issue #210 string-function guard sits on the DataFusion-dialect arm, which the Exasol-dialect arm bypasses), and only a node type unrenderable under BOTH dialects hard-errors. A top-level `function_aggregate` select-list item DOES reach the N-scan fallback — `classify_join_window` returning `ExasolPostProcessed` SKIPS broadcast eligibility and falls through to it — and does NOT hard-error there, because the shared dispatch has a `function_aggregate` arm that renders under both dialects; such an item widens only because the pushable-node whitelist deliberately excludes it, never because the translator refused it. A NESTED `function_aggregate` renders under both dialects for the same reason and likewise never hard-errors
* *AND* for every such request that returns a result — every single-table dispatch, every empty-result response, and every join whose select-list items that translator renders — the returned result SHALL equal the result of the same select list evaluated on a single node, while the scan-driving SQL for every request whose derived projection is NOT widened MUST remain byte-identical to its pre-delta output
<!-- /DELTA:CHANGED -->
