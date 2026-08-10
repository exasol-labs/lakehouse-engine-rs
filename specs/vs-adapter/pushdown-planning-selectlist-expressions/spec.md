# Feature: Pushdown Planning — Select-List Expression Pushdown

Extends pushdown planning (`vs-adapter/pushdown-planning`) so a scalar or boolean
expression in the select list — not only a bare column reference — is rendered by the
`crates/vs-expression` translator and pushed into the scan-driving query, and defines the
correctness-safe routing for a request the adapter cannot project item-for-item (a
WIDENED derived projection).

## Background

* Filter, select-list, group-key, and HAVING expressions are all rendered by the shared
  `crates/vs-expression` translator; an untranslatable expression is omitted/falls back
  rather than producing an incorrect result.
* This delta changes two things and nothing else: the pushable select-list node-type set gains
  the boolean predicate node types the translator already renders, and the routing of a
  select list the adapter cannot project item-for-item stops being decided by a column-count
  comparison. Every other capability-extensions scenario is unchanged.
* Exasol validates a returned pushdown query POSITIONALLY on BOTH column count and column
  type. A count divergence is `sqlCode 04000` "Expected number of columns is N but pushdown
  query has M"; a type divergence at matching count is `sqlCode 04000` "Data type mismatch in
  column number 10 (1-indexed). Expected BOOLEAN, but got DECIMAL(20,0)." Both were captured
  live against the `typed_distinct_probe` seed table (10 columns) through
  `scripts/capture-pushdown-payload.sh`; see `docs/debugging-pushdown.md`.
* The DERIVED PROJECTION is the projection-item list the adapter builds from the select list.
  It normally carries one item per select-list item. A separate widening replaces it with the
  FULL BASE ROW — every source column, bare — whenever any item is untranslatable, is an
  unknown or aggregate node, fails the string-function argument type guard (issue #210), or
  carries a declared EMITS type Exasol rejects (issue #218).
* Six boolean predicate node types Exasol pushes into a select list are renderable by the
  `crates/vs-expression` translator yet were absent from the pushable set, so each triggered
  that widening: `predicate_in_constlist`, `predicate_between`, `predicate_is_null`,
  `predicate_is_not_null` (issue #196), plus `predicate_notequal` and `predicate_like_regexp`,
  which the same gap covers and which issue #196 does not name. Each is advertised
  (`FN_PRED_IN_CONSTLIST`, `FN_PRED_BETWEEN`, `FN_PRED_IS_NULL`, `FN_PRED_IS_NOT_NULL`,
  `FN_PRED_NOTEQUAL`, `FN_PRED_REGEXP_LIKE`) and each has a translator arm, so Exasol pushes
  a node the adapter then refused to project.
* `predicate_greater` and `predicate_greaterequal` are deliberately NOT added. They are not
  Exasol capability names — Exasol normalises `a > b` to `b < a` before the request reaches
  the adapter — so a select-list `>` already arrives as the pushable `predicate_less`, and
  adding the two would be unreachable code.
* `function_aggregate` stays outside the pushable set even though the translator renders it.
  A row-scan projection evaluates its expressions PER SHARD, so an aggregate pushed as a
  projection item would compute a per-shard result; an aggregate select item must reach the
  aggregate planner's partial/merge decomposition or the wrapper instead. This delta adds no
  new exposure to a nested aggregate inside a predicate: `predicate_equal`, `predicate_less`,
  `predicate_and`, and `predicate_like` were already pushable and already recurse into one.
* The widening signal is already computed where the widening happens. Routing SHALL consume
  that signal directly instead of re-deriving it from an arity comparison, which is blind to
  a projection whose count coincides with the select-list arity.
* Issue #234 reports a DIFFERENT variant of this widening, and reports it as an arity
  MISMATCH: "The returned query then carries 4 columns where Exasol's `selectListDataTypes`
  positionally expects exactly 1 (the original select-list arity) — still `sqlCode 04000
  "Expected number of columns is 1 but pushdown query has 4"`", under a repro shape the issue
  itself labels "not yet verified live". A count divergence of that kind is what the
  PRE-EXISTING count comparison already catches (`1 != 4`), and it already routes such a
  request to the qualified single-table wrapper ahead of the declined-`ORDER BY` path. That
  comparison was authored in commit `e41e2b0` fourteen minutes BEFORE #234 was filed and
  reached `main` on 2026-07-26 (PR #229), so #234's own reported shape is expected to be
  non-reproducible on today's `main` independently of this delta.
* What this delta adds is the NARROWER variant no count comparison can see: a widened
  projection whose column count COINCIDES with the select-list arity, which the comparison
  admits and which then reaches the declined-`ORDER BY` wrapper. Routing on the widening signal
  covers that coincidental-arity case for every base-table column count. This delta therefore
  retires `(#234)`'s coincidental-arity variant only; it makes no claim about #234 wholesale,
  whose reported mismatch variant the earlier count comparison already covered.
* `fix-225-orderby-non-projected-column`'s delta to this feature carried the `(#234)` clause as
  a tracked exception on its declined-`ORDER BY` scenarios. That clause has been reworded to
  state the routing fact instead: an already-widened derived projection never reaches those
  scenarios, because the dispatcher routes it to the qualified wrapper first. No exception
  remains to carry into the permanent spec from either delta.
* Iceberg spec compliance: checked, not engaged. This delta changes only how the adapter
  renders an Exasol select list and which wrapper it routes to. Iceberg-level file selection
  is driven by column predicates — "Deriving partition predicates from column predicates on
  the table data is used to separate the logical queries from physical storage", and
  "Partition fields that use an unknown transform can be read by ignoring the partition field
  for the purpose of filtering data files during scan planning" (Apache Iceberg table spec,
  `format/spec.md`) — and the predicate tree handed to Iceberg file resolution is the
  unmodified `filter` node, which this delta does not touch. No manifest, schema-resolution,
  field-id, or type-mapping surface changes, so no normative Iceberg requirement applies and
  there is no deviation to fix or track.
* Issue #181 deletes `render_selectlist_item_qualified`, which this feature's widened-projection scenario names inside its dialect-chain *AND*. The function was a one-line pass-through whose whole body was one call to `render_expression_qualified` with the same arguments, so removing it removes a hop from the chain and nothing else. This delta rewrites that one parenthetical and changes no other clause of this feature.
* No dialect behaviour changes. The Exasol-dialect entry point is still `render_expression_exasol_safe`, still reached through `render_expression_qualified`, and the DataFusion-dialect entry point is still `render_expression_safe`. Which items the wrapper renders, which widen, and which hard-error are all unchanged; the issue #210 string-function guard still sits on the DataFusion-dialect arm that the Exasol-dialect arm bypasses.
* The deleted wrapper's design intent is relocated, not lost: `render_expression_qualified`'s doc comment now carries it — one recursive translator covering columns, literals, scalar expressions, a top-level `function_aggregate`, and a `function_aggregate` nested inside a scalar function, byte-compatibly with the former `render_aggregate_qualified`. `vs-adapter/pushdown-joins-module-structure`'s "The two join-rendering pass-through wrappers are deleted rather than retained" scenario owns that relocation.
* The refusal site this feature's scenario relies on is unaffected: `n_scan_join_select_items` still raises the wrapper's pre-existing hard error for a node type unrenderable under BOTH dialects. Issue #181 re-expresses that error through a shared decline constructor whose message is byte-identical, gated by a full-string assertion.

## Scenarios

### Scenario: Scalar select-list expression is pushed into the scan-driving query

* *GIVEN* a query whose select list contains a scalar or boolean expression over table columns (e.g. `UPPER(name)`, `price * qty`, `EXTRACT(YEAR FROM order_date)`, `CAST(id AS VARCHAR(2000000))`, `CASE WHEN qty > 0 THEN 1 ELSE 0 END`, `qty IN (1,2,3)`, `qty BETWEEN 1 AND 3`, `name IS NULL`, `name IS NOT NULL`, `name <> 'x'`, or `name REGEXP_LIKE '^a'`)
* *AND* the adapter advertises `SELECTLIST_EXPRESSIONS` and the capability backing that node type
* *WHEN* Exasol sends the `pushdown` request carrying that select-list expression
* *THEN* the adapter SHALL render each select-list expression node — recognizing the distinct `function_scalar_cast`, `function_scalar_extract`, and `function_scalar_case` node types Exasol emits for CAST, EXTRACT, and CASE (including CASE-expanded NULLIF/ZEROIFNULL), and the boolean predicate node types `predicate_in_constlist`, `predicate_between`, `predicate_is_null`, `predicate_is_not_null`, `predicate_notequal`, and `predicate_like_regexp` alongside the already-recognized `predicate_equal`, `predicate_less`, `predicate_lessequal`, `predicate_like`, `predicate_and`, `predicate_or`, and `predicate_not`, not only the generic `function_scalar` node — to a DataFusion SQL fragment using the VS expression translator (raising mode), and SHALL carry the rendered fragments in the scan spec so the scan UDF projects exactly those expressions rather than triggering the full-base-row fallback that yields a column count Exasol rejects
* *AND* the pushable node-type set SHALL be exactly the set the translator renders MINUS `function_aggregate` — so an aggregate select item reaches the aggregate planner or the wrapper rather than being evaluated per shard as a projection item, and a refused node type is never one that is both renderable and advertised (issue #196)
* *AND* the UDF's declared EMITS column list SHALL match the rendered select-list expressions in order and result type, where result types are read from the parallel top-level `selectListDataTypes` array in the pushdown request
* *AND* a select-list item the adapter cannot translate SHALL cause the adapter to fall back to projecting the underlying columns and let Exasol evaluate the expression, rather than producing an incorrect result

### Scenario: A widened derived projection routes to a native wrapper on every path

* *GIVEN* a row-scan or inner-join `pushdown` request with at least one select-list item the adapter cannot project item-for-item — an untranslatable expression, an unknown or aggregate node, a string-function argument the type guard declines (issue #210), or a rendered item whose declared EMITS type Exasol rejects (issue #218) — so the derived projection is widened to the full base row
* *WHEN* the adapter plans the request
* *THEN* the adapter SHALL decide the routing from the widening signal computed where the widening happens, and MUST NOT decide it by comparing the derived projection's column count against the select-list arity, because those counts COINCIDE whenever the base table's column count equals the select-list item count and the comparison then admits a full-base-row projection whose column types Exasol rejects (`sqlCode 04000`, "Data type mismatch in column number 10 (1-indexed). Expected BOOLEAN, but got DECIMAL(20,0)", verified live)
* *AND* on the single-table dispatch path a widened projection SHALL route to the qualified single-table wrapper for EVERY base-table column count including one equal to the select-list arity, and SHALL do so BEFORE the declined-`ORDER BY` path runs, retiring `(#234)`'s coincidental-arity variant — the variant the count comparison admitted — and not the arity-mismatch variant #234 itself reports, which that comparison already covered
* *AND* that wrapper SHALL render the original select list as native Exasol SQL over a sharded raw scan narrowed to the referenced columns for every widening trigger whose select-list items its qualified translator still renders — a string-function argument the DataFusion-dialect type guard declines (issue #210), or a rendered item whose declared EMITS type Exasol rejects (issue #218) — and for a select-list node that is UNKNOWN or untranslatable under BOTH dialects SHALL instead return the wrapper's PRE-EXISTING hard error rather than a rendered wrapper, because `qualified_single_table_fallback_pushdown` reaches the same `n_scan_join_select_items` refusal site (via `outer_wrapper_clauses`) as the N-scan join route below; that outcome is correctness-safe — Exasol receives an error, never wrong data — and is already the behaviour of every existing route into that wrapper, so this delta adds a route to an existing outcome rather than a new failure mode
* *AND* on the empty-result path — every data file pruned, single-table or join — the zero-row response for a widened projection SHALL carry one column per select-list item typed from `selectListDataTypes` rather than the full base row, so the empty and non-empty column shapes never diverge
* *AND* on the broadcast-join path a widened projection SHALL be a plain reason the broadcast plan is unavailable — the broadcast planner SHALL decline cleanly and MUST NOT raise an error — so the request falls through to the unified unaccelerated N-scan fallback (see `vs-adapter/pushdown-planning-join-fallback`, "A join outside the broadcast contract is declined safely")
* *AND* that N-scan fallback SHALL render the original select list natively for every widening trigger whose select-list items the wrapper's qualified translator still renders — a string-function argument the type guard declines (issue #210), a rendered item whose declared EMITS type Exasol rejects (issue #218), or a top-level `function_aggregate` — and for a select-list node that is UNKNOWN or untranslatable under BOTH dialects SHALL instead return the N-scan wrapper's pre-existing hard error, exactly as the single-table wrapper entry point above and at the same refusal site; that outcome is correctness-safe — Exasol receives an error, never wrong data — and is already the behaviour of every other route into the N-scan wrapper, so this delta adds a route to an existing outcome rather than a new failure mode
* *AND* the reason those triggers still render SHALL be understood as a DIALECT difference, not a second translator: the wrapper renders through the same shared node dispatch that refused the item, but via the Exasol-dialect entry point (`render_expression_exasol_safe`, reached by `render_expression_qualified`), whereas the widening was decided by the DataFusion-dialect entry point (`render_expression_safe`) — so an item only the DataFusion dialect declines still renders in the wrapper (the issue #210 string-function guard sits on the DataFusion-dialect arm, which the Exasol-dialect arm bypasses), and only a node type unrenderable under BOTH dialects hard-errors. A top-level `function_aggregate` select-list item DOES reach the N-scan fallback — `classify_join_window` returning `ExasolPostProcessed` SKIPS broadcast eligibility and falls through to it — and does NOT hard-error there, because the shared dispatch has a `function_aggregate` arm (`vs-expression/src/lib.rs:1149`) that renders under both dialects; such an item widens only because the pushable-node whitelist deliberately excludes it, never because the translator refused it
* *AND* for every such request that returns a result — every single-table dispatch, every empty-result response, and every join whose select-list items that translator renders — the returned result SHALL equal the result of the same select list evaluated on a single node, while the scan-driving SQL for every request whose derived projection is NOT widened MUST remain byte-identical to its pre-delta output
