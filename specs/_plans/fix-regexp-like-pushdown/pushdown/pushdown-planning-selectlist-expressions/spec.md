# Feature: Pushdown Planning — Select-List Expression Pushdown

Extends pushdown planning (`pushdown/pushdown-planning`) so a scalar or boolean
expression in the select list — not only a bare column reference — is rendered by the
`crates/vs-expression` translator and pushed into the scan-driving query, and defines the
correctness-safe routing for a request the adapter cannot project item-for-item (a
WIDENED derived projection).

<!-- DELTA:CHANGED -->
## Background

* Filter, select-list, group-key, and HAVING expressions are all rendered by the shared
  `crates/vs-expression` translator; an untranslatable expression is omitted/falls back
  rather than producing an incorrect result.
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
* The pushable select-list node-type set includes the boolean predicate node types Exasol
  pushes into a select list and the translator renders: `predicate_in_constlist`,
  `predicate_between`, `predicate_is_null`, `predicate_is_not_null` (issue #196), and
  `predicate_notequal`. Each is backed by an advertised capability (`FN_PRED_IN_CONSTLIST`,
  `FN_PRED_BETWEEN`, `FN_PRED_IS_NULL`, `FN_PRED_IS_NOT_NULL`, `FN_PRED_NOTEQUAL`) and a translator
  arm, so Exasol never pushes a node the adapter then refuses to project. `predicate_like_regexp` is
  not in the set: `FN_PRED_REGEXP_LIKE` is not advertised, so Exasol evaluates a select-list
  `REGEXP_LIKE` itself.
* `predicate_greater` and `predicate_greaterequal` are not in the set. They are not Exasol
  capability names — Exasol normalises `a > b` to `b < a` before the request reaches the
  adapter — so a select-list `>` already arrives as the pushable `predicate_less`.
* `function_aggregate` stays outside the pushable set even though the translator renders it.
  A row-scan projection evaluates its expressions PER SHARD, so an aggregate pushed as a
  projection item would compute a per-shard result; an aggregate select item must reach the
  aggregate planner's partial/merge decomposition or the wrapper instead.
* **The `function_aggregate` exclusion holds at every depth (issues #194, #188).** The
  translator's `function_aggregate` arm renders a nested aggregate verbatim as SQL text —
  deliberately, since the grouped merge substitution depends on that arm — so a
  `function_scalar` (or predicate) item wrapping an aggregate, such as `ROUND(SUM(col), 2)`,
  would otherwise render as a `ProjectionItem::Expr` and every shard would compute the whole
  aggregate over its own files. Verified live: one correct row against a native schema, four
  unmerged per-shard rows through the virtual schema, no error.
* **The guard is depth-insensitive and precedes the node-type match, so it has one owner.**
  Probing the item's whole subtree for a `function_aggregate` before the type dispatch
  subsumes the unknown-node arm's handling of a TOP-LEVEL aggregate, which widens either way,
  and extends it to every depth and every pushable node type in one decision instead of per
  arm.
* **Widening is the correctness floor, not the intended outcome for these shapes.**
  `pushdown-aggregates/pushdown-planning-single-group-agg-scalar-over-aggregate` decomposes the
  common ungrouped scalar-over-aggregate shape into the partial/merge plan, so it is
  classified as a single-group aggregate and never consults the widening signal. The guard
  is what the shapes that decomposition declines fall back to.
* Routing consumes the widening signal where the widening happens, instead of re-deriving it
  from a column-count comparison, which is blind to a projection whose count coincides with the
  select-list arity. A widened derived projection therefore routes to the qualified single-table
  wrapper whatever its column count (issue #234), and it never reaches the declined-`ORDER BY`
  scenarios, because the dispatcher routes it to the qualified wrapper first.
* Iceberg spec compliance: checked, not engaged. This feature changes only how the adapter
  renders an Exasol select list and which wrapper it routes to. Iceberg-level file selection
  is driven by column predicates — "Deriving partition predicates from column predicates on
  the table data is used to separate the logical queries from physical storage", and
  "Partition fields that use an unknown transform can be read by ignoring the partition field
  for the purpose of filtering data files during scan planning" (Apache Iceberg table spec,
  `format/spec.md`) — and the predicate tree handed to Iceberg file resolution is the
  unmodified `filter` node, which this feature does not touch. No manifest, schema-resolution,
  field-id, or type-mapping surface changes, so no normative Iceberg requirement applies and
  there is no deviation to fix or track.
* The Exasol-dialect entry point is `render_expression_exasol_safe`, reached through
  `render_expression_qualified`, and the DataFusion-dialect entry point is
  `render_expression_safe`. The issue #210 string-function guard sits on the DataFusion-dialect
  arm that the Exasol-dialect arm bypasses. `render_expression_qualified`'s doc comment carries
  the recursive translator's design intent — columns, literals, scalar expressions, a top-level
  `function_aggregate`, and a `function_aggregate` nested inside a scalar function — and
  `pushdown-joins/pushdown-joins-module-structure` owns it.
* `n_scan_join_select_items` raises the wrapper's hard error for a node type unrenderable under
  BOTH dialects, through a shared decline constructor.
* **This feature moves no `dispatch_golden` fixture byte.** None of the fixtures under
  `adapter/pushdown/testdata/dispatch_golden/` carries an aggregate inside a row-scan
  projection, so none depends on the depth-insensitive aggregate guard. The directory holds
  twenty-four fixtures: eighteen credential-bearing ones, whose `storage` value
  `storage-access/scan-spec-credential-reference` owns, and six `empty_*` ones that carry no
  `storage` value.
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Scalar select-list expression is pushed into the scan-driving query

* *GIVEN* a query whose select list contains a scalar or boolean expression over table columns (e.g. `UPPER(name)`, `price * qty`, `EXTRACT(YEAR FROM order_date)`, `CAST(id AS VARCHAR(2000000))`, `CASE WHEN qty > 0 THEN 1 ELSE 0 END`, `qty IN (1,2,3)`, `qty BETWEEN 1 AND 3`, `name IS NULL`, `name IS NOT NULL`, or `name <> 'x'`)
* *AND* the adapter advertises `SELECTLIST_EXPRESSIONS` and the capability backing that node type
* *WHEN* Exasol sends the `pushdown` request carrying that select-list expression
* *THEN* the adapter SHALL render each select-list expression node — recognizing the distinct `function_scalar_cast`, `function_scalar_extract`, and `function_scalar_case` node types Exasol emits for CAST, EXTRACT, and CASE (including CASE-expanded NULLIF/ZEROIFNULL), and the boolean predicate node types `predicate_in_constlist`, `predicate_between`, `predicate_is_null`, `predicate_is_not_null`, and `predicate_notequal` alongside the already-recognized `predicate_equal`, `predicate_less`, `predicate_lessequal`, `predicate_like`, `predicate_and`, `predicate_or`, and `predicate_not`, not only the generic `function_scalar` node — to a DataFusion SQL fragment using the VS expression translator (raising mode), and SHALL carry the rendered fragments in the scan spec so the scan UDF projects exactly those expressions rather than triggering the full-base-row fallback that yields a column count Exasol rejects
* *AND* the pushable node-type set SHALL be exactly the set the translator renders MINUS `function_aggregate` — so an aggregate select item reaches the aggregate planner or the wrapper rather than being evaluated per shard as a projection item, and a refused node type is never one that is both renderable and advertised (issue #196)
* *AND* that `function_aggregate` exclusion SHALL be enforced at EVERY DEPTH of a select-list item, not only at the item's root: a select-list item whose subtree contains a `function_aggregate` ANYWHERE — nested inside a `function_scalar`, a `function_scalar_cast`/`_extract`/`_case`, an arithmetic node, or any pushable predicate node — SHALL widen the derived projection exactly as a top-level `function_aggregate` already does, because the translator renders a nested aggregate verbatim as SQL text and the pushable arm would otherwise accept it and evaluate it PER SHARD (issues #194, #188)
* *AND* that subtree probe SHALL be a SINGLE decision applied before the node-type dispatch, so the rule has one owner rather than one guard per pushable arm, and the outcome for a TOP-LEVEL `function_aggregate` item MUST be unchanged — it widened through the unknown-node arm before and widens through the probe now
* *AND* every existing `dispatch_golden` fixture MUST remain byte-identical EXCEPT for its scan-spec `storage` value, which `storage-access/scan-spec-credential-reference` re-encodes as a connection reference carrying no credential — because none of the fixtures carries an aggregate inside a row-scan projection and therefore none encodes the pre-guard behaviour, so THIS feature moves no fixture byte at all; the eighteen credential-bearing fixtures change only in that one value, and the six `empty_*` fixtures carry no `storage` value and MUST stay byte-identical with NO exception
* *AND* the UDF's declared EMITS column list SHALL match the rendered select-list expressions in order and result type, where result types are read from the parallel top-level `selectListDataTypes` array in the pushdown request
* *AND* a select-list item the adapter cannot translate SHALL cause the adapter to fall back to projecting the underlying columns and let Exasol evaluate the expression, rather than producing an incorrect result
<!-- /DELTA:CHANGED -->
