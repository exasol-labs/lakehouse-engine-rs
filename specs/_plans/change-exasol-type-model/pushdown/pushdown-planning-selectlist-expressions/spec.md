# Feature: Pushdown Planning, Select-List Expression Pushdown

Renders a scalar or boolean select-list expression with the `crates/vs-expression` translator and pushes it into the scan-driving query. A request the adapter cannot project item for item routes to a native wrapper.

## Background

* Exasol validates a returned pushdown query positionally on column count and column type. A divergence in either is `sqlCode 04000`, for example "Data type mismatch in column number 10 (1-indexed). Expected BOOLEAN, but got DECIMAL(20,0)."
* The derived projection is the projection-item list the adapter builds from the select list, normally one item per select-list item. The adapter widens it to the full base row, every source column bare, when any item is untranslatable, is an unknown or aggregate node, fails the string-function argument type guard, or carries a declared EMITS type Exasol rejects.
* The pushable select-list node types are the node types the translator renders, minus `function_aggregate`. Each pushable type is backed by an advertised capability and a translator arm.
* A row-scan projection evaluates its expressions per shard. An aggregate pushed as a projection item would compute a per-shard result, so aggregate items reach the aggregate planner or the wrapper instead.
* The adapter probes a select-list item's whole subtree for `function_aggregate` before the node-type dispatch. The translator renders a nested aggregate verbatim as SQL text, so without the probe `ROUND(SUM(col), 2)` would compute per shard and return unmerged rows.
* `pushdown-aggregates/pushdown-planning-single-group-agg-scalar-over-aggregate` decomposes the ungrouped scalar-over-aggregate shape into the partial/merge plan, so that shape never consults the widening signal.
* The Exasol-dialect entry point is `render_expression_exasol_safe`, reached through `render_expression_qualified`. The DataFusion-dialect entry point is `render_expression_safe`, and it carries the string-function argument type guard.
* The `dispatch_golden` fixtures under `adapter/pushdown/testdata/` number twenty-four: eighteen credential-bearing ones, whose `storage` value `storage-access/scan-spec-credential-reference` owns, and six `empty_*` ones with no `storage` value.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Scalar select-list expression is pushed into the scan-driving query

* *GIVEN* a query whose select list contains a scalar or boolean expression over table columns (e.g. `UPPER(name)`, `price * qty`, `EXTRACT(YEAR FROM order_date)`, `CAST(id AS VARCHAR(2000000))`, `CASE WHEN qty > 0 THEN 1 ELSE 0 END`, `qty IN (1,2,3)`, `qty BETWEEN 1 AND 3`, `name IS NULL`, `name IS NOT NULL`, or `name <> 'x'`)
* *AND* the adapter advertises `SELECTLIST_EXPRESSIONS` and the capability backing that node type
* *WHEN* Exasol sends the `pushdown` request carrying that select-list expression
* *THEN* the adapter SHALL render each select-list expression node, recognizing the distinct `function_scalar_cast`, `function_scalar_extract`, and `function_scalar_case` node types Exasol emits for CAST, EXTRACT, and CASE (including CASE-expanded NULLIF/ZEROIFNULL), and the boolean predicate node types `predicate_in_constlist`, `predicate_between`, `predicate_is_null`, `predicate_is_not_null`, and `predicate_notequal` alongside the already-recognized `predicate_equal`, `predicate_less`, `predicate_lessequal`, `predicate_like`, `predicate_and`, `predicate_or`, and `predicate_not`, not only the generic `function_scalar` node, to a DataFusion SQL fragment using the VS expression translator, and SHALL carry the rendered fragments in the scan spec so the scan UDF projects exactly those expressions rather than triggering the full-base-row fallback that yields a column count Exasol rejects
* *AND* the pushable node-type set SHALL be exactly the set the translator renders MINUS `function_aggregate`, so an aggregate select item reaches the aggregate planner or the wrapper rather than being evaluated per shard as a projection item, and a refused node type is never one that is both renderable and advertised (issue #196)
* *AND* that `function_aggregate` exclusion SHALL be enforced at EVERY DEPTH of a select-list item, not only at the item's root: a select-list item whose subtree contains a `function_aggregate` ANYWHERE, nested inside a `function_scalar`, a `function_scalar_cast`/`_extract`/`_case`, an arithmetic node, or any pushable predicate node, SHALL widen the derived projection exactly as a top-level `function_aggregate` already does, because the translator renders a nested aggregate verbatim as SQL text and the pushable arm would otherwise accept it and evaluate it PER SHARD (issues #194, #188)
* *AND* the UDF's declared EMITS column list SHALL match the rendered select-list expressions in order and result type, where result types are read from the parallel top-level `selectListDataTypes` array in the pushdown request
* *AND* a select-list item the adapter cannot translate SHALL cause the adapter to fall back to projecting the underlying columns and let Exasol evaluate the expression, rather than producing an incorrect result
<!-- /DELTA:CHANGED -->
