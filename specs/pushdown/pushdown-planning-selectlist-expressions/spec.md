# Feature: Pushdown Planning, Select-List Expression Pushdown

Renders a scalar or boolean select-list expression with the `crates/vs-expression` translator and pushes it into the scan-driving query. A request the adapter cannot project item for item routes to a native wrapper.

## Background

* Exasol validates a returned pushdown query positionally on column count and column type. A divergence in either is `sqlCode 04000`, for example "Data type mismatch in column number 10 (1-indexed). Expected BOOLEAN, but got DECIMAL(20,0)."
* The derived projection is the projection-item list the adapter builds from the select list, normally one item per select-list item. The derived projection is wrapped, meaning the request has no item-for-item projection, when any item is untranslatable, is an unknown or aggregate node, is declined by the scan translator (`pushdown-types/pushdown-planning-scan-text-conversion`), or carries a declared EMITS type Exasol rejects. An Exasol wrapper then renders the select list over a scan of the columns the request names.
* The pushable select-list node types are the node types the translator renders, minus `function_aggregate`. Each pushable type is backed by an advertised capability and a translator arm.
* A row-scan projection evaluates its expressions per shard. An aggregate pushed as a projection item would compute a per-shard result, so aggregate items reach the aggregate planner or the wrapper instead.
* The adapter probes a select-list item's whole subtree for `function_aggregate` before the node-type dispatch. The translator renders a nested aggregate verbatim as SQL text, so without the probe `ROUND(SUM(col), 2)` would compute per shard and return unmerged rows.
* `pushdown-aggregates/pushdown-planning-single-group-agg-scalar-over-aggregate` decomposes the ungrouped scalar-over-aggregate shape into the partial/merge plan, so that shape never consults the wrapping signal.
* The Exasol-dialect entry point is `render_expression_exasol`, reached through `render_expression_qualified`, and it renders every item Exasol sends (`sql-comprehension/vs-expression-translator`). The DataFusion-dialect entry point is the scan translator, which carries the column types.
* The `dispatch_golden` fixtures under `adapter/pushdown/testdata/` number twenty-four: eighteen credential-bearing ones, whose `storage` value `storage-access/scan-spec-credential-reference` owns, and six `empty_*` ones with no `storage` value.

## Scenarios

### Scenario: Scalar select-list expression is pushed into the scan-driving query

* *GIVEN* a query whose select list contains a scalar or boolean expression over table columns (e.g. `UPPER(name)`, `price * qty`, `EXTRACT(YEAR FROM order_date)`, `CAST(id AS VARCHAR(2000000))`, `CASE WHEN qty > 0 THEN 1 ELSE 0 END`, `qty IN (1,2,3)`, `qty BETWEEN 1 AND 3`, `name IS NULL`, `name IS NOT NULL`, or `name <> 'x'`)
* *AND* the adapter advertises `SELECTLIST_EXPRESSIONS` and the capability backing that node type
* *WHEN* Exasol sends the `pushdown` request carrying that select-list expression
* *THEN* the adapter SHALL render each select-list expression node, recognizing the distinct `function_scalar_cast`, `function_scalar_extract`, and `function_scalar_case` node types Exasol emits for CAST, EXTRACT, and CASE (including CASE-expanded NULLIF/ZEROIFNULL), and the boolean predicate node types `predicate_in_constlist`, `predicate_between`, `predicate_is_null`, `predicate_is_not_null`, and `predicate_notequal` alongside the already-recognized `predicate_equal`, `predicate_less`, `predicate_lessequal`, `predicate_like`, `predicate_and`, `predicate_or`, and `predicate_not`, not only the generic `function_scalar` node, to a DataFusion SQL fragment using the VS expression translator, and SHALL carry the rendered fragments in the scan spec so the scan UDF projects exactly those expressions rather than triggering the full-base-row fallback that yields a column count Exasol rejects
* *AND* the pushable node-type set SHALL be exactly the set the translator renders MINUS `function_aggregate`, so an aggregate select item reaches the aggregate planner or the wrapper rather than being evaluated per shard as a projection item, and a refused node type is never one that is both renderable and advertised (issue #196)
* *AND* that `function_aggregate` exclusion SHALL be enforced at EVERY DEPTH of a select-list item, not only at the item's root: a select-list item whose subtree contains a `function_aggregate` ANYWHERE, nested inside a `function_scalar`, a `function_scalar_cast`/`_extract`/`_case`, an arithmetic node, or any pushable predicate node, SHALL wrap the derived projection exactly as a top-level `function_aggregate` already does, because the translator renders a nested aggregate verbatim as SQL text and the pushable arm would otherwise accept it and evaluate it PER SHARD (issues #194, #188)
* *AND* the UDF's declared EMITS column list SHALL match the rendered select-list expressions in order and result type, where result types are read from the parallel top-level `selectListDataTypes` array in the pushdown request
* *AND* a select-list item the adapter cannot translate SHALL cause the adapter to fall back to projecting the underlying columns and let Exasol evaluate the expression, rather than producing an incorrect result

### Scenario: A wrapped derived projection routes to a native wrapper on every path

* *GIVEN* a row-scan or inner-join `pushdown` request with at least one select-list item the adapter cannot project item-for-item, an untranslatable expression, an aggregate node, a select-list item whose subtree carries a nested `function_aggregate` (issues #194, #188), an item the scan translator declines, or a rendered item whose declared EMITS type Exasol rejects (issue #218), so the derived projection is wrapped
* *WHEN* the adapter plans the request
* *THEN* the returned SQL SHALL render the original select list in Exasol SQL over a sharded scan narrowed to the columns the request names, even when the derived projection's column count equals the select-list arity, because Exasol rejects a mistyped result (`sqlCode 04000`, "Data type mismatch in column number 10 (1-indexed). Expected BOOLEAN, but got DECIMAL(20,0)", verified live)
* *AND* a single-table request SHALL take the qualified single-table wrapper for every base-table column count, including one equal to the select-list arity, and a request that an aggregate planner serves first, including a decomposable nested aggregate (`pushdown-aggregates/pushdown-planning-single-group-agg-scalar-over-aggregate`), SHALL NOT be wrapped
* *AND* on the empty-result path, every data file pruned, single-table or join, the zero-row response for a wrapped projection SHALL carry one column per select-list item typed from `selectListDataTypes` rather than the full base row, so the empty and non-empty column shapes never diverge
* *AND* a join with a broadcast pair SHALL keep the broadcast: the pair's fan-out SHALL emit the plain columns the select list reads, and one outer Exasol `SELECT` SHALL render the original select list over them (`pushdown-joins/pushdown-planning-join`)
* *AND* a join served by the N-scan fallback SHALL render the original select list in Exasol SQL for every wrapping trigger
* *AND* the returned result SHALL equal the result of the same select list evaluated on a single node, for a single-table request, an empty result, a two-leg broadcast join, a join of three or more legs, and an N-scan join
