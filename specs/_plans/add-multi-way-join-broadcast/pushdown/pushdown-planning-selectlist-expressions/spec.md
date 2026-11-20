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

<!-- DELTA:CHANGED -->
### Scenario: A wrapped derived projection routes to a native wrapper on every path

* *GIVEN* a row-scan or inner-join `pushdown` request with at least one select-list item the adapter cannot project item-for-item, an untranslatable expression, an aggregate node, a select-list item whose subtree carries a nested `function_aggregate` (issues #194, #188), an item the scan translator declines, or a rendered item whose declared EMITS type Exasol rejects (issue #218), so the derived projection is wrapped
* *WHEN* the adapter plans the request
* *THEN* the returned SQL SHALL render the original select list in Exasol SQL over a sharded scan narrowed to the columns the request names, even when the derived projection's column count equals the select-list arity, because Exasol rejects a mistyped result (`sqlCode 04000`, "Data type mismatch in column number 10 (1-indexed). Expected BOOLEAN, but got DECIMAL(20,0)", verified live)
* *AND* a single-table request SHALL take the qualified single-table wrapper for every base-table column count, including one equal to the select-list arity, and a request that an aggregate planner serves first, including a decomposable nested aggregate (`pushdown-aggregates/pushdown-planning-single-group-agg-scalar-over-aggregate`), SHALL NOT be wrapped
* *AND* on the empty-result path, every data file pruned, single-table or join, the zero-row response for a wrapped projection SHALL carry one column per select-list item typed from `selectListDataTypes` rather than the full base row, so the empty and non-empty column shapes never diverge
* *AND* a join with a broadcast pair SHALL keep the broadcast: the pair's fan-out SHALL emit the plain columns the select list reads, and one outer Exasol `SELECT` SHALL render the original select list over them (`pushdown-joins/pushdown-planning-join`)
* *AND* a join served by the N-scan fallback SHALL render the original select list in Exasol SQL for every wrapping trigger
* *AND* the returned result SHALL equal the result of the same select list evaluated on a single node, for a single-table request, an empty result, a two-leg broadcast join, a join of three or more legs, and an N-scan join
<!-- /DELTA:CHANGED -->
