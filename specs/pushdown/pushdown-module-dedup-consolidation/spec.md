# Feature: Pushdown Module Dedup Consolidation

Gives each mechanism of the pushdown planning layer that was written more than once a single owned implementation: the shard-invariant dispatch base, the qualified single-table fallback helper, the request-shape classifier, the column-collecting traversal, the type-rewrite traversal, and the ordered type-rewrite pipeline. `pushdown/pushdown-module-structure` owns the module boundary and the frozen façade.

## Background

* Each extraction changes no decision. The generated scan-driving SQL stays byte-identical, with one permitted difference: the scan spec's `storage` value, which `storage-access/scan-spec-credential-reference` re-encodes as a connection reference or a sealed envelope. Every other byte of every golden stays unchanged, which keeps the gate falsifiable.
* The type-rewrite order is load-bearing. `string_function_arg_type_guard` runs before `rewrite_decimal_stringifications` because the guard wraps a bare DECIMAL string-function argument into a `decimal_to_varchar_exasol` node, which the rewriter then leaves alone. Reversed, the two would emit two trim wrappers. `pushdown-types/pushdown-planning-string-fn-type-coercion-composition` verifies this interaction.
* The type-rewrite primitive is a free function taking a per-node closure. The pushdown IR is untyped `serde_json`, and a typed AST would contradict the no-SQL-parser property of `vs-expression`.

## Scenarios

### Scenario: The dispatcher builds each fan-out spec from one shared shard-invariant base

* *GIVEN* the pushdown dispatcher's fan-out construction sites, which share the same shard-invariant fields: logical schema, name mapping, absent join, storage, the four DataFusion tuning fields, the memory-pool fields, and the S3 connection budget, plus an empty files list
* *WHEN* the dispatcher constructs the scan spec for the grouped-aggregate, group-by fallback, lone-`COUNT(DISTINCT)`, multi/mixed-`COUNT(DISTINCT)` decline, and single-group/row-scan dispatch shapes
* *THEN* every site SHALL derive its shard-invariant fields from one shared base value and set only the fields that differ at that site
* *AND* the base SHALL carry the one scan-spec storage value, and no construction site SHALL re-derive it or choose its variant
* *AND* the scan-driving SQL generated for each dispatch shape MUST be byte-identical to the golden SQL EXCEPT for the `storage` value, which `storage-access/scan-spec-credential-reference` encodes as a reference carrying no credential or as a sealed envelope over that backend's byte-identical (pre-encryption) encoding

### Scenario: Both qualified single-table fallback guards call one shared helper

* *GIVEN* the two dispatch guards that route to the qualified single-table wrapper: a `GROUP BY` request that declined grouped decomposition, and a multi or mixed `COUNT(DISTINCT)` single-group request
* *WHEN* each guard builds its referenced-column projection, its fan-out spec, and its wrapper SQL
* *THEN* both guards SHALL call one shared helper that performs the referenced-column-projection, fan-out-spec, and wrapper-SQL sequence
* *AND* the wrapper SQL each guard produces MUST match its committed golden, `group_by_fallback.sql` and `multi_count_distinct_decline.sql`, EXCEPT for the `storage` VALUE, and every other byte of both goldens, including each one's narrowed inner-scan `projection`, MUST be unchanged, because that unchanged remainder shows the shared helper is called

### Scenario: One classifier decides the request shape for both the dispatch and empty-result paths

* *GIVEN* the request-routing decision: grouped aggregate first, then single-group aggregate, then row scan, applying the same aggregate-column-type validation gates
* *WHEN* the adapter plans a pushdown request, whether data files remain or every file is pruned
* *THEN* the request shape SHALL be computed once by one shared classifier that both paths consume
* *AND* each path SHALL render only its own SQL from the shared decision: the non-empty path its scan-driving SQL, the empty path its shape-correct empty response
* *AND* the classifier SHALL resolve the grouped HAVING's merge-rendering, because an unmergeable HAVING removes the partial/merge grouped shape from the reachable set, SHALL carry the rendered HAVING fragment on the grouped shape it returns, and the non-empty dispatch path SHALL splice that fragment into its outer merge wrapper WITHOUT re-rendering it, so exactly one place decides whether a HAVING can be merged
* *AND* the classifier SHALL raise NO grouped-tier hard error: a grouped request that does not decompose, for any reason including a non-numeric aggregate column type whether or not a HAVING is present, or whose HAVING cannot be merged, SHALL yield the qualified single-table wrapper shape on both paths, the scan-driving wrapper SQL on the non-empty path and the typed zero-row wrapper shape on the empty path, because that wrapper renders the HAVING itself rather than dropping it
* *AND* the classifier SHALL resolve a single-group HAVING's merge-rendering the same way, and a single-group request that does not decompose or whose HAVING cannot be merged SHALL yield the qualified single-table wrapper shape on the non-empty path and the single-group empty shape on the empty path, never a row scan

### Scenario: One blind traversal primitive backs every column-collecting walk

* *GIVEN* the read-only column-collecting walks: `collect_all_column_names` in `pushdown/support.rs`, plus `collect_leg_column_names` and `possible_side_column_names` in `pushdown/joins/rendering.rs`
* *WHEN* the pushdown layer collects column references from a `selectList`, a `filter` or `having` expression tree, a `groupBy` or `orderBy` array, or a join condition
* *THEN* exactly one traversal primitive in the shared `support` submodule, `walk_column_nodes`, SHALL own both the recursion and the `type == "column"` test, invoking a caller-supplied callback once per `column` object node and passing that node's own field map
* *AND* the primitive SHALL traverse blindly, visiting every field of every object and every element of every array, because a collect rebuilds nothing, and a column reference can sit arbitrarily deep inside a function call, a `CASE`, or a comparison predicate
* *AND* each walk SHALL reduce to one call to that primitive with a closure over the `column` node's field map, and SHALL NOT retain a JSON-object or JSON-array recursion arm of its own, so the pushdown module tree holds ONE blind column-collecting traversal
* *AND* each closure MUST keep its own case fold, the Unicode `to_uppercase` for `collect_all_column_names` and the ASCII-only `to_ascii_uppercase` for both joins walks, because the two disagree for non-ASCII column and table names, so the folds SHALL NOT be unified

### Scenario: The three type-rewrite guards walk the expression tree through one shared post-order primitive

* *GIVEN* the three type-aware expression rewriters in `pushdown/support.rs`: the LIKE subject guard, the string-function argument guard, and the decimal-stringification rewriter
* *WHEN* the adapter rewrites a filter tree or a select-list expression tree through any of the three
* *THEN* all three SHALL recurse through ONE shared post-order primitive that rewrites every curated child FIRST and only then applies that guard's own per-node decision, so each guard contributes a per-node closure and no traversal code of its own
* *AND* the curated child-bearing field set, the array fields `expressions` / `arguments` / `results` and the single-child fields `expression` / `pattern` / `left` / `right` / `basis`, SHALL be declared in exactly one place, and the primitive SHALL NOT descend into a node's non-expression sub-objects (`dataType`, `name`), because the set is curated to rewrite expression children only and never to rebuild type or identifier metadata
* *AND* the primitive SHALL propagate a per-node decline from any depth to its top-level caller, so a fallible guard keeps its all-or-nothing decline contract and an infallible guard composes as the never-declining case without gaining a decline path, and SHALL apply a guard's per-node decision to a non-object leaf node, which is safe because every guard's per-node decision returns a node carrying no `type` it governs unchanged

### Scenario: One ordered pipeline function owns the type-rewrite pass order

* *GIVEN* the ordered type-rewrite passes: LIKE-subject guard, then string-function-argument guard, then decimal-stringification rewriter
* *WHEN* the adapter applies type rewrites to a single-table WHERE-clause filter tree, or to a select-list expression item in `project_columns`, including the select-list item reached through the broadcast join's shared use of `project_columns`
* *THEN* exactly ONE function in the shared `support` submodule, `apply_type_rewrites`, SHALL own the pass sequence for BOTH render surfaces, and every caller SHALL invoke that function instead of sequencing the passes itself, so adding or reordering a pass is an edit inside one function body
* *AND* that function's doc comment SHALL state its pass order AND why that order is load-bearing, so the rationale lives with the code that enforces it, and both render surfaces SHALL carry the SAME three-pass list, with NO second per-surface entry point and NO per-surface alias delegating to it
* *AND* that function SHALL take the expression tree and the column-type list and SHALL return an `Option`, `None` meaning the tree declined, propagated unchanged from whichever pass declined, so no caller composes a fallible pass against the infallible rewriter, and SHALL NOT absorb its caller's "is there a tree at all" question, keeping an absent filter distinguishable from a declined one
* *AND* the three passes SHALL be private to `support` and the pipeline function SHALL be declared at the narrowest visibility that compiles, `pub(super)`, so no module outside `support` can sequence the passes by hand, and the pipeline function SHALL NOT name what any caller does with the result, so the pipeline owns the pass order and nothing else, and the raw pushdown filter tree forwarded to Iceberg-level file pruning MUST remain unmodified because the pipeline feeds only the DataFusion-bound scan filter
