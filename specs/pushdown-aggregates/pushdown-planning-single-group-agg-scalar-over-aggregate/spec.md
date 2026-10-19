# Feature: Pushdown Planning, Single-Group Scalar-Over-Aggregate Select Items

The adapter decomposes an ungrouped select item that is a scalar function wrapping aggregates, such as `ROUND(SUM(L_QUANTITY), 2)`, into the same per-shard partial and outer-merge plan a bare aggregate gets. A nested aggregate the merge cannot decompose routes to the qualified single-table wrapper.

## Background

* Without decomposition the scalar item renders as a per-shard projection expression, so each shard computes a whole aggregate over its own files and Exasol returns one row per shard.
* `VARIANCE` is Exasol's alias for `VAR_SAMP`, and DataFusion has no function named `variance`. The decomposition resolves each nested aggregate name to an `AggKind` through the tables of `pushdown-aggregates/pushdown-agg-sql-consolidation`, so no aggregate function name reaches DataFusion.
* The decomposition reuses the grouped planner's four primitives: the aggregate-sentinelizing tree walk, the scalar-over-aggregate classifier, the merge rewrite that substitutes each sentinel with its merged partial expression, and the deduplicating plan fold.
* The merge rewrite binds each nested aggregate to the first structurally-equal plan slot, so the plan list must be deduplicated.
* The plan list and the select list are not one-to-one. A scalar item contributes no plan of its own and shifts later select-list indexes, so partial `EMITS` types come from the per-plan declared-type list and the outer CAST types come from each select item's own `selectListDataTypes` ordinal.
* A select item with no nested aggregate, such as `ROUND(price, 2)`, yields no aggregate to decompose and reaches the row-scan projection.
* Exasol delegates an advertised capability fully and never re-applies it, so the adapter renders equivalent SQL for any shape it cannot push.

## Scenarios

### Scenario: Single-group select item that is a scalar function wrapping aggregates is decomposed into partial columns and one merged row

* *GIVEN* a virtual schema over an Iceberg table backed by SeaweedFS whose data files partition into two or more shards
* *AND* an ungrouped single-table `pushdown` request (no `groupBy`) whose select list contains a select item that is a scalar function wrapping one or more aggregates, for example `ROUND(SUM(L_QUANTITY), 2)`
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL classify that item as a scalar-over-aggregate single-group item rather than declining to a row scan, and SHALL decompose every `function_aggregate` nested inside it into the same partial `AggregatePlan` list it builds for a top-level aggregate, so each inner aggregate contributes the `PARTIAL_*` columns the scan UDF emits once per shard
* *AND* the returned scan-driving SQL SHALL carry the aggregates in the scan spec's `aggregates` field and MUST NOT carry any `function_aggregate` inside the per-shard `EMITS` projection, so no shard computes a whole-table aggregate of its own
* *AND* the outer merge SELECT SHALL render the scalar function applied to the MERGED form of each inner aggregate, `ROUND(SUM("PARTIAL_sum_0"), 2)` for `ROUND(SUM(x), 2)`, at that item's original `selectList` ordinal, and MUST NOT reference any source column of the base table
* *AND* the query SHALL return EXACTLY ONE row whose value equals the same expression evaluated over all rows on a single node, `SELECT ROUND(SUM(l_quantity), 2) FROM LINEITEM` returning `25304.00` against the seeded TPC-H fixture, never one partial row per shard
* *AND* an `EXPLAIN VIRTUAL` of that query SHALL show a non-empty `"aggregates"` list and an empty `"projection"` in the emitted `LAKEHOUSE_SCAN` common spec, matching the shape a bare `SUM(l_quantity)` already emits

### Scenario: A scalar-wrapped statistical aggregate resolves through the shared AggKind tables, so no aggregate function name reaches DataFusion

* *GIVEN* an ungrouped `pushdown` request whose select list is a scalar function wrapping a statistical aggregate under an Exasol-only alias, `ROUND(VARIANCE(C_ACCTBAL), 4)`, and likewise `ABS(VARIANCE(C_ACCTBAL))`, `VARIANCE(C_ACCTBAL) + 1`, and `ROUND(STDDEV(C_ACCTBAL), 4)`
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL resolve each nested aggregate's function name to its `AggKind` through the SAME two name-mapping tables `pushdown-aggregates/pushdown-agg-sql-consolidation` gives one owner each, `VARIANCE` to `Variance`, `VAR_SAMP` to `VarSamp`, `VAR_POP` to `VarPop`, `STDDEV` to `Stddev`, `STDDEV_SAMP` to `StddevSamp`, `STDDEV_POP` to `StddevPop`, and MUST NOT introduce a second alias mapping of its own
* *AND* the per-shard scan SQL SHALL carry only the aggregate's `(cnt, mean, m2)` sufficient-statistic partial columns, so NO aggregate function name is spliced into the DataFusion query text and the query MUST NOT fail with `Error during planning: Invalid function 'variance'`
* *AND* the outer merge SELECT SHALL reconstruct the statistic through the single statistical merge owner `pushdown-aggregates/pushdown-agg-sql-consolidation` records, so a scalar-wrapped statistical aggregate and a bare one produce the same merge formula
* *AND* `SELECT ROUND(VARIANCE(c_acctbal), 4) FROM CUSTOMER` SHALL return one row equal to the same query evaluated against a native Exasol schema over the same rows
* *AND* a bare `VARIANCE(c_acctbal)` and `ROUND(VAR_SAMP(c_acctbal), 4)` SHALL each return one row equal to native Exasol

### Scenario: Inner aggregates shared across the single-group select list collapse into one deduplicated partial column

* *GIVEN* an ungrouped `pushdown` request whose select list references the same inner aggregate more than once, for example `SELECT COUNT(*), ROUND(SUM(L_QUANTITY) / COUNT(*), 2) FROM LINEITEM`, where `COUNT(*)` appears both as a bare select item and nested inside the scalar wrapper
* *WHEN* the adapter decomposes the select list into partial `AggregatePlan`s
* *THEN* the adapter SHALL collapse aggregates equal by kind and argument into a SINGLE plan slot and a single set of `PARTIAL_*` columns, through the same deduplicating fold the grouped planner uses, rather than emitting one plan per occurrence
* *AND* the deduplication SHALL be treated as a correctness requirement rather than an optimization, because the merge rewrite resolves each nested aggregate to the FIRST structurally-equal plan slot: an un-deduplicated `[Count, Sum, Count]` list would bind the nested `COUNT(*)` to slot 0 while its `EMITS` column was declared at slot 2
* *AND* a plan slot created by a NESTED-only occurrence SHALL take the default declared type, and a top-level occurrence of that same aggregate SHALL overwrite it with the authoritative `selectListDataTypes` entry at its own ordinal, regardless of which occurrence the select list places first
* *AND* both the bare occurrence and the nested occurrence SHALL render to the merged expression over that one shared partial column, and the returned row SHALL equal the same select list evaluated over all rows on a single node

### Scenario: Scalar-over-aggregate and plain aggregate items interleave in select-list order with per-item declared types

* *GIVEN* an ungrouped `pushdown` request whose select list places one or more scalar-over-aggregate items before, after, and between plain aggregates, for example `SELECT MIN(L_QUANTITY), ROUND(SUM(L_QUANTITY), 2), COUNT(*), ROUND(AVG(L_EXTENDEDPRICE), 3) FROM LINEITEM`
* *WHEN* the adapter builds the outer merge SELECT and the fan-out `EMITS` clause
* *THEN* the adapter SHALL emit the outer merge SELECT items in the exact order the corresponding items appear in `selectList`, interleaving merged plain-aggregate expressions and merged scalar-over-aggregate expressions as required, so Exasol's positional pushdown validation passes for every arrangement
* *AND* the adapter SHALL resolve the outer CAST for each item from the `selectListDataTypes` entry at that item's OWN `selectList` ordinal, matched by index rather than by comparing rendered SQL strings, through the single declared-type CAST helper `pushdown-aggregates/pushdown-agg-sql-consolidation` records
* *AND* the adapter SHALL resolve each partial column's `EMITS` type from the PER-PLAN declared-type list the deduplicating fold builds, and MUST NOT index the declared types by position among the `function_aggregate` items alone, because a skipped scalar item shifts every later index
* *AND* the returned row SHALL carry exactly one column per `selectList` item, with each column equal to that item evaluated over all rows on a single node

### Scenario: A nested aggregate the merge cannot decompose widens the projection instead of being evaluated per shard

* *GIVEN* an ungrouped or joined `pushdown` request whose select list contains a scalar function wrapping an aggregate the single-group merge cannot decompose, an inner `COUNT(DISTINCT …)` or other `DISTINCT` aggregate, a `STDDEV`/`VARIANCE` over a rendered expression rather than a bare column, an aggregate over a non-numeric column that the numeric gate demotes, a bare source `column` sitting outside every nested aggregate, or a residual scalar structure the translator cannot render
* *WHEN* the adapter plans the request
* *THEN* the scalar-over-aggregate classification SHALL decline and the adapter SHALL route the request to the qualified single-table wrapper, the same wrapper the grouped decline (`pushdown-grouped-agg/pushdown-planning-grouped-agg-wrapper-fallback`) and the multi-`DISTINCT` single-group decline (`pushdown-aggregates/pushdown-planning-single-group-agg`) already use, whose own rendered SQL computes the whole select list as native Exasol SQL over a materialized sharded raw scan narrowed to the referenced columns
* *AND* the adapter MUST NOT emit the item as a per-shard projection expression, and MUST NOT return a bare row scan whose column count or column types differ from `selectListDataTypes` (`sqlCode 04000`), because Exasol never re-aggregates a declined pushdown
* *AND* the decline SHALL hold on the BROADCAST-JOIN path too, where the widened projection is a plain reason the broadcast plan is unavailable, the broadcast planner declining cleanly rather than erroring, so the request falls through to the unaccelerated N-scan fallback that renders the select list natively, because a single-group aggregation over a join is not recognised as carrying an aggregation clause when its only aggregate is nested inside a scalar wrapper
* *AND* the decline SHALL hold on the EMPTY-RESULT path, where every data file is pruned, so the response is the single-group empty shape (`file-planning/pushdown-planning-empty-result`) rather than the full base row
* *AND* the returned result SHALL equal the same select list evaluated on a single node for every one of those shapes

### Scenario: A fully-pruned file list yields one shape-correct empty row for a scalar-over-aggregate select list

* *GIVEN* an ungrouped `pushdown` request carrying a decomposable scalar-over-aggregate select item, for example `SELECT ROUND(SUM(L_QUANTITY), 2), COUNT(*) FROM LINEITEM WHERE L_QUANTITY < -1`
* *AND* file pruning eliminates every data file, so there is nothing to scan
* *WHEN* the adapter builds the empty-result response
* *THEN* the response SHALL be the single-group empty shape (`file-planning/pushdown-planning-empty-result`): ONE row with one column per `selectList` item in select-list order, each item evaluated by Exasol over the empty set, and MUST NOT be the zero-row grouped shape or the full base row
* *AND* the empty response SHALL reference no scan and no merge UDF, because with zero files there is nothing to scan or merge

### Scenario: The scalar-over-aggregate decomposition mechanism has ONE owner shared by both aggregate planners

* *GIVEN* the four primitives of scalar-over-aggregate decomposition, the aggregate-sentinelizing tree walk, the scalar-over-aggregate classifier, the merge rewrite that substitutes each sentinel with its merged partial expression, and the deduplicating plan fold, all declared in the `scalar_over_agg` submodule of `adapter::pushdown`
* *WHEN* the grouped planner and the single-group planner both decompose a select item that wraps aggregates
* *THEN* those four primitives SHALL have exactly ONE owner reachable by BOTH planners, and the single-group planner MUST NOT carry a second copy of the tree walk, the sentinel token format, the classifier's decline rules, or the substitution rewrite
* *AND* the owning module SHALL expose the primitives at the narrowest visibility that compiles, and its tests SHALL live in its own sibling test file per `pushdown/pushdown-module-structure`
* *AND* the grouped planner's rendered SQL SHALL match the committed `dispatch_golden` fixtures by full-string equality
* *AND* a top-level bare aggregate, a grouped scalar-over-aggregate item, and a single-group scalar-over-aggregate item SHALL be rewritten by the SAME merge-rewriting path, so the three produce consistent merged SQL for the same inner aggregate
