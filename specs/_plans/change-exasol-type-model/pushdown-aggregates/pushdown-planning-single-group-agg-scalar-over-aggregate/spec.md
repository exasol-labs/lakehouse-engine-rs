# Feature: Pushdown Planning, Single-Group Scalar-Over-Aggregate Select Items

The adapter decomposes an ungrouped select item that is a scalar function wrapping aggregates, such as `ROUND(SUM(L_QUANTITY), 2)`, into the same per-shard partial and outer-merge plan a bare aggregate gets. A nested aggregate the merge cannot decompose routes to the qualified single-table wrapper.

## Background

* Without decomposition the scalar item renders as a per-shard projection expression, so each shard computes a whole aggregate over its own files and Exasol returns one row per shard.
* `VARIANCE` is Exasol's alias for `VAR_SAMP`, and DataFusion has no function named `variance`. The decomposition resolves each nested aggregate name to an `AggKind` through the same name tables a bare aggregate uses, so no aggregate function name reaches DataFusion.
* The decomposition reuses the grouped planner's four primitives: the aggregate-sentinelizing tree walk, the scalar-over-aggregate classifier, the merge rewrite that substitutes each sentinel with its merged partial expression, and the deduplicating plan fold.
* The merge rewrite binds each nested aggregate to the first structurally-equal plan slot, so the plan list must be deduplicated.
* The plan list and the select list are not one-to-one. A scalar item contributes no plan of its own and shifts later select-list indexes, so partial `EMITS` types come from the per-plan declared-type list and the outer CAST types come from each select item's own `selectListDataTypes` ordinal.
* A select item with no nested aggregate, such as `ROUND(price, 2)`, yields no aggregate to decompose and reaches the row-scan projection.
* Exasol delegates an advertised capability fully and never re-applies it, so the adapter renders equivalent SQL for any shape it cannot push.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Scalar-over-aggregate and plain aggregate items interleave in select-list order with per-item declared types

* *GIVEN* an ungrouped `pushdown` request whose select list places one or more scalar-over-aggregate items before, after, and between plain aggregates, for example `SELECT MIN(L_QUANTITY), ROUND(SUM(L_QUANTITY), 2), COUNT(*), ROUND(AVG(L_EXTENDEDPRICE), 3) FROM LINEITEM`
* *WHEN* the adapter builds the outer merge SELECT and the fan-out `EMITS` clause
* *THEN* the adapter SHALL emit the outer merge SELECT items in the exact order the corresponding items appear in `selectList`, interleaving merged plain-aggregate expressions and merged scalar-over-aggregate expressions as required, so Exasol's positional pushdown validation passes for every arrangement
* *AND* the adapter SHALL take each item's declared type from the `selectListDataTypes` entry at that item's own `selectList` ordinal, and SHALL cast or leave uncast each merged item by the merge-cast rule of `pushdown-aggregates/pushdown-planning-single-group-agg`, so `MIN(L_QUANTITY)` stays uncast and `ROUND(SUM(L_QUANTITY), 2)`, `COUNT(*)`, and `ROUND(AVG(L_EXTENDEDPRICE), 3)` are cast
* *AND* each partial column's `EMITS` type SHALL be the type of the aggregate that column carries, even when a scalar-over-aggregate item earlier in the select list shifts the ordinals of later aggregates
* *AND* the returned row SHALL carry exactly one column per `selectList` item, with each column equal to that item evaluated over all rows on a single node
<!-- /DELTA:CHANGED -->
