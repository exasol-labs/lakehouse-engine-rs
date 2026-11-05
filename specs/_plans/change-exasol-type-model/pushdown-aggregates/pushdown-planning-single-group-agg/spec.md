# Feature: Pushdown Planning, Single-Group Aggregate

The adapter advertises the supported aggregate capabilities, translates an ungrouped aggregate query into a partial-aggregate scan spec, and merges each shard's partial row into the final result.

## Background

* A single-group aggregate splits into a per-shard partial aggregate, computed by the `LAKEHOUSE_SCAN` scalar EMIT UDF, and an outer ungrouped merge aggregation in the wrapper SQL.
* The `GROUP BY shard_key` used for cluster fan-out lives only inside the nested `LAKEHOUSE_DISTRIBUTE_FILES` distributor subquery, never at the outer merge level.
* The scan UDF reads the `aggregates` field of the common spec on the aggregate path and derives the DataFusion projection from the partial-aggregate query text, so `projection` is left empty. Only the row-scan path reads `projection`.
* Exasol marks every aggregate without GROUP BY `aggregationType: single_group`, including an aggregate referenced only in HAVING. Such a request never reaches the row-scan projection, because a row scan returns raw rows instead of the aggregated row.
* Exasol does not re-apply a HAVING, because the adapter advertises `AGGREGATE_HAVING`.
* A select item may also be a scalar function wrapping aggregates (`pushdown-aggregates/pushdown-planning-single-group-agg-scalar-over-aggregate`).
* Exactly one `COUNT(DISTINCT <bare column>)` and no other select-list item (Case 1) becomes a DISTINCT row-scan fan-out (`pushdown-aggregates/pushdown-planning-count-distinct`). More than one distinct aggregate, or a distinct aggregate beside an ordinary aggregate (Case 2/3), routes to a qualified single-table wrapper (`pushdown-grouped-agg/pushdown-planning-grouped-agg-wrapper-fallback`). The wrapper computes every aggregate over a materialized sharded raw scan.
* Exasol never re-aggregates a declined pushdown, so a bare row scan where `selectListDataTypes` expects aggregate columns fails with `sqlCode 04000`. Exasol also rejects an emitting UDF call nested in a scalar subquery (`sqlCode 04000`).
* Exasol rejects an `OFFSET` in any ungrouped aggregated select (`sqlCode 42000`) and pushes no `orderBy` on an ungrouped aggregate request. The merge SELECT therefore takes no offset and renders no `ORDER BY`.

## Scenarios

<!-- DELTA:NEW -->
### Scenario: The merge casts a merged value only when merging can change its type

* *GIVEN* a single-group or grouped aggregate `pushdown` request that the adapter serves with the partial/merge shape
* *WHEN* the adapter builds the outer merge SELECT
* *THEN* the adapter SHALL cast each merged `SUM`, `COUNT`, `AVG`, and statistic, and each scalar expression over merged aggregates, to the type `selectListDataTypes` declares at that item's ordinal, because the merge can return a type other than the declared one
* *AND* the adapter SHALL leave a merged `MIN` or `MAX` uncast when its partial column is emitted in the declared type, for example `MIN("PARTIAL_min_3")` for a `MIN` over a TIMESTAMP column, because `MIN` and `MAX` return the type of their argument
* *AND* the adapter SHALL cast a group key whose emitted type differs from its declared type, and SHALL leave it uncast when the two types are equal, for example a key declared `VARCHAR(2000000)`
* *AND* a cast to a declared `VARCHAR(2000000)` SHALL be rendered like a cast to any other type, for example `CAST(SUM("PARTIAL_sum_0") AS VARCHAR(2000000))`
* *AND* the merged result SHALL equal the same select list evaluated over all rows on a single node, and the pushdown SQL SHALL pass Exasol's positional type check
<!-- /DELTA:NEW -->
