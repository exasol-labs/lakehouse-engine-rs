# Feature: Pushdown Planning, Single-Group Aggregate

The adapter advertises the supported aggregate capabilities, translates an ungrouped aggregate query into a partial-aggregate scan spec, and merges each shard's partial row into the final result.

## Background

* A single-group aggregate splits into a per-shard partial aggregate, computed by the `LAKEHOUSE_SCAN` scalar EMIT UDF, and an outer ungrouped merge aggregation in the wrapper SQL.
* The `GROUP BY shard_key` used for cluster fan-out lives only inside the nested `LAKEHOUSE_DISTRIBUTE_FILES` distributor subquery, never at the outer merge level.
* The scan UDF reads the `aggregates` field of the common spec on the aggregate path and derives the DataFusion projection from the partial-aggregate query text, so `projection` is left empty. Only the row-scan path reads `projection`.
* Exasol marks every aggregate without GROUP BY `aggregationType: single_group`, including an aggregate referenced only in HAVING, and a subquery whose aggregates the outer query does not reference, which Exasol strips to a select list of `NULL`. Such a request never reaches the row-scan projection, because a row scan returns raw rows instead of the aggregated row.
* Every qualified wrapper renders a `single_group` request with `GROUP BY ()`: the single-table wrapper, the wrapper over a join, and the empty relation. Over the empty grouping set Exasol returns one row even when neither the select list nor the HAVING holds an aggregate, and still applies HAVING and LIMIT.
* Exasol does not re-apply a HAVING, because the adapter advertises `AGGREGATE_HAVING`.
* A select item may also be a scalar function wrapping aggregates (`pushdown-aggregates/pushdown-planning-single-group-agg-scalar-over-aggregate`).
* Exactly one `COUNT(DISTINCT <bare column>)` and no other select-list item (Case 1) becomes a DISTINCT row-scan fan-out (`pushdown-aggregates/pushdown-planning-count-distinct`). More than one distinct aggregate, or a distinct aggregate beside an ordinary aggregate (Case 2/3), routes to a qualified single-table wrapper (`pushdown-grouped-agg/pushdown-planning-grouped-agg-wrapper-fallback`). The wrapper computes every aggregate over a materialized sharded raw scan.
* Exasol never re-aggregates a declined pushdown, so a bare row scan where `selectListDataTypes` expects aggregate columns fails with `sqlCode 04000`. Exasol also rejects an emitting UDF call nested in a scalar subquery (`sqlCode 04000`).
* Exasol rejects an `OFFSET` in any ungrouped aggregated select (`sqlCode 42000`) and pushes no `orderBy` on an ungrouped aggregate request. The merge SELECT therefore takes no offset and renders no `ORDER BY`.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: A single-group request that does not decompose routes to the qualified wrapper

* *GIVEN* a `pushdown` request marked `aggregationType: single_group` whose select list does not decompose, for example `SUM` or `VARIANCE` over a text column, `STDDEV` or `VARIANCE` over an expression, no aggregate outside HAVING (`SELECT 1 ... HAVING COUNT(*) > 0`), or no aggregate at all (`SELECT COUNT(*) FROM (SELECT SUM(col) FROM t)`). A constant beside an aggregate, such as `SELECT 'X', COUNT(*)` or a folded clock function (`pushdown/pushdown-planning-clock-functions`), decomposes, and the merge renders the constant
* *WHEN* the adapter plans the request over a non-empty file list
* *THEN* the adapter SHALL route it to the qualified single-table wrapper, whose own rendered SQL computes the select list and HAVING over the materialized sharded raw scan
* *AND* the adapter MUST NOT return a bare row scan
* *AND* the result SHALL equal native Exasol: one row, or no row when HAVING is false
<!-- /DELTA:CHANGED -->
