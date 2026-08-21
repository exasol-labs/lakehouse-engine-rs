# Feature: Pushdown Planning — Single-Group Aggregate

Single-group (ungrouped) aggregate pushdown: advertising which aggregate functions the
adapter supports, translating an ungrouped aggregate query into a partial-aggregate scan
spec, and merging each shard's partial-aggregate row into the final result. Split out of
`vs-adapter/pushdown-planning` to keep that feature's core file-resolution/projection/
filter/LIMIT scenarios separate from aggregate-specific ones. See
`vs-adapter/pushdown-planning-grouped-agg` for GROUP BY aggregate pushdown,
`vs-adapter/pushdown-planning-count-distinct` for `COUNT(DISTINCT ...)` decomposition, and
`vs-adapter/pushdown-planning-single-group-agg-scalar-over-aggregate` for an ungrouped
select item that is a scalar function wrapping aggregates.

## Background

<!-- DELTA:NEW -->
* **The scalar-wrapper shape is split out into its own feature.**
  `vs-adapter/pushdown-planning-single-group-agg-scalar-over-aggregate` owns the case where
  an ungrouped `selectList` item is a scalar function WRAPPING one or more aggregates
  (`ROUND(SUM(L_QUANTITY), 2)`, `ROUND(VARIANCE(C_ACCTBAL), 4)`), mirroring the split
  `vs-adapter/pushdown-planning-grouped-agg-scalar-over-aggregate` made against the grouped
  base feature. This feature keeps capability advertisement, bare-aggregate detection, the
  AVG sum/count pair, the empty-`projection` contract, the `COUNT(DISTINCT)` branch, and
  the no-`OFFSET` merge.
* **Detection is no longer "every item is literally `function_aggregate`".** A select-list
  item may also be a decomposable scalar-over-aggregate, classified by the shared mechanism
  the grouped planner already uses. The gate on `groupBy` being absent or empty is
  unchanged, no `aggregationType` check is added, and a select-list item carrying no nested
  aggregate still declines the aggregate tiers and reaches the row-scan projection exactly
  as before.
* **The per-plan and per-select-item declared-type lists split.** Once a
  scalar-over-aggregate item is present, the aggregate plan list and the `selectList` stop
  being 1:1, so `aggregate_exasol_types` — which filters `selectList` down to
  `function_aggregate` items and therefore shifts every index after a skipped scalar item —
  serves neither the `EMITS` types nor the outer CAST. The sibling feature records the
  replacement: a per-plan type list built by the deduplicating fold, and each select item's
  own `selectListDataTypes` entry.
<!-- /DELTA:NEW -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Aggregate query is translated into a partial-aggregate scan spec

* *GIVEN* a virtual schema over an Iceberg table backed by MinIO
* *AND* a query whose select list is one or more supported aggregate functions over the whole table, each either a bare aggregate or a scalar function wrapping aggregates (see `vs-adapter/pushdown-planning-single-group-agg-scalar-over-aggregate`)
* *WHEN* Exasol sends the corresponding `pushdown` request
* *THEN* the adapter SHALL recognise the request as an aggregate query and resolve the data-file list exactly once
* *AND* the adapter SHALL build a scan spec carrying, for each requested aggregate — including every aggregate nested inside a scalar wrapper — its function kind and target column (the wildcard for `COUNT(*)`), plus any pushed-down filter so the partial aggregate covers filtered rows only
* *AND* the adapter MUST NOT push down an aggregate the scan UDF cannot compute, falling back to the qualified single-table wrapper for that query instead of a bare row scan whenever the select list carries an aggregate at any depth, because Exasol never re-aggregates a declined pushdown
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: Single-group aggregate scan spec leaves the projection field empty

* *GIVEN* an ungrouped aggregate `pushdown` request over a table with more than one column (e.g. `SELECT COUNT(*)`, `SELECT SUM(col)`, `SELECT MIN(col), MAX(col)`, or `SELECT ROUND(SUM(col), 2)`)
* *WHEN* the adapter builds the partial-aggregate scan spec
* *THEN* the shard-invariant common spec's `projection` field SHALL be empty, NOT the full base-table column list
* *AND* the referenced-column information SHALL be carried in the `aggregates` field, which is the field the aggregate scan-dispatch path consults; the `projection` field MUST NOT be read on that path
* *AND* an `EXPLAIN VIRTUAL` of the same query SHALL show `"projection":[]` in the emitted `LAKEHOUSE_SCAN` common spec, so the diagnostic output no longer misreports a full-column projection for an aggregate query
* *AND* for a scalar-over-aggregate select item the emitted `"projection"` SHALL likewise be empty and the emitted `"aggregates"` SHALL be non-empty, so the diagnostic output MUST NOT show the aggregate rendered as a projection expression (`"projection":[{"expr":"round(SUM(\"L_QUANTITY\"), 2)"}]`, the `EXPLAIN VIRTUAL` signature of issue #194)
* *AND* the physical Parquet read SHALL remain pruned to the aggregate-referenced columns via DataFusion's own projection pushdown (see `datafusion-scan/scan-execution-partial-agg`), so the empty `projection` field does not widen the scan
<!-- /DELTA:CHANGED -->
