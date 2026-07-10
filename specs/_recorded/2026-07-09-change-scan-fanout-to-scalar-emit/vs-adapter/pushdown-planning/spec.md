# Feature: Pushdown Planning

Translates an Exasol query against the virtual schema into a pushdown plan: it resolves
the Iceberg data-file list once, captures the requested projection, filter, LIMIT, and
any supported aggregate, extracts the table's current Iceberg schema for field-id-based
projection, and emits the SQL that drives the DataFusion scan. Cluster fan-out is
separated from the scan: a nested `LAKEHOUSE_DISTRIBUTE_FILES` LUA SET distributor
subquery (`GROUP BY shard_key`) spreads each shard's per-file list across nodes, and an
outer ungrouped `LAKEHOUSE_SCAN` SCALAR EMIT UDF scans each distributed file list
node-locally and streams the rows. The scan-driving SQL splices the shard-invariant parts
(projection, filter, LIMIT, logical schema, credentials, and the Iceberg table root) once
as the scalar scan UDF's first-argument common literal and flows each shard's per-file
subset through the distributor as the second argument. A single-shard plan short-circuits
the distributor and calls the scalar scan directly on the file-list literal.

## Background

* The scan-driving SQL invokes the `LAKEHOUSE_SCAN` SCALAR EMIT UDF over a nested `LAKEHOUSE_DISTRIBUTE_FILES` distributor subquery; the shard-invariant common spec is spliced once as the scalar scan's first argument and each shard's file subset flows through the distributor.
* The outer scalar scan select is never wrapped in a `SELECT * FROM (...)` materialization boundary.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Pushdown resolves the file list once and builds a scan-driving query

* *GIVEN* a virtual schema over a namespace whose tables are backed by MinIO
* *AND* a query that projects a subset of columns from one of those tables
* *WHEN* Exasol sends the corresponding `pushdown` request
* *THEN* the adapter SHALL determine the target Iceberg table from the schema-metadata mapping, resolve that table's Iceberg snapshot, data-file list, and each file's byte size exactly once, and at that same seam extract the table's current Iceberg schema (from `current_schema()`) into a logical schema carrying, per column, its `field_id`, current name, Arrow type, and nullability
* *AND* the adapter SHALL return a JSON response of type `pushdown` containing SQL that invokes the `LAKEHOUSE_SCAN` SCALAR EMIT UDF, carrying the logical schema AND the Iceberg table root in the shard-invariant common spec spliced ONCE as the scalar scan's first-argument literal, and the resolved data-file list flowed through the nested `LAKEHOUSE_DISTRIBUTE_FILES` distributor as the per-shard argument, where each per-shard entry carries the file path together with its resolved byte size
* *AND* the outer scalar scan select MUST NOT be wrapped in a `SELECT * FROM (...)` materialization boundary
* *AND* the adapter MUST NOT require the scan UDF to discover files itself, and MUST NOT require the scan UDF to re-fetch any file's size
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: Projection is pushed into the scan-driving query

* *GIVEN* a query that selects only some of the table's columns
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the generated scan-driving SQL SHALL carry only the projected columns to the UDF, in the shard-invariant common spec spliced once as the scalar scan UDF's first-argument literal shared by all shards
* *AND* the projected column names SHALL be the current Iceberg logical names carried in the common spec's logical schema, so the UDF's registered table exposes them and the field-id adapter maps each to the correct physical column per file
* *AND* the scalar scan UDF's declared EMITS column list SHALL match the projected columns in order and type
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: LIMIT is pushed into the scan spec

* *GIVEN* a query with a LIMIT clause and NO `order_by` that governs which rows are selected
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the shard-invariant common spec spliced into the scalar scan UDF SHALL carry the row limit
* *AND* because the common spec is shared by every shard, each row-scan shard invocation SHALL observe the same limit
* *AND* the generated SQL SHALL attach the `LIMIT` DIRECTLY to the outer ungrouped scalar scan select (over the distributor subquery, or the from-less single-shard select) as a correctness backstop, with no `SELECT * FROM (...)` wrapper
* *AND* when the request DOES carry an `order_by`, the per-shard row limit SHALL be governed by ordered top-N (pushed only alongside the matching per-shard `ORDER BY`), never as a bare per-shard `LIMIT` ahead of a global sort
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: Aggregate wrapper SQL merges per-shard partial results

* *GIVEN* an aggregate pushdown over a file list partitioned into one or more shards
* *WHEN* the adapter builds the scan-driving SQL
* *THEN* the generated SQL SHALL drive the `LAKEHOUSE_SCAN` SCALAR EMIT UDF — fired once per distributed shard row from the nested `LAKEHOUSE_DISTRIBUTE_FILES` distributor (or once directly for a single-shard plan) — to emit one partial-aggregate row per shard
* *AND* the SQL SHALL wrap those partial rows in an OUTER ungrouped aggregation over the scalar scan select that merges them into the final result: `SUM` over per-shard partial counts for `COUNT`, `SUM` over partial sums for `SUM`, `MIN`/`MAX` over partial extrema for `MIN`/`MAX`
* *AND* the `GROUP BY shard_key` used for cluster fan-out SHALL live only inside the nested distributor subquery, never at the outer merge level
* *AND* the merged result SHALL equal the result of the same aggregate evaluated over all rows on a single node
<!-- /DELTA:CHANGED -->
