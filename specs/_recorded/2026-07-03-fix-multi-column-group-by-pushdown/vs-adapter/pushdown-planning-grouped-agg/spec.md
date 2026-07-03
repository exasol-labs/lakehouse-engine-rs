# Feature: Pushdown Planning — Grouped Aggregate Queries

Extends `vs-adapter/pushdown-planning` with the GROUP BY aggregate detection and
scan-driving SQL generation scenarios. When Exasol delegates a `GROUP BY` aggregate
query, the adapter detects the shape, renders group-key expressions via the VS
expression translator, builds a grouped common scan spec, and generates fan-out SQL
that runs DataFusion GROUP BY inside each shard invocation and merges the partials in
an outer wrapper. The grouped common spec is serialized once (shared by all shards)
and carries no LIMIT.

## Background

* A grouped aggregate pushdown arrives as `aggregationType: "group_by"` with a
  non-empty `groupBy` array and a select list of supported aggregate functions.
* The `groupBy` array MAY contain one, two, or more elements; the detection and
  scan-driving SQL builder treat the group-key list as arbitrary-length (emitting
  `GK_0..GK_{n-1}`) and impose no cap of one key.
* Group-key expressions are rendered by `vs_expression::render_expression` (raising
  mode); any failure on ANY element causes the adapter to fall back to row scanning.
* Each group-key element is rendered independently, so a multi-key GROUP BY MAY mix
  plain column references and scalar expressions in any combination.

## Scenarios

<!-- DELTA:NEW -->
### Scenario: Multi-column GROUP BY is pushed down as partial aggregation rather than a raw row scan

* *GIVEN* a virtual schema over an Iceberg table backed by MinIO
* *AND* a query grouping by two or more plain columns with a supported aggregate (e.g. `SELECT L_SHIPYEAR, L_RETURNFLAG, SUM(L_QUANTITY) FROM {vs_table} GROUP BY L_SHIPYEAR, L_RETURNFLAG`)
* *WHEN* Exasol sends the corresponding `pushdown` request with `aggregationType: "group_by"` and a `groupBy` array of length ≥ 2
* *THEN* the adapter SHALL detect the grouped aggregate, render every group-key element, and build a grouped scan spec carrying all group keys (`GK_0..GK_{n-1}`) and the aggregate plans, exactly as it does for a single-key GROUP BY
* *AND* the adapter MUST NOT fall back to a raw row-scan ScanSpec solely because the `groupBy` array has more than one element
* *AND* the generated scan-driving SQL SHALL perform node-local partial aggregation per shard and merge the partials in the outer wrapper, so only per-group partial rows (not raw rows) cross the network
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: Every element of a multi-key tuple may be an expression

* *GIVEN* a grouped aggregate `pushdown` request whose `groupBy` array contains two or more elements where each element is itself a scalar expression (e.g. `GROUP BY MOD(id,4), UPPER(name)`, or `GROUP BY YEAR(ts), LOWER(region)`), not merely a single expression key — the scalar functions being ones Exasol advertises and therefore actually sends as pushed group keys (arithmetic `/`/`*` and `CAST` are rendered by the translator but not advertised, so Exasol does not send them as group keys; that advertisement is future scope)
* *WHEN* the adapter builds the grouped scan spec
* *THEN* the adapter SHALL render EACH group-key expression element independently via the VS expression translator (raising mode) and carry the rendered fragments in `group_keys` in `groupBy` order
* *AND* the scan UDF SHALL use those same rendered expressions verbatim in its per-shard DataFusion GROUP BY clause, one per `GK_{i}` column
* *AND* if ANY single element of the tuple is not translatable, the adapter SHALL fall back to row scanning for the whole request (never pushing down a partial subset of the group keys)
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: Each group key in a multi-key tuple resolves its own declared result type

* *GIVEN* a grouped aggregate `pushdown` request with two or more group keys of differing declared Exasol types (e.g. a `DECIMAL` key and a `VARCHAR` key), possibly interleaved with aggregates in `selectList`
* *WHEN* the adapter resolves each group key's Exasol-declared result type and builds the outer wrapper cast list
* *THEN* the adapter SHALL resolve each `GK_{i}` column's declared type from the `selectListDataTypes` entry at that key's OWN `selectList` index (matched by index, not by comparing rendered SQL strings), so each of the N keys receives its correct CAST rather than a shared or defaulted `VARCHAR(2000000)`
* *AND* the outer wrapper SELECT SHALL emit each group-key cast expression at the ordinal position that key occupied in the user's `selectList`, so a mixed-type multi-key result passes Exasol's positional `selectListDataTypes` validation for any interleaving of keys and aggregates
* *AND* the merged per-group result SHALL equal the result of the same multi-key grouped aggregate evaluated over all rows on a single node
<!-- /DELTA:NEW -->
