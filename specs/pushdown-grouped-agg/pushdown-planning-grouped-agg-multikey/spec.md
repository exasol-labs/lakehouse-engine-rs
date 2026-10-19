# Feature: Pushdown Planning, Multi-Key Grouped Aggregate Queries

Extends `pushdown-grouped-agg/pushdown-planning-grouped-agg` to a GROUP BY with two or more keys. The adapter pushes it down as node-local partial aggregation, renders each key independently through the VS expression translator, and resolves each key's Exasol-declared result type by its select-list index.

## Background

* `pushdown-grouped-agg/pushdown-planning-grouped-agg` covers the detection, wrapper ordering, shard fan-out, LIMIT exclusion, and NULL grouping shared by every key count. The detection and scan-driving SQL builder accept a group-key list of any length and emit `GK_0..GK_{n-1}`.
* `vs_expression::render_expression` (raising mode) renders each key. A tuple can mix column references and scalar expressions. If any element fails to render, the adapter falls back to row scanning for the whole request and never pushes a subset of the keys.
* Evidence that a multi-key GROUP BY was pushed down is the scan spec's `group_keys` field and the outer wrapper's `PARTIAL_*` merge columns. `GROUP BY shard_key` is absent when a filter prunes the file list to a single shard.
* Expression keys use scalar functions Exasol advertises and so sends as pushed group keys, such as `MOD`, `UPPER`, and `YEAR`. The translator renders arithmetic operators and `CAST`, but Exasol does not advertise them, so it does not send them as group keys.
* Each `GK_{i}` column's declared type comes from the `selectListDataTypes` entry at that key's own `selectList` index, matched by index and not by comparing rendered SQL. A `DECIMAL` key and a `VARCHAR` key each get their own CAST and not a shared `VARCHAR(2000000)`.


## Scenarios

### Scenario: Multi-column GROUP BY is pushed down as partial aggregation rather than a raw row scan

* *GIVEN* a virtual schema over an Iceberg table backed by SeaweedFS
* *AND* a query grouping by two or more plain columns with a supported aggregate (e.g. `SELECT L_SHIPYEAR, L_RETURNFLAG, SUM(L_QUANTITY) FROM {vs_table} GROUP BY L_SHIPYEAR, L_RETURNFLAG`)
* *WHEN* Exasol sends the corresponding `pushdown` request with `aggregationType: "group_by"` and a `groupBy` array of length ≥ 2
* *THEN* the adapter SHALL detect the grouped aggregate, render every group-key element, and build a grouped scan spec carrying all group keys (`GK_0..GK_{n-1}`) and the aggregate plans, exactly as it does for a single-key GROUP BY
* *AND* the adapter MUST NOT fall back to a raw row-scan ScanSpec solely because the `groupBy` array has more than one element
* *AND* the generated scan-driving SQL SHALL perform node-local partial aggregation per shard and merge the partials in the outer wrapper, so only per-group partial rows (not raw rows) cross the network

### Scenario: Every element of a multi-key tuple may be an expression

* *GIVEN* a grouped aggregate `pushdown` request whose `groupBy` array contains two or more elements where each element is itself a scalar expression (e.g. `GROUP BY MOD(id,4), UPPER(name)`, or `GROUP BY YEAR(ts), LOWER(region)`), not merely a single expression key — the scalar functions being ones Exasol advertises and therefore actually sends as pushed group keys (arithmetic `/`/`*` and `CAST` are rendered by the translator but not advertised, so Exasol does not send them as group keys; that advertisement is future scope)
* *WHEN* the adapter builds the grouped scan spec
* *THEN* the adapter SHALL render EACH group-key expression element independently via the VS expression translator (raising mode) and carry the rendered fragments in `group_keys` in `groupBy` order
* *AND* the scan UDF SHALL use those same rendered expressions verbatim in its per-shard DataFusion GROUP BY clause, one per `GK_{i}` column
* *AND* if ANY single element of the tuple is not translatable, the adapter SHALL fall back to row scanning for the whole request (never pushing down a partial subset of the group keys)

### Scenario: Each group key in a multi-key tuple resolves its own declared result type

* *GIVEN* a grouped aggregate `pushdown` request with two or more group keys of differing declared Exasol types (e.g. a `DECIMAL` key and a `VARCHAR` key), possibly interleaved with aggregates in `selectList`
* *WHEN* the adapter resolves each group key's Exasol-declared result type and builds the outer wrapper cast list
* *THEN* the adapter SHALL resolve each `GK_{i}` column's declared type from the `selectListDataTypes` entry at that key's OWN `selectList` index (matched by index, not by comparing rendered SQL strings), so each of the N keys receives its correct CAST rather than a shared or defaulted `VARCHAR(2000000)`
* *AND* the outer wrapper SELECT SHALL emit each group-key cast expression at the ordinal position that key occupied in the user's `selectList`, so a mixed-type multi-key result passes Exasol's positional `selectListDataTypes` validation for any interleaving of keys and aggregates
* *AND* the merged per-group result SHALL equal the result of the same multi-key grouped aggregate evaluated over all rows on a single node
