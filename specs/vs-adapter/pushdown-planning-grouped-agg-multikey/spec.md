# Feature: Pushdown Planning — Multi-Key Grouped Aggregate Queries

Extends `vs-adapter/pushdown-planning-grouped-agg` with the N≥2 group-key scenarios:
pushing a multi-column GROUP BY down as node-local partial aggregation instead of
falling back to a raw row scan, rendering every element of a multi-key tuple
(including tuples where every element is itself a scalar expression) independently
through the VS expression translator, and resolving each key's OWN Exasol-declared
result type by its select-list index rather than sharing or defaulting a type across
keys.

## Background

* A grouped aggregate pushdown arrives as `aggregationType: "group_by"` with a
  non-empty `groupBy` array and a select list of supported aggregate functions; see
  `vs-adapter/pushdown-planning-grouped-agg` for the single/simple-key detection,
  wrapper-ordering, shard fan-out, LIMIT-exclusion, and NULL-grouping scenarios shared
  by every grouped-aggregate pushdown regardless of key count.
* The `groupBy` array MAY contain one, two, or more elements; the detection and
  scan-driving SQL builder treat the group-key list as arbitrary-length (emitting
  `GK_0..GK_{n-1}`) and impose no cap of one key.
* Each group-key element is rendered independently via
  `vs_expression::render_expression` (raising mode), so a multi-key GROUP BY MAY mix
  plain column references and scalar expressions in any combination; if ANY single
  element fails to render, the adapter falls back to row scanning for the whole
  request (never a partial subset of the group keys).
* Pushdown-occurred evidence for a multi-key GROUP BY is the scan spec's `group_keys`
  field (plus the outer wrapper's `PARTIAL_*` merge columns) — NOT `GROUP BY
  shard_key`, which is absent whenever a filter prunes the assigned file list to a
  single shard.
* Expression-valued tuple keys must be built from scalar functions Exasol actually
  advertises and therefore sends as pushed group keys (e.g. `MOD`, `UPPER`, `YEAR`).
  Arithmetic operators (`/`, `*`) and `CAST` are rendered by the translator but are
  NOT advertised, so Exasol does not send them as pushed group keys; advertising them
  is future scope, out of scope here.
* Each `GK_{i}` column's Exasol-declared result type is resolved from the
  `selectListDataTypes` entry at that key's OWN `selectList` index (matched by index,
  not by comparing rendered SQL strings), so a multi-key tuple of differing declared
  types (e.g. a `DECIMAL` key and a `VARCHAR` key) receives its correct per-key CAST
  rather than a shared or defaulted `VARCHAR(2000000)`.

## Scenarios

### Scenario: Multi-column GROUP BY is pushed down as partial aggregation rather than a raw row scan

* *GIVEN* a virtual schema over an Iceberg table backed by MinIO
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
