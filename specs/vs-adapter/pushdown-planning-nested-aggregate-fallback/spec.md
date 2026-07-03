# Feature: Pushdown Planning — Nested Aggregate Fallback

Extends `vs-adapter/pushdown-planning` with the guard against composed pushdown requests
whose `selectList`, `groupBy`, or `filter` nodes do not all resolve to plain columns of the
involved source table's current Iceberg schema — e.g. an outer aggregate over an inner
grouped-aggregate sub-select. Rather than rendering a scan-driving SQL that references a
phantom identifier absent from the source schema, the adapter falls back to row scanning so
Exasol applies the outer computation itself.

## Background

* A composed pushdown request (an outer aggregate over an inner grouped-aggregate sub-select,
  or any other shape with select-list/group-by/filter nodes that don't map onto the source
  table's own columns) MUST NOT be rendered into scan-driving SQL that references a column
  absent from the involved table's resolved logical schema.
* When the adapter cannot map every `selectList`/`groupBy` node of a composed request onto a
  supported single-group aggregate, grouped aggregate, or projection over the source table's
  own columns, it falls back to row scanning (a row-scan ScanSpec with no aggregates field) so
  Exasol applies the outer computation on the returned rows using its own engine.

## Scenarios

### Scenario: Composed pushdown request never renders a scan spec that references a non-source column

* *GIVEN* a virtual schema over an Iceberg table backed by MinIO
* *AND* a user query whose pushdown request composes an outer aggregate over an inner grouped-aggregate sub-select — e.g. `SELECT COUNT(*) FROM (SELECT L_ORDERKEY, COUNT(*) AS cnt FROM {vs_table} GROUP BY L_ORDERKEY) t` — so that some `selectList`, `groupBy`, or `filter` node does not resolve to a plain column of the involved source table's current Iceberg schema
* *WHEN* Exasol sends the corresponding `pushdown` request and the adapter builds the scan spec and the scan-driving SQL
* *THEN* every column reference in the per-shard scan-driving SQL the adapter emits SHALL name a column present in the involved table's resolved logical schema (or an aggregate/group-key expression rendered from one), and the adapter MUST NOT emit a scan spec whose rendered SQL references a phantom identifier such as `NULL` that is absent from the source schema
* *AND* when the adapter cannot map every `selectList`/`groupBy` node of a composed request onto a supported single-group aggregate, grouped aggregate, or projection over the source table's own columns, it SHALL fall back to row scanning (emitting a row-scan ScanSpec with no aggregates field) so Exasol applies the outer computation on the returned rows using its own engine
* *AND* the query MUST NOT fail with a DataFusion `Schema error: No field named ...` (or any planning-time SQL-generation error) raised from inside the scan UDF
