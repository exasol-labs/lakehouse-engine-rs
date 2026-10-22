# Feature: Pushdown Planning, Nested Aggregate Fallback

The adapter falls back to row scanning for a composed pushdown request, such as an outer aggregate over an inner grouped-aggregate sub-select, whose nodes do not all resolve to columns of the source table.

## Background

* A `selectList`, `groupBy`, or `filter` node that does not map onto a column of the involved table's resolved logical schema must not appear in scan-driving SQL.
* A row-scan ScanSpec has no aggregates field. Exasol then applies the outer computation on the returned rows.

## Scenarios

### Scenario: Composed pushdown request never renders a scan spec that references a non-source column

* *GIVEN* a virtual schema over an Iceberg table backed by SeaweedFS
* *AND* a user query whose pushdown request composes an outer aggregate over an inner grouped-aggregate sub-select, e.g. `SELECT COUNT(*) FROM (SELECT L_ORDERKEY, COUNT(*) AS cnt FROM {vs_table} GROUP BY L_ORDERKEY) t`, so that some `selectList`, `groupBy`, or `filter` node does not resolve to a plain column of the involved source table's current Iceberg schema
* *WHEN* Exasol sends the corresponding `pushdown` request and the adapter builds the scan spec and the scan-driving SQL
* *THEN* every column reference in the per-shard scan-driving SQL the adapter emits SHALL name a column present in the involved table's resolved logical schema (or an aggregate/group-key expression rendered from one), and the adapter MUST NOT emit a scan spec whose rendered SQL references a phantom identifier such as `NULL` that is absent from the source schema
* *AND* when the adapter cannot map every `selectList`/`groupBy` node of a composed request onto a supported single-group aggregate, grouped aggregate, or projection over the source table's own columns, it SHALL fall back to row scanning (emitting a row-scan ScanSpec with no aggregates field) so Exasol applies the outer computation on the returned rows using its own engine
* *AND* the query MUST NOT fail with a DataFusion `Schema error: No field named ...` (or any planning-time SQL-generation error) raised from inside the scan UDF
* *AND* the query SHALL return the same result a single-node evaluation of the composed query returns, so the outer `COUNT(*)` of the example equals the number of distinct inner group-key values
