# Feature: Pushdown Planning

Translates an Exasol query against the virtual schema into a pushdown plan: it resolves the Iceberg data-file list once, captures the requested projection, filter, LIMIT, and any supported aggregate, extracts the table's current Iceberg schema for field-id-based projection, and emits the SQL that drives the DataFusion scan SET UDF — sharded across cluster nodes — over exactly those files. The scan-driving SQL passes the shard-invariant parts (projection, filter, LIMIT, logical schema, credentials, and the Iceberg table root) once as the UDF's common argument and each shard's per-file `(path, size)` subset as the per-shard argument. See `vs-adapter/pushdown-planning-file-encoding` for the table-root-once and relative/absolute path encoding rules. See `vs-adapter/pushdown-planning-nested-aggregate-fallback` for the guard against composed requests (e.g. an outer aggregate over an inner grouped-aggregate sub-select) that don't map onto the source table's own columns.

## Background

* The data-file list, each file's byte size (from the Iceberg manifest), and the current Iceberg schema are resolved exactly once per pushdown, in the planning layer; the scan UDF never discovers files itself.
* The logical schema carried into the common scan-spec argument identifies each column by its Iceberg field-id, current name, Arrow type, and nullability.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Adapter advertises aggregate pushdown for supported functions

* *GIVEN* an Exasol session that has installed the VS adapter script
* *WHEN* Exasol sends a `getCapabilities` request to the adapter
* *THEN* the capabilities list SHALL include single-group aggregate pushdown for `COUNT`/`COUNT(*)`/`SUM`/`MIN`/`MAX`/`AVG`, `AGGREGATE_GROUP_BY_COLUMN`/`AGGREGATE_GROUP_BY_EXPRESSION`/`AGGREGATE_GROUP_BY_TUPLE`/`AGGREGATE_HAVING`, the decomposable statistical aggregates `FN_AGG_STDDEV`/`FN_AGG_STDDEV_POP`/`FN_AGG_STDDEV_SAMP`/`FN_AGG_VARIANCE`/`FN_AGG_VAR_POP`/`FN_AGG_VAR_SAMP`, single-group `FN_AGG_COUNT_DISTINCT`, and (still) column projection, scalar select-list expressions, filter predicates, and LIMIT
* *AND* the adapter SHALL advertise `AGGREGATE_GROUP_BY_TUPLE` only because the grouped-aggregate detection and scan-driving SQL builder handle an arbitrary number of group keys (see `vs-adapter/pushdown-planning-grouped-agg`), so a GROUP BY over two or more keys is pushed down as node-local partial aggregation rather than falling back to a raw row scan that Exasol aggregates itself
* *AND* the adapter SHALL advertise `FN_AGG_COUNT_DISTINCT` because a single-group `COUNT(DISTINCT col)` is decomposed via per-shard local distinct sets merged by a scalar merge UDF (see `vs-adapter/pushdown-planning-count-distinct`); a `COUNT(DISTINCT ...)` inside a GROUP BY request still falls back to row scanning
* *AND* the adapter SHALL advertise the inner equi-join capabilities `JOIN`/`JOIN_TYPE_INNER`/`JOIN_CONDITION_EQUI` (see `vs-adapter/pushdown-planning-join`), while the outer/all-condition join capabilities and any Cartesian-product capability remain absent
* *AND* the capabilities list MUST NOT include `FN_AGG_MEDIAN`, `FN_AGG_APPROXIMATE_COUNT_DISTINCT`, `FN_AGG_GROUP_CONCAT*`/`FN_AGG_LISTAGG`, `JOIN_TYPE_LEFT_OUTER`/`JOIN_TYPE_RIGHT_OUTER`/`JOIN_TYPE_FULL_OUTER`, or `JOIN_CONDITION_ALL`
<!-- /DELTA:CHANGED -->
