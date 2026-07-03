# Feature: Pushdown Planning

Translates an Exasol query against the virtual schema into a pushdown plan: it resolves the Iceberg data-file list once, captures the requested projection, filter, LIMIT, and any supported aggregate, extracts the table's current Iceberg schema for field-id-based projection, and emits the SQL that drives the DataFusion scan SET UDF — sharded across cluster nodes — over exactly those files. The scan-driving SQL passes the shard-invariant parts (projection, filter, LIMIT, logical schema, credentials, and the Iceberg table root) once as the UDF's common argument and each shard's per-file `(path, size)` subset as the per-shard argument. See `vs-adapter/pushdown-planning-file-encoding` for the table-root-once and relative/absolute path encoding rules.

## Background

* The data-file list, each file's byte size (from the Iceberg manifest), and the current Iceberg schema are resolved exactly once per pushdown, in the planning layer; the scan UDF never discovers files itself.
* The scan-driving SQL serializes the shard-invariant common spec once (projection, filter, LIMIT, aggregates, group keys, logical schema, EMITS types, credentials, tuning knobs, and the Iceberg table root) and carries only each shard's per-file `(path, size)` subset per shard.
* Credentials MUST NOT appear in any returned SQL string or error message, and MUST NOT be repeated per shard.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Adapter advertises aggregate pushdown for supported functions

* *GIVEN* an Exasol session that has installed the VS adapter script
* *WHEN* Exasol sends a `getCapabilities` request to the adapter
* *THEN* the capabilities list SHALL include single-group aggregate pushdown for `COUNT`/`COUNT(*)`/`SUM`/`MIN`/`MAX`/`AVG`, `AGGREGATE_GROUP_BY_COLUMN`/`AGGREGATE_GROUP_BY_EXPRESSION`/`AGGREGATE_GROUP_BY_TUPLE`/`AGGREGATE_HAVING`, the decomposable statistical aggregates `FN_AGG_STDDEV`/`FN_AGG_STDDEV_POP`/`FN_AGG_STDDEV_SAMP`/`FN_AGG_VARIANCE`/`FN_AGG_VAR_POP`/`FN_AGG_VAR_SAMP`, and (still) column projection, scalar select-list expressions, filter predicates, and LIMIT
* *AND* the adapter SHALL advertise `AGGREGATE_GROUP_BY_TUPLE` only because the grouped-aggregate detection and scan-driving SQL builder handle an arbitrary number of group keys (see `vs-adapter/pushdown-planning-grouped-agg`), so a GROUP BY over two or more keys is pushed down as node-local partial aggregation rather than falling back to a raw row scan that Exasol aggregates itself
* *AND* the capabilities list MUST NOT include `FN_AGG_COUNT_DISTINCT` (or any other `*_DISTINCT` aggregate), `FN_AGG_MEDIAN`, `FN_AGG_APPROXIMATE_COUNT_DISTINCT`, `FN_AGG_GROUP_CONCAT*`/`FN_AGG_LISTAGG`, or join pushdown
<!-- /DELTA:CHANGED -->
