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
the distributor and calls the scalar scan directly on the file-list literal. See
`vs-adapter/pushdown-planning-file-encoding` for the table-root-once and relative/absolute
path encoding rules. See `vs-adapter/pushdown-planning-nested-aggregate-fallback` for the
guard against composed requests (e.g. an outer aggregate over an inner grouped-aggregate
sub-select) that don't map onto the source table's own columns. This feature also extends
the resolve-once seam to associate each data file's positional-delete files and carry them
minimally in the per-shard argument. Single-group aggregate pushdown (capability
advertisement, partial-aggregate scan-spec translation, wrapper merge SQL, and AVG
sum/count decomposition) is covered separately in
`vs-adapter/pushdown-planning-single-group-agg`.

## Background

* The data-file list, each file's byte size (from the Iceberg manifest), and the current Iceberg schema are resolved exactly once per pushdown, in the planning layer; the scan UDF never discovers files itself.
* The logical schema carried into the common scan-spec argument identifies each column by its Iceberg field-id, current name, Arrow type, and nullability.
* The scan-driving SQL invokes the `LAKEHOUSE_SCAN` SCALAR EMIT UDF over a nested `LAKEHOUSE_DISTRIBUTE_FILES` distributor subquery; the shard-invariant common spec (projection, filter, LIMIT, aggregates, group keys, logical schema, EMITS types, credentials, tuning knobs, and the Iceberg table root) is spliced once as the scalar scan's first argument and each shard's file subset flows through the distributor as the second argument.
* The outer scalar scan select is never wrapped in a `SELECT * FROM (...)` materialization boundary.
* Each per-shard file entry carries both the file path and its byte size, so the scan UDF never re-discovers a size the adapter already resolved.
* Credentials MUST NOT appear in any returned SQL string or error message, and MUST NOT be repeated per shard.
* The data-file list, each file's byte size, and each file's associated positional-delete files are resolved exactly once, at the same seam; the scan UDF never discovers files or delete files.
* Delete support keeps the wire surface minimal — per-file delete references only, with no serialized Iceberg schema and no bound predicate added to the spec.
* The `LAKEHOUSE_SCAN` and `LAKEHOUSE_DISTRIBUTE_FILES` UDF names in the scan-driving SQL are schema-qualified from the schema of the running adapter script, read from the UDF handshake via `ctx.script_schema()`; there is no VS property that supplies this schema. The scan and distributor scripts are co-deployed in the adapter script's schema, so this single source qualifies both.
<!-- DELTA:NEW -->
* The cluster node count that sizes the shard fan-out is read per pushdown from the
  adapter script's own UDF handshake via `UdfContext::node_count()`. It is NOT read
  from `schemaMetadataInfo.adapterNotes`. Every VS request type reaches the adapter
  through the same single-call script invocation, so the handshake carries the node
  count on a `pushdown` request exactly as it does on a `createVirtualSchema` request;
  the request type lives in the JSON payload, not in the handshake.
* `node_count()` is a synchronous handshake read and MUST be captured in `dispatch`
  before the tokio runtime is entered, alongside `ctx.script_schema()` and the
  resolved CONNECTION credentials. The value is then threaded into the pushdown
  planning path as a plain integer, so the async planning code performs no ambient
  context read of its own.
* `node_count()` returns `0` only on a context carrying no live handshake metadata (a
  stub, a test double, or a broken handshake), so `0` maps to a node count of `1` and
  any live cluster (single-node included) reports `≥ 1`.
<!-- /DELTA:NEW -->
* The common spec's `projection` field carries the pushed-down projected columns ONLY for the row-scan and join dispatch paths. An aggregate or GROUP BY request leaves `projection` empty, because the aggregate scan-dispatch path derives its physical projection from the `aggregates`/`group_keys` fields rather than from `projection` (see `vs-adapter/pushdown-planning-single-group-agg` and `vs-adapter/pushdown-planning-grouped-agg`).
* See `vs-adapter/pushdown-planning-single-group-agg` for single-group aggregate pushdown (capability advertisement, partial-aggregate translation, wrapper merge SQL, and AVG decomposition).
* A predicate node the adapter cannot faithfully translate is OMITTED from the scan spec; Exasol keeps and evaluates the predicate itself as a correctness backstop.
* See `vs-adapter/pushdown-planning-like-type-coercion` for the type-aware LIKE/REGEXP_LIKE rule that dispatches on the subject column's Exasol type before rendering the filter.
* When a query aliases a table in its `FROM` clause (`FROM customer c`), Exasol stamps a `tableAlias` on every `column` node in the pushdown request — including nodes the user wrote unqualified. The single scan relation exposes only bare column names, so an alias-qualified reference does not resolve; the single-table push therefore strips the alias before rendering (see `vs-adapter/pushdown-planning-alias-stripping`). The `crates/vs-expression` translator itself always honors a present `tableAlias` (`sql-comprehension/vs-expression-translator`); stripping is the single-table caller's responsibility.

## Scenarios

<!-- DELTA:NEW -->
### Scenario: Pushdown reads the cluster node count from the UDF handshake

* *GIVEN* a virtual schema over an Iceberg table whose persisted `adapterNotes` carry NO `CLUSTER_NODES` entry
* *AND* a live UDF handshake on the running adapter script reporting `UdfContext::node_count()` as N where N is at least 1
* *WHEN* Exasol sends a `pushdown` request against that virtual schema
* *THEN* the adapter SHALL use N as the node count in the shard count `G = node_count × PARALLELISM_FACTOR` (see `parallelism/work-unit-sharding`)
* *AND* the adapter MUST NOT read the node count from `schemaMetadataInfo.adapterNotes`, and MUST NOT open a connect-back session or issue `SELECT NPROC()` to obtain it
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: Pushdown node count falls back to one when the handshake reports none

* *GIVEN* a `pushdown` request whose context reports `UdfContext::node_count()` as `0` (no live handshake node count)
* *WHEN* the adapter resolves the node count for that request
* *THEN* the adapter SHALL use a node count of `1`
* *AND* the resulting shard count MUST be identical to the shard count the pre-refactor path produced from an absent or unparseable `CLUSTER_NODES` adapterNote
* *AND* the adapter SHALL still return a successful `pushdown` response
<!-- /DELTA:NEW -->
