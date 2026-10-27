# Feature: Pushdown Planning

Translates an Exasol query against the virtual schema into a pushdown plan. The plan captures the requested projection, filter and LIMIT, and emits the SQL that drives the DataFusion scan over the table identity, file list and logical schema resolved by `file-planning/pushdown-planning-file-resolution`.

## Background

* The scan-driving SQL invokes the `LAKEHOUSE_SCAN` scalar EMIT UDF over a nested `LAKEHOUSE_DISTRIBUTE_FILES` distributor subquery. The shard-invariant common spec is spliced once as the scan's first argument, and each shard's file subset flows through the distributor as the second argument. A single-shard plan calls the scalar scan directly. See `parallelism/work-unit-sharding`.
* The outer scalar scan select is never wrapped in a `SELECT * FROM (...)` materialization boundary. That boundary exists only on the declined-filter path.
* The common spec carries a CONNECTION name or sealed envelope, never a plaintext credential: see `storage-access/scan-spec-credential-reference`.
* The adapter schema-qualifies the UDF names from `ctx.script_schema()`. No VS property supplies the schema.
* The adapter reads the node count from `UdfContext::node_count()` per pushdown, not from `adapterNotes`. It captures the value in `dispatch` before the tokio runtime is entered and passes it to planning as a plain integer. `node_count()` returns `0` only without a live handshake, and `0` maps to a node count of `1`.
* The common spec's `projection` field carries the projected columns only for the row-scan and join paths. An aggregate or GROUP BY request leaves it empty: see `pushdown-aggregates/pushdown-planning-single-group-agg` and `pushdown-grouped-agg/pushdown-planning-grouped-agg`.
* Exasol does not re-apply a predicate whose capability the adapter advertised, so the adapter's own SQL applies any predicate it cannot translate. See `pushdown/pushdown-declined-filter-self-apply`. An absent or trivially-true filter is omitted and the wrapper-free scan is unchanged. A declined filter routes to the qualified single-table wrapper, which applies the predicate in its `WHERE`.
* The type-aware LIKE rule is in `pushdown-types/pushdown-planning-like-type-coercion`.
* The plan-time file-pruning predicate has a per-format owner. For Iceberg it is `iceberg::expr::Predicate`, owned by `file-planning/pushdown-file-pruning`. For Delta it is the Delta stats predicate, owned by `delta/delta-file-pruning`. Scenario clauses naming `iceberg::expr::Predicate` describe the Iceberg arm.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Scan-driving UDF invocations are schema-qualified from the running adapter script's schema

* *GIVEN* a virtual schema whose adapter script, `LAKEHOUSE_SCAN` scan script, and `LAKEHOUSE_DISTRIBUTE_FILES` distributor are all deployed in one Exasol schema
* *WHEN* Exasol sends a `pushdown` request and the adapter builds the scan-driving SQL
* *THEN* the adapter SHALL qualify the `LAKEHOUSE_SCAN` and `LAKEHOUSE_DISTRIBUTE_FILES` UDF names with the schema reported by the running adapter script's UDF handshake (`ctx.script_schema()`), and MUST NOT read any VS property to obtain that schema
* *AND* because those scripts are co-deployed in the adapter script's schema, the qualified names SHALL resolve when the scan-driving SQL executes outside the adapter script's own schema context
* *AND* when `ctx.script_schema()` reports an empty schema, the adapter SHALL emit the `LAKEHOUSE_SCAN` and `LAKEHOUSE_DISTRIBUTE_FILES` UDF names unqualified, relying on the session's current schema to resolve them
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: Pushdown reads the cluster node count from the UDF handshake

* *GIVEN* a virtual schema over an Iceberg table whose persisted `adapterNotes` carry no node-count entry
* *AND* a live UDF handshake on the running adapter script reporting `UdfContext::node_count()` as N where N is at least 1
* *WHEN* Exasol sends a `pushdown` request against that virtual schema
* *THEN* the adapter SHALL use N as the node count in the shard count `G = node_count × PARALLELISM_FACTOR` (see `parallelism/work-unit-sharding`)
* *AND* the adapter MUST NOT read the node count from `schemaMetadataInfo.adapterNotes`, and MUST NOT open a connect-back session or issue `SELECT NPROC()` to obtain it
<!-- /DELTA:CHANGED -->
