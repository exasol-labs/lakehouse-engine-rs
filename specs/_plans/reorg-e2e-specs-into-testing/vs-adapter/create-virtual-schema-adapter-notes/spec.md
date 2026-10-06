# Feature: Create Virtual Schema — AdapterNotes

Records the resource budgets, the Exasol-name to Iceberg-identifier map, and the skipped-table list in the `createVirtualSchema` response `adapterNotes`. Later pushdowns read the budgets and the map back to bound each scan UDF instance's CPU and memory usage and to recover the scanned Iceberg table from the involved virtual table name. `adapterNotes` carries only values a pushdown cannot recompute, plus the skipped-table list, which no pushdown reads. Two counts are excluded. Every pushdown reads the cluster node count from its own UDF handshake. No pushdown reads the per-node core count at all, so the adapter uses it as a derivation input and discards it.

<!-- DELTA:CHANGED -->
## Background

* The active cluster node count is NOT recorded in `adapterNotes`. It is UDF handshake
  metadata that every VS request already carries, so each `pushdown` reads it directly
  from `UdfContext::node_count()` instead of from a persisted note (see
  `pushdown/pushdown-planning`). `adapterNotes` is reserved for values derived at
  create time that a pushdown cannot recompute, such as `TABLE_MAP`, and for one
  diagnostic entry.
* `SKIPPED_TABLES` is that one write-only diagnostic entry. It records the tables the
  listing skipped, so a user reads each skip reason from
  `SYS.EXA_ALL_VIRTUAL_SCHEMAS.ADAPTER_NOTES`. No pushdown reads it.
* Exasol rejects an `adapterNotes` value longer than 2,000,000 characters, the declared
  `ADAPTER_NOTES` size, with sqlCode `04000` on both CREATE and REFRESH, and keeps the
  previous stored value (measured live on Exasol 2025.1.16). The adapter therefore caps
  `SKIPPED_TABLES` by serialized byte length and records the dropped count as
  `SKIPPED_TABLES_OMITTED`. `TABLE_MAP` is not capped.
* The per-node core count is NOT recorded in `adapterNotes` either, and for a stronger
  reason than the node count: no pushdown reads it back. The adapter resolves it,
  feeds it into the parallelism-factor, DataFusion-threading, and connection-concurrency
  derivations, and discards it. Only those derived entries round-trip.
* The adapter needs no migration mechanism for an `adapterNotes` entry it does not
  write itself. An entry persisted by another adapter version, such as `CLUSTER_NODES`
  or `NR_OF_CORES`, survives the merge like any other foreign key, unread and inert.
  An operator who wants such an entry gone drops and recreates the virtual schema,
  because no in-place migration path exists.
* The per-node core count is read directly on the executing node via
  `std::thread::available_parallelism()`, once per request, at the single call site
  that resolves it. This is the same host-core-count source the scan UDF already
  trusts for DataFusion `target_partitions` (see
  `scan-runtime/scan-execution-threading`).
* `std::thread::available_parallelism()` returns an `io::Error` on a platform that
  cannot report a core count. The adapter then uses a core count of `1`. There is no
  distinct unknown sentinel: every derivation the core count feeds already floors its
  own result, so the unavailable case and a genuine single-core node produce identical
  budgets.
* No VS or connection property configures the core count. The adapter reads
  properties by name and ignores every name it does not know, so an unknown name in a
  `createVirtualSchema` statement is accepted and has no effect. An operator who needs
  a smaller effective core count constrains the executing node's CPU affinity, which
  `std::thread::available_parallelism()` honours. The Docker scenario below verifies
  that the adapter VM reads that affinity set.
* A CPU limit expressed only as a CFS bandwidth quota, such as Docker `cpus:` or a
  Kubernetes `limits.cpu` without the static CPU manager policy, is not visible to the
  adapter. The Exasol UDF sandbox mounts no cgroup filesystem, so no UDF reads the
  quota, and `std::thread::available_parallelism()` reports the affinity count. A node
  limited that way receives budgets sized for more CPU than it may use. This is a
  deliberate limitation tracked in (#421), not an unrecorded gap. An affinity-based
  limit (Docker `cpuset`, the Kubernetes static CPU manager policy, or ordinary bare
  metal) is detected exactly.
* No topology value uses a connect-back session, at create time or at pushdown time;
  the adapter opens no read-only SQL session for topology discovery, issues no
  `SELECT NPROC()` or `SELECT PARAM_VALUE(...)`, and honours no `CONNECTION_NAME` VS
  property for this purpose. `CONNECTION_NAME` is no longer a supported VS property.
* The parallelism factor is supplied as a VS/connection property and recorded in
  `adapterNotes`; when absent it defaults to a hardware-aware value derived from the
  resolved core count.
* The DataFusion threading configuration is selected by a `DATAFUSION_THREADING_MODE`
  VS/connection property (`AUTO` or `FIXED`, default `AUTO`) and recorded in
  `adapterNotes`. In `FIXED` mode the two independent properties
  `DATAFUSION_TARGET_PARTITIONS` and `DATAFUSION_THREADS_PER_UDF` are used verbatim
  (each defaulting to `max(nr_of_cores, 1)`); in `AUTO` mode the adapter derives a
  per-instance thread budget that does not oversubscribe a node (see
  `scan-runtime/scan-execution-threading`). Whichever mode is selected, only the
  resolved integer `DATAFUSION_TARGET_PARTITIONS` / `DATAFUSION_THREADS_PER_UDF`
  values are round-tripped into the per-shard scan spec.
* The per-instance memory budget is two independent VS/connection properties —
  `MEMORY_POOL_FRACTION` (default `0.6`) and `INSTANCE_OVERHEAD_MB` (default `200`) —
  each recorded in `adapterNotes` and round-tripped into every per-shard scan spec,
  where the scan UDF sizes its DataFusion pool to
  `fraction × (per_instance_limit − overhead_bytes)`.
* The adapter holds no state between requests; all values are resolved per request
  and returned in the `createVirtualSchema` response's `adapterNotes` (stringified JSON),
  which Exasol persists and round-trips back at pushdown time.
  The adapter MUST NOT use `schemaMetadata.properties` for this purpose, as Exasol
  2025.2.1 silently drops adapter-returned properties. The `adapterNotes` channel is
  queryable via `SYS.EXA_ALL_VIRTUAL_SCHEMAS.ADAPTER_NOTES`.
* See `vs-adapter/create-virtual-schema-adapter-notes-resources` for the resource
  configuration scenarios (parallelism factor, DataFusion threading, memory budget).
<!-- /DELTA:CHANGED -->

## Scenarios

### Scenario: createVirtualSchema adapterNotes omit the cluster node count

* *GIVEN* an Exasol session that has installed the VS adapter script, with the catalog and storage connection properties supplied
* *WHEN* Exasol sends a `createVirtualSchema`, `refresh`, or `setProperties` request naming an Iceberg table
* *THEN* the `adapterNotes` of the returned response MUST NOT carry a `CLUSTER_NODES` entry
* *AND* the `adapterNotes` MUST NOT carry an `NR_OF_CORES` entry either, because no pushdown reads the per-node core count back
* *AND* the `adapterNotes` SHALL still carry `PARALLELISM_FACTOR` and `TABLE_MAP`, so omitting both counts does not disturb any other recorded entry
