# Feature: Create Virtual Schema — AdapterNotes

Records the resource budgets and the Exasol-name to Iceberg-identifier map in the `createVirtualSchema` response `adapterNotes`. Later pushdowns read these back to bound each scan UDF instance's CPU and memory usage and to recover the scanned Iceberg table from the involved virtual table name. `adapterNotes` carries only values a pushdown cannot recompute. Two counts are excluded. Every pushdown reads the cluster node count from its own UDF handshake. No pushdown reads the per-node core count at all, so the adapter uses it as a derivation input and discards it.

## Background

* The active cluster node count is NOT recorded in `adapterNotes`. It is UDF handshake
  metadata that every VS request already carries, so each `pushdown` reads it directly
  from `UdfContext::node_count()` instead of from a persisted note (see
  `vs-adapter/pushdown-planning`). `adapterNotes` is reserved for values derived at
  create time that a pushdown cannot recompute, such as `TABLE_MAP`.
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
  `datafusion-scan/scan-execution-threading`).
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
  `datafusion-scan/scan-execution-threading`). Whichever mode is selected, only the
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

## Scenarios

### Scenario: createVirtualSchema adapterNotes omit the cluster node count

* *GIVEN* an Exasol session that has installed the VS adapter script, with the catalog and storage connection properties supplied
* *WHEN* Exasol sends a `createVirtualSchema`, `refresh`, or `setProperties` request naming an Iceberg table
* *THEN* the `adapterNotes` of the returned response MUST NOT carry a `CLUSTER_NODES` entry
* *AND* the `adapterNotes` MUST NOT carry an `NR_OF_CORES` entry either, because no pushdown reads the per-node core count back
* *AND* the `adapterNotes` SHALL still carry `PARALLELISM_FACTOR` and `TABLE_MAP`, so omitting both counts does not disturb any other recorded entry

### Scenario: Adapter derives the per-node core count from available_parallelism() on every request

* *GIVEN* an Exasol session that has installed the VS adapter script and supplies the catalog and storage connection properties
* *WHEN* Exasol sends a `createVirtualSchema` request naming an Iceberg table
* *THEN* the adapter SHALL read the per-node core count from `std::thread::available_parallelism()` on the executing node, WITHOUT opening any connect-back session and WITHOUT reading any VS or connection property
* *AND* the adapter SHALL read that core count EXACTLY ONCE per request, at one call site, and pass the resolved value as a plain argument into the parallelism-factor, DataFusion-threading, and connection-concurrency derivations
* *AND* the adapter MUST NOT write the resolved core count into the response `adapterNotes`, and MUST NOT persist it anywhere else
* *AND* the adapter SHALL accept a property name it does not recognize and SHALL resolve every budget to the same value it resolves without that name, so no property reaches the core count

### Scenario: Adapter uses a core count of 1 when available_parallelism() cannot report one

* *GIVEN* an executing node whose platform cannot report a core count, so `std::thread::available_parallelism()` returns an error
* *WHEN* the adapter resolves the per-node core count for a `createVirtualSchema` request
* *THEN* the adapter SHALL use a core count of `1`, and MUST NOT use a distinct unknown sentinel value such as `0`
* *AND* the adapter SHALL return a successful `createVirtualSchema` response
* *AND* every budget derived from that core count SHALL equal the budget a genuine single-core node produces

### Scenario: The adapter VM detects the Docker container's CPU affinity set

* *GIVEN* the local Docker Exasol container constrained to a CPU affinity set, read back from the running container's own `cpuset.cpus.effective` rather than from the environment of the process that launched it
* *AND* a test host whose `std::thread::available_parallelism()` is strictly greater than the number of CPUs in that set, without which the assertion below holds equally for an adapter reading the unconstrained host count and therefore evidences nothing
* *AND* a virtual schema created with `PARALLELISM_FACTOR = '1'` and no `DATAFUSION_THREADING_MODE` property, so the AUTO derivation divides the detected core count by a per-node instance share of `1` and records it unchanged as `DF_THREADS_PER_UDF`
* *WHEN* the test reads the `DF_THREADS_PER_UDF` entry from that schema's `SYS.EXA_ALL_VIRTUAL_SCHEMAS.ADAPTER_NOTES` row
* *THEN* the recorded value SHALL equal the number of CPUs in the affinity set read back from the container, which evidences that the adapter VM reads the container's affinity set rather than the unavailable fallback of `1` or the host's unconstrained core count
* *AND* the test MUST FAIL rather than skip when the value differs, because auto-detection is the only source of the core count and a skipped check would leave that source unevidenced, and MUST FAIL naming the unmet precondition rather than report a pass when the container's affinity set is not strictly smaller than the test host's core count, so a non-discriminating configuration records no false evidence
