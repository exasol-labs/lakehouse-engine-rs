# Feature: Tuning Resolution

Every pushdown resolves the resource budgets from the virtual schema properties Exasol sends with it: the parallelism factor, the DataFusion threading mode with its per-instance thread and partition counts, and the memory-pool parameters. Each per-shard scan UDF instance then gets a sized CPU and memory envelope, and the adapter persists none of these values.

## Background

* Exasol sends the virtual schema properties with every adapter request. The adapter parses them once per request into one typed value, so a `SET` of a tuning property applies from the next query on, with no `REFRESH`.
* `nr_of_cores` comes from `std::thread::available_parallelism()` alone, resolved once per request and defaulting to `1` when the platform cannot report it. No property overrides it. Each derivation takes the count as an argument, so a unit test can inject any count.
* The parallelism factor defaults to 1, one shard per node.
* `DATAFUSION_THREADING_MODE` (`AUTO` or `FIXED`, default `AUTO`) selects the threading configuration. `FIXED` uses `DATAFUSION_TARGET_PARTITIONS` and `DATAFUSION_THREADS_PER_UDF` as given, each falling back to its `AUTO` value. `AUTO` gives the node's single scan instance every core (`scan-runtime/scan-execution-threading`). Only the resolved integers reach the per-shard scan spec.
* `MEMORY_POOL_FRACTION` (default `0.6`) and `INSTANCE_OVERHEAD_MB` (default `200`) are independent properties. The adapter copies both into every per-shard scan spec. The scan UDF sizes its DataFusion pool to `fraction × (per_instance_limit − overhead_bytes)`.
* `METADATA_READ_CONCURRENCY` (default `16`) bounds the adapter's concurrent metadata reads.
* `vs-properties/property-value-validation` rejects an invalid value on `CREATE`, `REFRESH`, and `SET`. A pushdown reads a value it rejects as absent (`vs-properties/property-name-validation`).

## Scenarios

### Scenario: Adapter derives the per-node core count from available_parallelism() on every request

* *GIVEN* an Exasol session that has installed the VS adapter script
* *WHEN* Exasol sends a `pushdown` request
* *THEN* the adapter SHALL read the per-node core count from `std::thread::available_parallelism()` on the executing node, WITHOUT opening any connect-back session and WITHOUT reading any VS or connection property
* *AND* the adapter SHALL read that core count EXACTLY ONCE per request, at one call site, and pass the resolved value as a plain argument into the DataFusion-threading and connection-concurrency derivations
* *AND* the adapter MUST NOT persist the resolved core count

### Scenario: Adapter uses a core count of 1 when available_parallelism() cannot report one

* *GIVEN* an executing node whose platform cannot report a core count, so `std::thread::available_parallelism()` returns an error
* *WHEN* the adapter resolves the per-node core count
* *THEN* the adapter SHALL use a core count of `1`, and MUST NOT use a distinct unknown sentinel value such as `0`
* *AND* every budget derived from that core count SHALL equal the budget a genuine single-core node produces

### Scenario: The adapter detects the node's CPU affinity set

* *GIVEN* an Exasol node whose CPU affinity set holds `k` CPUs, with `2 ≤ k <` the host's core count
* *AND* a virtual schema created with no `DATAFUSION_THREADING_MODE` property
* *WHEN* a pushdown builds the scan spec
* *THEN* its `df_threads_per_udf` SHALL equal `k`, not the fallback of `1` or the host's core count

### Scenario: Pushdown resolves every tuning value from the properties

* *GIVEN* a virtual schema whose properties set any of `PARALLELISM_FACTOR`, `DATAFUSION_THREADING_MODE`, `DATAFUSION_TARGET_PARTITIONS`, `DATAFUSION_THREADS_PER_UDF`, `DATAFUSION_BATCH_SIZE`, `MEMORY_POOL_FRACTION`, `INSTANCE_OVERHEAD_MB`, `S3_MAX_CONNECTIONS`, and `JOIN_BROADCAST_MAX_BYTES`
* *WHEN* the adapter plans a pushdown
* *THEN* each per-shard scan spec SHALL carry the value each property resolves to, and each absent property's default: a parallelism factor of `1`, the `AUTO` threading mode, a batch size of `8192`, a memory-pool fraction of `0.6`, and `200` overhead megabytes
* *AND* in `FIXED` mode the target partition and threads-per-UDF counts SHALL be the supplied values, each falling back to its `AUTO` value when absent, and in `AUTO` mode they SHALL be `2 × nr_of_cores` and `nr_of_cores`, whatever the supplied values
* *AND* the adapter MUST NOT record any of these values in `adapterNotes`

### Scenario: A SET of a tuning property applies from the next query

* *GIVEN* a virtual schema and an `ALTER VIRTUAL SCHEMA ... SET` that changes or unsets a tuning property
* *WHEN* a user queries one of its virtual tables after the `SET`
* *THEN* the pushdown SHALL plan with the new value, or with the default of an unset property, without a `REFRESH`

### Scenario: The parallelism factor drives work-unit sharding

* *GIVEN* a virtual schema whose resolved parallelism factor is P
* *WHEN* the adapter plans a pushdown
* *THEN* the adapter SHALL multiply P by the node count from `UdfContext::node_count()` to give the shard count `G`, capped at 300 (`pushdown/pushdown-planning`)

### Scenario: Planning keeps at most METADATA_READ_CONCURRENCY metadata reads in flight

* *GIVEN* a virtual schema whose resolved metadata read concurrency is N
* *WHEN* the adapter reads Iceberg manifests at pushdown, loads catalog tables or Glue metadata during listing, or reads direct-storage Parquet footers
* *THEN* the adapter SHALL keep at most N of those reads in flight at once, whatever the core count of the adapter VM
* *AND* the scan UDF MUST NOT read N

### Scenario: The memory budget controls reach the scan spec

* *GIVEN* a virtual schema whose memory-pool fraction and instance-overhead megabytes resolve to F and O
* *WHEN* the adapter builds each per-shard scan spec
* *THEN* every per-shard scan spec SHALL carry F and O
* *AND* a scan spec that lacks these fields SHALL deserialize to the default fraction `0.6` and default overhead `200` megabytes
