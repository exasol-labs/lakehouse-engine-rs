# Feature: DataFusion Scan Execution — Object-Store Connection Concurrency

Controls how many concurrent connections each scan UDF instance holds open to
the S3-compatible object store, so that a node's network / IO bandwidth is
saturated when data-file fetching — not CPU — is the throughput bottleneck.
The budget is a single operator-facing knob (mirroring Exasol's native
`IMPORT FROM PARQUET` `MaxConnections` parameter): an explicit positive value
pins it, otherwise the adapter derives a per-instance budget from the node's
capacity. Configuration is round-tripped from a VS property through
`adapterNotes` and the shard-invariant common spec argument to the scan UDF,
which applies it to the object store's HTTP client — an axis independent of the
CPU thread/partition budget of `datafusion-scan/scan-execution-threading`.

## Background

* The `ScanSpec` carries a shard-invariant `s3_max_connections` field (a
  positive integer connection-concurrency budget) that defaults to a
  conservative built-in value when absent from the JSON, so pre-existing scan
  specs remain backward-compatible.
* Data-file fetching from S3 is a distinct throughput axis from CPU work: the
  DataFusion thread/partition budget (`datafusion-scan/scan-execution-threading`)
  governs decode/compute concurrency, while this budget governs how many
  concurrent HTTP connections to the object store the instance may keep warm.
  With one DataFusion instance per node (`PARALLELISM_FACTOR=1` → `G = node_count`),
  a serial or under-concurrent fetch path can leave the node's network idle even
  when all cores are busy; raising the connection budget lets a single instance
  pull many byte ranges / files in parallel.
* The budget is applied to the object store's HTTP client options when the scan
  UDF builds its S3 store, on both the raw-row scan path and the partial-aggregate
  path (both decode source Parquet fetched over the same object store).
* The budget is resolved in the adapter at `createVirtualSchema` time from an
  `S3_MAX_CONNECTIONS` VS/connection property and recorded in `adapterNotes`. An
  explicit positive integer is used verbatim; an absent, empty, zero, or invalid
  value triggers an AUTO derivation from the node's core count and its per-node
  UDF-instance share (mirroring the AUTO thread-budget derivation in
  `datafusion-scan/scan-execution-threading`). Only the resolved integer reaches
  the scan UDF; the UDF stays resolution-agnostic.
* The budget travels in the shard-invariant common spec argument, serialized once
  for the whole work-unit shard fan-out (see `parallelism/work-unit-sharding`),
  never repeated per shard.
* Whether raising connection concurrency actually closes the gap to the native
  `IMPORT FROM PARQUET` throughput ceiling is an open empirical question answered
  by benchmark sweeps, not by this spec. This spec only guarantees the budget is
  selectable, correctly derived, round-tripped, and applied to the object store.
* See `datafusion-scan/scan-execution-memory-and-credentials` for how the S3
  object store is built from vended credentials, and
  `vs-adapter/create-virtual-schema-adapter-notes` for how the property is
  recorded in `adapterNotes`.

## Scenarios

### Scenario: Scan configures its object store from the resolved connection budget

* *GIVEN* a scan spec whose `s3_max_connections` field is a positive integer N
* *WHEN* the scan UDF builds its S3 object store for the files listed in its per-shard argument
* *THEN* the UDF SHALL configure the object store's HTTP client options with a connection-concurrency budget of N, so up to N concurrent connections to the object store are held warm per host rather than leaving the client at its default pooling behaviour
* *AND* the budget SHALL apply on both the raw-row scan path and the partial-aggregate path, since both decode Parquet fetched over the same object store
* *AND* a credential value MUST NOT appear in any error the UDF surfaces while building the client options

### Scenario: Scan falls back to a built-in default budget when the field is absent

* *GIVEN* a scan spec whose JSON omits the `s3_max_connections` field
* *WHEN* the scan UDF deserializes the spec and builds its S3 object store
* *THEN* the UDF SHALL use a conservative built-in default connection-concurrency budget, clamped to at least 1
* *AND* the scan SHALL otherwise execute identically to the explicit-value path

### Scenario: FIXED value overrides the AUTO derivation at createVirtualSchema

* *GIVEN* a `createVirtualSchema` request whose `S3_MAX_CONNECTIONS` property is a positive integer M
* *WHEN* the adapter resolves the connection-concurrency budget
* *THEN* the adapter SHALL record M as the connection-concurrency budget, without applying the AUTO derivation
* *AND* the adapter SHALL record the resolved value in the `createVirtualSchema` `adapterNotes` so the per-shard scan spec carries an integer field the scan UDF consumes unchanged

### Scenario: AUTO derivation sizes the per-instance budget from node capacity

* *GIVEN* a `createVirtualSchema` request that supplies no positive-integer `S3_MAX_CONNECTIONS` property (absent, empty, zero, or invalid)
* *AND* a resolved per-node core count greater than 0 and a per-node UDF-instance share derived from the work-unit shard fan-out
* *WHEN* the adapter resolves the connection-concurrency budget
* *THEN* the adapter SHALL derive a per-instance connection-concurrency budget from the core count and the per-node UDF-instance share, mirroring the AUTO thread-budget derivation, so the budget scales with a node's capacity and the per-node instance share without collapsing below 1
* *AND* the adapter SHALL record the derived value in `adapterNotes`

### Scenario: AUTO derivation falls back to the default budget when the core count is unknown

* *GIVEN* a `createVirtualSchema` request that supplies no positive-integer `S3_MAX_CONNECTIONS` property
* *AND* a resolved per-node core count of 0 (the unknown / unavailable sentinel)
* *WHEN* the adapter resolves the connection-concurrency budget
* *THEN* the adapter SHALL fall back to the conservative built-in default budget rather than producing a zero or negative budget
* *AND* the adapter SHALL still return a successful `createVirtualSchema` response

### Scenario: Connection budget travels once in the shard-invariant common spec

* *GIVEN* a scan-driving query fanned across more than one work-unit shard
* *WHEN* the adapter serializes the scan-driving SQL arguments
* *THEN* the resolved connection-concurrency budget SHALL travel in the shard-invariant common spec argument, serialized EXACTLY ONCE for the whole fan-out, and MUST NOT be repeated in any per-shard argument
* *AND* the `ScanSpec` reconstituted for every shard SHALL carry the same connection-concurrency budget
