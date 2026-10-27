# Feature: DataFusion Scan Execution, Object-Store Connection Concurrency

Each scan UDF instance holds a bounded number of connections open to the S3-compatible object store. An `S3_MAX_CONNECTIONS` property pins the budget. When the property is absent, the adapter derives the budget from the resolved thread count. Either way the adapter passes it to the UDF through `adapterNotes` and the common spec.

## Background

* The budget is a different axis from the CPU thread and partition budget of `scan-runtime/scan-execution-threading`. It sets how many HTTP connections to the object store the instance keeps warm.
* The adapter resolves the budget at `createVirtualSchema` time from the `S3_MAX_CONNECTIONS` property, whose value `vs-properties/property-value-validation` checks first. The adapter records the result in `adapterNotes`, and only the integer reaches the UDF, in the shard-invariant common spec (`parallelism/work-unit-sharding`).
* The budget maps to the `object_store` option `pool_max_idle_per_host`, which bounds warm idle connections. `object_store` exposes no hard in-flight ceiling, so the bound is on sockets and file descriptors, not request rate.
* The row-position delete pipeline (`scan-read-path/scan-execution-row-position-deletes`) issues object-store reads in two phases. Phase A reads every delete payload the shard needs: the footer and then each surviving row group of each unique positional-delete file, each unique Delta deletion-vector sidecar once, and each merged byte-range request over a Puffin file. Phase B fetches the Parquet footer of each delete-carrying data file to get the per-row-group row counts for the base `ParquetAccessPlan`.

## Scenarios

### Scenario: Scan configures its object store from the resolved connection budget

* *GIVEN* a scan spec whose `s3_max_connections` field is a positive integer N
* *WHEN* the scan UDF builds its S3 object store for the files listed in its per-shard argument
* *THEN* the UDF SHALL configure the object store's HTTP client options with a connection-concurrency budget of N, so up to N concurrent connections to the object store are held warm per host rather than leaving the client at its default pooling behaviour
* *AND* the UDF SHALL apply that same budget of N to EACH object store it builds, so a two-side broadcast join, which builds one store per side to read each side through its own credential, holds up to 2N warm connections rather than N
* *AND* the UDF MUST NOT divide the budget across a join's sides, because the resulting per-side value would make a fact-side scan's fetch parallelism depend on whether the query is a join
* *AND* the budget SHALL apply on both the raw-row scan path and the partial-aggregate path, since both decode Parquet fetched over the same object store
* *AND* a credential value MUST NOT appear in any error the UDF surfaces while building the client options

### Scenario: Scan falls back to a built-in default budget when the field is absent

* *GIVEN* a scan spec whose JSON omits the `s3_max_connections` field
* *WHEN* the scan UDF deserializes the spec and builds its S3 object store
* *THEN* the UDF SHALL use a conservative built-in default connection-concurrency budget, clamped to at least 1
* *AND* the scan SHALL otherwise execute identically to the explicit-value path

### Scenario: FIXED value overrides the AUTO derivation at createVirtualSchema

* *GIVEN* a `createVirtualSchema` request whose `S3_MAX_CONNECTIONS` property is a whole number M that `vs-properties/property-value-validation` accepts
* *WHEN* the adapter resolves the connection-concurrency budget
* *THEN* the adapter SHALL record M as the connection-concurrency budget, without applying the AUTO derivation
* *AND* the adapter SHALL record the resolved value in the `createVirtualSchema` `adapterNotes` so the per-shard scan spec carries an integer field the scan UDF consumes unchanged

### Scenario: AUTO derivation sizes the per-instance budget from the resolved thread count

* *GIVEN* a `createVirtualSchema` request whose `S3_MAX_CONNECTIONS` property is absent or empty
* *AND* a resolved `df_threads_per_udf` of T, in either threading mode
* *WHEN* the adapter resolves the connection-concurrency budget
* *THEN* the adapter SHALL derive a budget of `T × 4`
* *AND* the adapter SHALL record the derived value in `adapterNotes`
* *AND* a value that `vs-properties/property-value-validation` rejects, including `AUTO` and `0`, SHALL fail the request, and the adapter MUST NOT apply the AUTO derivation for it

### Scenario: AUTO derivation yields the single-core budget when the core count cannot be detected

* *GIVEN* a `createVirtualSchema` request whose `S3_MAX_CONNECTIONS` property is absent or empty
* *AND* an executing node whose operating system cannot report a core count, so the adapter resolves a core count of `1`
* *WHEN* the adapter resolves the connection-concurrency budget
* *THEN* the adapter SHALL apply the ordinary AUTO formula to the single thread that core count yields, a budget of 4
* *AND* the scan-side built-in default budget SHALL serve as the default for a scan-spec JSON payload that omits `s3_max_connections`, and as the pushdown-side fallback when the `S3_MAX_CONNECTIONS` adapterNote is absent or not a positive integer

### Scenario: Connection budget travels once in the shard-invariant common spec

* *GIVEN* a scan-driving query fanned across more than one work-unit shard
* *WHEN* the adapter serializes the scan-driving SQL arguments
* *THEN* the resolved connection-concurrency budget SHALL travel in the shard-invariant common spec argument, serialized EXACTLY ONCE for the whole fan-out, and MUST NOT be repeated in any per-shard argument
* *AND* the scan spec reconstituted for every shard SHALL carry the same connection-concurrency budget

### Scenario: The connection budget also bounds the delete path's object-store reads

* *GIVEN* a scan spec whose `s3_max_connections` field is a positive integer N and whose assigned data files carry Parquet positional-delete files, Iceberg deletion vectors, or Delta deletion vectors
* *WHEN* the scan UDF prepares those deletes across every scan table it registers for the query, a single table, or both the fact and dimension sides of a broadcast join, which DataFusion may plan concurrently
* *THEN* a single fan-out limiter of size N, one semaphore constructed once per scan invocation and shared by every registered scan table, SHALL bound BOTH the Phase A delete-payload reads (positional-delete file footers and row groups, Delta deletion-vector sidecars, and Puffin byte-range requests) AND the Phase B data-file Parquet footer fetches that build the delete-carrying files' base access plans, so across every such fan-out active in one scan invocation AT MOST N of those object-store reads are in flight at any instant
* *AND* a SECOND size-N limiter SHALL NOT be introduced along any axis: not one per provider, because concurrently planned join-side scan leaves would then allow up to 2N in-flight reads, not one per phase, because a Phase-B-private semaphore would let a single provider run N delete-payload reads and N footer fetches at once, and not one per delete mechanism; each breaks the instance-level bound
* *AND* every task in either fan-out that reads object storage SHALL acquire exactly ONE permit, hold it across ONE object-store read, and release it on completion, holding no permit while awaiting another; a footer that does not fit the metadata size hint adds a second, sequential read under that same permit, and a task that only decodes positions or builds a set SHALL take no permit, so contention between the two phases, and between the two sides of a broadcast join, queues rather than deadlocks
* *AND* a data file carrying NO deletes SHALL NOT acquire a permit, because it issues no Phase B footer fetch
* *AND* this bound SHALL be an application-level concurrency limit, distinct from the HTTP client idle-pool size that the same N configures on the object store
