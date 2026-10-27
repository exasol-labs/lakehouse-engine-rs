# Feature: DataFusion Scan Execution, Object-Store Connection Concurrency

Each scan UDF instance holds a bounded number of connections open to the S3-compatible object store. An explicit positive `S3_MAX_CONNECTIONS` property pins the budget. Otherwise the adapter derives it from the resolved thread count and passes it to the UDF through `adapterNotes` and the common spec.

<!-- DELTA:CHANGED -->
## Background

* The budget is a different axis from the CPU thread and partition budget of `scan-runtime/scan-execution-threading`. It sets how many HTTP connections to the object store the instance keeps warm.
* The adapter resolves the budget at `createVirtualSchema` time from the `S3_MAX_CONNECTIONS` property, whose value `vs-adapter/property-validation` checks first. The adapter records the result in `adapterNotes`, and only the integer reaches the UDF, in the shard-invariant common spec (`parallelism/work-unit-sharding`).
* The budget maps to the `object_store` option `pool_max_idle_per_host`, which bounds warm idle connections. `object_store` exposes no hard in-flight ceiling, so the bound is on sockets and file descriptors, not request rate.
* The row-position delete pipeline (`scan-read-path/scan-execution-row-position-deletes`) issues object-store reads in two phases. Phase A reads every delete payload the shard needs: the footer and then each surviving row group of each unique positional-delete file, each unique Delta deletion-vector sidecar once, and each merged byte-range request over a Puffin file. Phase B fetches the Parquet footer of each delete-carrying data file to get the per-row-group row counts for the base `ParquetAccessPlan`.
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: AUTO derivation sizes the per-instance budget from the resolved thread count

* *GIVEN* a `createVirtualSchema` request whose `S3_MAX_CONNECTIONS` property is absent or empty
* *AND* a resolved `df_threads_per_udf` of T, in either threading mode
* *WHEN* the adapter resolves the connection-concurrency budget
* *THEN* the adapter SHALL derive a budget of `T × 4`
* *AND* the adapter SHALL record the derived value in `adapterNotes`
* *AND* a zero, negative, or non-integer value SHALL fail the request per `vs-adapter/property-validation`, and the adapter MUST NOT apply the AUTO derivation for it
<!-- /DELTA:CHANGED -->
