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

<!-- DELTA:NEW -->
* **The budget is per built object store, and a join now builds one per side.** Issue #294 gives each join side its own inner object store so each reads through its own credential (`datafusion-scan/scan-execution-join`). Each inner store gets its own HTTP client, so each is configured with the budget N and a two-side join can hold up to 2N warm idle connections to one host rather than N.
* **Dividing the budget across the sides was rejected.** `budget / side_count` would silently halve fact-side fetch parallelism on EVERY broadcast join, making a tuning knob's effective value depend on whether the query happens to be a join — a data-dependent performance regression, in exchange for bounding a resource that is not the bottleneck.
* **What doubles is warm idle sockets, not concurrency.** The knob maps to `object_store` 0.13.2's `pool_max_idle_per_host`, which bounds how many established connections the pool keeps warm and reusable; `object_store` 0.13.2 exposes no hard in-flight ceiling. The bound this delta widens is therefore a socket / file-descriptor bound, not a request-rate bound.
* **A join whose sides live in different buckets already held 2N before this delta**, because two buckets already yielded two registered stores with two clients. This delta makes the shared-bucket join match that shape rather than introducing a new one.
<!-- /DELTA:NEW -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Scan configures its object store from the resolved connection budget

* *GIVEN* a scan spec whose `s3_max_connections` field is a positive integer N
* *WHEN* the scan UDF builds its S3 object store for the files listed in its per-shard argument
* *THEN* the UDF SHALL configure the object store's HTTP client options with a connection-concurrency budget of N, so up to N concurrent connections to the object store are held warm per host rather than leaving the client at its default pooling behaviour
* *AND* the UDF SHALL apply that same budget of N to EACH object store it builds, so a two-side broadcast join — which builds one store per side to read each side through its own credential — holds up to 2N warm connections rather than N
* *AND* the UDF MUST NOT divide the budget across a join's sides, because the resulting per-side value would make a fact-side scan's fetch parallelism depend on whether the query is a join
* *AND* the budget SHALL apply on both the raw-row scan path and the partial-aggregate path, since both decode Parquet fetched over the same object store
* *AND* a credential value MUST NOT appear in any error the UDF surfaces while building the client options
<!-- /DELTA:CHANGED -->
