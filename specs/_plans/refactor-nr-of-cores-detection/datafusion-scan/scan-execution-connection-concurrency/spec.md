# Feature: DataFusion Scan Execution — Object-Store Connection Concurrency

Unchanged by this plan, and reproduced here only because the delta validator requires a description. The recorded text stands as written in `specs/datafusion-scan/scan-execution-connection-concurrency/spec.md`.

## Background

* Unchanged by this plan. This section is unmarked, so `speq record` ignores it and the recorded Background survives intact. See `specs/datafusion-scan/scan-execution-connection-concurrency/spec.md`.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: AUTO derivation sizes the per-instance budget from node capacity

* *GIVEN* a `createVirtualSchema` request that supplies no positive-integer `S3_MAX_CONNECTIONS` property (absent, empty, zero, or invalid)
* *AND* a resolved per-node core count of at least 1 and a per-node UDF-instance share derived from the work-unit shard fan-out
* *WHEN* the adapter resolves the connection-concurrency budget
* *THEN* the adapter SHALL derive a per-instance connection-concurrency budget from the core count and the per-node UDF-instance share, mirroring the AUTO thread-budget derivation, so the budget scales with a node's capacity and the per-node instance share without collapsing below 1
* *AND* the adapter SHALL apply that one derivation to EVERY resolved core count, with no separate branch for a core count the platform could not report
* *AND* the adapter SHALL record the derived value in `adapterNotes`
<!-- /DELTA:CHANGED -->

<!-- DELTA:REMOVED -->
### Scenario: AUTO derivation falls back to the default budget when the core count is unknown

* *GIVEN* a `createVirtualSchema` request that supplies no positive-integer `S3_MAX_CONNECTIONS` property
* *AND* a resolved per-node core count of 0 (the unknown / unavailable sentinel)
* *WHEN* the adapter resolves the connection-concurrency budget
* *THEN* the adapter SHALL fall back to the conservative built-in default budget rather than producing a zero or negative budget
* *AND* the adapter SHALL still return a successful `createVirtualSchema` response
<!-- /DELTA:REMOVED -->

<!-- DELTA:NEW -->
### Scenario: AUTO derivation yields the single-core budget when the core count cannot be detected

* *GIVEN* a `createVirtualSchema` request that supplies no positive-integer `S3_MAX_CONNECTIONS` property
* *AND* an executing node where `std::thread::available_parallelism()` cannot report a core count, so the adapter resolves a core count of `1`
* *WHEN* the adapter resolves the connection-concurrency budget
* *THEN* the adapter SHALL apply the ordinary AUTO formula to that core count of `1`, yielding `1 × S3_CONNECTIONS_PER_THREAD`, which is 4 at the recorded multiplier, and MUST NOT substitute the scan-side built-in default of 16
* *AND* this SHALL be recorded as a deliberate behavior change, because the deleted unknown-core branch returned 16 and no other derivation carried such a branch
* *AND* the scan-side built-in default SHALL remain in place in both of its other roles, as the serde default for a `ScanSpec` JSON payload that omits `s3_max_connections` and as the pushdown-side fallback when the `S3_MAX_CONNECTIONS` adapterNote is absent or not a positive integer, so that only the create-time AUTO unknown-core branch stops using it and neither pushdown-side nor deserialization behavior changes
<!-- /DELTA:NEW -->
