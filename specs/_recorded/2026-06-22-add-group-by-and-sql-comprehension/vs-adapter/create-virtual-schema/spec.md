# Feature: Create Virtual Schema

Lets an Exasol user register an Iceberg table as a queryable virtual schema, mapping its columns to Exasol SQL types and recording — in the response `adapterNotes` — both the cluster's active node count (via `NPROC()`) and a parallelism factor, so later pushdowns can size the oversubscribed work-unit shard count.

## Background

* The adapter holds no state between requests; cluster information is resolved per request and returned in the `createVirtualSchema` response's `adapterNotes` (stringified JSON), which Exasol persists and round-trips back at pushdown time.
* `NPROC()` is obtained over a read-only connect-back session and recorded as `CLUSTER_NODES`.
* The parallelism factor is supplied as a VS/connection property and recorded alongside `CLUSTER_NODES`.
* Credentials MUST NOT appear in any error message or returned property.

## Scenarios

<!-- DELTA:NEW -->
### Scenario: Adapter records the parallelism factor in the virtual-schema adapterNotes

* *GIVEN* a `createVirtualSchema` request that supplies a `PARALLELISM_FACTOR` connection/VS property
* *WHEN* Exasol sends the `createVirtualSchema` request naming an Iceberg table
* *THEN* the adapter SHALL record the supplied parallelism factor in the `createVirtualSchema` response's `adapterNotes` (stringified JSON) alongside `CLUSTER_NODES`
* *AND* the adapter SHALL default the parallelism factor to a sensible value (8) when the property is absent or not a positive integer
* *AND* the adapter MUST NOT persist the parallelism factor anywhere other than that returned `adapterNotes`

### Scenario: Recorded node count and parallelism factor drive later work-unit sharding

* *GIVEN* a `createVirtualSchema` request for which `NPROC()` resolves the active node count
* *WHEN* the adapter returns the `createVirtualSchema` response
* *THEN* the `adapterNotes` SHALL carry both the resolved `CLUSTER_NODES` node count and the `PARALLELISM_FACTOR`
* *AND* both values SHALL be round-tripped back to the adapter at pushdown time so the shard count G can be computed as `CLUSTER_NODES × PARALLELISM_FACTOR` capped at 300
<!-- /DELTA:NEW -->
