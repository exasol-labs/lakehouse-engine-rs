# Feature: Create Virtual Schema

Lets an Exasol user register an Iceberg table (resolved through an Iceberg REST
catalog over S3-compatible storage) as a queryable virtual schema, so the table's
columns appear to Exasol with correctly mapped SQL types, and records the cluster's
active node count so later pushdowns can shard work across nodes.

## Background

* The adapter holds no state between requests; any cluster information it needs is
  resolved per request and returned in the virtual-schema properties, never persisted.
* Connect-back is a read-only SQL session opened via `ctx.cluster_ip()` against
  `<container-eth0-ip>:8563` using CONNECTION-object credentials.
* Credentials MUST NOT appear in any error message or returned property.

## Scenarios

<!-- DELTA:NEW -->
### Scenario: Adapter records the cluster node count as a virtual-schema property

* *GIVEN* an Exasol session that has installed the VS adapter script
* *AND* the catalog and storage connection properties are supplied to the adapter
* *WHEN* Exasol sends a `createVirtualSchema` request naming an Iceberg table
* *THEN* the adapter SHALL open a connect-back session to Exasol and run `SELECT NPROC()` to obtain the count of active cluster nodes
* *AND* the adapter SHALL return the resolved node count as a positive-integer virtual-schema property named `CLUSTER_NODES` in the `createVirtualSchema` response
* *AND* the adapter MUST NOT persist the node count anywhere other than the returned virtual-schema properties
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: Cluster node count defaults to one when it cannot be determined

* *GIVEN* the VS adapter cannot open a connect-back session or `SELECT NPROC()` fails
* *WHEN* Exasol sends a `createVirtualSchema` request
* *THEN* the adapter SHALL set the `CLUSTER_NODES` property to `1`
* *AND* the adapter SHALL still return a successful `createVirtualSchema` response describing the mapped table
* *AND* the resulting single-shard behaviour MUST be identical to the pre-sharding single-node execution path
<!-- /DELTA:NEW -->
