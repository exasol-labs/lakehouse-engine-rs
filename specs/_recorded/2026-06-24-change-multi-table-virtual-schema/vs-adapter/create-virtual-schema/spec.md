# Feature: Create Virtual Schema

Lets an Exasol user register every Iceberg table in a configured namespace (resolved through an Iceberg REST catalog over S3-compatible storage, including AWS Glue with SigV4-signed requests) as queryable virtual tables, so each table's columns appear to Exasol with correctly mapped SQL types, and records — in the response adapterNotes — the cluster's active node count, per-node core count, parallelism factor, DataFusion threading and memory-budget controls, and the Exasol-name to Iceberg-identifier map so later pushdowns can size sharding and recover the scanned table.

## Background

The catalog endpoint and storage credentials are supplied through the Exasol CONNECTION object named by the `CATALOG_CONNECTION` property; the namespace to expose is supplied by the `ICEBERG_NAMESPACE` property. The adapter holds no state between requests other than the values it returns in `schemaMetadata.adapterNotes`, which Exasol persists.

## Scenarios

<!-- DELTA:CHANGED -->
### Create virtual schema enumerates every table in the configured namespace

* *GIVEN* an Iceberg REST catalog reachable through the CONNECTION named by `CATALOG_CONNECTION`
* *AND* a `createVirtualSchema` request that supplies an `ICEBERG_NAMESPACE` property naming an Iceberg namespace (one or more dot-separated levels, e.g. `finance` or `prod.finance`)
* *WHEN* Exasol sends the `createVirtualSchema` request
* *THEN* the adapter SHALL list every table contained in that namespace and in each of its descendant namespaces, resolving credentials via `CATALOG_CONNECTION` and SigV4-signing the catalog requests when enabled
* *AND* the adapter SHALL return a JSON response describing one virtual table per discovered Iceberg table, whose Exasol name is the namespace segments below the configured namespace plus the table name joined with `__` and uppercased, mapping each Iceberg field to an Exasol SQL type per the type-mapping table and declaring any incompatible type as VARCHAR rather than failing
* *AND* the adapter MUST NOT persist any catalog metadata between requests other than the table-name map recorded in `adapterNotes`
<!-- /DELTA:CHANGED -->

<!-- DELTA:NEW -->
### Create virtual schema records the Exasol-name to Iceberg-identifier map in adapterNotes

* *GIVEN* a `createVirtualSchema` request that enumerates one or more tables in the configured namespace
* *WHEN* the adapter builds the `createVirtualSchema` response
* *THEN* the adapter SHALL record, inside the response's `schemaMetadata.adapterNotes` (a stringified JSON object), a `TABLE_MAP` entry mapping each uppercased `__`-flattened Exasol table name to its original-cased fully-qualified Iceberg identifier (dot-joined namespace segments plus table name)
* *AND* the adapter SHALL preserve every other pre-existing `adapterNotes` entry (`CLUSTER_NODES`, `NR_OF_CORES`, `PARALLELISM_FACTOR`, and the DataFusion threading and memory-budget entries) when writing `TABLE_MAP`
* *AND* the recorded map SHALL round-trip back to the adapter at pushdown time so a pushdown can recover the exact Iceberg identifier from the Exasol table name without re-listing the catalog
* *AND* the adapter MUST NOT persist the map anywhere other than the returned `adapterNotes`
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Multi-level Iceberg namespaces flatten deterministically into Exasol table names

* *GIVEN* a configured namespace `prod.finance` containing an Iceberg table `orders` and a child namespace `prod.finance.eu` containing a table `orders`
* *WHEN* Exasol sends the `createVirtualSchema` request naming namespace `prod.finance`
* *THEN* the adapter SHALL name the first virtual table `ORDERS` and the second `EU__ORDERS`, flattening only the namespace segments below the configured namespace using `__` and uppercasing the result
* *AND* the adapter SHALL apply the same flatten function when building the `TABLE_MAP` so the Exasol name maps back to the correct original-cased Iceberg identifier
* *AND* when two distinct Iceberg identifiers flatten to the same Exasol name (a `__` collision) the adapter SHALL return an error naming the colliding Exasol table name rather than silently dropping or overwriting a table
<!-- /DELTA:NEW -->

<!-- DELTA:CHANGED -->
### Create virtual schema fails clearly when the catalog is unreachable

* *GIVEN* the Iceberg REST catalog endpoint resolved from the CONNECTION cannot be reached
* *WHEN* Exasol sends a `createVirtualSchema` request
* *THEN* the adapter SHALL return an error describing that the catalog could not be reached or the namespace could not be listed
* *AND* the error message MUST NOT contain storage access keys, secret keys, session tokens, or any SigV4 signing key
<!-- /DELTA:CHANGED -->
