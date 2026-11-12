<!-- DELTA:CHANGED -->
# Feature: Virtual Schema Property Name Validation

`CREATE VIRTUAL SCHEMA` and `ALTER VIRTUAL SCHEMA ... SET` reject a property name the adapter does not read, with one error that names every such property (#534). This feature owns the complete list of virtual schema property names. `vs-properties/property-value-validation` owns the checks of tuning values and the request lifecycle around both checks.
<!-- /DELTA:CHANGED -->

## Background

* Exasol passes each property name of `CREATE VIRTUAL SCHEMA ... WITH` and `ALTER VIRTUAL SCHEMA ... SET` to the adapter without checking it, so only the adapter can reject an unknown name.
* A request's effective properties are its own properties merged with the persisted ones, per the precedence `vs-adapter/refresh-and-set-properties` states for each request type.
* The required-property checks of `CATALOG_CONNECTION` and `NAMESPACE` belong to `vs-adapter/create-virtual-schema` and `direct-storage/direct-storage-properties`, and run after the name check.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: An unknown property name fails the statement

* *GIVEN* a `createVirtualSchema` or `setProperties` request whose effective properties hold a name outside the known property names
* *WHEN* the adapter receives the request
* *THEN* the adapter SHALL fail the request with an error that names each unknown property as `unknown property '<name>'`
* *AND* the adapter SHALL match names exactly and case-sensitively, so `"Namespace"` is unknown even though `NAMESPACE` is known
* *AND* the error MUST NOT contain the value of an unknown property, because a misspelled property can hold any text
* *AND* the adapter SHALL fail before it reads the CONNECTION or contacts any catalog or object store, so a request that also names a CONNECTION that does not exist fails with the unknown-property error
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: Every property the adapter reads is known under every catalog kind

* *GIVEN* a `createVirtualSchema` request under any `CATALOG_KIND`
* *WHEN* the adapter checks the property names
* *THEN* the adapter SHALL accept exactly these 22 names: `CATALOG_KIND`, `CATALOG_CONNECTION`, `NAMESPACE`, `ALLOW_HTTP`, `PARALLELISM_FACTOR`, `DATAFUSION_THREADING_MODE`, `DATAFUSION_TARGET_PARTITIONS`, `DATAFUSION_THREADS_PER_UDF`, `DATAFUSION_BATCH_SIZE`, `MEMORY_POOL_FRACTION`, `INSTANCE_OVERHEAD_MB`, `JOIN_BROADCAST_MAX_BYTES`, `S3_MAX_CONNECTIONS`, `METADATA_READ_CONCURRENCY`, `MERGE_SCHEMA`, `HIVE_PARTITIONING`, `BINARY_AS_STRING`, `PERMISSION_CHECK`, `USER_MAPPING`, `TABLE_INCLUDE`, `TABLE_EXCLUDE`, and `EXCLUDED_CAPABILITIES`
* *AND* the set SHALL NOT depend on the catalog kind, so `MERGE_SCHEMA` under the Iceberg REST kind and `USER_MAPPING` without `PERMISSION_CHECK` are accepted and have no effect
<!-- /DELTA:CHANGED -->
