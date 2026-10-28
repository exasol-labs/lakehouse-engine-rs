# Feature: Virtual Schema Property Name Validation

`CREATE VIRTUAL SCHEMA`, `ALTER VIRTUAL SCHEMA ... REFRESH`, and `ALTER VIRTUAL SCHEMA ... SET` reject a property name the adapter does not read, with one error that names every such property (#534). This feature owns the complete list of virtual schema property names. `vs-properties/property-value-validation` owns the checks of tuning values and the request lifecycle around both checks.

## Background

* Exasol passes each property name of `CREATE VIRTUAL SCHEMA ... WITH` and `ALTER VIRTUAL SCHEMA ... SET` to the adapter without checking it, so only the adapter can reject an unknown name.
* A request's effective properties are its own properties merged with the persisted ones, per the precedence `vs-adapter/refresh-and-set-properties` states for each request type.
* The required-property checks of `CATALOG_CONNECTION` and `NAMESPACE` belong to `vs-adapter/create-virtual-schema` and `direct-storage/direct-storage-properties`, and run after the name check.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Every property the adapter reads is known under every catalog kind

* *GIVEN* a `createVirtualSchema` request under any `CATALOG_KIND`
* *WHEN* the adapter checks the property names
* *THEN* the adapter SHALL accept exactly these 19 names: `CATALOG_KIND`, `CATALOG_CONNECTION`, `NAMESPACE`, `ALLOW_HTTP`, `PARALLELISM_FACTOR`, `DATAFUSION_THREADING_MODE`, `DATAFUSION_TARGET_PARTITIONS`, `DATAFUSION_THREADS_PER_UDF`, `DATAFUSION_BATCH_SIZE`, `MEMORY_POOL_FRACTION`, `INSTANCE_OVERHEAD_MB`, `JOIN_BROADCAST_MAX_BYTES`, `S3_MAX_CONNECTIONS`, `MERGE_SCHEMA`, `HIVE_PARTITIONING`, `PERMISSION_CHECK`, `USER_MAPPING`, `TABLE_INCLUDE`, and `TABLE_EXCLUDE`
* *AND* the set SHALL NOT depend on the catalog kind, so `MERGE_SCHEMA` under the Iceberg REST kind and `USER_MAPPING` without `PERMISSION_CHECK` are accepted and have no effect
<!-- /DELTA:CHANGED -->
