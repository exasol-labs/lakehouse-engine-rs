# Feature: Unity Catalog Parquet Table Planning

Resolves a Unity Catalog table whose `data_source_format` is `PARQUET` into the engine's existing `ResolvedScan` shape at plan time. Unity Catalog is this table's catalog: it declares the columns, the partition columns, the storage location, and the credential-vending key. The data files are the Parquet objects under the storage location. File-level sharding, the pushdown wire format, streaming emit, and the memory model are unchanged.

## Background

* Unity Catalog reports a Parquet external table with `table_type` `EXTERNAL`, `data_source_format` `PARQUET`, a `storage_location`, and a `columns[]` array whose entries carry `type_json` and `partition_index`. Verified live against the project's OSS Unity Catalog fixture (`docker-compose.unity.yml`): `GET /tables/{full_name}` and `GET /tables?catalog_name=...&schema_name=...` both return that shape, with `partition_index` `null` on a data column and `0` on the first partition column.
* A column's `type_json` is its Spark SQL `StructField` JSON (Databricks Tables API: "Full data type specification, JSON-serialized."). The Delta protocol records a table schema in the same representation: "Delta uses a subset of Spark SQL's JSON Schema representation to record the schema of a table in the transaction log" (PROTOCOL.md § Schema Serialization Format).
* Unity Catalog reads a partitioned external table's partition values from its directory layout: "By default, Unity Catalog recursively lists all directories in the table location to automatically discover partitions", for tables that "use Parquet, ORC, CSV, Avro, or JSON" (Databricks, *Partition discovery for external tables*).

## Scenarios

<!-- DELTA:REMOVED -->
### Scenario: A predicate on a string partition column prunes files and no other predicate does

* *GIVEN* the partitioned table above, and three queries carrying `region = 'eu'`, `year = 2024`, and `region = 'eu' OR amount > 10`
* *WHEN* the reader resolves each query's scan
* *THEN* the `region = 'eu'` query SHALL resolve only the `region=eu` files, evaluated by the partition predicate of `vs-adapter/direct-storage-hive-partitioning` under its string comparison rules
* *AND* the reader SHALL present to that predicate ONLY the partition columns whose declared Spark type is `string`, so the `year = 2024` query and every predicate on a non-string partition column keep every file, because the predicate compares values as strings and string order is not the order of an integer, date, or timestamp column
* *AND* the `OR` query SHALL keep every file, because its non-partition branch can be TRUE for any file
* *AND* each query SHALL return the same rows as the same query without pruning, because the full predicate still applies above the scan
<!-- /DELTA:REMOVED -->

<!-- DELTA:NEW -->
### Scenario: A predicate on a partition column prunes files under the column's declared type

* *GIVEN* a Unity Parquet table partitioned by `year` (`INT`, `partition_index` 0) and `region` (`STRING`, `partition_index` 1), holding files under `year=2024/region=eu/`, `year=2024/region=us/`, and `year=2025/region=eu/`, and three queries carrying `region = 'eu'`, `year = 2024`, and `region = 'eu' OR amount > 10`
* *WHEN* the reader resolves each query's scan
* *THEN* the `region = 'eu'` query SHALL resolve only the `region=eu` files, and the `year = 2024` query only the `year=2024` files, evaluated by the shared predicate of `vs-adapter/partition-predicate-declared-types` under each partition column's declared type
* *AND* the `OR` query SHALL keep every file, because its non-partition branch can be TRUE for any file
* *AND* each query SHALL return the same rows as the same query without pruning, because the full predicate still applies above the scan
<!-- /DELTA:NEW -->
