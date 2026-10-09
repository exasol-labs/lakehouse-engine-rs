# Feature: Unity Catalog Parquet Table Planning

Resolves a Unity Catalog table whose `data_source_format` is `PARQUET` into the engine's existing `ResolvedScan` shape at plan time. Unity Catalog is this table's catalog: it declares the columns, the partition columns, the storage location, and the credential-vending key. The data files are the Parquet objects under the storage location. File-level sharding, the pushdown wire format, streaming emit, and the memory model are unchanged. The same catalog-declared Parquet reader plans a Glue Parquet table under these logical-schema rules (`glue/glue-table-planning`).

## Background

* Unity Catalog reports a Parquet external table with `table_type` `EXTERNAL`, `data_source_format` `PARQUET`, a `storage_location`, and a `columns[]` array whose entries carry `type_json` and `partition_index`. Verified live against the project's OSS Unity Catalog fixture (`docker-compose.unity.yml`): `GET /tables/{full_name}` and `GET /tables?catalog_name=...&schema_name=...` both return that shape, with `partition_index` `null` on a data column and `0` on the first partition column.
* A column's `type_json` is its Spark SQL `StructField` JSON (Databricks Tables API: "Full data type specification, JSON-serialized."). The Delta protocol records a table schema in the same representation: "Delta uses a subset of Spark SQL's JSON Schema representation to record the schema of a table in the transaction log" (PROTOCOL.md § Schema Serialization Format).
* Unity Catalog reads a partitioned external table's partition values from its directory layout: "By default, Unity Catalog recursively lists all directories in the table location to automatically discover partitions", for tables that "use Parquet, ORC, CSV, Avro, or JSON" (Databricks, *Partition discovery for external tables*).

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: The logical schema is the catalog's declared column list

* *GIVEN* a Unity Parquet table whose columns declare, through their `type_json`, a native scalar type, a `struct` column, a `binary` column, a column `CustomerId` that a data file spells `customerid`
* *WHEN* `createVirtualSchema` declares the table and the reader then resolves its scan
* *THEN* `createVirtualSchema` SHALL build one logical field per catalog column, in the catalog's declared column order, named by the column's Unity Catalog `name` exactly as declared, and SHALL record each in the column's note (`vs-adapter/column-source-notes`)
* *AND* `createVirtualSchema` SHALL classify each column's `type_json` through the SAME Spark-type classification the Delta reader applies to a Delta schema (`delta/delta-type-mapping`), under no column mapping, so a native type keeps its own Arrow tag, a `struct`, `array`, or `map` column carries the string tag plus the nested descriptor the JSON renderer reads, and a `binary` or `variant` column is refused by name
* *AND* the reader SHALL take the logical schema from the column notes and MUST NOT classify a `type_json` at pushdown
* *AND* every logical field SHALL carry NEITHER a field-id NOR a declared physical name, so the scan binds it through the identity binding, which binds `customerid` to `CustomerId` because the names differ only in letter case (`datafusion-scan/scan-execution-column-case-fold`)
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: A Unity Parquet column with no usable type descriptor is refused, and nullability always follows the file

* *GIVEN* a Unity Parquet table whose columns include one with no `type_json` and one whose `type_json` does not parse as a Spark field
* *WHEN* `createVirtualSchema` declares the table and a query over each column is planned
* *THEN* every logical field SHALL be declared NULLABLE whatever nullability the catalog declares, because a column absent from one data file reads NULL for that file's rows, and a required declaration would fail the scan instead
* *AND* a column whose `type_json` is absent or does not parse as a Spark field SHALL be refused by name, with a reason naming the missing or unreadable descriptor, recorded in its note at `createVirtualSchema` rather than typed by a guess
* *AND* a table whose every column is refused SHALL be refused as a whole at pushdown, per `delta/delta-type-mapping`, and `createVirtualSchema` SHALL still declare it
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: Partition columns come from the catalog and partition values from the file paths

* *GIVEN* a Unity Parquet table whose columns declare `year` (`INT`, `partition_index` 0) and `region` (`STRING`, `partition_index` 1), holding files under `year=2024/region=eu/`, `year=2024/region=us/`, and `year=2025/region=__HIVE_DEFAULT_PARTITION__/`, plus one file directly under the storage location
* *WHEN* the reader resolves that table's scan
* *THEN* the returned partition columns SHALL be the columns whose `partition_index` is set, ordered by `partition_index`, so `year` precedes `region`, as the column notes record them
* *AND* each file's partition values SHALL come from its own `key=value` directory segments, each matched to a partition column by the uppercase fold, keyed by the column's catalog spelling, and percent-decoded
* *AND* a `__HIVE_DEFAULT_PARTITION__` or empty value, and a partition column whose segment the file's path lacks, SHALL read NULL, so the file directly under the storage location reads NULL for both columns and MUST NOT fail the query
* *AND* a storage location whose files carry two spellings of one partition key, such as `year=` and `Year=`, SHALL fail the query naming both spellings, per `direct-storage/parquet-directory-seam-file-listing`
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: The reader never infers partition columns from paths or partition metadata logging, and fails a plan whose partition column is refused

* *GIVEN* the same partitioned Unity Parquet table, once with Databricks partition metadata logging enabled and once with a partition column the type classification refuses
* *WHEN* the reader resolves that table's scan
* *THEN* the reader MUST NOT infer a partition column from the paths, because the catalog declares the table's partition columns and the column notes record them
* *AND* the reader SHALL read partition values from the directories under the storage location even when partition metadata logging is enabled, so the reader does not consult the partitions that log registers
* *AND* a partition column whose note records a refusal SHALL fail the plan with an error naming the column, because the scan cannot materialize a partition column absent from the logical schema
<!-- /DELTA:CHANGED -->
