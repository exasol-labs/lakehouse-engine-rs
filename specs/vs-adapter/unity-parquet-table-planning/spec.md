# Feature: Unity Catalog Parquet Table Planning

Resolves a Unity Catalog table whose `data_source_format` is `PARQUET` into the engine's existing `ResolvedScan` shape at plan time. Unity Catalog is this table's catalog: it declares the columns, the partition columns, the storage location, and the credential-vending key. The data files are the Parquet objects under the storage location. File-level sharding, the pushdown wire format, streaming emit, and the memory model are unchanged.

## Background

* Unity Catalog reports a Parquet external table with `table_type` `EXTERNAL`, `data_source_format` `PARQUET`, a `storage_location`, and a `columns[]` array whose entries carry `type_json` and `partition_index`. Verified live against the project's OSS Unity Catalog fixture (`docker-compose.unity.yml`): `GET /tables/{full_name}` and `GET /tables?catalog_name=...&schema_name=...` both return that shape, with `partition_index` `null` on a data column and `0` on the first partition column.
* A column's `type_json` is its Spark SQL `StructField` JSON (Databricks Tables API: "Full data type specification, JSON-serialized."). The Delta protocol records a table schema in the same representation: "Delta uses a subset of Spark SQL's JSON Schema representation to record the schema of a table in the transaction log" (PROTOCOL.md § Schema Serialization Format).
* Unity Catalog reads a partitioned external table's partition values from its directory layout: "By default, Unity Catalog recursively lists all directories in the table location to automatically discover partitions", for tables that "use Parquet, ORC, CSV, Avro, or JSON" (Databricks, *Partition discovery for external tables*).

## Scenarios

### Scenario: A Unity Parquet table lists its files through the shared directory seam and reads no footer

* *GIVEN* a Unity Catalog table reporting `data_source_format` `PARQUET` whose storage location holds Parquet data files at more than one directory depth, plus a `_SUCCESS` object, a `_temporary/` directory, and a `.hidden` object
* *WHEN* the Unity Parquet format reader resolves that table's scan
* *THEN* the reader SHALL obtain the file list, each file's byte size, and each file's partition values from the listing answer of the ONE shared directory seam (`vs-adapter/parquet-directory-seam`), under that seam's data-file rule unchanged
* *AND* the reader MUST NOT read any Parquet footer at plan time, because the catalog already declares the schema
* *AND* each returned file entry SHALL carry its path percent-encoded relative to the table root, its byte size, and an EMPTY delete-mechanism list, exactly as a direct-storage file entry does

### Scenario: A Unity Parquet scan carries no field-id mapping and is file-pruning-blind

* *GIVEN* the same Unity Catalog Parquet table
* *WHEN* the Unity Parquet format reader resolves that table's scan
* *THEN* the returned table root SHALL be the table's catalog-reported storage location, and the returned name mapping SHALL be EMPTY
* *AND* a data file whose name lacks the `.parquet` suffix SHALL NOT be read, as a documented trade-off: a Spark or Databricks writer always names a Parquet data file with that suffix, and the user documentation SHALL state the rule
* *AND* a filter on a non-partition column SHALL narrow the rows the scan emits but not the files it reads, because Unity Catalog carries no per-file statistics for a Parquet table and this reader reads no footer

### Scenario: The logical schema is the catalog's declared column list

* *GIVEN* a Unity Parquet table whose columns declare, through their `type_json`, a native scalar type, a `struct` column, a `binary` column, a column `CustomerId` that a data file spells `customerid`
* *WHEN* the reader resolves that table's scan
* *THEN* the reader SHALL build one logical field per catalog column, in the catalog's declared column order, named by the column's Unity Catalog `name` exactly as declared
* *AND* the reader SHALL classify each column's `type_json` through the SAME Spark-type classification the Delta reader applies to a Delta schema (`vs-adapter/delta-type-mapping`), under no column mapping, so a native type keeps its own Arrow tag, a `struct`, `array`, or `map` column carries the string tag plus the nested descriptor the JSON renderer reads, and a `binary` or `variant` column is refused by name
* *AND* every logical field SHALL carry NEITHER a field-id NOR a declared physical name, so the scan binds it through the identity binding, which binds `customerid` to `CustomerId` because the names differ only in letter case (`datafusion-scan/scan-execution-column-case-fold`)

### Scenario: A Unity Parquet column with no usable type descriptor is refused, and nullability always follows the file

* *GIVEN* a Unity Parquet table whose columns include one with no `type_json` and one whose `type_json` does not parse as a Spark field
* *WHEN* the reader resolves that table's scan
* *THEN* every logical field SHALL be declared NULLABLE whatever nullability the catalog declares, because a column absent from one data file reads NULL for that file's rows, and a required declaration would fail the scan instead
* *AND* a column whose `type_json` is absent or does not parse as a Spark field SHALL be refused by name, with a reason naming the missing or unreadable descriptor, rather than typed by a guess
* *AND* a table whose every column is refused SHALL be refused as a whole, exactly as a Delta table is

### Scenario: The scan applies its shared cast and admission rules, unchanged, to a Unity Parquet column

* *GIVEN* a Unity Parquet table with a column whose data files physically carry a narrower type than the catalog declares, and an `int` column that a data file stores as `double`
* *WHEN* the reader resolves that table's scan
* *THEN* the scan SHALL cast the narrower physical column to the declared type, because that pair is a row of the supported widening set, and SHALL fail a query that reads that column from the `double` file, naming the table's storage location and the column (`datafusion-scan/type-relaxation`)
* *AND* the reader SHALL add no cast, type check, or name match of its own, because the scan applies the same binding and admission rules to every format

### Scenario: Partition columns come from the catalog and partition values from the file paths

* *GIVEN* a Unity Parquet table whose columns declare `year` (`INT`, `partition_index` 0) and `region` (`STRING`, `partition_index` 1), holding files under `year=2024/region=eu/`, `year=2024/region=us/`, and `year=2025/region=__HIVE_DEFAULT_PARTITION__/`, plus one file directly under the storage location
* *WHEN* the reader resolves that table's scan
* *THEN* the returned partition columns SHALL be the columns whose `partition_index` is set, ordered by `partition_index`, so `year` precedes `region`
* *AND* each file's partition values SHALL come from its own `key=value` directory segments, each matched to a partition column by the uppercase fold, keyed by the column's catalog spelling, and percent-decoded
* *AND* a `__HIVE_DEFAULT_PARTITION__` or empty value, and a partition column whose segment the file's path lacks, SHALL read NULL, so the file directly under the storage location reads NULL for both columns and MUST NOT fail the query

### Scenario: An undeclared path segment contributes nothing, and an unconvertible partition value fails the query

* *GIVEN* the same partitioned Unity Parquet table, plus a file under a `other=x/` segment
* *WHEN* the reader resolves that table's scan
* *THEN* a `key=value` segment naming no declared partition column SHALL be a plain directory and SHALL contribute no value
* *AND* the scan SHALL convert each value to its partition column's declared type, and a value that type cannot represent SHALL fail the query with an error naming the column, the type, and the value, exactly as a Delta partition value does

### Scenario: The reader never infers partition columns from paths or partition metadata logging, and fails a plan whose partition column is refused

* *GIVEN* the same partitioned Unity Parquet table, once with Databricks partition metadata logging enabled and once with a partition column the type classification refuses
* *WHEN* the reader resolves that table's scan
* *THEN* the reader MUST NOT infer a partition column from the paths, because the catalog declares the table's partition columns
* *AND* the reader SHALL read partition values from the directories under the storage location even when partition metadata logging is enabled, so the reader does not consult the partitions that log registers
* *AND* a partition column that the classification refuses SHALL fail the plan with an error naming the column, because the scan cannot materialize a partition column absent from the logical schema

### Scenario: A predicate on a string partition column prunes files and no other predicate does

* *GIVEN* the partitioned table above, and three queries carrying `region = 'eu'`, `year = 2024`, and `region = 'eu' OR amount > 10`
* *WHEN* the reader resolves each query's scan
* *THEN* the `region = 'eu'` query SHALL resolve only the `region=eu` files, evaluated by the partition predicate of `vs-adapter/direct-storage-hive-partitioning` under its string comparison rules
* *AND* the reader SHALL present to that predicate ONLY the partition columns whose declared Spark type is `string`, so the `year = 2024` query and every predicate on a non-string partition column keep every file, because the predicate compares values as strings and string order is not the order of an integer, date, or timestamp column
* *AND* the `OR` query SHALL keep every file, because its non-partition branch can be TRUE for any file
* *AND* each query SHALL return the same rows as the same query without pruning, because the full predicate still applies above the scan

### Scenario: Storage is resolved through the table's own catalog exactly as for a Delta table

* *GIVEN* a Unity Parquet table and a CONNECTION that either enables `use_vended_credentials` or supplies static storage credentials
* *WHEN* the reader resolves that table's scan
* *THEN* the reader SHALL resolve its storage through the SAME component the Delta reader uses, so the empty-location refusal, the per-table `READ`-scoped vend against the table's own vending key, the refusal without a static fallback when no vending key is reported, the static backend when vending is disabled, and the shared vended-storage policy all apply unchanged (`vs-adapter/delta-table-planning`, `vs-adapter/unity-catalog-vended-credentials`)
* *AND* the reader SHALL list the files through an object store built from that effective storage, and SHALL return that effective storage, which the shard-invariant common spec carries sealed under vending and as a CONNECTION reference otherwise
* *AND* every error the reader surfaces SHALL be redacted against the effective storage's secret values, and MUST NOT contain any vended or static credential value
