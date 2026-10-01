# Feature: Glue Table Planning

Resolves a pushdown over a `GLUE` virtual table into the engine's `ResolvedScan`. The one Iceberg planner plans an Iceberg table from the metadata file Glue points to. The one catalog-declared Parquet reader that plans a Unity Parquet table plans a Parquet table, with Glue as the source of its column types and its partitions.

## Background

* Iceberg table spec, § Optimistic Concurrency: "Once a writer has created an update, it commits by swapping the table’s metadata file pointer from the base version to the new version." § Metastore Tables: "The atomic swap needed to commit new versions of table metadata can be implemented by storing a pointer in a metastore or database that is updated with a check-and-put operation". Glue's `Parameters.metadata_location` is that pointer, so it names the snapshot a REST `loadTable` names.
* Glue records a partition location as a raw S3 key. A partition value `a b/c` is stored at the literal key `…/p_str=a b%2Fc/` (verified live, #410).
* Trino writes unbucketed Hive Parquet data files with no file extension (verified live, #410).
* Every pushdown re-reads the Glue metadata: one `GetTable`, the paginated `GetPartitions`, and one LIST per kept partition. The adapter caches nothing.
* The adapter reads storage with the CONNECTION's static credentials.

## Scenarios

### Scenario: A Glue Iceberg table is planned from its metadata location by the one Iceberg planner

* *GIVEN* a Glue Iceberg table whose `metadata_location` names its current `metadata.json`
* *WHEN* the pushdown resolves the table
* *THEN* the adapter SHALL load the table with `GetTable` and read that metadata file once, through the CONNECTION's static storage
* *AND* the adapter SHALL plan the table with the SAME Iceberg planner that plans an Iceberg REST table, so delete handling, the date-promotion refusal, the name mapping, and file pruning apply to it as to an Iceberg REST table
* *AND* the metadata file SHALL be the schema authority, per `vs-adapter/glue-catalog-client`

### Scenario: A Glue Parquet table is planned by the shared catalog-declared Parquet reader

* *GIVEN* a Glue Parquet table and a Unity Parquet table
* *WHEN* the pushdown resolves each table
* *THEN* ONE reader SHALL plan both tables, and SHALL differ between them only in its type source (the Glue Hive type string per `vs-adapter/glue-hive-type-mapping`, or the Unity `type_json`), its file source (the registered partitions or the table directory), and its storage (the CONNECTION's static storage, or the Unity storage resolution)
* *AND* the Glue table's logical schema SHALL follow the logical-schema rules of `vs-adapter/unity-parquet-table-planning` over its Glue columns: declared column order, NULLABLE fields, no field-id or declared physical name, the case-fold binding, and no footer read at plan time
* *AND* a partition key that a data file also stores as a column SHALL read the partition value

### Scenario: Each kept partition's location is listed and its files carry the partition's Glue values

* *GIVEN* a partitioned Glue Parquet table whose partitions include a NULL partition, a partition registered at a location with no `key=value` segment, a partition with the value `a b/c`, a partition outside the table location in the same bucket, and a partition addressed with `s3a://`
* *WHEN* the pushdown resolves the table
* *THEN* the reader SHALL list each kept partition's location by its raw object key, per `vs-adapter/parquet-directory-seam`
* *AND* every file SHALL carry its partition's Glue values, NULL for the default partition, and never a value parsed from its path
* *AND* a file outside the table location SHALL carry an absolute path, and `s3a://` SHALL address the same store as `s3://`
* *AND* the reader SHALL list the kept partitions concurrently, bounded by the admission limit of the table's store

### Scenario: A kept partition the reader cannot read faithfully fails the query naming it

* *GIVEN* a Glue Parquet table with a kept ORC partition, a kept partition in a bucket other than the table's, and a pruned ORC partition
* *WHEN* the pushdown resolves the table
* *THEN* the query SHALL fail with an error naming the partition's values, its location, and the cause: its input format, or its bucket
* *AND* the reader MUST NOT skip such a partition, because a skipped partition returns wrong rows
* *AND* the pruned ORC partition SHALL NOT fail the query, because it contributes no row

### Scenario: A Glue location's data files are its direct children of any name

* *GIVEN* a partition location holding `20240101_abc` (no extension), `part-0.snappy.parquet`, `_SUCCESS`, `.hidden`, a zero-length object, and `nested/x.parquet`, and an unpartitioned Glue table whose location holds the same objects
* *WHEN* the pushdown resolves each table
* *THEN* the reader SHALL read `20240101_abc` and `part-0.snappy.parquet` and no other object, through the seam's any-direct-child file pattern
* *AND* the reader SHALL list the unpartitioned table's location under the same pattern

### Scenario: A partition predicate prunes partitions before their locations are listed

* *GIVEN* a Glue Parquet table partitioned by `p_int int`, `p_date date`, and `p_str string`, and a query carrying `p_int = 1 AND p_date >= DATE '2024-01-01'`
* *WHEN* the pushdown resolves the table
* *THEN* the reader SHALL evaluate the shared partition predicate of `vs-adapter/partition-predicate-declared-types` on each partition's Glue values
* *AND* the reader SHALL list only the kept partitions' locations
* *AND* the adapter MUST NOT send the predicate in the `Expression` of `GetPartitions`, because an error in a second predicate translator returns wrong rows once a capability is advertised
* *AND* the query SHALL return the same rows as the same query without pruning

### Scenario: A Glue table resolves from its recorded identifier and fails loud when it is no longer plannable

* *GIVEN* a Glue virtual table recorded in `TABLE_MAP` as `sales.orders`, later dropped from Glue, and a second one later re-registered with an ORC input format
* *WHEN* the pushdown resolves each table
* *THEN* the adapter SHALL resolve the Glue database `sales` and the table `orders` from the recorded identifier, splitting it at its first dot
* *AND* the dropped table SHALL fail with an error naming it and stating that it does not exist
* *AND* the re-registered table SHALL fail with an error naming the reason the listing would skip it
* *AND* no error SHALL contain a credential value
