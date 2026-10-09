# Feature: Glue Table Planning

Resolves a pushdown over a `GLUE` virtual table into the engine's `ResolvedScan`. The one Iceberg planner plans an Iceberg table from the metadata file Glue points to. The one catalog-declared Parquet reader that plans a Unity Parquet table plans a Parquet table, with Glue as the source of its column types and its partitions.

## Background

* Iceberg table spec, § Optimistic Concurrency: "Once a writer has created an update, it commits by swapping the table’s metadata file pointer from the base version to the new version." § Metastore Tables: "The atomic swap needed to commit new versions of table metadata can be implemented by storing a pointer in a metastore or database that is updated with a check-and-put operation". Glue's `Parameters.metadata_location` is that pointer, so it names the snapshot a REST `loadTable` names.
* Glue records a partition location as a raw S3 key (verified live, #410). `direct-storage/parquet-directory-seam-file-listing` owns the raw-key listing rule.
* Trino writes unbucketed Hive Parquet data files with no file extension (verified live, #410).
* Every pushdown re-reads the Glue metadata: one `GetTable`, the paginated `GetPartitions`, and one LIST per kept partition. The adapter caches nothing.
* The adapter reads storage with the CONNECTION's static credentials, or with the session of the role the CONNECTION names, per `connection/connection-credentials-assume-role-session-use`.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: A Glue Iceberg table is planned from its metadata location by the one Iceberg planner

* *GIVEN* a Glue Iceberg table whose `metadata_location` names its current `metadata.json`
* *WHEN* the pushdown resolves the table
* *THEN* the adapter SHALL load the table with `GetTable` and read that metadata file once, through the CONNECTION's static storage
* *AND* the adapter SHALL plan the table with the SAME Iceberg planner that plans an Iceberg REST table, so delete handling, the date-promotion refusal, the name mapping, and file pruning apply to it as to an Iceberg REST table
* *AND* the metadata file `createVirtualSchema` read SHALL be the schema authority, per `glue/glue-catalog-client`, and the pushdown SHALL take the table's logical schema from the column notes built from it (`vs-adapter/column-source-notes`)
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: A Glue Parquet table is planned by the shared catalog-declared Parquet reader

* *GIVEN* a Glue Parquet table and a Unity Parquet table
* *WHEN* `createVirtualSchema` declares each table and the pushdown resolves it
* *THEN* ONE declaration rule SHALL classify both tables' columns at `createVirtualSchema`, differing between them only in its type source (the Glue Hive type string per `glue/glue-hive-type-mapping`, or the Unity `type_json`)
* *AND* ONE reader SHALL plan both tables at pushdown, differing between them only in its file source (the registered partitions or the table directory) and its storage (the CONNECTION's static storage, or the Unity storage resolution)
* *AND* the Glue table's logical schema SHALL follow the logical-schema rules of `unity-catalog/unity-parquet-table-planning` over its Glue columns: declared column order, NULLABLE fields, no field-id or declared physical name, the case-fold binding, and no footer read at plan time
* *AND* the pushdown SHALL take the Glue table's logical schema, partition columns, and refused columns from its column notes, and MUST NOT classify a Glue column type at pushdown
* *AND* a partition key that a data file also stores as a column SHALL read the partition value
<!-- /DELTA:CHANGED -->
