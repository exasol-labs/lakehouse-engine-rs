# Feature: Glue Table Planning

Resolves a pushdown over a `GLUE` virtual table into the engine's `ResolvedScan`. The one Iceberg planner plans an Iceberg table from the metadata file Glue points to. The one catalog-declared Parquet reader that plans a Unity Parquet table plans a Parquet table, with Glue as the source of its column types and its partitions.

## Background

* Iceberg table spec, § Metastore Tables: "The atomic swap needed to commit new versions of table metadata can be implemented by storing a pointer in a metastore or database that is updated with a check-and-put operation". Glue's `Parameters.metadata_location` is that pointer, so it names the snapshot a REST `loadTable` names.
* Glue records a partition location as a raw S3 key. `direct-storage/parquet-directory-seam-file-listing` owns the raw-key listing rule.
* Trino writes unbucketed Hive Parquet data files with no file extension.
* The adapter reads storage with the CONNECTION's static credentials, or with the session of the role the CONNECTION names (`connection/connection-credentials-assume-role-session-use`).

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: A Glue table resolves from its recorded identifier and fails loud when it is no longer plannable

* *GIVEN* a Glue virtual table whose identifier note per `vs-adapter/create-virtual-schema` records `sales.orders`, later dropped from Glue, and a second one later re-registered with an ORC input format
* *WHEN* the pushdown resolves each table
* *THEN* the adapter SHALL resolve the Glue database `sales` and the table `orders` from the recorded identifier, splitting it at its first dot
* *AND* the dropped table SHALL fail with an error naming it and stating that it does not exist
* *AND* the re-registered table SHALL fail with an error naming the reason the listing would skip it
* *AND* no error SHALL contain a credential value
<!-- /DELTA:CHANGED -->
