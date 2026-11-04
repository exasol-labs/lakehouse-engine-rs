# Feature: Direct-Storage Table Planning

The adapter resolves a directory of Parquet files into the engine's `ScanSpec` at plan time. File-level sharding, the pushdown wire format, streaming emit, and the memory model are the same as for an Iceberg or Delta table.

<!-- DELTA:CHANGED -->
## Background

* A direct-storage table has no snapshot, transaction log, or manifest. A pushdown plans it from its table root and the table's column notes, and sends no catalog request.
* A pushdown lists the table root by the rules of `direct-storage/parquet-directory-seam-file-listing`, the same rules table enumeration applies, and reads no Parquet footer. The table's logical schema, partition columns, and refused columns come from the column notes that enumeration recorded (`vs-adapter/column-source-notes`).
* A column whose note records neither a field-id nor a physical name reads the Parquet column of its own name, as Delta's `none` column-mapping mode does (`scan-read-path/scan-execution-field-id-projection`).
* A direct-storage table has no catalog statistics to prune from. Its plan-time file list is every file under the table root that `direct-storage/direct-storage-hive-partitioning` keeps, and no footer statistics prune it, because planning reads no footer.
* Nothing bounds a table's file count. Enumeration reads the footers the merge mode selects, so its cost is unbounded on a directory holding hundreds of thousands of files.
* Direct storage implements neither the Iceberg nor the Delta format. Reading a directory that holds an Iceberg or Delta table as raw Parquet is a different read, selected through `CATALOG_KIND`, not a deviation from either specification.
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:REMOVED -->
### Scenario: The format reader is selected at one exhaustive site for a direct-storage source

* *GIVEN* the recorded scenario of this title
* *WHEN* this plan is recorded
* *THEN* this scenario SHALL be REMOVED, because it states code structure rather than behavior
* *AND* it SHALL be REPLACED by "A direct-storage pushdown loads its table from the directory its identifier note names" below
<!-- /DELTA:REMOVED -->

<!-- DELTA:NEW -->
### Scenario: A direct-storage pushdown loads its table from the directory its identifier note names

* *GIVEN* a virtual schema with `CATALOG_KIND = 'DIRECT_STORAGE'` and a pushdown request over one of its tables
* *WHEN* the adapter loads the table
* *THEN* the adapter SHALL compose the table root from the CONNECTION address, the `NAMESPACE` property, and the directory name that the table's identifier note records, and SHALL list the files under that root
* *AND* the error text that refuses a table with no mappable column or with a refused partition column SHALL name the virtual table instead of a format label
<!-- /DELTA:NEW -->
