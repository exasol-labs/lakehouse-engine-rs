# Feature: Direct-Storage Table Planning

Resolves a directory of Parquet files into the engine's existing `ScanSpec` shape at plan time.
That shape serves a catalog-free table exactly as it already serves an Iceberg and a Delta table.
File-level sharding, the pushdown wire format, streaming emit, and the memory model are unchanged.

## Background

* There is no snapshot, no transaction log, and no manifest. The reader's whole job is to turn a
  table root plus the directory options into the resolved scan every other reader returns.
* Format dispatch stays ONE exhaustive match over the scan source. That source pairs a resolved
  session with the table it reads. A third variant is a compile error at that one site.
  `delta/delta-table-planning` already records that guarantee.
* The reader owns NO listing, NO partition discovery, and NO footer parsing of its own: all three
  come from `direct-storage/parquet-directory-seam`, the same seam table enumeration reads.
* Identity column binding is not a new mechanism. A logical field carrying neither a field-id nor a
  declared physical name binds by its own name. Delta's `none` column-mapping mode already ships
  that binding, and `scan-read-path/scan-execution-field-id-projection` already specifies it.
* This reader has no catalog statistics to prune from. Its plan-time file list is every file under
  the table root that `direct-storage/direct-storage-hive-partitioning` keeps. Footer-statistics
  pruning is issue [#412](https://github.com/exasol-labs/lakehouse-engine-rs/issues/412), recorded
  here as an explicit tracked exception rather than an unstated gap.
* **Apache Iceberg and Delta specification check.** This reader implements NEITHER format and
  claims conformance to neither. Reading a directory that happens to hold an Iceberg or a Delta
  table as raw Parquet is therefore not a deviation from either specification. It is a different
  read, selected by the operator through `CATALOG_KIND`. The scenario below states that trade-off
  normatively, with the correct `CATALOG_KIND` named as the fix. The behavior is therefore a
  recorded decision rather than a silent gap.

## Scenarios

<!-- DELTA:NEW -->
### Scenario: Until a refresh, a query reads the current files under the declared columns

* *GIVEN* a direct-storage virtual schema that leaves `MERGE_SCHEMA` and `HIVE_PARTITIONING` absent, and since its last `CREATE VIRTUAL SCHEMA` or `REFRESH` one table has gained a data file of its declared columns, one has gained a file carrying an undeclared column, one has gained a file under a `key=value` directory segment that no earlier file of it carried, one table directory has lost its last data file while it still holds `_SUCCESS`, and a new first-level directory holding a data file has appeared
* *WHEN* an Exasol user queries the virtual schema before any `REFRESH`
* *THEN* a query on the table that gained a data file SHALL return the new file's rows together with the old ones, because the adapter lists a table's files again for every query
* *AND* the undeclared column, the new partition column, and the new directory's table SHALL stay unknown to Exasol until a `REFRESH`, per `vs-adapter/refresh-and-set-properties`, so a query naming any of them SHALL fail in Exasol with an error stating that the object is not found
* *AND* a query on the declared columns of a table that gained a file carrying an undeclared column or a new partition segment SHALL return the rows of every current data file of that table
* *AND* a query on the table whose directory lost its last data file SHALL return zero rows without an error, because the per-query listing resolves an empty file list for a table Exasol still declares
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: Until a refresh, a file the declaration cannot hold fails the query and never returns a wrong value

* *GIVEN* a direct-storage virtual schema that leaves `MERGE_SCHEMA` absent, a table whose 32-bit integer column `QTY` is declared `DECIMAL(10,0)` and has since gained a file storing `QTY` as a 64-bit integer, holding one value outside the 32-bit range but within `DECIMAL(10,0)` and one value outside `DECIMAL(10,0)`
* *AND* a second, unpartitioned table whose column `X` one file stores as a 64-bit integer, which has since gained a file storing `X` as a string, a pair no widening rule folds
* *WHEN* an Exasol user queries both tables before any `REFRESH`
* *THEN* a query that returns the `QTY` value outside `DECIMAL(10,0)` SHALL fail with a numeric out-of-range error, and MUST NOT return a truncated, wrapped, or NULL value, because the declared type bounds what Exasol accepts from the scan and the scan emits the stored value without narrowing it
* *AND* a query that returns only the `QTY` value within `DECIMAL(10,0)` SHALL return that value unchanged, because the declared Exasol type, not the 32-bit physical type of the older file, bounds the emitted value
* *AND* every query on the second table, including one that does not read `X`, SHALL fail with the fold error naming `X`, both types, and both file paths, per `direct-storage/parquet-directory-seam`, because the planner folds every kept file's footer before it plans any column, and the query MUST NOT return a row
* *AND* no error message SHALL contain a credential value
<!-- /DELTA:NEW -->
