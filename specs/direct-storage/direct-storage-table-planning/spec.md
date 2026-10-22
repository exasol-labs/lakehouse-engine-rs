# Feature: Direct-Storage Table Planning

The adapter resolves a directory of Parquet files into the engine's `ScanSpec` at plan time. File-level sharding, the pushdown wire format, streaming emit, and the memory model are the same as for an Iceberg or Delta table.

## Background

* There is no snapshot, transaction log, or manifest. The reader turns a table root and the table's column notes into the resolved scan every other reader returns.
* Format dispatch is one exhaustive match over the scan source, which pairs a resolved session with the table it reads. A further variant is a compile error at that site (`delta/delta-table-planning`).
* The reader does no listing and no partition parsing of its own. Both come from the listing answer of `direct-storage/parquet-directory-seam-file-listing`, which table enumeration also reads. The reader parses no footer. Its logical schema, partition columns, and refused columns come from the column notes that enumeration recorded (`vs-adapter/column-source-notes`).
* A logical field with neither a field-id nor a declared physical name binds by its own name, the same binding as Delta's `none` column-mapping mode (`scan-read-path/scan-execution-field-id-projection`).
* The reader has no catalog statistics to prune from. Its plan-time file list is every file under the table root that `direct-storage/direct-storage-hive-partitioning` keeps. Footer-statistics pruning is not available, because planning reads no footer.
* Nothing bounds a table's file count. Enumeration reads the footers the merge mode selects, so its cost is unbounded on a directory holding hundreds of thousands of files.
* The reader implements neither the Iceberg nor the Delta format. Reading a directory that holds an Iceberg or Delta table as raw Parquet is a different read, selected through `CATALOG_KIND`, not a deviation from either specification.


## Scenarios

### Scenario: The format reader is selected at one exhaustive site for a direct-storage source

* *GIVEN* the scan source whose variant pairs one resolved session with the table it reads, matched exhaustively at exactly ONE site returning a boxed format reader
* *WHEN* the adapter selects the reader for a direct-storage table
* *THEN* the scan source SHALL have a direct-storage variant carrying the request's object store and the table root, and the selection site SHALL match every variant EXHAUSTIVELY, so a new source is a compile error there rather than a silent fall-through
* *AND* that variant MUST NOT carry the directory options or a declared-column list, because the table's declaration reaches every reader from the column notes, and the directory options take effect only at enumeration
* *AND* the direct-storage variant SHALL carry NO catalog table metadata, because this kind loads no table: its root is composed from the CONNECTION address, the namespace property, and the directory name recorded in the column notes
* *AND* the error text of the two refusals that run before the format is known, a table with no mappable column and a refused partition column, SHALL name the virtual table instead of a format label

### Scenario: A direct-storage table resolves its files and schema through the shared seam

* *GIVEN* a direct-storage table root holding Parquet data files at more than one depth, and the column notes enumeration recorded for that table
* *WHEN* the direct-storage format reader resolves that table's scan
* *THEN* the reader SHALL obtain the file list and each file's byte size and partition values from the listing answer of the ONE shared directory seam, filled against the partition columns the notes record, and MUST NOT list objects, parse a path, or read a footer itself
* *AND* the returned logical schema, partition columns, and refused columns SHALL be the ones the column notes record, per `vs-adapter/column-source-notes`
* *AND* each returned file entry SHALL carry its path, its byte size, an EMPTY delete-mechanism list, and the seam's partition values, so an unpartitioned table's entries keep the compact form an unpartitioned delete-free Iceberg scan already produces
* *AND* each file entry's path SHALL be percent-encoded segment by segment, so a key segment carrying `%`, `#`, or `?` resolves at scan time to the object the listing returned
* *AND* every logical field SHALL carry NEITHER a field-id NOR a declared physical name, so the scan binds it by its own name through the identity binding already shipped, and enumeration MUST NOT synthesize an ordinal field-id
* *AND* the returned scan SHALL carry an EMPTY name-mapping list, because a raw directory declares no name mapping
* *AND* the returned effective storage SHALL be the CONNECTION's static backend unchanged, because this kind reaches no credential-vending catalog
* *AND* the returned table root SHALL be the composed directory root, so the shard-invariant common spec carries it once and each file path is encoded relative to it by the existing rules

### Scenario: An Iceberg or Delta table directory read as raw Parquet ignores its table format

* *GIVEN* a storage base path under which an operator has placed a directory that is an Iceberg table or a Delta table, and a virtual schema created over that base path with the direct-storage catalog kind
* *WHEN* an Exasol user queries the virtual table that directory produces
* *THEN* the engine SHALL read every Parquet file under that directory as raw data, and SHALL apply NO Delta deletion vector, NO Delta `remove` action, NO Iceberg delete file, and NO Iceberg snapshot selection
* *AND* the returned rows MAY therefore include rows the table format considers deleted, and MAY include one logical row more than once where several snapshots reference overlapping files
* *AND* this SHALL be a deliberate trade-off rather than a defect of this reader, because the operator selected a `CATALOG_KIND` that names no table format, and an operator who wants table-format semantics selects the catalog kind that supplies them
* *AND* the trade-off SHALL be stated in the user documentation alongside the direct-storage recipe, naming the correct `CATALOG_KIND` as the fix, so an operator meets it before the result surprises them
* *AND* the engine MUST NOT detect a table-format directory and refuse it, because a user may legitimately want the raw files, and a detection heuristic would make a supported read fail on a directory layout it guessed wrong about

### Scenario: A direct-storage pushdown plans without reading a footer

* *GIVEN* a direct-storage table `sales/` holding `year=2025/p1.parquet` and `year=2026/p2.parquet`, where only the `year=2026` file carries a column `DISCOUNT`, and a table `dims/` holding one file, both enumerated by `createVirtualSchema`
* *AND* the data files of `sales/` afterwards overwritten with bytes that are not Parquet
* *WHEN* `EXPLAIN VIRTUAL` plans `SELECT ID, DISCOUNT FROM SALES WHERE YEAR = '2025'`, and a join of `SALES` with `DIMS`, and the first query then runs
* *THEN* both plans SHALL succeed, list the kept files, and exclude the `year=2026` file from the filtered query, because planning reads no footer
* *AND* the executed query SHALL fail inside the scan UDF with the footer error of the unreadable file, because the scan is the first component that reads a footer
* *AND* before the overwrite, the same filtered query SHALL return the `year=2025` rows with `DISCOUNT` NULL, because the column notes declare `DISCOUNT` even though no kept file carries it
* *AND* each side of a broadcast join SHALL compare its OWN summed file sizes against the broadcast threshold, read from the seam's listing rather than from any Parquet read, so side selection costs no data access

### Scenario: Until a refresh, a query reads the current files under the declared columns

* *GIVEN* a direct-storage virtual schema that leaves `MERGE_SCHEMA` and `HIVE_PARTITIONING` absent, and since its last `CREATE VIRTUAL SCHEMA` or `REFRESH` one table has gained a data file of its declared columns, one has gained a file carrying an undeclared column, one has gained a file under a `key=value` directory segment that no earlier file of it carried, one table directory has lost its last data file while it still holds `_SUCCESS`, and a new first-level directory holding a data file has appeared
* *WHEN* an Exasol user queries the virtual schema before any `REFRESH`
* *THEN* a query on the table that gained a data file SHALL return the new file's rows together with the old ones, because the adapter lists a table's files again for every query
* *AND* the undeclared column, the new partition column, and the new directory's table SHALL stay unknown to Exasol until a `REFRESH`, per `vs-adapter/refresh-and-set-properties`, so a query naming any of them SHALL fail in Exasol with an error stating that the object is not found
* *AND* a query on the declared columns of a table that gained a file carrying an undeclared column or a new partition segment SHALL return the rows of every current data file of that table
* *AND* a query on the table whose directory lost its last data file SHALL return zero rows without an error, because the per-query listing resolves an empty file list for a table Exasol still declares

### Scenario: Until a refresh, a file the declaration cannot hold fails the query and never returns a wrong value

* *GIVEN* a direct-storage virtual schema that leaves `MERGE_SCHEMA` absent, a table whose 32-bit integer column `QTY` is declared `DECIMAL(10,0)` and has since gained a file storing `QTY` as a 64-bit integer, holding one value outside the 32-bit range but within `DECIMAL(10,0)` and one value outside `DECIMAL(10,0)`
* *AND* a second, unpartitioned table whose column `X` one file stores as a 64-bit integer, which has since gained a file storing `X` as a string, a pair no widening rule folds
* *WHEN* an Exasol user queries both tables before any `REFRESH`
* *THEN* every query that reads `QTY` from the newer file SHALL fail at scan time naming the column, both types, and the table's storage location, per `vs-adapter/column-source-notes`, because the column notes declare the 32-bit type and the scan refuses a stored type its declaration does not admit rather than cast it
* *AND* such a query MUST NOT return a truncated, wrapped, or NULL value, and this SHALL hold for the value within `DECIMAL(10,0)` too
* *AND* a query on the second table that reads `X` SHALL fail at scan time the same way, naming `X`, both types, and the table's storage location
* *AND* a query on the second table that reads only its other columns SHALL return the rows of every current data file, because planning reads no footer and the scan checks only the columns it reads
* *AND* no error message SHALL contain a credential value
