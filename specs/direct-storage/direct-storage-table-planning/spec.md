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

### Scenario: The format reader is selected at the same one site for a third source

* *GIVEN* the scan source whose variant pairs one resolved session with the table it reads, matched exhaustively at exactly ONE site returning a boxed format reader
* *WHEN* the adapter selects the reader for a direct-storage table
* *THEN* the scan source SHALL gain a THIRD variant carrying the request's object store, the table root, the resolved directory options, and the table's Exasol-declared columns, and the selection site SHALL match all three variants EXHAUSTIVELY, so a fourth table format is a compile error there rather than a silent fall-through
* *AND* the selection site MUST NOT match the catalog kind, so the permitted-site set `vs-adapter/catalog-kind-selection` records gains no file
* *AND* the direct-storage variant SHALL carry NO catalog table metadata, because this kind loads no table: its root is composed from the CONNECTION address, the namespace property, and the recorded directory name
* *AND* the Iceberg and Delta arms SHALL be UNCHANGED, and every existing Iceberg and Delta test MUST pass with no change to any assertion or expected value

### Scenario: A direct-storage table resolves its files and schema through the shared seam

* *GIVEN* a direct-storage table root holding Parquet data files at more than one depth, and the directory options the virtual schema resolved
* *WHEN* the direct-storage format reader resolves that table's scan
* *THEN* the reader SHALL obtain the file list, each file's byte size and partition values, the partition columns, and the folded schema from the ONE shared directory seam, and MUST NOT list objects or parse a footer or a path itself
* *AND* each returned file entry SHALL carry its path, its byte size, an EMPTY delete-mechanism list, and the seam's partition values, so an unpartitioned table's entries keep the compact form an unpartitioned delete-free Iceberg scan already produces
* *AND* each file entry's path SHALL be percent-encoded segment by segment, so a key segment carrying `%`, `#`, or `?` resolves at scan time to the object the listing returned
* *AND* every returned logical field SHALL carry NEITHER a field-id NOR a declared physical name, so the scan binds it by its own name through the identity binding already shipped, and the reader MUST NOT synthesize an ordinal field-id
* *AND* the returned scan SHALL carry the seam's partition columns and an EMPTY name-mapping list, because a raw directory declares no name mapping
* *AND* the returned effective storage SHALL be the CONNECTION's static backend unchanged, because this kind reaches no credential-vending catalog
* *AND* the returned table root SHALL be the composed directory root, so the shard-invariant common spec carries it once and each file path is encoded relative to it by the existing rules
* *AND* the reader SHALL refuse NO column for this kind, because every Parquet type the seam folds resolves to a declared Exasol type through the existing mapping or its string fallback

### Scenario: The kept files' footers are read at plan time and the resulting cost is stated

* *GIVEN* a direct-storage table whose root holds many data files, and a query carrying a filter
* *WHEN* the adapter plans that query
* *THEN* the reader SHALL read, AT PLAN TIME, the footers the seam selects for the resolved merge mode among the files partition pruning keeps, so per-file schema variation and nested structure are exact rather than inferred from one file
* *AND* a filter on no partition column SHALL narrow the rows the scan emits without narrowing the files it reads, because this kind has no catalog statistics and no manifest to prune from
* *AND* the absence of footer-statistics pruning SHALL be recorded as an explicit tracked exception citing issue #412, and MUST NOT be left as an unstated gap
* *AND* the ABSENCE of any file-count or file-size bound on a table SHALL likewise be recorded here as an explicit exception rather than left unstated, because nothing refuses a directory holding hundreds of thousands of files, so the footer-read cost is unbounded at `createVirtualSchema` and at every plan that prunes nothing. That exception SHALL cite its tracking issue (#419) inline, because a bound that is not yet designed is a follow-up rather than a shipped behavior
* *AND* each side of a broadcast join SHALL compare its OWN summed file sizes against the broadcast threshold, read from the seam's listing rather than from any Parquet read, so side selection costs no data access

### Scenario: An Iceberg or Delta table directory read as raw Parquet ignores its table format

* *GIVEN* a storage base path under which an operator has placed a directory that is an Iceberg table or a Delta table, and a virtual schema created over that base path with the direct-storage catalog kind
* *WHEN* an Exasol user queries the virtual table that directory produces
* *THEN* the engine SHALL read every Parquet file under that directory as raw data, and SHALL apply NO Delta deletion vector, NO Delta `remove` action, NO Iceberg delete file, and NO Iceberg snapshot selection
* *AND* the returned rows MAY therefore include rows the table format considers deleted, and MAY include one logical row more than once where several snapshots reference overlapping files
* *AND* this SHALL be a recorded, deliberate trade-off rather than a defect of this reader, because the operator selected a `CATALOG_KIND` that names no table format, and an operator who wants table-format semantics selects the catalog kind that supplies them
* *AND* the trade-off SHALL be stated in the user documentation alongside the direct-storage recipe, naming the correct `CATALOG_KIND` as the fix, so an operator meets it before the result surprises them
* *AND* the engine MUST NOT detect a table-format directory and refuse it, because a user may legitimately want the raw files, and a detection heuristic would make a supported read fail on a directory layout it guessed wrong about

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
* *THEN* a query that returns the `QTY` value outside `DECIMAL(10,0)` SHALL fail with a numeric out-of-range error, and MUST NOT return a truncated, wrapped, or NULL value, because the declared type bounds what Exasol accepts from the scan and the scan emits the stored value without narrowing it
* *AND* a query that returns only the `QTY` value within `DECIMAL(10,0)` SHALL return that value unchanged, because the declared Exasol type, not the 32-bit physical type of the older file, bounds the emitted value
* *AND* every query on the second table, including one that does not read `X`, SHALL fail with the fold error naming `X`, both types, and both file paths, per `direct-storage/parquet-directory-seam`, because the planner folds every kept file's footer before it plans any column, and the query MUST NOT return a row
* *AND* no error message SHALL contain a credential value
