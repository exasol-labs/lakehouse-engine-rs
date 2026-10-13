# Feature: Direct-Storage Table Planning

Resolves a directory of Parquet files into the engine's existing `ScanSpec` shape at plan time.
That shape serves a catalog-free table exactly as it already serves an Iceberg and a Delta table.
File-level sharding, the pushdown wire format, streaming emit, and the memory model are unchanged.

## Background

* There is no snapshot, no transaction log, and no manifest. The reader's whole job is to turn a
  table root plus the table's column notes into the resolved scan every other reader returns.
* Format dispatch stays ONE exhaustive match over the scan source. That source pairs a resolved
  session with the table it reads. A third variant is a compile error at that one site.
  `delta/delta-table-planning` already records that guarantee.
* The reader owns NO listing and NO partition parsing of its own: both come from the listing answer
  of `direct-storage/parquet-directory-seam-file-listing`, the seam table enumeration also reads.
  The reader parses NO footer. Its logical schema, partition columns, and refused columns come from
  the column notes that enumeration recorded (`vs-adapter/column-source-notes`).
* Identity column binding is not a new mechanism. A logical field carrying neither a field-id nor a
  declared physical name binds by its own name. Delta's `none` column-mapping mode already ships
  that binding, and `scan-read-path/scan-execution-field-id-projection` already specifies it.
* This reader has no catalog statistics to prune from. Its plan-time file list is every file under
  the table root that `direct-storage/direct-storage-hive-partitioning` keeps. Footer-statistics
  pruning would need the footer reads planning no longer performs, so issue
  [#412](https://github.com/exasol-labs/lakehouse-engine-rs/issues/412) is closed as superseded by
  #426. This is recorded here as an explicit exception rather than an unstated gap.
* Nothing bounds a table's file count. Enumeration reads the footers the merge mode selects, so its
  footer-read cost is unbounded on a directory holding hundreds of thousands of files. That bound is
  issue [#419](https://github.com/exasol-labs/lakehouse-engine-rs/issues/419), recorded here as an
  explicit exception.
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
* *THEN* the scan source SHALL gain a THIRD variant carrying the request's object store and the table root, and the selection site SHALL match all three variants EXHAUSTIVELY, so a fourth table format is a compile error there rather than a silent fall-through
* *AND* that variant MUST NOT carry the directory options or a declared-column list, because the table's declaration reaches every reader from the column notes, and the directory options take effect only at enumeration
* *AND* the selection site MUST NOT match the catalog kind, so the permitted-site set `vs-adapter/catalog-kind-selection` records gains no file
* *AND* the direct-storage variant SHALL carry NO catalog table metadata, because this kind loads no table: its root is composed from the CONNECTION address, the namespace property, and the recorded directory name
* *AND* the Iceberg and Delta arms SHALL be UNCHANGED, and every existing Iceberg and Delta test MUST pass with no change to any assertion or expected value, except the error text of the two refusals that now run before the format is known (a table with no mappable column, and a refused partition column), which names the virtual table instead of a format label

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
* *AND* this SHALL be a recorded, deliberate trade-off rather than a defect of this reader, because the operator selected a `CATALOG_KIND` that names no table format, and an operator who wants table-format semantics selects the catalog kind that supplies them
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
