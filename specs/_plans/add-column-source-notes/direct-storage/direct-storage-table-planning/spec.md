# Feature: Direct-Storage Table Planning

Resolves a directory of Parquet files into the engine's existing `ScanSpec` shape at plan time.
That shape serves a catalog-free table exactly as it already serves an Iceberg and a Delta table.
File-level sharding, the pushdown wire format, streaming emit, and the memory model are unchanged.

<!-- DELTA:CHANGED -->
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
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: The format reader is selected at the same one site for a third source

* *GIVEN* the scan source whose variant pairs one resolved session with the table it reads, matched exhaustively at exactly ONE site returning a boxed format reader
* *WHEN* the adapter selects the reader for a direct-storage table
* *THEN* the scan source SHALL gain a THIRD variant carrying the request's object store and the table root, and the selection site SHALL match all three variants EXHAUSTIVELY, so a fourth table format is a compile error there rather than a silent fall-through
* *AND* that variant MUST NOT carry the directory options or a declared-column list, because the table's declaration reaches every reader from the column notes, and the directory options take effect only at enumeration
* *AND* the selection site MUST NOT match the catalog kind, so the permitted-site set `vs-adapter/catalog-kind-selection` records gains no file
* *AND* the direct-storage variant SHALL carry NO catalog table metadata, because this kind loads no table: its root is composed from the CONNECTION address, the namespace property, and the recorded directory name
* *AND* the Iceberg and Delta arms SHALL be UNCHANGED, and every existing Iceberg and Delta test MUST pass with no change to any assertion or expected value
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
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
<!-- /DELTA:CHANGED -->

<!-- DELTA:REMOVED -->
### Scenario: The kept files' footers are read at plan time and the resulting cost is stated

* *GIVEN* a direct-storage table whose root holds many data files, and a query carrying a filter
* *WHEN* the adapter plans that query
* *THEN* the reader SHALL read, AT PLAN TIME, the footers the seam selects for the resolved merge mode among the files partition pruning keeps, so per-file schema variation and nested structure are exact rather than inferred from one file
* *AND* a filter on no partition column SHALL narrow the rows the scan emits without narrowing the files it reads, because this kind has no catalog statistics and no manifest to prune from
* *AND* the absence of footer-statistics pruning SHALL be recorded as an explicit tracked exception citing issue #412, and MUST NOT be left as an unstated gap
* *AND* the ABSENCE of any file-count or file-size bound on a table SHALL likewise be recorded here as an explicit exception rather than left unstated, because nothing refuses a directory holding hundreds of thousands of files, so the footer-read cost is unbounded at `createVirtualSchema` and at every plan that prunes nothing. That exception SHALL cite its tracking issue (#419) inline, because a bound that is not yet designed is a follow-up rather than a shipped behavior
* *AND* each side of a broadcast join SHALL compare its OWN summed file sizes against the broadcast threshold, read from the seam's listing rather than from any Parquet read, so side selection costs no data access
<!-- /DELTA:REMOVED -->

<!-- DELTA:NEW -->
### Scenario: A direct-storage pushdown plans without reading a footer

* *GIVEN* a direct-storage table `sales/` holding `year=2025/p1.parquet` and `year=2026/p2.parquet`, where only the `year=2026` file carries a column `DISCOUNT`, and a table `dims/` holding one file, both enumerated by `createVirtualSchema`
* *AND* the data files of `sales/` afterwards overwritten with bytes that are not Parquet
* *WHEN* `EXPLAIN VIRTUAL` plans `SELECT ID, DISCOUNT FROM SALES WHERE YEAR = '2025'`, and a join of `SALES` with `DIMS`, and the first query then runs
* *THEN* both plans SHALL succeed, list the kept files, and exclude the `year=2026` file from the filtered query, because planning reads no footer
* *AND* the executed query SHALL fail inside the scan UDF with the footer error of the unreadable file, because the scan is the first component that reads a footer
* *AND* before the overwrite, the same filtered query SHALL return the `year=2025` rows with `DISCOUNT` NULL, because the column notes declare `DISCOUNT` even though no kept file carries it
* *AND* each side of a broadcast join SHALL compare its OWN summed file sizes against the broadcast threshold, read from the seam's listing rather than from any Parquet read, so side selection costs no data access
<!-- /DELTA:NEW -->
