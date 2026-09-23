# Feature: Direct-Storage Table Planning

Resolves a directory of Parquet files into the engine's existing `ScanSpec` shape at plan time.
That shape serves a catalog-free table exactly as it already serves an Iceberg and a Delta table.
File-level sharding, the pushdown wire format, streaming emit, and the memory model are unchanged.

<!-- DELTA:CHANGED -->
## Background

* There is no snapshot, no transaction log, and no manifest. The reader's whole job is to turn a
  table root plus the directory options into the resolved scan every other reader returns.
* Format dispatch stays ONE exhaustive match over the scan source. That source pairs a resolved
  session with the table it reads. A third variant is a compile error at that one site.
  `vs-adapter/delta-table-planning` already records that guarantee.
* The reader owns NO listing, NO partition discovery, and NO footer parsing of its own: all three
  come from `vs-adapter/parquet-directory-seam`, the same seam table enumeration reads.
* Identity column binding is not a new mechanism. A logical field carrying neither a field-id nor a
  declared physical name binds by its own name. Delta's `none` column-mapping mode already ships
  that binding, and `datafusion-scan/scan-execution-field-id-projection` already specifies it.
* This reader has no catalog statistics to prune from. Its plan-time file list is every file under
  the table root that `vs-adapter/direct-storage-hive-partitioning` keeps. Footer-statistics
  pruning is issue [#412](https://github.com/exasol-labs/lakehouse-engine-rs/issues/412), recorded
  here as an explicit tracked exception rather than an unstated gap.
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
* *THEN* the scan source SHALL gain a THIRD variant carrying the request's object store, the table root, the resolved directory options, and the table's Exasol-declared columns, and the selection site SHALL match all three variants EXHAUSTIVELY, so a fourth table format is a compile error there rather than a silent fall-through
* *AND* the selection site MUST NOT match the catalog kind, so the permitted-site set `vs-adapter/catalog-kind-selection` records gains no file
* *AND* the direct-storage variant SHALL carry NO catalog table metadata, because this kind loads no table: its root is composed from the CONNECTION address, the namespace property, and the recorded directory name
* *AND* the Iceberg and Delta arms SHALL be UNCHANGED, and every existing Iceberg and Delta test MUST pass with no change to any assertion or expected value
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
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
<!-- /DELTA:CHANGED -->

<!-- DELTA:REMOVED -->
### Scenario: Every plan-time footer is read and the resulting cost is stated

* *GIVEN* a direct-storage table whose root holds many data files, and a query carrying a filter
* *WHEN* the adapter plans that query
* *THEN* the reader SHALL read the footers the resolved merge mode selects AT PLAN TIME, so per-file schema variation and nested structure are exact rather than inferred from one file
* *AND* the plan-time file list SHALL be EVERY data file under the table root, because this kind has no catalog statistics and no manifest to prune from, so a filter narrows the rows the scan emits without narrowing the files it reads
* *AND* that absence of pruning SHALL be recorded as an explicit tracked exception citing issue #408 for path-based partition pruning and issue #412 for footer-statistics pruning, and MUST NOT be left as an unstated gap
* *AND* the ABSENCE of any file-count or file-size bound on a table SHALL likewise be recorded here as an explicit exception rather than left unstated, because nothing refuses a directory holding hundreds of thousands of files, so the footer-read cost is unbounded at `createVirtualSchema` and at every plan. That exception SHALL cite its tracking issue (#419) inline, because a bound that is not yet designed is a follow-up rather than a shipped behavior
* *AND* each side of a broadcast join SHALL compare its OWN summed file sizes against the broadcast threshold, read from the seam's listing rather than from any Parquet read, so side selection costs no data access
<!-- /DELTA:REMOVED -->

<!-- DELTA:NEW -->
### Scenario: The kept files' footers are read at plan time and the resulting cost is stated

* *GIVEN* a direct-storage table whose root holds many data files, and a query carrying a filter
* *WHEN* the adapter plans that query
* *THEN* the reader SHALL read, AT PLAN TIME, the footers the seam selects for the resolved merge mode among the files partition pruning keeps, so per-file schema variation and nested structure are exact rather than inferred from one file
* *AND* a filter on no partition column SHALL narrow the rows the scan emits without narrowing the files it reads, because this kind has no catalog statistics and no manifest to prune from
* *AND* the absence of footer-statistics pruning SHALL be recorded as an explicit tracked exception citing issue #412, and MUST NOT be left as an unstated gap
* *AND* the ABSENCE of any file-count or file-size bound on a table SHALL likewise be recorded here as an explicit exception rather than left unstated, because nothing refuses a directory holding hundreds of thousands of files, so the footer-read cost is unbounded at `createVirtualSchema` and at every plan that prunes nothing. That exception SHALL cite its tracking issue (#419) inline, because a bound that is not yet designed is a follow-up rather than a shipped behavior
* *AND* each side of a broadcast join SHALL compare its OWN summed file sizes against the broadcast threshold, read from the seam's listing rather than from any Parquet read, so side selection costs no data access
<!-- /DELTA:NEW -->
