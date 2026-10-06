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
  the table root that `vs-adapter/direct-storage-hive-partitioning` keeps and whose footer
  statistics `vs-adapter/direct-storage-statistics-pruning` cannot rule out.
* **Apache Iceberg and Delta specification check.** This reader implements NEITHER format and
  claims conformance to neither. Reading a directory that happens to hold an Iceberg or a Delta
  table as raw Parquet is therefore not a deviation from either specification. It is a different
  read, selected by the operator through `CATALOG_KIND`. The scenario below states that trade-off
  normatively, with the correct `CATALOG_KIND` named as the fix. The behavior is therefore a
  recorded decision rather than a silent gap.
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: The kept files' footers are read at plan time and the resulting cost is stated

* *GIVEN* a direct-storage table whose root holds many data files, and a query carrying a filter
* *WHEN* the adapter plans that query
* *THEN* the reader SHALL read, AT PLAN TIME, the footers the seam selects for the resolved merge mode among the files partition pruning keeps, so per-file schema variation and nested structure are exact rather than inferred from one file
* *AND* a filter on no partition column SHALL narrow the files only as far as `vs-adapter/direct-storage-statistics-pruning` proves from those same footers, and SHALL otherwise narrow only the rows the scan emits, because this kind has no catalog statistics and no manifest to prune from
* *AND* the ABSENCE of any file-count or file-size bound on a table SHALL likewise be recorded here as an explicit exception rather than left unstated, because nothing refuses a directory holding hundreds of thousands of files, so the footer-read cost is unbounded at `createVirtualSchema` and at every plan that prunes nothing. That exception SHALL cite its tracking issue (#419) inline, because a bound that is not yet designed is a follow-up rather than a shipped behavior
* *AND* each side of a broadcast join SHALL compare its OWN summed file sizes against the broadcast threshold, read from the seam's listing rather than from any Parquet read, so side selection costs no data access
<!-- /DELTA:CHANGED -->
