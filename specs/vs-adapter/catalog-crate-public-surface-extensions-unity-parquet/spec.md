# Feature: Catalog Crate Public Surface Extensions — Unity Parquet Table Planning

Records the reviewed extension of `lakehouse-catalog`'s enumerated public surface that lets the
engine plan a Unity Catalog table whose `data_source_format` is `PARQUET`: the neutral table's
ordered partition-column names and the neutral Unity column's Spark `type_json` string.

This is the sibling of `vs-adapter/catalog-crate-public-surface-extensions`, split out once that
feature's own scenario count crossed this library's per-spec organization threshold. The parent
feature keeps every prior reviewed addition; this feature owns the entry this plan
(`add-unity-parquet-table-routing`, issue #409) adds and any later addition made for the same
Unity Parquet planning seam.

## Background

* `vs-adapter/catalog-crate-structure` owns the crate's existence, its behavior-preservation
  guarantee, and its concept-level API shape. `vs-adapter/catalog-crate-public-surface-extensions`
  and this feature together own the running history of what gets ADDED to that `pub` set and why,
  each entry an explicit reviewed edit to the crate's reachability probe at
  `crates/lakehouse-catalog/tests/catalog_public_surface.rs`.
* The one-way dependency holds unchanged: no `lakehouse-catalog` source names `lakehouse-engine`,
  `iceberg`, `datafusion`, `arrow`, `parquet`, `object_store`, or `delta_kernel`. The raw Unity
  Catalog `partition_index` wire field stays crate-private; only the derived, ordered
  partition-column name list crosses the boundary. The `type_json` string crosses the boundary
  verbatim and is parsed only in `lakehouse-engine`, because this crate MUST NOT declare
  `delta_kernel`.

## Scenarios

### Scenario: The neutral table's partition columns and the Unity column's type descriptor extend the crate's public surface through an explicit reviewed edit

* *GIVEN* the enumerated public surface of `lakehouse-catalog`, its recorded neutral table and neutral column source type, and the external-vantage reachability probe that fails to compile if any enumerated item is narrowed below `pub`
* *WHEN* the engine gains a reader that plans a Unity Catalog Parquet table from the columns and partition columns its catalog declares
* *THEN* the neutral table SHALL gain its ordered partition-column names, SUPERSEDING the recorded enumeration of that type's fields, and the Unity Catalog variant of the neutral column source type SHALL gain the column's optional Spark `StructField` JSON as a plain string
* *AND* the Iceberg REST client and the direct-storage client SHALL set an EMPTY partition-column list on every neutral table they return, because neither catalog declares partition columns by name: the Iceberg reader reads its partition spec from the table metadata, and the direct-storage reader discovers its keys from the paths

### Scenario: The addition stays narrow, and its reviewed probe supersedes the format-guard clause it replaces

* *GIVEN* the same two neutral-type additions and the same external-vantage reachability probe
* *WHEN* the engine consumes the new fields to plan a Unity Catalog Parquet table
* *THEN* neither addition SHALL be a new public type, the raw Unity Catalog `partition_index` wire field SHALL stay crate-private, and the `type_json` string SHALL cross the boundary verbatim and be parsed only in `lakehouse-engine`, because this crate MUST NOT declare `delta_kernel`
* *AND* the reachability probe SHALL be edited, as an explicit reviewed change to the probe file, to construct and observe both additions from its external vantage, and that edit MUST NOT add a source-text assertion
* *AND* the recorded clause that names the Delta reader-selection arm's equality check as the guard against misrouting a Parquet-tagged table SHALL be SUPERSEDED, because the Unity scan source now selects its reader by an exhaustive match on the format tag (`vs-adapter/delta-table-planning`)
