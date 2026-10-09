# Feature: Direct-Storage Virtual-Schema Properties

Defines the three virtual-schema properties a `DIRECT_STORAGE` virtual schema reads, so an operator
scopes and tunes a catalog-free virtual schema from `CREATE VIRTUAL SCHEMA` alone. The optional
`NAMESPACE` prefix scopes which subtree holds the tables. The `MERGE_SCHEMA` switch decides whether
a table's declared schema is folded from every data file or sampled from one. The
`HIVE_PARTITIONING` switch decides whether `key=value` directory segments declare partition
columns.

<!-- DELTA:CHANGED -->
## Background

* The three properties are PLAIN virtual-schema properties, read the same way `CATALOG_KIND` is.
  Exasol round-trips the plain VS properties on every request. `NAMESPACE` is read on the
  `createVirtualSchema` path and on the `pushdown` path, because both compose a table root from it.
* `MERGE_SCHEMA` and `HIVE_PARTITIONING` decide a table's declared schema, so only
  `createVirtualSchema`, `refresh`, and `setProperties` read them. A pushdown plans from the column
  notes those requests recorded (`vs-adapter/column-source-notes`), so a changed value takes effect
  at the next `ALTER VIRTUAL SCHEMA ... REFRESH` or `SET`.
* `MERGE_SCHEMA` follows the Spark and Delta `mergeSchema` convention with one deliberate
  difference. Spark defaults it false for performance. This engine defaults it true for
  correctness. An operator who copies a Spark mental model gets a WIDER declared schema here, never
  a narrower one.
* `direct-storage/direct-storage-hive-partitioning` owns what `HIVE_PARTITIONING` does. This feature
  owns its parsing, its default, and its path to the shared seam.
* The base path a virtual schema reads is the CONNECTION address joined with `NAMESPACE`.
  `connection/connection-credentials-direct-storage` owns the CONNECTION address. This feature
  owns only the property that extends it.
* These properties are read ONLY under `CatalogKind::DirectStorage`. Under the Iceberg REST and the
  Unity Catalog kinds they are ignored rather than rejected. That matches the treatment issue #407
  records for Unity Catalog's unread `warehouse`. It also keeps cross-kind property rejection out of
  this plan.
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:REMOVED -->
### Scenario: MERGE_SCHEMA selects one footer or every footer, on both the refresh and the plan path

* *GIVEN* two direct-storage virtual schemas over the same table directory holding data files whose Parquet footers declare different column types, one virtual schema leaving `MERGE_SCHEMA` absent and one setting it to `FALSE`
* *WHEN* each virtual schema is created and then queried
* *THEN* the absent-property schema SHALL resolve `MERGE_SCHEMA` to TRUE and SHALL fold EVERY data file's footer into the table's schema at `createVirtualSchema`, and EVERY KEPT data file's footer at pushdown, where "kept" means the file survives the seam's file-keep predicate `direct-storage/direct-storage-hive-partitioning` specifies
* *AND* the `FALSE` schema SHALL read EXACTLY ONE data file's footer per table, at `createVirtualSchema` and at pushdown alike, so the two paths cannot disagree about which files were sampled
* *AND* the resolved value SHALL reach the ONE shared footer seam `direct-storage/parquet-directory-seam` specifies as its mode argument, and the adapter MUST NOT carry a second `MERGE_SCHEMA` policy for either path, because two policies over one property is the drift this single seam exists to prevent
* *AND* the property SHALL be compared case-insensitively, so `false`, `False`, and `FALSE` select the same mode
<!-- /DELTA:REMOVED -->

<!-- DELTA:NEW -->
### Scenario: MERGE_SCHEMA selects one footer or every footer at enumeration

* *GIVEN* two direct-storage virtual schemas over the same table directory holding data files whose Parquet footers declare different column types, one virtual schema leaving `MERGE_SCHEMA` absent and one setting it to `FALSE`
* *WHEN* each virtual schema is created and then queried
* *THEN* the absent-property schema SHALL resolve `MERGE_SCHEMA` to TRUE and SHALL fold EVERY data file's footer into the table's schema at `createVirtualSchema`
* *AND* the `FALSE` schema SHALL read EXACTLY ONE data file's footer per table at `createVirtualSchema`
* *AND* a pushdown under either schema SHALL read no footer, because it plans from the column notes
* *AND* the resolved value SHALL reach the ONE shared footer seam `direct-storage/parquet-directory-seam` specifies as its mode argument, and the adapter MUST NOT carry a second `MERGE_SCHEMA` policy, because two policies over one property is the drift this single seam exists to prevent
* *AND* the property SHALL be compared case-insensitively, so `false`, `False`, and `FALSE` select the same mode
<!-- /DELTA:NEW -->

<!-- DELTA:REMOVED -->
### Scenario: HIVE_PARTITIONING reaches the shared seam on both paths

* *GIVEN* a direct-storage virtual schema over a table directory whose data files sit under `key=value` path segments, created with `HIVE_PARTITIONING` absent, `'TRUE'`, or `'FALSE'`
* *WHEN* the adapter creates that virtual schema and plans a query over that table
* *THEN* the adapter SHALL resolve an absent value to TRUE and SHALL compare the value case-insensitively
* *AND* the resolved value SHALL reach the ONE shared seam `direct-storage/parquet-directory-seam` specifies as its partitioning switch, at `createVirtualSchema` and at pushdown alike, and the adapter MUST NOT carry a second `HIVE_PARTITIONING` policy for either path
* *AND* the seam's merge mode and partitioning switch SHALL be derived from the resolved properties at ONE site, so the two paths cannot resolve them differently
<!-- /DELTA:REMOVED -->

<!-- DELTA:NEW -->
### Scenario: HIVE_PARTITIONING decides the declared partition columns at enumeration

* *GIVEN* a direct-storage virtual schema over a table directory whose data files sit under `key=value` path segments, created with `HIVE_PARTITIONING` absent, `'TRUE'`, or `'FALSE'`
* *WHEN* the adapter creates that virtual schema and plans a query over that table
* *THEN* the adapter SHALL resolve an absent value to TRUE and SHALL compare the value case-insensitively
* *AND* the resolved value SHALL reach the ONE shared seam `direct-storage/parquet-directory-seam` specifies as its partitioning switch at `createVirtualSchema`, and the adapter MUST NOT carry a second `HIVE_PARTITIONING` policy
* *AND* the pushdown SHALL fill partition values only for the partition columns the column notes record, so a virtual schema created with `'FALSE'` fills none
* *AND* the seam's merge mode and partitioning switch SHALL be derived from the resolved properties at ONE site
<!-- /DELTA:NEW -->
