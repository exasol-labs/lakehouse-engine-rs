# Feature: Direct-Storage Virtual-Schema Properties

A `DIRECT_STORAGE` virtual schema reads three properties. `NAMESPACE` scopes which subtree holds the tables. `MERGE_SCHEMA` decides whether a table's declared schema is folded from every data file or sampled from one. `HIVE_PARTITIONING` decides whether `key=value` directory segments declare partition columns.

## Background

* The three properties are plain virtual-schema properties, read the same way `CATALOG_KIND` is. `NAMESPACE` is read on the `createVirtualSchema` and `pushdown` paths, because both compose a table root from it.
* `MERGE_SCHEMA` and `HIVE_PARTITIONING` decide a table's declared schema, so only `createVirtualSchema`, `refresh`, and `setProperties` read them. A pushdown plans from the column notes those requests recorded (`vs-adapter/column-source-notes`), so a changed value takes effect at the next `ALTER VIRTUAL SCHEMA ... REFRESH` or `SET`.
* `MERGE_SCHEMA` follows the Spark and Delta `mergeSchema` convention, except that it defaults to true. Spark defaults it to false. The declared schema is therefore wider here, never narrower.
* `direct-storage/direct-storage-hive-partitioning` owns what `HIVE_PARTITIONING` does. This feature owns its parsing, its default, and its path to the shared seam.
* The base path a virtual schema reads is the CONNECTION address joined with `NAMESPACE`. `connection/connection-credentials-direct-storage` owns the CONNECTION address.
* The properties are read only under `CatalogKind::DirectStorage`. The Iceberg REST and Unity Catalog kinds ignore them and do not reject them.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: An unparseable MERGE_SCHEMA or HIVE_PARTITIONING value is rejected, never defaulted

* *GIVEN* a `createVirtualSchema` request resolving `CatalogKind::DirectStorage` whose `MERGE_SCHEMA` or `HIVE_PARTITIONING` property carries a value that is neither a true spelling nor a false spelling
* *WHEN* the adapter resolves that property
* *THEN* the adapter SHALL return an error naming the property, naming the rejected value, and naming the accepted spellings
* *AND* the adapter MUST NOT fall back to the property's default, because `MERGE_SCHEMA` decides how wide a table's declared schema is and a typo silently selecting the opposite mode returns a narrower schema rather than an error
* *AND* an ABSENT or EMPTY value SHALL resolve to the property's default, TRUE for both, and SHALL NOT be treated as unparseable, so a virtual schema that names neither property keeps working
* *AND* this check SHALL stay the only check of these two properties' values, because `vs-adapter/property-validation` checks only their names, so an unparseable value is reported once
* *AND* all three of this feature's properties SHALL be IGNORED rather than rejected under the Iceberg REST and the Unity Catalog kinds, even when a value is unparseable, because those kinds never read them and rejecting a property one kind ignores would make a virtual schema fail on a value nothing consumes
<!-- /DELTA:CHANGED -->
