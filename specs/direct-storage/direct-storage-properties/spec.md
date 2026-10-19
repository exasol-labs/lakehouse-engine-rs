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

### Scenario: NAMESPACE is optional under direct storage and scopes the table subtree

* *GIVEN* a `createVirtualSchema` request resolving `CatalogKind::DirectStorage`, whose CONNECTION address is a storage base path
* *WHEN* the adapter resolves the virtual schema's base path
* *THEN* a request that supplies NO `NAMESPACE` property SHALL succeed and SHALL resolve the base path to the CONNECTION address alone, SUPERSEDING for this kind the `vs-adapter/create-virtual-schema` rule that an absent `NAMESPACE` fails with a required-property error, because direct storage has no catalog namespace to name and the CONNECTION address alone denotes a complete storage subtree
* *AND* a request that supplies a non-empty `NAMESPACE` SHALL resolve the base path to the CONNECTION address followed by that value, joined by exactly ONE `/` regardless of whether either side already carries one, so `s3://bucket/lake/` with `finance` and `s3://bucket/lake` with `finance/` both resolve `s3://bucket/lake/finance/`
* *AND* the adapter SHALL treat the `NAMESPACE` value as SLASH-delimited, so `finance/eu` names the subtree two levels below the address, and MUST NOT split it on `.`, because a directory name legitimately contains a dot
* *AND* the adapter SHALL keep requiring `NAMESPACE` under the Iceberg REST and the Unity Catalog kinds, so this relaxation is scoped to the one kind that has no catalog

### Scenario: A NAMESPACE carrying a scheme or a leading slash is rejected at create time

* *GIVEN* a `createVirtualSchema` request resolving `CatalogKind::DirectStorage` whose `NAMESPACE` property carries a URI scheme (`s3://other/finance`) or begins with `/` (`/finance`)
* *WHEN* the adapter resolves the virtual schema's base path
* *THEN* the adapter SHALL return an error naming `NAMESPACE`, naming the rejected value, and stating that the property is a path RELATIVE to the CONNECTION address
* *AND* the adapter MUST NOT strip the scheme, strip the leading `/`, or otherwise repair the value, because both spellings express an intent the property cannot carry: a scheme names a different store than the CONNECTION addresses, and a leading `/` reads as an absolute path that the join would silently relativize
* *AND* the rejection SHALL happen at `CREATE VIRTUAL SCHEMA`, before any object-store request, so a misconfigured prefix costs zero storage access
* *AND* the error message MUST NOT contain any credential value

### Scenario: An unparseable MERGE_SCHEMA or HIVE_PARTITIONING value is rejected, never defaulted

* *GIVEN* a `createVirtualSchema` request resolving `CatalogKind::DirectStorage` whose `MERGE_SCHEMA` or `HIVE_PARTITIONING` property carries a value that is neither a true spelling nor a false spelling
* *WHEN* the adapter resolves that property
* *THEN* the adapter SHALL return an error naming the property, naming the rejected value, and naming the accepted spellings
* *AND* the adapter MUST NOT fall back to the property's default, because `MERGE_SCHEMA` decides how wide a table's declared schema is and a typo silently selecting the opposite mode returns a narrower schema rather than an error
* *AND* an ABSENT or EMPTY value SHALL resolve to the property's default — TRUE for both — and SHALL NOT be treated as unparseable, so a virtual schema that names neither property keeps working
* *AND* this rejection SHALL apply to these two properties only and MUST NOT change how any other virtual-schema property handles an unrecognized value
* *AND* all three of this feature's properties SHALL be IGNORED rather than rejected under the Iceberg REST and the Unity Catalog kinds, even when a value is unparseable, because those kinds never read them and rejecting a property one kind ignores would make a virtual schema fail on a value nothing consumes

### Scenario: MERGE_SCHEMA selects one footer or every footer at enumeration

* *GIVEN* two direct-storage virtual schemas over the same table directory holding data files whose Parquet footers declare different column types, one virtual schema leaving `MERGE_SCHEMA` absent and one setting it to `FALSE`
* *WHEN* each virtual schema is created and then queried
* *THEN* the absent-property schema SHALL resolve `MERGE_SCHEMA` to TRUE and SHALL fold EVERY data file's footer into the table's schema at `createVirtualSchema`
* *AND* the `FALSE` schema SHALL read EXACTLY ONE data file's footer per table at `createVirtualSchema`
* *AND* a pushdown under either schema SHALL read no footer, because it plans from the column notes
* *AND* the resolved value SHALL reach the ONE shared footer seam `direct-storage/parquet-directory-seam` specifies as its mode argument, and the adapter MUST NOT carry a second `MERGE_SCHEMA` policy, because two policies over one property is the drift this single seam exists to prevent
* *AND* the property SHALL be compared case-insensitively, so `false`, `False`, and `FALSE` select the same mode

### Scenario: HIVE_PARTITIONING decides the declared partition columns at enumeration

* *GIVEN* a direct-storage virtual schema over a table directory whose data files sit under `key=value` path segments, created with `HIVE_PARTITIONING` absent, `'TRUE'`, or `'FALSE'`
* *WHEN* the adapter creates that virtual schema and plans a query over that table
* *THEN* the adapter SHALL resolve an absent value to TRUE and SHALL compare the value case-insensitively
* *AND* the resolved value SHALL reach the ONE shared seam `direct-storage/parquet-directory-seam` specifies as its partitioning switch at `createVirtualSchema`, and the adapter MUST NOT carry a second `HIVE_PARTITIONING` policy
* *AND* the pushdown SHALL fill partition values only for the partition columns the column notes record, so a virtual schema created with `'FALSE'` fills none
* *AND* the seam's merge mode and partitioning switch SHALL be derived from the resolved properties at ONE site
