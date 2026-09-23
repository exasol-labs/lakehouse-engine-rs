# Feature: Direct-Storage Virtual-Schema Properties

Defines the three virtual-schema properties a `DIRECT_STORAGE` virtual schema reads, so an operator
scopes and tunes a catalog-free virtual schema from `CREATE VIRTUAL SCHEMA` alone. The optional
`NAMESPACE` prefix scopes which subtree holds the tables. The `MERGE_SCHEMA` switch decides whether
a table's declared schema is folded from every data file or sampled from one. The
`HIVE_PARTITIONING` switch is parsed and validated by this plan ahead of its implementation.

## Background

* The three properties are PLAIN virtual-schema properties, read the same way `CATALOG_KIND` is.
  Exasol round-trips the plain VS properties on every request. Each property is therefore readable
  on the `createVirtualSchema` path and on the `pushdown` path without a persisted note.
* `MERGE_SCHEMA` follows the Spark and Delta `mergeSchema` convention with one deliberate
  difference. Spark defaults it false for performance. This engine defaults it true for
  correctness. An operator who copies a Spark mental model gets a WIDER declared schema here, never
  a narrower one.
* `HIVE_PARTITIONING` is parsed and validated by this plan and acted on by issue
  [#408](https://github.com/exasol-labs/lakehouse-engine-rs/issues/408). Parsing it now is what
  keeps a virtual schema created today from being rejected by the adapter version that implements
  it.
* The base path a virtual schema reads is the CONNECTION address joined with `NAMESPACE`.
  `vs-adapter/connection-credentials-direct-storage` owns the CONNECTION address. This feature
  owns only the property that extends it.
* These properties are read ONLY under `CatalogKind::DirectStorage`. Under the Iceberg REST and the
  Unity Catalog kinds they are ignored rather than rejected. That matches the treatment issue #407
  records for Unity Catalog's unread `warehouse`. It also keeps cross-kind property rejection out of
  this plan.

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

### Scenario: MERGE_SCHEMA selects one footer or every footer, on both the refresh and the plan path

* *GIVEN* two direct-storage virtual schemas over the same table directory holding data files whose Parquet footers declare different column types, one virtual schema leaving `MERGE_SCHEMA` absent and one setting it to `FALSE`
* *WHEN* each virtual schema is created and then queried
* *THEN* the absent-property schema SHALL resolve `MERGE_SCHEMA` to TRUE and SHALL fold EVERY data file's footer into the table's schema, at `createVirtualSchema` and at pushdown alike
* *AND* the `FALSE` schema SHALL read EXACTLY ONE data file's footer per table, at `createVirtualSchema` and at pushdown alike, so the two paths cannot disagree about which files were sampled
* *AND* the resolved value SHALL reach the ONE shared footer seam `vs-adapter/parquet-directory-seam` specifies as its mode argument, and the adapter MUST NOT carry a second `MERGE_SCHEMA` policy for either path, because two policies over one property is the drift this single seam exists to prevent
* *AND* the property SHALL be compared case-insensitively, so `false`, `False`, and `FALSE` select the same mode

### Scenario: An unparseable MERGE_SCHEMA or HIVE_PARTITIONING value is rejected, never defaulted

* *GIVEN* a `createVirtualSchema` request resolving `CatalogKind::DirectStorage` whose `MERGE_SCHEMA` or `HIVE_PARTITIONING` property carries a value that is neither a true spelling nor a false spelling
* *WHEN* the adapter resolves that property
* *THEN* the adapter SHALL return an error naming the property, naming the rejected value, and naming the accepted spellings
* *AND* the adapter MUST NOT fall back to the property's default, because `MERGE_SCHEMA` decides how wide a table's declared schema is and a typo silently selecting the opposite mode returns a narrower schema rather than an error
* *AND* an ABSENT or EMPTY value SHALL resolve to the property's default — TRUE for both — and SHALL NOT be treated as unparseable, so a virtual schema that names neither property keeps working
* *AND* this rejection SHALL apply to these two properties only and MUST NOT change how any other virtual-schema property handles an unrecognized value
* *AND* all three of this feature's properties SHALL be IGNORED rather than rejected under the Iceberg REST and the Unity Catalog kinds, even when a value is unparseable, because those kinds never read them and rejecting a property one kind ignores would make a virtual schema fail on a value nothing consumes

### Scenario: HIVE_PARTITIONING is parsed and validated but not yet acted on

* *GIVEN* a direct-storage virtual schema over a table directory whose data files sit under `key=value` path segments, created with `HIVE_PARTITIONING` absent, `'TRUE'`, or `'FALSE'`
* *WHEN* the adapter creates that virtual schema and plans a query over that table
* *THEN* the adapter SHALL accept all three inputs and SHALL resolve an absent value to TRUE
* *AND* the resolved value SHALL change NO declared column, NO resolved file list, and NO generated SQL in this plan, so all three inputs produce identical output
* *AND* the adapter SHALL declare NO partition column and SHALL carry an EMPTY per-file partition-value map for every direct-storage table, so a `key=value` segment contributes no column until issue [#408](https://github.com/exasol-labs/lakehouse-engine-rs/issues/408) lands
* *AND* this deferral SHALL be recorded as an explicit tracked exception citing issue #408, and MUST NOT be left as an unstated gap
