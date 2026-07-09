# Feature: DataFusion Scan Execution — Field-Id-Based Column Projection

Extends the scan UDF to bind columns by Iceberg field-id (with a physical-name fallback)
when the scan spec carries a logical schema, so projection is correct across Iceberg
schema evolution — renamed, dropped, and added-nullable columns all resolve correctly
per file without rewriting the schema adapter.

## Background

* When the scan spec carries a logical schema (a list of `{field_id, name, arrow_type,
  nullable}` tuples), the scan UDF registers the `ListingTable` with that schema (each
  field tagged with `PARQUET:field_id` metadata) and installs a `FieldIdExprAdapter`
  that resolves each logical column to its physical Parquet column by, in order: (1) an
  embedded `PARQUET:field_id` match; (2) for a physical field that carries NO embedded
  field-id, the table's `schema.name-mapping.default` mapping of that physical name to a
  field-id present in the logical schema; (3) a physical-name match. Steps (2) and (3)
  apply only to fields without an embedded field-id; step (2) augments — never replaces —
  the physical-name fallback of step (3).
<!-- DELTA:CHANGED -->
* The adapter delegates type divergence → cast and the ordinary added-nullable-column
  case (nullable column absent from a file, and NOT covered by a no-null-fill guard →
  NULL) to `DefaultPhysicalExprAdapter`, keeping the change minimal. BEFORE delegating,
  the `FieldIdExprAdapterFactory` applies a per-file **no-null-fill guard**: for a logical
  field-id that resolves to no physical column in the file AND is in the query's guard set
  (see the guard-set bullet below), the factory returns a clean, credential-free error
  instead of letting the default adapter substitute a value. This converts the two Iceberg
  column-projection cases where a NULL (or a misattributed required-missing error) would be
  silently wrong — an identity-partition source column and a column with a defined
  `initial-default` — into fail-loud errors, pending full value reconstruction (out of
  scope; tracked — see the Out-of-scope bullet).
<!-- /DELTA:CHANGED -->
* The adapter is applied per file by the Parquet opener, so files with divergent
  physical layouts within one shard each bind correctly.
* The `schema.name-mapping.default` table property is resolved ONCE per query in the VS
  planning layer (at `resolve_file_list`, alongside the logical schema), parsed into a
  flat list of `{name, field_id}` entries, and threaded into the scan spec. The scan UDF
  never re-reads Iceberg table properties. Only the top-level (flat) name-mapping entries
  are honored; nested `fields` entries for struct / map / list children are NOT parsed in
  this phase (see Out-of-scope).
<!-- DELTA:NEW -->
* The **no-null-fill guard set** is resolved ONCE per query in the VS planning layer
  (at `resolve_file_list`, alongside the logical schema and name-mapping) from the table's
  own metadata, and threaded — shard-invariant — into the scan spec as a list of
  `{field_id, reason}` entries. A field-id is added to the guard set when EITHER: (a) it is
  the `source_id` of an Identity-Transform partition field in any of the table's partition
  specs (Iceberg column-projection rule #1 — a value missing from a data file is
  reconstructable from that file's partition metadata); OR (b) the field carries a non-null
  Iceberg `initial-default` in the current schema (rule #3 — a value missing from a data
  file that predates the field's addition takes the declared default). The scan UDF never
  re-reads Iceberg table properties or partition specs; it consumes only the threaded guard
  set. When a field-id qualifies under both, the identity-partition reason is recorded (its
  error names partition reconstruction). When the table has no identity-partition source and
  no field with a non-null initial-default the guard set is empty and behavior is unchanged.
<!-- /DELTA:NEW -->
* When the scan spec does NOT carry a logical schema, the field-id adapter is not
  installed and the scan falls back to first-file schema inference unchanged.
<!-- DELTA:CHANGED -->
* Out-of-scope (fail-loud now, full value materialization deferred and tracked):
  reconstructing an identity-partition source column's value from a data file's partition
  metadata when the field-id is absent from the file — Iceberg column-projection rule #1
  (issue #99, backlog BL-003); and filling ANY added column (optional OR required) from its
  Iceberg `initial-default` — rule #3 (issue #27, whose scope is broadened here from
  required-only to any-nullability, backlog BL-004). Both cases now return a clean error via
  the no-null-fill guard rather than silently wrong data; materializing the correct value is
  the deferred follow-on. Also out of scope: parsing nested `fields` entries of
  `schema.name-mapping.default` for struct / map / list children (#83).
<!-- /DELTA:CHANGED -->

## Scenarios

### Scenario: Column projection binds by Iceberg field-id across physical layouts

* *GIVEN* a scan spec whose logical schema carries a column bound to a stable Iceberg field-id
* *AND* the assigned files include one file whose physical Parquet column for that field-id has a different physical name than the current logical name (a renamed column), each physical field tagged with its `PARQUET:field_id`
* *WHEN* the scan UDF reads that file
* *THEN* the UDF SHALL resolve each logical column to its physical column by matching the logical field's `PARQUET:field_id` against the physical fields' `PARQUET:field_id`, independent of physical name
* *AND* the emitted values for the renamed column SHALL be the real physical values (never NULL) under the current logical name
* *AND* the resolution SHALL run per file, so files with divergent physical layouts within one scan SHALL each bind correctly

### Scenario: Field-id resolution honors schema.name-mapping.default for a file field without an embedded field-id

* *GIVEN* a scan spec whose logical schema carries a column bound to a stable Iceberg field-id under its current logical name, and a threaded `schema.name-mapping.default` entry mapping a physical column name to that field-id
* *AND* an assigned file whose physical field for that column carries NO embedded `PARQUET:field_id` and whose physical name equals the mapped name but differs from the current logical name (a rename resolved only by the name-mapping)
* *WHEN* the scan UDF reads that file
* *THEN* the UDF SHALL resolve that logical column to the physical column named by the matching name-mapping entry, binding it to the field-id the mapping supplies
* *AND* the emitted values for that column SHALL be the real physical values (never NULL) under the current logical name
* *AND* an embedded `PARQUET:field_id` on a physical field SHALL take precedence over the name-mapping for that field (the name-mapping applies only to fields lacking an embedded field-id)

### Scenario: Field-id resolution falls back to physical name when no name-mapping resolves a file field without an embedded field-id

* *GIVEN* a scan spec whose logical schema carries field-ids
* *AND* an assigned file whose physical fields carry no embedded `PARQUET:field_id`
* *AND* either no `schema.name-mapping.default` is threaded into the spec, OR the threaded name-mapping does not map a given physical field's name to a field-id present in the logical schema
* *WHEN* the scan UDF reads that file
* *THEN* for each such unmapped physical field the UDF SHALL resolve the logical column to a physical column whose physical name equals the logical (current) name
* *AND* this physical-name fallback SHALL remain unchanged from prior behavior for the no-name-mapping case and for any field the mapping does not cover

### Scenario: The VS resolves schema.name-mapping.default once per query into the scan spec

* *GIVEN* a virtual schema query whose Iceberg table defines a `schema.name-mapping.default` property
* *WHEN* the VS planning layer resolves the file list for that query
* *THEN* the VS SHALL parse the property exactly once, into a flat list of `{name, field_id}` entries taken from the top-level mapping objects (each name in an entry's `names` mapped to that entry's `field-id`), and thread it into the shard-invariant scan spec alongside the logical schema
* *AND* the VS SHALL skip any top-level mapping object that carries no `field-id`, and SHALL NOT recurse into nested `fields` child entries
* *AND* when the table defines no `schema.name-mapping.default` property the threaded name-mapping SHALL be empty, so scan specs that carry no name-mapping deserialize unchanged (backward-compatible)
* *AND* when the property is present but is not valid name-mapping JSON the VS SHALL fail the query with a clean plan-time error naming the malformed property, and MUST NOT leak credentials in that error

<!-- DELTA:NEW -->
### Scenario: The VS resolves the no-null-fill guard set once per query into the scan spec

* *GIVEN* a virtual schema query whose Iceberg table has, in its metadata, at least one Identity-Transform partition field and/or at least one schema field carrying a non-null `initial-default`
* *WHEN* the VS planning layer resolves the file list for that query
* *THEN* the VS SHALL, exactly once, collect into the shard-invariant scan spec a guard-set entry `{field_id, reason}` for every field-id that is the `source_id` of an Identity-Transform partition field (across the table's partition specs) with `reason` = identity-partition, and for every schema field with a non-null `initial-default` with `reason` = initial-default
* *AND* when a field-id qualifies under both conditions the VS SHALL record it once with the identity-partition reason
* *AND* when the table has no Identity-Transform partition field and no field with a non-null `initial-default` the threaded guard set SHALL be empty, so scan specs that carry no guard set deserialize unchanged (backward-compatible)
* *AND* the VS MUST NOT leak credentials while reading partition-spec or schema metadata to build the guard set
<!-- /DELTA:NEW -->

<!-- DELTA:CHANGED -->
### Scenario: Added nullable column absent from an older file is NULL-filled only when no guard applies

* *GIVEN* a scan spec whose logical schema carries a nullable column with a field-id that is absent from one of the assigned files
* *AND* that field-id is NOT in the threaded no-null-fill guard set (it is neither an identity-partition source nor a field with a non-null `initial-default`)
* *AND* another assigned file that does carry that field-id
* *WHEN* the scan UDF reads both files
* *THEN* the UDF SHALL emit NULL for that column for rows from the file lacking the field-id
* *AND* the UDF SHALL emit the real physical values for that column for rows from the file that carries it
<!-- /DELTA:CHANGED -->

<!-- DELTA:NEW -->
### Scenario: Optional identity-partition source column missing from a file errors cleanly instead of NULL-filling

* *GIVEN* a scan spec whose logical schema carries a nullable column whose field-id is in the threaded guard set with the identity-partition reason
* *AND* an assigned file that does not contain a physical column resolving to that field-id (a metadata-only migration where the partition source column is not materialized in the data file)
* *WHEN* the scan UDF reads that file
* *THEN* the UDF SHALL return a clean error identifying the column and stating that its value would have to be reconstructed from the file's identity-partition metadata, which is not implemented
* *AND* the UDF MUST NOT emit NULL or any other fabricated value for that column
* *AND* the error MUST NOT leak storage or catalog credentials
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: Optional column with a non-null initial-default missing from a file errors cleanly instead of NULL-filling

* *GIVEN* a scan spec whose logical schema carries a nullable column whose field-id is in the threaded guard set with the initial-default reason
* *AND* an assigned file written before the column was added, which does not contain a physical column resolving to that field-id
* *WHEN* the scan UDF reads that file
* *THEN* the UDF SHALL return a clean error identifying the column and stating that its `initial-default` value materialization is not implemented
* *AND* the UDF MUST NOT emit NULL for that column (NULL would be wrong: the column has a defined non-null default for pre-add rows)
* *AND* the error MUST NOT leak storage or catalog credentials
<!-- /DELTA:NEW -->

<!-- DELTA:CHANGED -->
### Scenario: Added required column missing from an older file errors cleanly

* *GIVEN* a scan spec whose logical schema carries a non-nullable (required) column with a field-id that is absent from one of the assigned files
* *WHEN* the scan UDF reads that file
* *THEN* the UDF SHALL return a clean error identifying that the required column cannot be resolved from the file
* *AND* when that field-id is in the no-null-fill guard set the error SHALL name the accurate reason (identity-partition reconstruction, or `initial-default` materialization, is not implemented) rather than only a generic required-missing message
* *AND* the UDF MUST NOT emit wrong or fabricated data for that column
* *AND* the UDF MUST NOT attempt to synthesize the column from any Iceberg `initial-default` or partition metadata (that value materialization is out of scope)
<!-- /DELTA:CHANGED -->

### Scenario: Scan without a logical schema falls back to first-file inference

* *GIVEN* a scan spec that predates the logical-schema field (the logical schema is absent)
* *WHEN* the scan UDF runs for that spec
* *THEN* the UDF SHALL register the files with a schema inferred from the first file and bind columns by physical name, unchanged from prior behavior
* *AND* the field-id expression adapter MUST NOT be installed for that scan
