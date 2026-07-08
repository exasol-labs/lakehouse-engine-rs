# Feature: DataFusion Scan Execution — Field-Id-Based Column Projection

Extends the scan UDF to bind columns by Iceberg field-id (with a physical-name fallback)
when the scan spec carries a logical schema, so projection is correct across Iceberg
schema evolution — renamed, dropped, and added-nullable columns all resolve correctly
per file without rewriting the schema adapter.

## Background

<!-- DELTA:CHANGED -->
* When the scan spec carries a logical schema (a list of `{field_id, name, arrow_type,
  nullable}` tuples), the scan UDF registers the `ListingTable` with that schema (each
  field tagged with `PARQUET:field_id` metadata) and installs a `FieldIdExprAdapter`
  that resolves each logical column to its physical Parquet column by, in order: (1) an
  embedded `PARQUET:field_id` match; (2) for a physical field that carries NO embedded
  field-id, the table's `schema.name-mapping.default` mapping of that physical name to a
  field-id present in the logical schema; (3) a physical-name match. Steps (2) and (3)
  apply only to fields without an embedded field-id; step (2) augments — never replaces —
  the physical-name fallback of step (3).
<!-- /DELTA:CHANGED -->
* The adapter delegates null-fill (nullable column absent from a file → NULL), type
  divergence → cast, and required-missing → clean error to
  `DefaultPhysicalExprAdapter`, keeping the change minimal.
* The adapter is applied per file by the Parquet opener, so files with divergent
  physical layouts within one shard each bind correctly.
<!-- DELTA:NEW -->
* The `schema.name-mapping.default` table property is resolved ONCE per query in the VS
  planning layer (at `resolve_file_list`, alongside the logical schema), parsed into a
  flat list of `{name, field_id}` entries, and threaded into the scan spec. The scan UDF
  never re-reads Iceberg table properties. Only the top-level (flat) name-mapping entries
  are honored; nested `fields` entries for struct / map / list children are NOT parsed in
  this phase (see Out-of-scope).
<!-- /DELTA:NEW -->
* When the scan spec does NOT carry a logical schema, the field-id adapter is not
  installed and the scan falls back to first-file schema inference unchanged.
<!-- DELTA:CHANGED -->
* Out-of-scope: filling an added REQUIRED column from its Iceberg `initial-default`
  (#27); parsing nested `fields` entries of `schema.name-mapping.default` for
  struct / map / list children (#83). The adjacent Iceberg column-projection resolution
  rules for a field id absent from a data file — rule #1 (substitute a partition value
  when an Identity Transform exists) and rule #3 (return a defined `initial-default`) —
  are not implemented anywhere in this engine and remain out of scope; only rule #2
  (name-mapping) is implemented here.
<!-- /DELTA:CHANGED -->

## Scenarios

### Scenario: Column projection binds by Iceberg field-id across physical layouts

* *GIVEN* a scan spec whose logical schema carries a column bound to a stable Iceberg field-id
* *AND* the assigned files include one file whose physical Parquet column for that field-id has a different physical name than the current logical name (a renamed column), each physical field tagged with its `PARQUET:field_id`
* *WHEN* the scan UDF reads that file
* *THEN* the UDF SHALL resolve each logical column to its physical column by matching the logical field's `PARQUET:field_id` against the physical fields' `PARQUET:field_id`, independent of physical name
* *AND* the emitted values for the renamed column SHALL be the real physical values (never NULL) under the current logical name
* *AND* the resolution SHALL run per file, so files with divergent physical layouts within one scan SHALL each bind correctly

<!-- DELTA:NEW -->
### Scenario: Field-id resolution honors schema.name-mapping.default for a file field without an embedded field-id

* *GIVEN* a scan spec whose logical schema carries a column bound to a stable Iceberg field-id under its current logical name, and a threaded `schema.name-mapping.default` entry mapping a physical column name to that field-id
* *AND* an assigned file whose physical field for that column carries NO embedded `PARQUET:field_id` and whose physical name equals the mapped name but differs from the current logical name (a rename resolved only by the name-mapping)
* *WHEN* the scan UDF reads that file
* *THEN* the UDF SHALL resolve that logical column to the physical column named by the matching name-mapping entry, binding it to the field-id the mapping supplies
* *AND* the emitted values for that column SHALL be the real physical values (never NULL) under the current logical name
* *AND* an embedded `PARQUET:field_id` on a physical field SHALL take precedence over the name-mapping for that field (the name-mapping applies only to fields lacking an embedded field-id)
<!-- /DELTA:NEW -->

<!-- DELTA:CHANGED -->
### Scenario: Field-id resolution falls back to physical name when no name-mapping resolves a file field without an embedded field-id

* *GIVEN* a scan spec whose logical schema carries field-ids
* *AND* an assigned file whose physical fields carry no embedded `PARQUET:field_id`
* *AND* either no `schema.name-mapping.default` is threaded into the spec, OR the threaded name-mapping does not map a given physical field's name to a field-id present in the logical schema
* *WHEN* the scan UDF reads that file
* *THEN* for each such unmapped physical field the UDF SHALL resolve the logical column to a physical column whose physical name equals the logical (current) name
* *AND* this physical-name fallback SHALL remain unchanged from prior behavior for the no-name-mapping case and for any field the mapping does not cover
<!-- /DELTA:CHANGED -->

<!-- DELTA:NEW -->
### Scenario: The VS resolves schema.name-mapping.default once per query into the scan spec

* *GIVEN* a virtual schema query whose Iceberg table defines a `schema.name-mapping.default` property
* *WHEN* the VS planning layer resolves the file list for that query
* *THEN* the VS SHALL parse the property exactly once, into a flat list of `{name, field_id}` entries taken from the top-level mapping objects (each name in an entry's `names` mapped to that entry's `field-id`), and thread it into the shard-invariant scan spec alongside the logical schema
* *AND* the VS SHALL skip any top-level mapping object that carries no `field-id`, and SHALL NOT recurse into nested `fields` child entries
* *AND* when the table defines no `schema.name-mapping.default` property the threaded name-mapping SHALL be empty, so scan specs that carry no name-mapping deserialize unchanged (backward-compatible)
* *AND* when the property is present but is not valid name-mapping JSON the VS SHALL fail the query with a clean plan-time error naming the malformed property, and MUST NOT leak credentials in that error
<!-- /DELTA:NEW -->

### Scenario: Added nullable column absent from an older file is NULL-filled

* *GIVEN* a scan spec whose logical schema carries a nullable column with a field-id that is absent from one of the assigned files
* *AND* another assigned file that does carry that field-id
* *WHEN* the scan UDF reads both files
* *THEN* the UDF SHALL emit NULL for that column for rows from the file lacking the field-id
* *AND* the UDF SHALL emit the real physical values for that column for rows from the file that carries it

### Scenario: Added required column missing from an older file errors cleanly

* *GIVEN* a scan spec whose logical schema carries a non-nullable (required) column with a field-id that is absent from one of the assigned files
* *WHEN* the scan UDF reads that file
* *THEN* the UDF SHALL return a clean error identifying that the required column cannot be resolved from the file
* *AND* the UDF MUST NOT emit wrong or fabricated data for that column
* *AND* the UDF MUST NOT attempt to synthesize the column from any Iceberg `initial-default` (that is out of scope)

### Scenario: Scan without a logical schema falls back to first-file inference

* *GIVEN* a scan spec that predates the logical-schema field (the logical schema is absent)
* *WHEN* the scan UDF runs for that spec
* *THEN* the UDF SHALL register the files with a schema inferred from the first file and bind columns by physical name, unchanged from prior behavior
* *AND* the field-id expression adapter MUST NOT be installed for that scan
