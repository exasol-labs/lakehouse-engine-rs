# Feature: DataFusion Scan Execution — Field-Id-Based Column Projection

Extends the scan UDF to bind columns by Iceberg field-id (with a physical-name fallback)
when the scan spec carries a logical schema, so projection is correct across Iceberg
schema evolution — renamed, dropped, and added-nullable columns all resolve correctly
per file without rewriting the schema adapter.

## Background

* When the scan spec carries a logical schema (a list of `{field_id, name, arrow_type,
  nullable}` tuples), the scan UDF registers the `ListingTable` with that schema (each
  field tagged with `PARQUET:field_id` metadata) and installs a `FieldIdExprAdapter`
  that resolves each logical column to its physical Parquet column by field-id match,
  falling back to a physical-name match when a file field carries no embedded field-id.
* The adapter delegates null-fill (nullable column absent from a file → NULL), type
  divergence → cast, and required-missing → clean error to
  `DefaultPhysicalExprAdapter`, keeping the change minimal.
* The adapter is applied per file by the Parquet opener, so files with divergent
  physical layouts within one shard each bind correctly.
* When the scan spec does NOT carry a logical schema, the field-id adapter is not
  installed and the scan falls back to first-file schema inference unchanged.
* Out-of-scope: filling an added REQUIRED column from its Iceberg `initial-default`
  (#27); honoring the `schema.name-mapping.default` table property (#28).

## Scenarios

### Scenario: Column projection binds by Iceberg field-id across physical layouts

* *GIVEN* a scan spec whose logical schema carries a column bound to a stable Iceberg field-id
* *AND* the assigned files include one file whose physical Parquet column for that field-id has a different physical name than the current logical name (a renamed column), each physical field tagged with its `PARQUET:field_id`
* *WHEN* the scan UDF reads that file
* *THEN* the UDF SHALL resolve each logical column to its physical column by matching the logical field's `PARQUET:field_id` against the physical fields' `PARQUET:field_id`, independent of physical name
* *AND* the emitted values for the renamed column SHALL be the real physical values (never NULL) under the current logical name
* *AND* the resolution SHALL run per file, so files with divergent physical layouts within one scan SHALL each bind correctly

### Scenario: Field-id resolution falls back to physical name when a file field carries no field-id

* *GIVEN* a scan spec whose logical schema carries field-ids
* *AND* an assigned file whose physical fields carry no embedded `PARQUET:field_id`
* *WHEN* the scan UDF reads that file
* *THEN* the UDF SHALL resolve each logical column to a physical column whose physical name equals the logical (current) name
* *AND* the UDF MUST NOT parse or honor any table-level name-mapping property (that is out of scope)

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
