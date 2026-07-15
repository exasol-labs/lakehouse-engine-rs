# Feature: DataFusion Scan Execution — Field-Id-Based Column Projection

Extends the scan UDF to bind columns by Iceberg field-id (with a physical-name fallback)
when the scan spec carries a logical schema, so projection is correct across Iceberg
schema evolution — renamed, dropped, and added columns all resolve correctly per file
without rewriting the schema adapter. An added column absent from an older data file
returns its defined Iceberg `initial-default` value, falling back to NULL only when the
column is nullable and defines no default.

## Background

* When the scan spec carries a logical schema (a list of `{field_id, name, arrow_type,
  nullable, initial_default}` tuples), the scan UDF registers the `ListingTable` with that
  schema (each field tagged with `PARQUET:field_id` metadata) and installs a
  `FieldIdExprAdapter` that resolves each logical column to its physical Parquet column by,
  in order: (1) an embedded `PARQUET:field_id` match; (2) for a physical field that carries
  NO embedded field-id, the table's `schema.name-mapping.default` mapping of that physical
  name to a field-id present in the logical schema; (3) a physical-name match. Steps (2) and
  (3) apply only to fields without an embedded field-id; step (2) augments — never
  replaces — the physical-name fallback of step (3).
* This feature implements the Iceberg table-spec "Column Projection" ordered resolution for
  a logical field-id NOT present in a data file. The spec defines the ordered process:
  (1) "Return the value from partition metadata if an Identity Transform exists for the
  field and the partition value is present in the `partition` struct on `data_file` object
  in the manifest"; (2) name-mapping fallback (locate columns lacking field IDs via the
  table's `schema.name-mapping.default`); (3) "Return the default value if it has a defined
  `initial-default`"; (4) "Return `null` in all other cases". The spec defines
  `initial-default` as applied to "all records that were written before the field was added
  to the schema" and `write-default` as used for "any records written after the field was
  added to the schema, if the writer does not supply the field's value".
* This engine implements rule (2) (name-mapping) and rule (3) (`initial-default`) and the
  rule (4) NULL fallback. Rule (3) reads ONLY `initial-default`; `write-default` is
  irrelevant to reads and MUST NOT be consulted (it governs writer-side backfill, not the
  read of pre-existing rows).
* Resolution for a logical field-id absent from a data file, applied per file:
  the field defines an `initial-default` → emit that default value for that file's rows
  (whether the field is required or nullable); else the field is nullable → emit NULL; else
  the field is required with no default → return a clean error. A field that DOES resolve to
  a physical column (by field-id or name-mapping) always binds to that column's real values
  and is never defaulted.
* Only PRIMITIVE-typed `initial-default` values are applied. The logical schema's compact
  Arrow-type tag vocabulary is primitive-only (bool, int32, int64, float32, float64, utf8,
  date32, timestamp/timestamptz, decimal128); a Struct / List / Map `initial-default` is not
  represented and such a column falls through to NULL (nullable) or the required-absent
  error. This is a deliberate trade-off: Exasol has no struct / list / map types (those
  columns surface only as JSON-fallback VARCHAR), and the Iceberg spec itself requires
  columns of `unknown`, `variant`, `geometry`, and `geography` types to default to null.
* Timestamptz IS covered by the all-types initial-default E2E fixture: an Iceberg
  `timestamptz` column is declared and emitted as plain Exasol `TIMESTAMP` (see
  `datafusion-scan/type-mapping`) carrying the UTC-instant value, so it crosses the scan UDF
  emit boundary like any other primitive. Micros-precision `timestamptz` is exercised
  end-to-end by the fixture; nanosecond-precision `timestamptz_ns` (like `timestamp_ns`) is
  not Iceberg-expressible in this catalog version and stays covered by the unit round-trip
  test (the `timestamptz_us` / `timestamptz_ns` tags in the round-trip scenario). The former
  `TIMESTAMP WITH LOCAL TIME ZONE` emit exclusion is closed (#118).
* The adapter delegates type divergence → cast and, for an absent field with no encoded
  default, null-fill (nullable) or required-missing → clean error to
  `DefaultPhysicalExprAdapter`. The `initial-default` fill intercepts the absent-field case
  BEFORE that delegation: a required-absent field would otherwise error before any
  post-processing could substitute a default.
* The adapter is applied per file by the Parquet opener, so files with divergent physical
  layouts within one shard each bind correctly, and the default fill is decided per file
  from that file's actually-present field-ids.
* The `schema.name-mapping.default` table property and each field's `initial-default` are
  resolved ONCE per query in the VS planning layer (at `resolve_file_list`, when the logical
  schema is read from the Iceberg current schema). The scan UDF never re-reads Iceberg table
  metadata. The encoded `initial-default` carried in the scan spec is JSON-portable and
  credential-free.
* Non-null Iceberg `initial-default` values require table format-version 3. The Iceberg
  table spec defines `initial-default` as "used to populate the field's value for all records
  that were written before the field was added to the schema" (Schemas and Data Types →
  Default values), and Iceberg's schema-compatibility check rejects a non-null `initial-default`
  on a v1/v2 table ("non-null default ... is not supported until v3"). This constrains only
  tables that DEFINE such defaults; the READ path here is format-version-agnostic — it reads
  `initial-default` off the current-schema metadata regardless of the table's format version,
  so no format-version handling exists or is needed in the scan or VS code. (The E2E fixture
  that exercises this therefore creates its table at v3.)
* When the scan spec does NOT carry a logical schema, the field-id adapter is not installed
  and the scan falls back to first-file schema inference unchanged.
* Out-of-scope: parsing nested `fields` entries of `schema.name-mapping.default` for
  struct / map / list children (#83); and Iceberg column-projection rule (1) — substituting
  an Identity-Transform partition value for an absent field — which is not implemented
  anywhere in this engine. Because rule (1) is unimplemented, if BOTH an Identity-Transform
  partition value and an `initial-default` could resolve the same absent field-id, this
  engine returns the `initial-default` (rule 3) rather than the partition value (rule 1). For
  an ADDED column read from older files this is the correct and only-available value, so this
  ordering is a deliberate, accurately-scoped trade-off, not a silent gap.

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

### Scenario: The VS encodes each field's Iceberg initial-default once per query into the scan spec

* *GIVEN* a virtual schema query whose Iceberg current schema defines a field carrying a primitive `initial-default` (required or nullable), a field carrying no `initial-default`, and a field carrying a non-primitive (struct / list / map) `initial-default`
* *WHEN* the VS planning layer builds the logical schema from the Iceberg current schema
* *THEN* the VS SHALL read each field's `initial-default` exactly once and encode a primitive default into that field's logical-schema entry in a JSON-portable, credential-free form that reconstructs to a `ScalarValue` matching the field's Arrow-type tag, and SHALL NOT read `write-default`
* *AND* the VS SHALL leave the encoded default absent for a field with no `initial-default` and for a field whose `initial-default` is non-primitive, so those fields fall through to NULL or the required-absent error at scan time
* *AND* a scan spec whose logical fields carry no encoded default SHALL deserialize unchanged (the encoded default is an optional field, backward-compatible with specs written before this feature)

### Scenario: Every supported primitive initial-default survives the scan-spec serialization round-trip

* *GIVEN* a scan spec whose logical schema carries one field for every supported primitive Arrow-type tag (`bool`, `int32`, `int64`, `float32`, `float64`, `utf8`, `date32`, `timestamp_us`, `timestamp_ns`, `timestamptz_us`, `timestamptz_ns`, and a `decimal128(p,s)` with non-trivial precision and scale), each field encoding that type's `initial-default`
* *AND* one further field whose Iceberg `initial-default` is non-primitive (struct / list / map)
* *WHEN* the scan spec is serialized to JSON, deserialized, and each field's encoded default is reconstructed to a `ScalarValue`
* *THEN* the reconstructed `ScalarValue` for each primitive field SHALL equal the originally encoded value and SHALL match that field's Arrow-type tag, for every supported primitive tag in the vocabulary
* *AND* the non-primitive field SHALL carry no encoded default after the round-trip, so it falls through to NULL (nullable) or the required-absent error at scan time
* *AND* the serialized form SHALL be credential-free

### Scenario: Added nullable column absent from a file with no initial-default is NULL-filled

* *GIVEN* a scan spec whose logical schema carries a NULLABLE column with a field-id that is absent from one of the assigned files and that defines NO `initial-default`
* *AND* another assigned file that does carry that field-id
* *WHEN* the scan UDF reads both files
* *THEN* the UDF SHALL emit NULL for that column for rows from the file lacking the field-id
* *AND* the UDF SHALL emit the real physical values for that column for rows from the file that carries it

### Scenario: Absent field with a defined initial-default returns the default value per file

* *GIVEN* a scan spec whose logical schema carries two columns that each define a primitive `initial-default` — one REQUIRED and one NULLABLE — each bound to a field-id that is absent from one assigned file (an older file written before the column was added) and present in another assigned file
* *WHEN* the scan UDF reads both files
* *THEN* the UDF SHALL emit each column's defined `initial-default` value for rows from the file that lacks the column's field-id, for both the required and the nullable column
* *AND* the UDF SHALL emit the real physical values for those columns for rows from the file that carries the field-id, so the default fill is decided per file from that file's present field-ids
* *AND* a column whose field-id DOES resolve to a physical column (by embedded field-id or by name-mapping) SHALL bind to that column's real values and MUST NOT be replaced by its `initial-default`
* *AND* the UDF MUST NOT consult `write-default` for any column
* *AND* this resolution SHALL hold for every supported primitive type in the all-types fixture, including an Iceberg `timestamptz` column declared and emitted as plain Exasol `TIMESTAMP` (see `datafusion-scan/type-mapping`), which MUST cross the scan UDF emit boundary without a `sqlCode 22002` type error

### Scenario: Added required column absent from a file with no initial-default errors cleanly

* *GIVEN* a scan spec whose logical schema carries a non-nullable (required) column with a field-id that is absent from one of the assigned files and that defines NO `initial-default`
* *WHEN* the scan UDF reads that file
* *THEN* the UDF SHALL return a clean error identifying that the required column cannot be resolved from the file
* *AND* the UDF MUST NOT emit wrong or fabricated data for that column
* *AND* the UDF MUST NOT substitute NULL for the required column

### Scenario: Scan without a logical schema falls back to first-file inference

* *GIVEN* a scan spec that predates the logical-schema field (the logical schema is absent)
* *WHEN* the scan UDF runs for that spec
* *THEN* the UDF SHALL register the files with a schema inferred from the first file and bind columns by physical name, unchanged from prior behavior
* *AND* the field-id expression adapter MUST NOT be installed for that scan
