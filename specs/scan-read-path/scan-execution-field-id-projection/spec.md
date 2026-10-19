# Feature: DataFusion Scan Execution, Field-Id-Based Column Projection

Binds each logical column of the scan spec's logical schema to a physical Parquet column, file by file. A logical field binds by Iceberg field-id, by a declared physical name, or by its own name, depending on which key it carries. Projection therefore stays correct across Iceberg schema evolution and Delta column mapping, and a renamed column returns its real values in every file.

## Background

* The logical schema is a list of `{field_id, name, arrow_type, nullable, initial_default}` tuples. The scan UDF registers its file-list table provider with that schema, tags each field with `PARQUET:field_id` metadata, and installs a `FieldIdExprAdapter`. The Parquet opener applies the adapter per file, so files with divergent physical layouts in one shard each bind correctly.
* A logical field carries a field-id, a physical name, or neither. By field-id: Iceberg (always) and Delta `id` column mapping. By physical name: Delta `name` column mapping. By identity, matching the logical name itself: Delta `none` column mapping and every field of a direct-storage table. The Iceberg planning path populates a field-id on every logical field and never a physical name.
* The adapter resolves each physical field in this order. (1) An embedded `PARQUET:field_id` matching a logical field's id is authoritative. A field carrying an id that no logical field declares keeps its physical name. (2) A logical field that declares this physical name claims it. (3) `schema.name-mapping.default` maps this physical name to a field-id present in the logical schema. (4) The physical name stays unchanged, which makes identity binding resolve. Steps (2) to (4) apply only to a field without an embedded field-id. Step (2) sits above the name-mapping because a per-column declaration from the table's own metadata is authoritative, while a name-mapping entry is a table-level fallback.
* All three strategies wrap the same `DefaultPhysicalExprAdapterFactory`, which decides type-divergence cast, per-file NULL-fill, `initial-default` substitution, and the required-absent error.
* The VS resolves `schema.name-mapping.default` and each field's `initial-default` once per query. The scan UDF never re-reads Iceberg table metadata.
* A scan spec without a logical schema installs no adapter and infers the schema from the first file.
* Out of scope: parsing nested `fields` entries of `schema.name-mapping.default` for struct, map and list children. Also out of scope is Iceberg Column Projection rule (1), which substitutes an Identity-Transform partition value for an absent field. When both a partition value and an `initial-default` could resolve an absent field-id, the engine returns the `initial-default`. That is a named trade-off, and for an added column read from older files it is the only available value.
* `scan-read-path/scan-execution-field-id-projection-absent-fields` specifies absent-field resolution.

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

### Scenario: Column projection binds by a logical field's declared physical name

* *GIVEN* a scan spec whose logical schema carries a field declaring a PHYSICAL NAME that differs from
  its logical name and carrying NO field-id (the shape a Delta `name` column mapping produces)
* *AND* an assigned file whose physical column bears that declared physical name
* *WHEN* the scan UDF reads that file
* *THEN* the UDF SHALL resolve that logical column to the physical column whose name equals the
  declared physical name, and SHALL emit that column's real physical values (never NULL) under the
  current logical name
* *AND* the declared physical name SHALL take precedence over any `schema.name-mapping.default` entry
  covering the same physical name, because a per-column declaration from the table's own metadata is
  authoritative and a name-mapping entry is a table-level fallback
* *AND* a physical field carrying an embedded `PARQUET:field_id` that no logical field declares SHALL keep
  its physical name, so a declared-physical-name binding resolves it at step (2) rather than being
  consumed by a field-id match
* *AND* per-file NULL-fill, `initial-default` substitution, and the required-absent clean error SHALL behave
  identically to the field-id-bound case, because both strategies wrap the same default
  physical-expression adapter

### Scenario: A logical field carrying no binding key binds by its own name

* *GIVEN* a scan spec whose logical schema carries a field with NO field-id and NO declared physical name — the identity binding a Delta `none` column mapping produces, and the one a direct-storage table's merged Parquet schema produces for EVERY field — a second such field that is ABSENT from one assigned file and nullable, and a third such field that is absent from that file, required, and carries no `initial-default`
* *WHEN* the scan UDF reads the assigned files
* *THEN* the UDF SHALL install the column-binding adapter, because the scan spec CARRIES a logical schema, and SHALL bind the identity-bound field to the physical column whose name equals its logical name
* *AND* the UDF SHALL emit NULL for the absent nullable field for rows from the file lacking it, and SHALL emit its `initial-default` instead when one is encoded, per file
* *AND* the UDF SHALL return the same clean required-absent error for the absent required field as it returns for a field-id-bound required column, and MUST NOT substitute NULL for it
* *AND* the UDF MUST NOT tag an identity-bound logical field with a `PARQUET:field_id`, because a synthesized id is a value no writer put in any file and would invite a false match against a file that does carry field-ids
* *AND* this identity binding MUST NOT be reached by an Iceberg scan, because the Iceberg planning path populates a field-id on every logical field
* *AND* the binding SHALL hold for a shard whose assigned files carry DIFFERENT column sets, because a direct-storage table's declared schema is the union of its files' columns, so the absent-column path is the ordinary case for that kind rather than an evolution edge case
* *AND* the binding MUST NOT require the physical field's Arrow type to equal the logical one, so an identity-bound field whose file carries a narrower type is cast per file by the adapter `scan-types/type-relaxation` owns, exactly as a field-id-bound field is

### Scenario: Scan without a logical schema falls back to first-file inference

* *GIVEN* a scan spec that predates the logical-schema field (the logical schema is absent)
* *WHEN* the scan UDF runs for that spec
* *THEN* the UDF SHALL register the files with a schema inferred from the first file and bind columns by physical name, unchanged from prior behavior
* *AND* this fallback SHALL be selected by the ABSENCE of a logical schema ALONE, so a spec whose logical schema IS present still installs the column-binding adapter even when every one of its fields carries neither a field-id nor a declared physical name
* *AND* identity binding MUST NOT be conflated with this fallback: identity binding resolves a DECLARED logical field through the same adapter and therefore keeps per-file NULL-fill, `initial-default` substitution, and the required-absent error, none of which first-file inference provides
