# Feature: DataFusion Scan Execution, Field-Id-Based Column Projection

Binds each logical column of the scan spec's logical schema to a physical Parquet column, file by file. A logical field binds by Iceberg field-id, by a declared physical name, or by its own name, depending on which key it carries. Projection therefore stays correct across Iceberg schema evolution and Delta column mapping, and a renamed column returns its real values in every file.

## Background

* The logical schema is a list of `{field_id, name, arrow_type, nullable, initial_default}` tuples. The scan UDF registers its file-list table provider with that schema, tags each field with `PARQUET:field_id` metadata, and installs a `FieldIdExprAdapter`. The Parquet opener applies the adapter per file, so files with divergent physical layouts in one shard each bind correctly.
* A logical field carries a field-id, a physical name, or neither. By field-id: Iceberg (always) and Delta `id` column mapping. By physical name: Delta `name` column mapping. By identity, matching the logical name itself: Delta `none` column mapping and every field of a direct-storage table. The Iceberg planning path populates a field-id on every logical field and never a physical name.
* The adapter resolves each physical field in this order. (1) An embedded `PARQUET:field_id` matching a logical field's id is authoritative. A field carrying an id that no logical field declares keeps its physical name. (2) A logical field that declares this physical name claims it. (3) `schema.name-mapping.default` maps this physical name to a field-id present in the logical schema. (4) The physical name stays unchanged, which makes identity binding resolve. Steps (2) to (4) apply only to a field without an embedded field-id. Step (2) sits above the name-mapping because a per-column declaration from the table's own metadata is authoritative, while a name-mapping entry is a table-level fallback. A column whose value the file entry carries leaves that file's schema before binding, so no step binds it: see `scan-read-path/scan-execution-identity-partition-fill`.
* All three strategies wrap the same `DefaultPhysicalExprAdapterFactory` for type-divergence casts, per-file NULL-fill, and the required-absent error. Before it delegates, the adapter substitutes an absent field's `initial-default`. The VS resolves `schema.name-mapping.default`, each field's `initial-default`, and each file's `identity` partition values once per query, and the scan UDF never re-reads Iceberg table metadata.
* The Iceberg table spec (`format/spec.md`, "Column Projection") nests the mapping: "`fields`: An optional list of field mappings for child field of structs, maps, and lists." "List types should contain a mapping in `fields` for `element`." "Map types should contain mappings in `fields` for `key` and `value`." A nested member resolves in the same order, against the entries beneath the entry that matched its parent's physical name. The mapping keys current names, so a name that a later schema change gave another field binds that field.
* `scan-read-path/scan-execution-field-id-projection-absent-fields` specifies absent-field resolution, including Iceberg Column Projection rule (1).

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

### Scenario: A nested member of a file without field ids binds through the name mapping

* *GIVEN* an Iceberg table with a metadata-only imported data file that embeds no field ids, whose struct members, list-element struct members, map-value struct members, and members of a struct inside a struct were renamed after the import, one of them twice
* *AND* the table's `schema.name-mapping.default` lists every name of each renamed member beneath its parent's entry
* *WHEN* the scan renders those nested columns from the imported file
* *THEN* each renamed member SHALL bind to the field id of the mapping entry beneath its parent, and SHALL render its real values under its current name, never NULL
* *AND* a list element SHALL descend through its parent's `element` entry, and a map key and value through the `key` and `value` entries, whatever names the file gives them
* *AND* a member whose file name the mapping does not cover SHALL bind by its own name, and SHALL render NULL when no logical member carries that name

### Scenario: An embedded nested field id wins over the name mapping

* *GIVEN* an Iceberg table with the imported file above and a natively written file, whose struct member embeds the field id of a member that was later renamed, under a name the mapping now gives a different member
* *WHEN* the scan renders that struct from both files
* *THEN* the native file's member SHALL bind by its embedded field id, and MUST NOT bind through the mapping
* *AND* the mapping SHALL apply only to members without an embedded field id

### Scenario: The VS resolves schema.name-mapping.default once per query into the scan spec

* *GIVEN* a virtual schema query whose Iceberg table defines a `schema.name-mapping.default` property
* *WHEN* the VS planning layer resolves the file list for that query
* *THEN* the VS SHALL parse the property exactly once, into a list of `{name, field_id, fields}` entries, one per name in a mapping object's `names`, whose `fields` holds the same entries for the object's nested `fields` at every depth, skipping any mapping object without a `field-id` together with its nested entries, and thread it into the shard-invariant scan spec alongside the logical schema
* *AND* an entry without nested entries SHALL serialize without `fields`, so the encoding of a flat mapping is unchanged
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

* *GIVEN* a scan spec whose logical schema carries a field with NO field-id and NO declared physical name, the identity binding a Delta `none` column mapping produces, and the one a direct-storage table's merged Parquet schema produces for EVERY field, a second such field that is ABSENT from one assigned file and nullable, and a third such field that is absent from that file, required, and carries no `initial-default`
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
