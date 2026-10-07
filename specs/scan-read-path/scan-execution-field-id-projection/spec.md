# Feature: DataFusion Scan Execution — Field-Id-Based Column Projection

When the scan spec carries a logical schema, the scan UDF binds each logical column to a physical Parquet column, file by file. A logical field binds by Iceberg field-id, by a declared physical name, or by its own name, depending on which key it carries. For a file field with no embedded field-id, field-id binding falls back to the table's `schema.name-mapping.default` and then to the physical name. Projection therefore stays correct across Iceberg schema evolution and Delta column mapping, and a renamed column returns its real values in every file.

This feature covers the three binding strategies, the per-physical-field resolution order, the once-per-query resolution of `schema.name-mapping.default`, and the first-file inference fallback for a scan spec that carries no logical schema.

## Background

* When the scan spec carries a logical schema (a list of `{field_id, name, arrow_type,
  nullable, initial_default}` tuples), the scan UDF registers its file-list table provider with that
  schema (each field tagged with `PARQUET:field_id` metadata) and installs a
  `FieldIdExprAdapter` that resolves each logical column to its physical Parquet column by,
  in order: (1) an embedded `PARQUET:field_id` match; (2) for a physical field that carries
  NO embedded field-id, the table's `schema.name-mapping.default` mapping of that physical
  name to a field-id present in the logical schema; (3) a physical-name match. Steps (2) and
  (3) apply only to fields without an embedded field-id; step (2) augments — never
  replaces — the physical-name fallback of step (3).
* The adapter is applied per file by the Parquet opener, so files with divergent physical
  layouts within one shard each bind correctly, and the default fill is decided per file
  from that file's actually-present field-ids.
* The `schema.name-mapping.default` table property and each field's `initial-default` are
  resolved ONCE per query in the VS planning layer (at `resolve_file_list`, when the logical
  schema is read from the Iceberg current schema). The scan UDF never re-reads Iceberg table
  metadata. The encoded `initial-default` carried in the scan spec is JSON-portable and
  credential-free.
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
* **This delta is issue #342.** It generalizes the column-binding adapter from ONE binding strategy
  to THREE, so a logical field declares HOW it binds instead of the adapter assuming every field binds
  by Iceberg field-id. A logical field carries either a field-id, or a physical name, or neither; the
  adapter dispatches on which one is populated.
* **The three strategies and who populates them.** By field-id — Iceberg (always) and Delta `id`
  column mapping, matched against the physical field's `PARQUET:field_id`. By physical name — Delta
  `name` column mapping, matched against the Parquet column's own name. By identity — Delta `none`
  column mapping, matched against the logical name itself.
* **Iceberg behavior is unchanged, and the recorded field-id scenarios stay accurate.** The Iceberg
  planning path populates a field-id on EVERY logical field and never populates a physical name, so
  the recorded embedded-field-id, `schema.name-mapping.default`, and physical-name-fallback scenarios
  describe exactly what an Iceberg scan still does. They are now scoped by construction to fields that
  carry a field-id.
* **Per-physical-field resolution order, extended by one step.** For each physical field: (1) an
  embedded `PARQUET:field_id` matching a logical field's id is authoritative, and a field carrying an
  id that no logical field declares keeps its physical name; (2) a logical field DECLARING this
  physical field's name as its physical name claims it; (3) `schema.name-mapping.default` maps this
  physical name to a field-id present in the logical schema; (4) the physical name is kept unchanged,
  which is what makes identity binding resolve. Step (2) is new and sits ABOVE the name-mapping
  because a per-column declaration read from the table's own metadata is authoritative, while a
  name-mapping entry is a table-level fallback for files that carry no field-id at all. The two never
  co-occur today: the Iceberg path populates no physical name and the Delta path carries an empty
  name-mapping.
* **`name_mapping` and `initial_default` are unchanged.** Their resolution, their once-per-query VS
  encoding step, their round-trip vocabulary, and the decimal-domain gate on the encoding step all
  stay exactly as recorded.
* **Every strategy wraps the SAME `DefaultPhysicalExprAdapterFactory`.** Type-divergence cast, per-file
  NULL-fill for an absent nullable column, `initial-default` substitution, and the required-absent
  clean error are decided by one delegate for all three strategies, so a Delta `none`-mode table gets
  the same semantics as an Iceberg table rather than a thinner path.
* **Apache Iceberg spec check — no Iceberg rule changes and no deviation is introduced.** The table
  spec's Column Projection section states "Columns in Iceberg data files are selected by field id" and
  that "projection must be done using field ids"; the Iceberg reader still gives every logical field
  its field-id, so this holds unchanged. The spec's ordered resolution for a field id not present in a
  data file — "(1) Return the value from partition metadata if an Identity Transform exists for the
  field and the partition value is present in the `partition` struct on `data_file` object in the
  manifest. (2) Use `schema.name-mapping.default` metadata to map field id to columns without field id
  ... (3) Return the default value if it has a defined `initial-default` ... (4) Return `null` in all
  other cases." — keeps its recorded implementation status exactly: rules (2), (3), and (4) are
  implemented and unchanged, and rule (1) stays the deliberate, accurately-scoped trade-off this
  feature already records. This delta neither closes nor widens that gap, and the physical-name and
  identity strategies are unreachable from an Iceberg scan.
* See `scan-read-path/scan-execution-field-id-projection-absent-fields` for absent-field resolution: `initial-default` encoding and substitution, NULL-fill, and the required-absent error.

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
