# Feature: DataFusion Scan Execution, Field-Id-Based Column Projection

Binds each logical column of the scan spec's logical schema to a physical Parquet column, file by file. A logical field binds by Iceberg field-id, by a declared physical name, or by its own name, depending on which key it carries. Projection therefore stays correct across Iceberg schema evolution and Delta column mapping, and a renamed column returns its real values in every file.

<!-- DELTA:CHANGED -->
## Background

* The logical schema is a list of `{field_id, name, arrow_type, nullable, initial_default}` tuples. The scan UDF registers its file-list table provider with that schema, tags each field with `PARQUET:field_id` metadata, and installs a `FieldIdExprAdapter`. The Parquet opener applies the adapter per file, so files with divergent physical layouts in one shard each bind correctly.
* A logical field carries a field-id, a physical name, or neither. By field-id: Iceberg (always) and Delta `id` column mapping. By physical name: Delta `name` column mapping. By identity, matching the logical name itself: Delta `none` column mapping and every field of a direct-storage table. The Iceberg planning path populates a field-id on every logical field and never a physical name.
* The adapter resolves each physical field in this order. (1) An embedded `PARQUET:field_id` matching a logical field's id is authoritative. A field carrying an id that no logical field declares keeps its physical name. (2) A logical field that declares this physical name claims it. (3) `schema.name-mapping.default` maps this physical name to a field-id present in the logical schema. (4) The physical name stays unchanged, which makes identity binding resolve. Steps (2) to (4) apply only to a field without an embedded field-id. Step (2) sits above the name-mapping because a per-column declaration from the table's own metadata is authoritative, while a name-mapping entry is a table-level fallback.
* All three strategies wrap the same `DefaultPhysicalExprAdapterFactory` for type-divergence casts, per-file NULL-fill, and the required-absent error. Before it delegates, the adapter substitutes an absent field's identity partition value or `initial-default`. The VS resolves `schema.name-mapping.default`, each field's `initial-default`, and each file's `identity` partition values once per query, and the scan UDF never re-reads Iceberg table metadata.
* Out of scope: parsing nested `fields` entries of `schema.name-mapping.default` for struct, map and list children. `scan-read-path/scan-execution-field-id-projection-absent-fields` specifies absent-field resolution, including Iceberg Column Projection rule (1).
<!-- /DELTA:CHANGED -->

## Scenarios

### Scenario: Scan without a logical schema falls back to first-file inference

* *GIVEN* a scan spec that predates the logical-schema field (the logical schema is absent)
* *WHEN* the scan UDF runs for that spec
* *THEN* the UDF SHALL register the files with a schema inferred from the first file and bind columns by physical name, unchanged from prior behavior
* *AND* this fallback SHALL be selected by the ABSENCE of a logical schema ALONE, so a spec whose logical schema IS present still installs the column-binding adapter even when every one of its fields carries neither a field-id nor a declared physical name
* *AND* identity binding MUST NOT be conflated with this fallback: identity binding resolves a DECLARED logical field through the same adapter and therefore keeps per-file NULL-fill, `initial-default` substitution, and the required-absent error, none of which first-file inference provides
