# Feature: DataFusion Scan Execution

A disposable Rust SET UDF that, for one query, builds a DataFusion session, registers exactly the Iceberg/Parquet data files assigned to its shard, sizes its DataFusion memory pool from the per-instance memory limit reported in UDF metadata, applies the pushed-down projection, filter, and LIMIT, and streams the matching rows back as Arrow IPC batches. It holds no state and discovers no files of its own.

## Background

* The scan UDF registers ONLY its assigned files and never discovers files from the catalog.
* When the scan spec carries a logical Iceberg schema, column projection binds by Iceberg field-id (with a physical-name fallback) so results are correct across schema evolution; when it does not, the UDF falls back to first-file schema inference and physical-name binding.
* Error messages MUST NOT contain storage access keys, secret keys, or session tokens.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Scan registers only its assigned files and returns matching rows

* *GIVEN* a scan spec listing specific Iceberg Parquet files in MinIO, carrying the logical Iceberg schema (each entry a `{field_id, name, arrow_type, nullable}` tuple derived once by the adapter)
* *AND* a projection naming a subset of columns
* *WHEN* the scan UDF runs for that spec
* *THEN* the UDF SHALL create a DataFusion session and register only the assigned files as one `ListingTable` whose declared schema is the logical Iceberg schema (each field carrying its `PARQUET:field_id` metadata), NOT a schema inferred from the first file
* *AND* the UDF MUST NOT resolve or discover any additional files from the catalog
* *AND* the UDF SHALL emit one output row per scanned source row containing only the projected columns
<!-- /DELTA:CHANGED -->

<!-- DELTA:NEW -->
### Scenario: Column projection binds by Iceberg field-id across physical layouts

* *GIVEN* a scan spec whose logical schema carries a column bound to a stable Iceberg field-id
* *AND* the assigned files include one file whose physical Parquet column for that field-id has a different physical name than the current logical name (a renamed column), each physical field tagged with its `PARQUET:field_id`
* *WHEN* the scan UDF reads that file
* *THEN* the UDF SHALL resolve each logical column to its physical column by matching the logical field's `PARQUET:field_id` against the physical fields' `PARQUET:field_id`, independent of physical name
* *AND* the emitted values for the renamed column SHALL be the real physical values (never NULL) under the current logical name
* *AND* the resolution SHALL run per file, so files with divergent physical layouts within one scan SHALL each bind correctly
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: Field-id resolution falls back to physical name when a file field carries no field-id

* *GIVEN* a scan spec whose logical schema carries field-ids
* *AND* an assigned file whose physical fields carry no embedded `PARQUET:field_id`
* *WHEN* the scan UDF reads that file
* *THEN* the UDF SHALL resolve each logical column to a physical column whose physical name equals the logical (current) name
* *AND* the UDF MUST NOT parse or honor any table-level name-mapping property (that is out of scope)
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: Added nullable column absent from an older file is NULL-filled

* *GIVEN* a scan spec whose logical schema carries a nullable column with a field-id that is absent from one of the assigned files
* *AND* another assigned file that does carry that field-id
* *WHEN* the scan UDF reads both files
* *THEN* the UDF SHALL emit NULL for that column for rows from the file lacking the field-id
* *AND* the UDF SHALL emit the real physical values for that column for rows from the file that carries it
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: Added required column missing from an older file errors cleanly

* *GIVEN* a scan spec whose logical schema carries a non-nullable (required) column with a field-id that is absent from one of the assigned files
* *WHEN* the scan UDF reads that file
* *THEN* the UDF SHALL return a clean error identifying that the required column cannot be resolved from the file
* *AND* the UDF MUST NOT emit wrong or fabricated data for that column
* *AND* the UDF MUST NOT attempt to synthesize the column from any Iceberg `initial-default` (that is out of scope)
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: Scan without a logical schema falls back to first-file inference

* *GIVEN* a scan spec that predates the logical-schema field (the logical schema is absent)
* *WHEN* the scan UDF runs for that spec
* *THEN* the UDF SHALL register the files with a schema inferred from the first file and bind columns by physical name, unchanged from prior behavior
* *AND* the field-id expression adapter MUST NOT be installed for that scan
<!-- /DELTA:NEW -->
