# Feature: DataFusion Scan Execution: Absent Fields and Initial Defaults

Defines what the scan UDF emits for a logical field that is absent from a data file. A field that defines an Iceberg `initial-default` returns that value for the file's rows, a nullable field with no default returns NULL, and a required field with no default fails the scan with a clean error. A field whose `identity` partition value the file entry carries takes that value instead, per `scan-read-path/scan-execution-identity-partition-fill`.

The VS reads each field's `initial-default` once per query and encodes a primitive default into the scan spec in a JSON-portable, credential-free form. It leaves the default absent for a non-primitive default and for a decimal outside Exasol's catalog-decimal domain.

<!-- DELTA:CHANGED -->
## Background

* The Iceberg table spec (`format/spec.md`, "Column Projection") resolves a field id absent from a data file in this order: (1) "Return the value from partition metadata if an Identity Transform exists for the field and the partition value is present in the `partition` struct on `data_file` object in the manifest", (2) `schema.name-mapping.default`, (3) "Return the default value if it has a defined `initial-default`", (4) "Return `null` in all other cases". The engine implements all four. Rule (2) is a binding rule of `scan-read-path/scan-execution-field-id-projection`, and rule (3) never reads `write-default`, which governs writer-side backfill.
* Only primitive `initial-default` values apply, because the Arrow-type tag vocabulary that `scan-types/type-mapping` owns is primitive-only. The read path reads `initial-default` from the current schema, and a non-null default requires format-version 3. An Iceberg `timestamptz` column is declared and emitted as plain Exasol `TIMESTAMP`.
* `scan-types/type-mapping` maps a decimal outside `1 ≤ p ≤ 36`, `0 ≤ s ≤ p` to the `utf8` tag. The scan reconstructs an encoded default against the tag alone, so the VS encodes no `initial-default` for such a field, and the field takes rule (4) where the Iceberg spec takes rule (3). This is a named Exasol target-type trade-off.
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: A decimal initial-default outside Exasol's catalog-decimal domain is not encoded as a numeric default

* *GIVEN* a virtual schema query whose Iceberg current schema defines a field of type `decimal(P,S)` outside `1 ≤ P ≤ 36`, `0 ≤ S ≤ P`, for example `decimal(38,10)` or `decimal(5,10)`, and which declares a non-null primitive `initial-default`
* *AND* that field's Arrow-type tag is therefore `utf8` rather than `decimal128(P,S)`, per `scan-types/type-mapping`
* *WHEN* the VS builds the logical schema from the Iceberg current schema and encodes each field's `initial-default` into the scan spec
* *THEN* the VS SHALL encode a decimal default exactly when the field's tag is `decimal128(P,S)`, so the domain that decides the tag also decides whether a default is encoded
* *AND* the VS SHALL leave that field's encoded default ABSENT, so the field falls through to NULL (nullable) or the required-absent error at scan time, exactly as a field carrying a non-primitive `initial-default` already does
* *AND* the VS MUST NOT encode the decimal literal's raw unscaled mantissa as that field's default, because the scan reconstructs an encoded default against the Arrow-type tag alone: under a `utf8` tag that mantissa reconstructs as its DIGITS in a string column, a fabricated value rather than a clean fallback
* *AND* a decimal field INSIDE the domain SHALL keep its recorded behavior: its tag stays `decimal128(P,S)`, its `initial-default` is still encoded, and it still reconstructs to a value of that type across the scan-spec serialization round-trip
<!-- /DELTA:CHANGED -->
