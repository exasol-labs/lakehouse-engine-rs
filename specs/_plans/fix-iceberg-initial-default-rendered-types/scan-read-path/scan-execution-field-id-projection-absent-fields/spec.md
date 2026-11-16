<!-- DELTA:CHANGED -->
# Feature: DataFusion Scan Execution: Absent Fields and Initial Defaults

Defines what the scan UDF emits for a logical field that is absent from a data file. A field that defines an Iceberg `initial-default` returns that value for the file's rows, a nullable field with no default returns NULL, and a required field with no default fails the scan with a clean error. A field whose `identity` partition value the file entry carries takes that value instead, per `scan-read-path/scan-execution-identity-partition-fill`.

The VS reads each field's `initial-default` once per query and encodes every primitive default into the scan spec in a JSON-portable, credential-free form. It leaves the default absent only for a non-primitive default, and refuses a column whose primitive default it cannot render at the column's declared type (#367).
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
## Background

* The Iceberg table spec (`format/spec.md`, "Column Projection") resolves a field id absent from a data file in this order: (1) "Return the value from partition metadata if an Identity Transform exists for the field and the partition value is present in the `partition` struct on `data_file` object in the manifest", (2) `schema.name-mapping.default`, (3) "Return the default value if it has a defined `initial-default`", (4) "Return `null` in all other cases". The engine implements all four. Rule (2) is a binding rule of `scan-read-path/scan-execution-field-id-projection`, and rule (3) never reads `write-default`, which governs writer-side backfill.
* Only primitive `initial-default` values apply, because the Arrow-type tag vocabulary that `scan-types/type-mapping` owns is primitive-only. The read path reads `initial-default` from the current schema, and a non-null default requires format-version 3. An Iceberg `timestamptz` column is declared and emitted as plain Exasol `TIMESTAMP`.
* A primitive whose Arrow-type tag holds its value natively carries the raw value: the integer, float, string, days, ticks, or `i128` decimal mantissa. A `time` column and a decimal with `37 ≤ p ≤ 38`, `0 ≤ s ≤ p` map to the `utf8` tag and carry the default as the text a stored value of the column reads as. A `binary`, `fixed(L)`, or `uuid` column carries the default as lowercase hex, the form an identity partition value of the same column carries (`scan-read-path/scan-execution-identity-partition-fill`).
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: The VS encodes each field's Iceberg initial-default once per query into the scan spec

* *GIVEN* a virtual schema query whose Iceberg current schema defines a field carrying a primitive `initial-default` (required or nullable), a field carrying no `initial-default`, and a field carrying a non-primitive (struct / list / map) `initial-default`
* *WHEN* the VS planning layer builds the logical schema from the Iceberg current schema
* *THEN* the VS SHALL read each field's `initial-default` exactly once and encode every primitive default into that field's logical-schema entry in a JSON-portable, credential-free form that reconstructs to a `ScalarValue` matching the field's Arrow-type tag, and SHALL NOT read `write-default`
* *AND* the VS SHALL leave the encoded default absent for a field with no `initial-default` and for a field whose `initial-default` is non-primitive, so those fields fall through to NULL or the required-absent error at scan time
* *AND* a scan spec whose logical fields carry no encoded default SHALL deserialize unchanged (the encoded default is an optional field, backward-compatible with specs written before this feature)
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: Every supported primitive initial-default survives the scan-spec serialization round-trip

* *GIVEN* a scan spec whose logical schema carries one field for every supported primitive Arrow-type tag, meaning `bool`, `int32`, `int64`, `float32`, `float64`, `utf8`, `date32`, `timestamp_us`, `timestamp_ns`, `timestamptz_us`, `timestamptz_ns`, a `decimal128(p,s)` with non-trivial precision and scale, `binary`, `fixed_size_binary(L)`, and each further primitive tag the widened vocabulary carries for `Int8`, `Int16`, `UInt8`, `UInt16`, `UInt32`, `UInt64`, `LargeUtf8`, and the remaining `Timestamp` time units, each field encoding that type's `initial-default`
* *AND* one further field whose Iceberg `initial-default` is non-primitive (struct / list / map)
* *WHEN* the scan spec is serialized to JSON, deserialized, and each field's encoded default is reconstructed to a `ScalarValue`
* *THEN* the reconstructed `ScalarValue` for each primitive field SHALL equal the originally encoded value and SHALL match that field's Arrow-type tag, for every supported primitive tag in the vocabulary
* *AND* the tag list SHALL be read from the ONE vocabulary owner `scan-types/type-mapping` specifies, so widening the vocabulary cannot leave this scenario pinning a shorter list
* *AND* the vocabulary SHALL stay PRIMITIVE-ONLY, so the non-primitive field SHALL carry no encoded default after the round-trip and falls through to NULL (nullable) or the required-absent error at scan time
* *AND* a `binary` or `fixed_size_binary(L)` default SHALL reconstruct from its lowercase hex, and a `fixed_size_binary(L)` default whose hex does not decode to exactly L bytes SHALL fail reconstruction with a clean error, a guard for a scan spec the VS did not write, because the VS refuses such a column
* *AND* the serialized form SHALL be credential-free
<!-- /DELTA:CHANGED -->

<!-- DELTA:REMOVED -->
### Scenario: A decimal initial-default outside Exasol's DECIMAL domain is not encoded as a numeric default

* *GIVEN* a field of type `decimal(P,S)` outside `1 ≤ P ≤ 36`, `0 ≤ S ≤ P` with a non-null `initial-default`
* *WHEN* the VS encodes the field's `initial-default`
* *THEN* this behavior SHALL be removed; replaced by "An initial-default of a type without a native Arrow tag reads as a stored value of its column reads"
<!-- /DELTA:REMOVED -->

<!-- DELTA:NEW -->
### Scenario: An initial-default of a type without a native Arrow tag reads as a stored value of its column reads

* *GIVEN* a format-version 3 Iceberg table with one data file, after which columns are added, each with an `initial-default`: a nullable `time`, a REQUIRED `time`, a `decimal(38,2)`, a `binary`, a `fixed(4)`, and a `uuid`
* *AND* a second data file written after the columns were added
* *WHEN* a virtual schema query selects every column of the table
* *THEN* each row of the first file SHALL carry each added column's `initial-default`, rendered exactly as a stored value of that column renders in a row of the second file: `time` and the out-of-domain decimal as their Exasol `VARCHAR` text, and `binary`, `fixed(4)`, and `uuid` as lowercase hex
* *AND* the REQUIRED `time` column MUST NOT fail the scan with the required-absent error
* *AND* the VS MUST NOT encode a decimal literal's raw unscaled mantissa under a `utf8` tag, because the scan would read its digits as the column's text
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: A primitive initial-default that cannot be read at its column's declared type refuses the column

* *GIVEN* an Iceberg current schema whose fields declare a primitive `initial-default` that the VS cannot render at the field's declared type: a `decimal(5,10)` field, whose scale exceeds its precision, and a `fixed(4)` field whose default does not hold exactly 4 bytes
* *WHEN* the adapter records that column's declaration note
* *THEN* the note SHALL carry `refused` with a reason naming the column and stating that its `initial-default` cannot be read at the column's declared type, per `vs-adapter/column-source-notes`
* *AND* a query that reads that column SHALL fail with that reason, and a query that reads only other columns of the table SHALL succeed
* *AND* the VS MUST NOT leave the default absent in its place, because a NULL or the required-absent error would return wrong rows for files written before the column was added
<!-- /DELTA:NEW -->
