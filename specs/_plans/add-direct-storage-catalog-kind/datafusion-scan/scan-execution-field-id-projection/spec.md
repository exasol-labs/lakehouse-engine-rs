# Feature: Scan Execution Field-Id Projection

Binds each logical column to its physical Parquet field by field-id, declared physical name, name-mapping, or identity. This delta changes no recorded description. The paragraph here is plan-local context only.

## Background

* This delta amends exactly ONE scenario, "A logical field carrying no binding key binds by its own
  name", and is issue #407. Identity binding gains a SECOND producer. The direct-storage catalog
  kind declares every logical field with no field-id and no declared physical name, because a raw
  Parquet directory carries neither. The binding MECHANISM is unchanged. No production code path
  of this feature changes.
* The recorded scenario names the Delta `none` column mapping as the identity binding's source. That
  parenthetical becomes incomplete rather than wrong once the second producer exists, so the
  scenario names both.
* The new producer exercises one combination the recorded scenario already covers but no shipped
  path reached: a column ABSENT from some assigned files. A Delta table's `none` mapping declares
  one schema over files a single writer produced. A raw directory's merged schema is the UNION
  of its files' column sets. The NULL-fill path is therefore reached routinely rather than only
  after a schema evolution.
* The widening half is owned elsewhere. `datafusion-scan/type-relaxation` owns the per-file cast an
  identity-bound field receives when its physical type is narrower than its logical one.
  `vs-adapter/parquet-directory-seam` owns which pairs the declaration may widen across.
* The closing clause, that identity binding MUST NOT be reached by an Iceberg scan, stays TRUE and
  is carried unchanged. The Iceberg planning path still populates a field-id on every logical field.
* **The recorded twelve-tag enumeration of the logical schema's Arrow-type tag vocabulary is
  SUPERSEDED, in this feature's Background and in the round-trip scenario's GIVEN.**
  `datafusion-scan/type-mapping` widens that vocabulary to every Arrow type the compatible-type
  classifier admits, because the direct-storage producer folds types the recorded twelve cannot
  spell. The vocabulary stays PRIMITIVE-ONLY, so the recorded trade-off that a struct, list, or map
  `initial-default` is not represented is unchanged. Only the enumeration of primitives grows. The
  round-trip scenario below reads the widened list from its one owner rather than restating a
  fixed twelve.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: A logical field carrying no binding key binds by its own name

* *GIVEN* a scan spec whose logical schema carries a field with NO field-id and NO declared physical name — the identity binding a Delta `none` column mapping produces, and the one a direct-storage table's merged Parquet schema produces for EVERY field — a second such field that is ABSENT from one assigned file and nullable, and a third such field that is absent from that file, required, and carries no `initial-default`
* *WHEN* the scan UDF reads the assigned files
* *THEN* the UDF SHALL install the column-binding adapter, because the scan spec CARRIES a logical schema, and SHALL bind the identity-bound field to the physical column whose name equals its logical name
* *AND* the UDF SHALL emit NULL for the absent nullable field for rows from the file lacking it, and SHALL emit its `initial-default` instead when one is encoded, per file
* *AND* the UDF SHALL return the same clean required-absent error for the absent required field as it returns for a field-id-bound required column, and MUST NOT substitute NULL for it
* *AND* the UDF MUST NOT tag an identity-bound logical field with a `PARQUET:field_id`, because a synthesized id is a value no writer put in any file and would invite a false match against a file that does carry field-ids
* *AND* this identity binding MUST NOT be reached by an Iceberg scan, because the Iceberg planning path populates a field-id on every logical field
* *AND* the binding SHALL hold for a shard whose assigned files carry DIFFERENT column sets, because a direct-storage table's declared schema is the union of its files' columns, so the absent-column path is the ordinary case for that kind rather than an evolution edge case
* *AND* the binding MUST NOT require the physical field's Arrow type to equal the logical one, so an identity-bound field whose file carries a narrower type is cast per file by the adapter `datafusion-scan/type-relaxation` owns, exactly as a field-id-bound field is
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: Every supported primitive initial-default survives the scan-spec serialization round-trip

* *GIVEN* a scan spec whose logical schema carries one field for every supported primitive Arrow-type tag, meaning `bool`, `int32`, `int64`, `float32`, `float64`, `utf8`, `date32`, `timestamp_us`, `timestamp_ns`, `timestamptz_us`, `timestamptz_ns`, a `decimal128(p,s)` with non-trivial precision and scale, and each further primitive tag the widened vocabulary carries for `Int8`, `Int16`, `UInt8`, `UInt16`, `UInt32`, `UInt64`, `LargeUtf8`, and the remaining `Timestamp` time units, each field encoding that type's `initial-default`
* *AND* one further field whose Iceberg `initial-default` is non-primitive (struct / list / map)
* *WHEN* the scan spec is serialized to JSON, deserialized, and each field's encoded default is reconstructed to a `ScalarValue`
* *THEN* the reconstructed `ScalarValue` for each primitive field SHALL equal the originally encoded value and SHALL match that field's Arrow-type tag, for every supported primitive tag in the vocabulary
* *AND* the tag list SHALL be read from the ONE vocabulary owner `datafusion-scan/type-mapping` specifies, SUPERSEDING the recorded twelve-tag enumeration both in this GIVEN and in this feature's Background, so widening the vocabulary cannot leave this scenario pinning a shorter list
* *AND* the vocabulary SHALL stay PRIMITIVE-ONLY, so the non-primitive field SHALL carry no encoded default after the round-trip and falls through to NULL (nullable) or the required-absent error at scan time, exactly as recorded
* *AND* the serialized form SHALL be credential-free
<!-- /DELTA:CHANGED -->
