# Feature: DataFusion Scan Value Conversion

Converts each output Arrow column to the SDK `Value` variant (or coerced Arrow type) the emit
boundary requires. This covers the Arrow-to-`Value` variant mapping, the emission of Exasol-
incompatible columns as JSON strings, and the coercion of a batch's Arrow types to the declared
EMITS `ExaType` before `emit_batch`. It owns the value-conversion boundary only — file
registration, filtering, LIMIT, streaming, and error handling belong to
`datafusion-scan/scan-execution`; Iceberg-to-Arrow and Arrow-to-Exasol type-mapping *rules* belong
to `datafusion-scan/type-mapping`.

## Background

<!-- DELTA:NEW -->
* **This delta is issue #399.** It changes WHERE the emit boundary reads its declared output type
  from. It changes ONE scenario. The Arrow-to-`Value` mapping and the JSON-string scenario are
  untouched.
* **`LAKEHOUSE_SCAN` is declared `EMITS (...)`, the dynamic-output form.** The script DDL
  (`deploy/scripts/install.sh`, `crates/lakehouse-engine/tests/common/e2e_harness.rs`) carries no
  static column list, so the call-site `EMITS (...)` clause the adapter generates is the sole
  declaration of that call's output schema.
* **`CommonScanSpec::emit_exa_types` was a second copy of that clause.** The adapter built both
  from one `proj_types` vector: the clause Exasol parses, and a JSON array the UDF trusted without
  checking. Two transmissions of one decision, with nothing enforcing agreement.
* **`exasol-udf-sdk` 0.26.0 supplies the authoritative reading.** `UdfContext::output_column(idx)`
  returns the `ColumnInfo` the database reported for output column `idx`, carrying the `ExaType`
  the column's values travel in plus `precision` and `scale`; `output_column_count()` reports the
  declared arity. Upstream `language-container-rs` PR #105 wires these from the call-site `EMITS`
  list. Its live `column-meta` integration fixture covers one shape only: the same entry point
  follows a re-registered STATIC `EMITS` column list carried in the script DDL. `LAKEHOUSE_SCAN`
  uses the dynamic form, where the DDL carries `EMITS (...)` with no list and the adapter supplies
  the columns at the call site. That dynamic call-site form is proven separately, against the local
  Exasol Docker container, before any removal lands.
* **Reading `ExaType` removes a replicated engine decision.** `exasol_type_to_arrow` parsed a type
  string and re-derived Exasol's DECIMAL-to-ExaType precision binning (scale-0 precision at most 9
  to Int32, at most 18 to Int64, otherwise Numeric). The engine reports the bin it chose, so the
  scan reads the result instead of recomputing it and can no longer disagree with it.
* **`exasol_type_to_arrow` keeps its `pub` visibility and its tests.** It loses its only production
  call site. `datafusion-scan/type-mapping-module-structure` already records the same status for
  its inverse `arrow_to_exasol_type`: it is a CLAUDE.md § Data types compliance surface, and
  removing a public API item is a scope ADD.
* **The declared-type list is no longer optional, so the pre-`emit_exa_types` fallback is gone.**
  Every scan call has a call-site `EMITS` clause. An absent or wrong-arity declaration is drift, not
  a legacy spec, and is reported rather than worked around.
* **`ExaType::Numeric`'s optional `precision` and `scale` are a wire artifact, not an expected
  absence.** The protobuf `column_definition` carries one `optional precision` and `optional scale`
  pair for every column type, unset outside DECIMAL. `column_from_pb` (`exa-zmq-protocol`'s
  `meta.rs`) forwards both into `ExaType::Numeric` unchecked. A valid Exasol NUMERIC declaration
  always carries both. Exasol caps DECIMAL precision at 36, inside what `Decimal128` represents. An
  absent or out-of-range payload is therefore drift, and both emit paths fail the call on it.
* **The Iceberg and Delta type contracts are untouched.** Apache Iceberg `#### Schemas and Data
  Types` states "A table's **schema** is a list of named columns. Data types are primitive, nested,
  or semi-structured", and Delta `Schema Serialization Format` holds the schema in the `metaData`
  action's required `schemaString`. Neither governs the Exasol-side output declaration. The
  Iceberg-to-Exasol and Delta-to-Exasol mapping rules stay in
  `crates/lakehouse-engine/src/types/mapping.rs`, applied at `createVirtualSchema` and at `EMITS`
  rendering, and this delta changes none of them. No new deviation and no tracked exception arises.
<!-- /DELTA:NEW -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Output columns are coerced to the Arrow type the declared EMITS ExaType requires before emit_batch

* *GIVEN* a scan call whose generated `EMITS (...)` clause declares one Exasol type per output column
* *AND* a result Arrow batch whose column types diverge from those declarations, for example an `Int32` column declared `DECIMAL(20,0)`, a `Float32` column declared `DOUBLE PRECISION`, or a `Utf8View` column declared `VARCHAR(2000000)`
* *WHEN* the scan UDF processes the batch
* *THEN* the UDF SHALL read the declared type of output column `i` from `UdfContext::output_column(i)`, and MUST NOT read any declared type carried in the scan spec
* *AND* the UDF SHALL coerce each output column to the Arrow type that `emit_batch`'s strict IPC feed requires for the reported `ExaType`, before passing the batch to `emit_batch`, taking the DECIMAL binning from the reported variant rather than re-deriving it from a type string: `Int32` to `Int32`, `Int64` to `Int64`, and `Numeric { precision, scale }` to `Decimal128(precision, scale)`
* *AND* the remaining variants SHALL map as `Double` to `Float64`, `Boolean` to `Boolean`, `Date` to `Date32`, `Timestamp` to `Timestamp(Microsecond, None)`, `TimestampTz` to `Timestamp(Microsecond, Some("UTC"))`, and every other variant, `String` and `Char` included, to `Utf8`, which subsumes `Utf8View`/`BinaryView` normalization and preserves the behavior the removed type-string path gave an unrecognized declaration
* *AND* the UDF SHALL fail the call on either emit path, naming the offending output column, when a `Numeric` column reports an absent `precision` or `scale`, or reports a `precision` or `scale` outside what `Decimal128` represents, and MUST NOT substitute `Utf8` or any other target type, because a valid Exasol NUMERIC declaration always carries a precision and a scale within that range
* *AND* a column already of the required Arrow type SHALL be passed through unchanged, on the zero-copy fast path
* *AND* the UDF SHALL fail the call, and MUST NOT emit the batch, when `output_column_count()` does not equal the batch's column count, naming both counts, or when `output_column(i)` returns an error for a column the batch carries, naming `i`
* *AND* the UDF MUST NOT fall back to a spec-carried or source-derived type in either failure case, because an absent declaration is drift rather than a legacy spec
<!-- /DELTA:CHANGED -->
