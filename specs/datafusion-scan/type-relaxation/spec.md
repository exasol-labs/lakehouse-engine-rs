# Feature: Type Relaxation

Reads a data file whose physical column type is NARROWER than the table's current logical type — the
shape Delta type widening and Iceberg type promotion both leave behind — by casting each file's
column up to the current type at scan time, so a schema-evolved table returns its real values under
its current types instead of wrong values or an unresolved-column error.

## Background

**Type relaxation is ONE format-neutral read behavior with two writers.** Delta calls it type
widening and Iceberg calls it type promotion; both leave older data files carrying the pre-change
physical type while the table's current schema carries the changed one. This feature owns the read
answer for both. It owns no format-specific rule: which pairs a writer may produce is
`vs-adapter/delta-type-mapping`'s and `vs-adapter/iceberg-type-promotion`'s business, and this
feature reads whatever the logical schema declares.

**Both formats state the reader obligation normatively.** The Delta Lake protocol specification
(`delta-io/delta`, `PROTOCOL.md`, `master`, § Reader Requirements for Type Widening) states:
*"Readers must allow reading data files written before the table underwent any supported type
change, and must convert such values to the current, wider type."* Its § Consistency Between Table
Metadata and Data Files supplies the licence the old file relies on: *"Any data file column that
exists in the table schema MUST have the same type (except as allowed by the [Type Widening] table
feature, if enabled)."* The Apache Iceberg table specification
(<https://iceberg.apache.org/spec/#schema-evolution>) carries no equivalently direct sentence — its
obligation is derived, and this feature records that honestly rather than quoting a MUST that does
not exist: *"Columns in Iceberg data files are selected by field id … projection must be done using
field ids"* (§ Column Projection), combined with promotion never rewriting a data file, leaves the
cast as the reader's only way to answer. The spec's nearest direct analogue is its manifest rule,
*"reading `int` as `long` for promoted fields"*.

**The cast mechanism ALREADY EXISTS and this feature is not building it.** `register_file_list`
(`crates/lakehouse-engine/src/scan/raw_scan.rs`) registers the DataFusion table schema from the scan
spec's `LogicalField` list — never from a Parquet footer — whenever that list is non-empty, which it
is for every Iceberg and every Delta scan. The Parquet opener then sees a logical file schema
carrying the CURRENT type and a physical file schema carrying the OLD one, and
`FieldIdExprAdapterFactory` (`crates/lakehouse-engine/src/scan/field_id_projection.rs`) hands both to
DataFusion's `DefaultPhysicalExprAdapterFactory`, whose `rewrite_column` wraps the resolved column in
a `CastExpr` on any field inequality. `bind_columns` renames a physical field to the logical name
that claims it and NEVER compares data types, which is precisely why a narrow physical field arrives
at the delegate under the logical name still carrying its narrow type. This feature's work is to
verify that path over every pair in the supported set and to record the answer, not to add a cast
layer.

**The recorded claim that `.without_row_transforms()` opens a type-widening hole is WRONG and this
feature supersedes it.** `vs-adapter/delta-reader-feature-gating` records that
*"`DeltaSnapshot::active_files` builds its kernel scan with `.without_row_transforms()`, so no
per-file cast transform is applied"*. `delta_kernel` 0.26's own doc on that builder method scopes it
to *"partition column injection, column-mapping renames, and generated row ids"*, and `delta_kernel`
0.26 implements NO type-widening cast anywhere — its `TableFeature::TypeWidening` handling is a
capability declaration and a schema-comparison validator, never a cast. There was therefore no cast
transform for `.without_row_transforms()` to discard. The correct statement is that ANY engine on
`delta_kernel` 0.26 must apply the widening cast itself, and this engine already does, through the
format-neutral adapter chain above.

**The cast is inserted per FILE, not per table**, because the Parquet opener creates the adapter from
each file's own footer schema. A scan whose assigned files straddle the change therefore binds the
old files through a cast and the new files through the zero-cost identity path, within one shard.

**DataFusion validates castability with `arrow::compute::can_cast_types`, not with either format's
promotion rule.** `validate_data_type_compatibility` (`datafusion-common`) is the whole check for a
scalar pair, and a pair it rejects becomes a clean `DataFusionError::Execution` naming the column and
both types — never a silent passthrough. That permissiveness is why this engine's supported set is
decided at PLAN time by the two format features, not left to the cast to police: `can_cast_types`
would equally accept a NARROWING cast that no format permits.

**The two cast sites carry OPPOSITE overflow policies, and only the widening direction makes that
safe.** DataFusion's read-side `CastExpr` uses `safe: false` and errors on overflow; the emit
boundary's `coerce_batch_to_exa_types` calls `arrow::compute::cast`, whose default `CastOptions` is
`safe: true` and turns an overflowing value into a NULL with an `Ok` result. Every pair in the
supported set is a WIDENING, so no value can overflow either site, and the asymmetry is unreachable
from this feature. It is recorded because it is what makes the supported set's widening-only
membership load-bearing rather than incidental.

**The emit boundary needs no pair knowledge and gains none.** `coerce_batch_to_exa_types`
(`crates/lakehouse-engine/src/scan/emit.rs`) already casts every column whose Arrow type differs from
the target `exasol_type_to_arrow` derives from the declared EMITS type, through one unguarded
`arrow::compute::cast` call with no per-pair match. A relaxed column reaches it already carrying the
CURRENT logical type, so the emit cast sees exactly what it would have seen had the table never
evolved.

**A relaxation can change a column's DECLARED Exasol type, which a stale virtual schema will not
show.** `int` declares `DECIMAL(10,0)` and `long` declares `DECIMAL(20,0)`, so an `int → long`
relaxation moves the declaration. `createVirtualSchema` reads the catalog's CURRENT schema, so a
virtual schema created BEFORE the relaxation still declares the old type until it is refreshed
(`vs-adapter/refresh-and-set-properties`). This is the ordinary stale-metadata consequence of schema
evolution, not a defect of the cast, and it is recorded here because a type change is the case where
a stale declaration is most likely to be read as a bug in this feature.

**The supported set is the union of the two formats' rules, decided pair by pair.** Every row below
is a WIDENING for which `arrow::compute::can_cast_types` reports `true` and no value can be lost. The
"Physical → logical Arrow" column names what the scan actually casts, which is what makes several
Delta rows collapse: this engine tags `byte`, `short`, and `integer` all as `int32`
(`vs-adapter/delta-type-mapping`), so those three widenings are invisible in the logical schema and
show up only as an `Int8`/`Int16` physical column under an `Int32` logical one.

| # | Source → target | Delta | Iceberg | Physical → logical Arrow |
|---|---|---|---|---|
| 1 | `int` → `long` | yes | v1+ | `Int32` → `Int64` |
| 2 | `float` → `double` | yes | v1+ | `Float32` → `Float64` |
| 3 | `decimal(P,S)` → `decimal(P',S)`, `P' > P` | yes | v1+ | `Decimal128(P,S)` → `Decimal128(P',S)` |
| 4 | `byte` → `short` | yes | — | `Int8` → `Int32` |
| 5 | `byte` → `int` | yes | — | `Int8` → `Int32` |
| 6 | `byte` → `long` | yes | — | `Int8` → `Int64` |
| 7 | `short` → `int` | yes | — | `Int16` → `Int32` |
| 8 | `short` → `long` | yes | — | `Int16` → `Int64` |
| 9 | `byte` / `short` / `int` → `double` | yes | — | `Int8` / `Int16` / `Int32` → `Float64` |
| 10 | `byte` / `short` / `int` → `decimal(10+k1,k2)` | yes | — | `Int8` / `Int16` / `Int32` → `Decimal128` |
| 11 | `long` → `decimal(20+k1,k2)` | yes | — | `Int64` → `Decimal128` |
| 12 | `decimal(p,s)` → `decimal(p+k1,s+k2)`, `k1 ≥ k2 > 0` | yes | — | `Decimal128(p,s)` → `Decimal128(p+k1,s+k2)` |
| 13 | `date` → `timestamp without time zone` | yes | — | `Date32` → `Timestamp(us, None)` |

**Three Iceberg promotions are NOT in the supported set, and each is refused rather than attempted.**
Iceberg's `date` → `timestamp` and `date` → `timestamp_ns` (both v3+) are refused at plan time by
`vs-adapter/iceberg-type-promotion`, which also owns the reason; Iceberg's `unknown` → any type is
unreachable because `iceberg` 0.10.0 has no `PrimitiveType::Unknown`. Delta's `date` →
`timestampNtz` (row 13) IS supported, and that asymmetry between two spellings of the same logical
pair is deliberate: the difference lives entirely in how each format stores per-file bounds, not in
the cast, and `vs-adapter/iceberg-type-promotion` records it.

**`long` → `double` is in NEITHER format's rules and is therefore NOT in the supported set.** The
Delta protocol lists *"`Byte`, `Short` or `Int` -> `Double`"* and deliberately omits `Long`, which is
lossy above 2^53; Iceberg's promotion table has no such row at all. It is named here because a reader
scanning the widening list for "integer to floating point" would otherwise assume it.

**Apache Iceberg spec check.** The three Iceberg promotions this feature DOES support — rows 1, 2,
and 3 — are exactly the `int` → `long`, `float` → `double`, and `decimal(P,S)` → `decimal(P',S)` rows
of the spec's § Schema Evolution promotion table, whose decimal Requirements cell reads *"Widen
precision only"* with the scale symbol `S` unchanged on both sides. Row 12's scale growth is
therefore Delta-only and MUST NOT be read as an Iceberg promotion. The spec's § Column Projection
ordered resolution for an absent field id is untouched by this feature, and
`datafusion-scan/scan-execution-field-id-projection`'s recorded deviation on its rule (1) stays
exactly as recorded — relaxation changes what a PRESENT column is cast to, never how an ABSENT one is
resolved.

## Scenarios

### Scenario: A narrow physical column binds to the current wider logical type and is cast per file

* *GIVEN* a scan spec whose logical schema declares a column at the table's CURRENT type — the type
  after a Delta type widening or an Iceberg type promotion
* *AND* two assigned files, one written BEFORE the change whose physical Parquet column carries the
  narrow source type, and one written AFTER whose physical column carries the current type
* *WHEN* the scan UDF reads both files in one shard
* *THEN* the UDF SHALL register the DataFusion table schema from the scan spec's `LogicalField` list
  and MUST NOT infer it from any data file, so the column's declared type is the CURRENT one for both
  files
* *AND* the column-binding adapter SHALL resolve the narrow physical field to that logical column by
  its binding key alone — field-id, declared physical name, or identity — and MUST NOT require, test,
  or compare the physical field's Arrow data type, because a binding that depended on type equality
  would fail on exactly the file this scenario exists for
* *AND* the delegated `DefaultPhysicalExprAdapter` SHALL insert the physical-to-logical cast into the
  physical expression tree, so every filter, projection, aggregate, and join key evaluated by
  DataFusion sees the column at its CURRENT type rather than its physical one
* *AND* the emitted rows from the OLD file SHALL carry that file's real values widened to the current
  type, and the emitted rows from the NEW file SHALL carry theirs unchanged, so the two files return
  one consistent column
* *AND* the cast SHALL be decided PER FILE from that file's own footer schema, so a shard straddling
  the change needs no per-shard grouping by physical layout
* *AND* this resolution SHALL hold identically for an Iceberg scan and a Delta scan, because both
  populate the same `LogicalField` list and install the same adapter — the scan side MUST NOT branch
  on table format

### Scenario: Every supported relaxation pair is proven castable rather than assumed

* *GIVEN* the 13 rows of the supported-set table above, whose membership rests on
  `arrow::compute::can_cast_types` reporting `true` for each physical-to-logical Arrow pair and on
  the cast losing no value
* *WHEN* the type-relaxation test suite runs
* *THEN* it SHALL assert `arrow::compute::can_cast_types(physical, logical)` directly for every row's
  Arrow pair — `Int8`/`Int16`/`Int32` → `Int32`/`Int64`, `Int8`/`Int16`/`Int32` → `Float64`,
  `Float32` → `Float64`, `Int8`/`Int16`/`Int32`/`Int64` → `Decimal128`, `Decimal128(P,S)` →
  `Decimal128(P',S)` and `Decimal128(p+k1,s+k2)`, and `Date32` → `Timestamp(us, None)`
* *AND* it SHALL additionally read each pair through a real Parquet file written at the physical type
  and registered under a logical schema carrying the target type, asserting the returned VALUES, so a
  pair that is castable in principle but wrong in practice fails here rather than in an E2E suite
* *AND* the suite SHALL assert `can_cast_types` is `false` for NO row of the table, so an
  `arrow-cast` upgrade that withdraws a pair FAILS rather than silently re-partitioning the supported
  set
* *AND* the suite SHALL assert that `long` → `double` is absent from the supported set, because
  neither format permits it and `can_cast_types` alone would admit it

### Scenario: A relaxed column crosses the emit boundary at its declared Exasol type

* *GIVEN* a scan whose relaxed column has been cast to the table's current logical Arrow type, and an
  `EMITS` declaration derived from that same current type
* *WHEN* the scan coerces the batch at the emit boundary before emitting it
* *THEN* `coerce_batch_to_exa_types` SHALL treat the column exactly as it treats an unevolved column
  of that type, and this feature MUST NOT add any relaxation-aware branch, pair table, or allow-list
  to the emit path, because the column already carries its current type by the time it arrives
* *AND* the emitted value SHALL equal the source file's value widened to the current type, with no
  value replaced by NULL, because every supported pair is a widening and no widening can overflow the
  emit cast's `safe: true` policy
* *AND* the `EMITS` type SHALL be derived from the catalog's CURRENT schema for the request being
  planned, so a relaxation that moves a column's declared Exasol type — `int`'s `DECIMAL(10,0)`
  becoming `long`'s `DECIMAL(20,0)` — moves the emitted type with it
* *AND* a virtual schema created before the relaxation SHALL keep declaring the OLD Exasol type until
  it is refreshed, and that staleness SHALL be resolved by `REFRESH VIRTUAL SCHEMA`
  (`vs-adapter/refresh-and-set-properties`) rather than by any scan-side compensation, because the
  scan is not the owner of a declaration Exasol already stored
