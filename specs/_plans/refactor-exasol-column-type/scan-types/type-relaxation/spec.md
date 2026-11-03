# Feature: Type Relaxation

Reads a data file whose physical column type is narrower than the table's current logical type, the shape Delta type widening and Iceberg type promotion both leave behind. The scan casts each file's column up to the current type, so a schema-evolved table returns its real values under its current types.

<!-- DELTA:CHANGED -->
## Background

* Type relaxation is one format-neutral read behavior with two writers. Delta calls it type widening and Iceberg calls it type promotion. Both leave older data files with the pre-change physical type while the current schema carries the changed one. Which pairs a writer can produce is the business of `delta/delta-type-mapping` and `file-planning/iceberg-type-promotion`. This feature reads whatever the logical schema declares.
* Delta protocol, § Reader Requirements for Type Widening: "Readers must allow reading data files written before the table underwent any supported type change, and must convert such values to the current, wider type." § Consistency Between Table Metadata and Data Files: "Any data file column that exists in the table schema MUST have the same type (except as allowed by the [Type Widening] table feature, if enabled)."
* The Iceberg spec has no equivalently direct sentence. Its obligation is derived: § Column Projection says "Columns in Iceberg data files are selected by field id … projection must be done using field ids", and promotion never rewrites a data file, so the cast is the reader's only way to answer. The spec's nearest analogue is its manifest rule, "reading `int` as `long` for promoted fields".
* The cast mechanism already exists. `register_file_list` registers the DataFusion table schema from the scan spec's `LogicalField` list, never from a Parquet footer, for every Iceberg and Delta scan. The Parquet opener then sees a logical file schema with the current type and a physical file schema with the old one. `FieldIdExprAdapterFactory` hands both to DataFusion's `DefaultPhysicalExprAdapterFactory`, whose `rewrite_column` wraps the column in a `CastExpr` on any field inequality. `bind_columns` renames a physical field to the logical name that claims it and never compares data types.
* The cast is inserted per file, because the Parquet opener creates the adapter from each file's own footer schema. A shard whose files straddle the change binds the old files through a cast and the new files through the identity path.
* DataFusion validates castability with `arrow::compute::can_cast_types`, not with either format's promotion rule, and a rejected pair becomes a clean `DataFusionError::Execution` naming the column and both types. `can_cast_types` also accepts narrowing casts that no format permits, so the supported set is decided at plan time by the two format features.
* The two cast sites carry opposite overflow policies. DataFusion's read-side `CastExpr` uses `safe: false` and errors on overflow. The emit boundary's `coerce_batch_to_exa_types` calls `arrow::compute::cast` with the default `safe: true`, which turns an overflowing value into NULL. Every pair in the supported set is a widening, so no value overflows at either site.
* The emit boundary needs no pair knowledge. `coerce_batch_to_exa_types` casts every column whose Arrow type differs from the type `target_arrow_type` derives from the `ExaType` the SDK reports for the declared EMITS column. A relaxed column arrives with the current logical type.
* A relaxation can change a column's declared Exasol type: `int` declares `DECIMAL(10,0)` and `long` declares `DECIMAL(20,0)`. `createVirtualSchema` reads the catalog's current schema, so a virtual schema created before the relaxation declares the old type until `REFRESH` (`vs-adapter/refresh-and-set-properties`).
* The supported set is the union of the two formats' rules. Every row is a widening for which `arrow::compute::can_cast_types` reports `true` and no value is lost. This engine tags `byte`, `short`, and `integer` all as `int32` (`delta/delta-type-mapping`), so those widenings show only as an `Int8` or `Int16` physical column under an `Int32` logical one.

  | # | Source → target | Delta | Iceberg | Physical → logical Arrow |
  |---|---|---|---|---|
  | 1 | `int` → `long` | yes | v1+ | `Int32` → `Int64` |
  | 2 | `float` → `double` | yes | v1+ | `Float32` → `Float64` |
  | 3 | `decimal(P,S)` → `decimal(P',S)`, `P' > P` | yes | v1+ | `Decimal128(P,S)` → `Decimal128(P',S)` |
  | 4 | `byte` → `short` | yes | no | `Int8` → `Int32` |
  | 5 | `byte` → `int` | yes | no | `Int8` → `Int32` |
  | 6 | `byte` → `long` | yes | no | `Int8` → `Int64` |
  | 7 | `short` → `int` | yes | no | `Int16` → `Int32` |
  | 8 | `short` → `long` | yes | no | `Int16` → `Int64` |
  | 9 | `byte` / `short` / `int` → `double` | yes | no | `Int8` / `Int16` / `Int32` → `Float64` |
  | 10 | `byte` / `short` / `int` → `decimal(10+k1,k2)` | yes | no | `Int8` / `Int16` / `Int32` → `Decimal128` |
  | 11 | `long` → `decimal(20+k1,k2)` | yes | no | `Int64` → `Decimal128` |
  | 12 | `decimal(p,s)` → `decimal(p+k1,s+k2)`, `k1 ≥ k2 > 0` | yes | no | `Decimal128(p,s)` → `Decimal128(p+k1,s+k2)` |
  | 13 | `date` → `timestamp without time zone` | yes | no | `Date32` → `Timestamp(us, None)` |

* Iceberg's `date` → `timestamp` and `date` → `timestamp_ns` (both v3+) are refused at plan time by `file-planning/iceberg-type-promotion`. Iceberg's `unknown` → any type is unreachable because the `iceberg` dependency has no `PrimitiveType::Unknown`. Delta's `date` → `timestampNtz` (row 13) is supported.
* `long` → `double` is in neither format's rules and is not supported. The Delta protocol lists "`Byte`, `Short` or `Int` -> `Double`" and omits `Long`, which is lossy above 2^53. Iceberg's promotion table has no such row.
* The Iceberg promotions in the supported set (rows 1, 2, and 3) are the `int` → `long`, `float` → `double`, and `decimal(P,S)` → `decimal(P',S)` rows of § Schema Evolution, whose decimal Requirements cell reads "Widen precision only". Row 12's scale growth is Delta-only. Relaxation changes what a present column is cast to, never how an absent one is resolved (`scan-read-path/scan-execution-field-id-projection`).
<!-- /DELTA:CHANGED -->

## Scenarios

### Scenario: A narrow physical column binds to the current wider logical type and is cast per file

* *GIVEN* a scan spec whose logical schema declares a column at the table's CURRENT type, the type after a Delta type widening, an Iceberg type promotion, or a direct-storage footer fold
* *AND* two assigned files, one written BEFORE the change whose physical Parquet column carries the narrow source type, and one written AFTER whose physical column carries the current type
* *WHEN* the scan UDF reads both files in one shard
* *THEN* the UDF SHALL register the DataFusion table schema from the scan spec's `LogicalField` list and MUST NOT infer it from any data file, so the column's declared type is the CURRENT one for both files; the column-binding adapter SHALL choose the narrow physical field by its binding key alone, field-id, declared physical name, or identity, and MUST NOT compare Arrow data types to decide which field binds, because a type-equality binding would fail on exactly the file this scenario exists for
* *AND* the delegated `DefaultPhysicalExprAdapter` SHALL insert the physical-to-logical cast into the physical expression tree, so every filter, projection, aggregate, and join key evaluated by DataFusion sees the column at its CURRENT type rather than its physical one
* *AND* the emitted rows from the OLD file SHALL carry that file's real values widened to the current type, and the emitted rows from the NEW file SHALL carry theirs unchanged; the cast SHALL be decided PER FILE from that file's own footer schema, so a shard straddling the change needs no per-shard grouping by physical layout
