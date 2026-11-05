# Feature: Timestamp Precision in DataFusion-to-Exasol Type Mapping

Defines the version-gated fractional-second precision Exasol declares for a catalog timestamp column (Iceberg `timestamp`/`timestamptz`/`timestamp_ns`/`timestamptz_ns`, Delta/Unity Catalog `TIMESTAMP`/`TIMESTAMP_NTZ`). It also records the `timestamptz` zone-flattening trade-off, which the precision gate does not change.

## Background

* Both timezone-naive (`Timestamp(_, None)`) and timezone-aware (`Timestamp(_, Some(_))`) Arrow timestamps map to plain Exasol `TIMESTAMP`. An Iceberg `timestamptz` or `timestamptz_ns` column is registered internally as `Timestamp(_, Some("UTC"))`, so DataFusion's comparisons, date functions, and predicate binding stay timezone-correct. It is declared to Exasol and emitted as plain `TIMESTAMP` carrying the UTC-instant value.
* Iceberg table spec: a `timestamptz` value is an instant whose values "are stored as UTC and do not retain a source time zone", so no per-value time zone is lost. Exasol rejects `TIMESTAMP WITH LOCAL TIME ZONE` as a UDF `EMITS` output type (`sqlCode 22002`), so the declared column cannot distinguish `timestamptz` from `timestamp`. This is a named Exasol target-type trade-off, not a change to any emitted value.
* Zone-awareness and precision are independent decisions.
* Exasol version strings look like `8.29.13` (8.x line) and `2025.2.1` (calendar-versioned line). A leading-component parse separates the two lines.
* The default on an unreadable or empty version is the unclamped arm. A loud failure on an unknown engine is preferred over silent data loss on every known one.
* `arrow_to_exasol_type` is not threaded through the version gate. The direct-storage kind declares a Parquet timestamp through the gate before it reaches that function. `needs_json_fallback` answers `false` for every `Timestamp(_, _)` at any precision.
* Parquet format, `LogicalTypes.md` § Timestamp: "`unit` must be one of `MILLIS`, `MICROS` or `NANOS`".
* `ExaType::Timestamp { precision }` carries the declared fractional-second precision, so the emit boundary reads it and the Arrow coercion target follows it.
* A timestamp column's precision has two independent axes, each owned by one type in `types/mapping.rs`. `TimestampPrecision` owns the source axis: the width the catalog declares for one column, its Exasol declaration type, its Arrow `TimeUnit`, and the reverse reading of a declared Exasol precision. `EngineTimestampSupport` owns the engine axis: the width the running engine can emit, which is a property of the request. It owns the version rule and the clamp, and takes a version `&str` rather than a `UdfContext`, so the module performs no I/O and the single `ctx.database_version()` read stays in `vs-adapter/create-virtual-schema`. Both producers name a source width and clamp it through the engine value, and neither carries a declaration literal.
* The source axis takes three values: millisecond, microsecond, nanosecond. These are the three sub-second Parquet timestamp encodings and the three sub-second Arrow `TimeUnit`s, so the set is format-neutral. A format reader names a value, never an Exasol type.
* Iceberg table spec, § Primitive Types: `timestamp` and `timestamptz` are "Timestamp, microsecond precision", and `timestamp_ns` and `timestamptz_ns` are "Timestamp, nanosecond precision". § Appendix E: "Types `variant`, `geometry`, `geography`, `unknown`, `timestamp_ns`, and `timestamptz_ns` are added in v3".
* The Delta Lake protocol defines no nanosecond timestamp type. § Schema Serialization Format types `timestamp` as "Microsecond precision timestamp elapsed since the Unix epoch". Spark's type vocabulary, which Unity's `type_json` uses, carries only `timestamp` and `timestamp_ntz`. The Delta/Unity producer therefore stays at microsecond by protocol, not by omission.
* The engine axis is an emit capability. An Exasol 8.x UDF emits only millisecond precision, and Exasol 2025.x and later emit 3, 6, and 9. A nanosecond Iceberg column queried through an 8.x engine loses six digits, as a microsecond one loses three.
* `[C1]` on 2025.2.1: a column declared `{"type":"timestamp","fractionalSecondsPrecision":9}` is accepted and honored, reported by `SYS.EXA_ALL_COLUMNS` as `TIMESTAMP(9)`. `[C1]` on 8.29.13: the same DDL is accepted and both 6 and 9 are silently downgraded to `TIMESTAMP(3)`. `[C2]` on 2025.2.1: the pushdown request echoes `fractionalSecondsPrecision`. On 8.29.13 it omits the key, so `exasol_type_from_json` declares bare `TIMESTAMP`.
* The nanosecond digits reach DataFusion intact: the `iceberg` dependency maps `TimestampNs`/`TimestamptzNs` to Arrow `Timestamp(Nanosecond, _)`, and the scan reads through DataFusion's `ParquetSource` with a field-id adapter.
* `raw_scan.rs` sets DataFusion's `coerce_int96 = "us"`, which forces an INT96 physical column to microseconds whatever its logical type. Iceberg writes `timestamp_ns` as an INT64 `TIMESTAMP(NANOS)` column, so a spec-conformant column is unaffected. A legacy file storing a nanosecond-typed column as INT96 arrives at microsecond. The direct-storage footer read applies the same coercion.
* For a catalog timestamp column, the adapter declares the clamped width and the scan emits at exactly that width, so no component truncates afterwards. For a projected `CAST(x AS TIMESTAMP(p))`, the adapter echoes Exasol's own `selectListDataTypes` type verbatim. DataFusion computes the value when `p` is in `{0, 3, 6, 9}`, and Exasol computes it in the adapter's wrapper otherwise (`sql-comprehension/vs-expression-translator-cast`).

## Scenarios

<!-- DELTA:REMOVED -->
### Scenario: The Arrow-input type resolver stays outside the version gate

* *GIVEN* `arrow_to_exasol_type` and its private `compatible_exasol_type`, the ARROW-INPUT direction, whose `DataType::Timestamp(_, _)` arm returns the bare string `TIMESTAMP`
* *WHEN* the version gate is threaded through the catalog-declared producers
* *THEN* neither function SHALL gain a precision parameter, and both SHALL keep their recorded signature and their recorded answer for every input byte-identical, including `TIMESTAMP` for every `Timestamp(_, _)`
* *AND* the exclusion SHALL hold because the direct-storage kind resolves a Parquet timestamp through the version gate before it reaches the Arrow-input direction, and the only production consumer of `compatible_exasol_type` is `needs_json_fallback`, whose answer for a `Timestamp(_, _)` is `false` at every precision
* *AND* `needs_json_fallback` SHALL keep its recorded `fn(&DataType) -> bool` signature and its answer for every input unchanged, so none of its call sites move
* *AND* the compatible-Arrow-types table's `Timestamp(_, _) | TIMESTAMP` row SHALL stay unamended (`scan-types/type-mapping`), and a reader MUST NOT read it as governing the catalog-declared declaration this delta version-gates
* *AND* the exclusion SHALL be stated here rather than left silent, because `arrow_to_exasol_type` looks like a gate target and an unexplained omission is indistinguishable from an oversight
<!-- /DELTA:REMOVED -->
