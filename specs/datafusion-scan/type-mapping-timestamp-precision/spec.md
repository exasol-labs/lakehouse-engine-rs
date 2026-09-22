# Feature: Timestamp Precision in DataFusion-to-Exasol Type Mapping

Defines the version-gated fractional-second precision Exasol declares for a catalog timestamp
column (Iceberg `timestamp`/`timestamptz`/`timestamp_ns`/`timestamptz_ns`, Delta/Unity Catalog
`TIMESTAMP`/`TIMESTAMP_NTZ`), and the `timestamptz` zone-flattening trade-off that precision
change does not alter. Split out of `datafusion-scan/type-mapping` once that feature's scenario
count crossed this library's per-spec organization threshold; this feature owns every scenario
issue #359 added, and the parent feature keeps the general Arrow/Exasol type-compatibility surface.

## Background

* Both timezone-naive (`Timestamp(_, None)`) and timezone-aware (`Timestamp(_, Some(_))`)
  Arrow timestamps map to plain Exasol `TIMESTAMP`. An Iceberg `timestamptz` /
  `timestamptz_ns` column is registered internally as the timezone-aware Arrow
  `Timestamp(_, Some("UTC"))` — so DataFusion's timestamp comparisons, date-function
  evaluation, and predicate binding stay timezone-correct — but is declared to Exasol and
  emitted as plain `TIMESTAMP` carrying the UTC-instant value. The Iceberg table spec
  defines a `timestamptz` value as an instant whose values "are stored as UTC and do not
  retain a source time zone" (`2017-11-16 17:10:34 PST` is stored/retrieved as
  `2017-11-17 01:10:34 UTC` and these values are considered identical), so no per-value
  timezone information exists to lose. Mapping to plain `TIMESTAMP` is a deliberate, named
  Exasol target-type trade-off: because Exasol rejects `TIMESTAMP WITH LOCAL TIME ZONE` as
  a UDF `EMITS` output type, the declared Exasol column type cannot distinguish
  `timestamptz` from `timestamp` at the Exasol SQL surface. This is analogous to the
  struct/list/map JSON-`VARCHAR` trade-off — a target-type limitation, not a change to any
  emitted value.
* Zone-awareness and precision are independent decisions. The `timestamptz`-to-plain-`TIMESTAMP`
  trade-off (Exasol rejects `TIMESTAMP WITH LOCAL TIME ZONE` as a UDF EMITS type, `sqlCode
  22002`) is unaffected by the precision gate.
* Exasol version strings: `8.29.13` (8.x line) and `2025.2.1` (calendar-versioned line). A
  leading-component parse separates the two lines.
* The default on an unreadable or empty version is the UNCLAMPED arm, a deliberate reversal of the
  conservative choice. A loud failure on an unknown engine is preferred over silent data loss on
  every known one.
* `arrow_to_exasol_type` is not threaded through the version gate because no production path
  declares an Exasol type from an Arrow type. `needs_json_fallback`'s answer for every
  `Timestamp(_, _)` is `false` at any precision.
* `ExaType::Timestamp { precision }` carries the declared fractional-second precision since
  `exasol-udf-sdk` 0.28.1 (issue #405). The precision is now readable at the emit boundary, so the
  Arrow coercion target follows it instead of being fixed at microsecond.
* A timestamp column's precision has TWO independent axes, owned by TWO types in
  `types/mapping.rs`. The SOURCE axis is the width the catalog declares for one COLUMN.
  `TimestampPrecision` owns it, its Exasol declaration string, its Arrow `TimeUnit`, and the
  reverse reading of a declared Exasol precision. The ENGINE axis is the width the running engine
  can emit, a property of the REQUEST. `EngineTimestampSupport` owns the version rule and the
  clamp, and takes a version `&str` rather than a `UdfContext`, so this module performs no I/O and
  the single `ctx.database_version()` read stays in `vs-adapter/create-virtual-schema`. Both
  producers name a source width and clamp it through the engine value. Neither carries a
  declaration literal.
* The SOURCE axis takes THREE values: millisecond, microsecond, nanosecond. These are the three
  sub-second Parquet timestamp encodings (MILLIS, MICROS, NANOS) and the three sub-second Arrow
  `TimeUnit`s, so the set is format-neutral and a future non-Iceberg, non-Delta reader populates it
  the same way. A format reader names a value, never an Exasol type.
* Apache Iceberg table spec, `§ Primitive Types`: `timestamp` and `timestamptz` are "Timestamp,
  microsecond precision"; `timestamp_ns` and `timestamptz_ns` are "Timestamp, nanosecond
  precision". `§ Appendix E: Format version changes` confirms "Types `variant`, `geometry`,
  `geography`, `unknown`, `timestamp_ns`, and `timestamptz_ns` are added in v3".
* The Delta Lake protocol defines NO nanosecond timestamp type, so the Delta/Unity producer stays
  at microsecond by protocol rather than by omission. `§ Schema Serialization Format → Primitive
  Types` types `timestamp` as "Microsecond precision timestamp elapsed since the Unix epoch" and
  `timestamp without time zone` as "Microsecond precision timestamp in a local timezone", and the
  string `nanosecond` does not occur in the protocol. Unity Catalog's `ColumnTypeName` enumeration,
  the domain of the `type_name` field this repo matches on, carries only `TIMESTAMP` and
  `TIMESTAMP_NTZ`. This is a protocol fact, not a tracked gap and not a deviation.
* The ENGINE axis is an EMIT capability, distinct from the CAST-target domain. An Exasol 8.x UDF
  emits only millisecond precision; Exasol 2025.x and later emit 3, 6 and 9. The 8.x limit is a
  named target-type trade-off: a nanosecond Iceberg column queried through an 8.x engine loses six
  digits, exactly as a microsecond one loses three.
* Four live measurements bound the declaration side, each naming the ONE engine build it was
  captured on (`specs/_recorded/2026-08-19-add-timestamp-precision-versioning`). `[C1]` on
  2025.2.1: a column declared `{"type":"timestamp","fractionalSecondsPrecision":9}` is accepted and
  honored, reported by `SYS.EXA_ALL_COLUMNS` as `TIMESTAMP(9)`. `[C1]` on 8.29.13: the same DDL is
  accepted and both 6 and 9 are silently downgraded to `TIMESTAMP(3)`. `[C2]` on 2025.2.1: the
  pushdown request echoes `fractionalSecondsPrecision`; on 8.29.13 it omits the key, so
  `exasol_type_from_json` declares bare `TIMESTAMP` there. `[C3]` on 8.29.13: that build rejects
  `TIMESTAMP(p)` for every `p` outside `{3, 6}` as `0A000 Feature not supported`.
* `[C1]` and `[C2]` hold on 2025.1.16 as well, each confirmed by its own check because they answer
  different questions. The echo is confirmed by requiring the expected `TIMESTAMP(p)` literal in
  the generated `EMITS` clause, BEFORE any emitted value is asserted. The engine honoring that
  declaration is confirmed by the VALUE assertion, which requires the distinct count the declared
  width admits. A failed VALUE assertion under a query that raised no error is an accept-and-clamp,
  an engine limit of the measured build rather than an SLC rejection.
* The nanosecond digits reach DataFusion intact. iceberg 0.10.0 maps `TimestampNs`/`TimestamptzNs`
  to Arrow `Timestamp(Nanosecond, _)` and its INT96 coercion visitor declines to coerce a
  nanosecond target, and the scan reads through DataFusion's own `ParquetSource` with a field-id
  adapter rather than through iceberg-rust's reader at all.
* `raw_scan.rs` sets DataFusion's `coerce_int96 = "us"`, which forces an INT96 PHYSICAL column to
  microseconds whatever its logical type. Iceberg writes `timestamp_ns` as an INT64
  `TIMESTAMP(NANOS)` column, so a spec-conformant nanosecond column is unaffected. A legacy file
  storing a nanosecond-typed column as INT96 still arrives at microsecond, a named limit of the
  INT96 setting rather than a silent gap.
* Which component truncates depends on the declaration path. For a CATALOG timestamp column the
  adapter declares the clamped width and the scan emits at exactly that width, so no component
  truncates afterwards. For a projected `CAST(x AS TIMESTAMP(p))` the adapter echoes Exasol's own
  `selectListDataTypes` type verbatim, and DataFusion computes the value when `p` is in
  `{0, 3, 6, 9}` and Exasol computes it in the adapter's own wrapper otherwise
  (`sql-comprehension/vs-expression-translator-cast`).

## Scenarios

### Scenario: A catalog timestamp column is declared TIMESTAMP(6) on Exasol 2025.x and later

* *GIVEN* a column whose catalog-declared type is a MICROSECOND-precision timestamp: an Iceberg `timestamp` or `timestamptz`, or a Delta/Unity Catalog `TIMESTAMP` or `TIMESTAMP_NTZ`
* *AND* a database version string read from the running Exasol engine's UDF handshake metadata
* *WHEN* the adapter resolves that column's Exasol type for the `createVirtualSchema` declaration
* *THEN* the resolver SHALL return `TIMESTAMP(6)` when the version's leading dot-separated component parses as an integer `>= 2025`, and SHALL return the bare string `TIMESTAMP` when it parses as an integer `< 2025`, so `2025.2.1` yields `TIMESTAMP(6)` and `8.29.13` yields `TIMESTAMP`
* *AND* the resolution SHALL be the composition of exactly TWO decisions, each owned by exactly ONE type in `crates/lakehouse-engine/src/types/mapping.rs`: a SOURCE width the producer names PER COLUMN, and an ENGINE clamp resolved ONCE per `createVirtualSchema` request from the version string; the request-scoped value MUST NOT be substituted for the per-column one, and neither producer SHALL carry a declaration literal
* *AND* the SOURCE width SHALL be a three-valued, FORMAT-NEUTRAL property (millisecond, microsecond, nanosecond), and MUST NOT name Iceberg, Delta, or any other table format in its own vocabulary, so a future format reader populates it without widening the type
* *AND* BOTH producers, `iceberg_primitive_to_exasol` and `unity_type_name_to_exasol`, SHALL name their source width and read their declaration from that shared owner, so an Iceberg `timestamp` and a Delta `TIMESTAMP` cannot be declared at different precisions
* *AND* the engine owner SHALL take the version as a STRING and MUST NOT take a `UdfContext`, so the type-mapping module reads no ambient state and performs no I/O; the single `ctx.database_version()` read and the threading of the resolved value belong to `vs-adapter/create-virtual-schema`
* *AND* the resolved engine value SHALL be threaded as a plain value through `column_source_type_to_exasol` and `iceberg_type_to_exasol` to both producers, and the context MUST NOT be threaded into the type-mapping module in its place
* *AND* the resolver MUST NOT fail, return a `Result`, or abort the enumeration on any version string, keeping `column_source_type_to_exasol` and `build_listing_virtual_tables` infallible
* *AND* a parameterized precision MUST NOT be declared for any Iceberg or Delta type OTHER than the four Iceberg timestamp variants and the two Unity timestamp names: `date` stays `DATE` and Iceberg `time` stays `VARCHAR(2000000)`, both byte-identical
* *AND* every other declared type SHALL stay byte-identical on BOTH version arms, so this delta changes exactly the timestamp rows of the declaration surface
* *AND* on the `>= 2025` arm the emitted VALUE SHALL retain every microsecond digit the Iceberg spec stores, and on the `< 2025` arm it SHALL be truncated to milliseconds, a named Exasol 8.x target-type limitation, not a defect and not a tracked exception

### Scenario: A nanosecond catalog timestamp column is declared TIMESTAMP(9) on Exasol 2025.x and later

* *GIVEN* an Iceberg `timestamp_ns` or `timestamptz_ns` column, typed "Timestamp, nanosecond precision" in the Apache Iceberg table spec's `§ Primitive Types`, added in format version 3 per `§ Appendix E: Format version changes`
* *WHEN* the adapter resolves that column's Exasol type for the `createVirtualSchema` declaration
* *THEN* the resolver SHALL return `TIMESTAMP(9)` on an engine whose version's leading dot-separated component parses as an integer `>= 2025`, and MUST NOT collapse the four Iceberg timestamp variants onto one declaration, because `iceberg_primitive_to_arrow` registers the two `_ns` variants as Arrow `Timestamp(Nanosecond, _)`
* *AND* the `TIMESTAMP(9)` declaration SHALL be accepted and honored by the engine, which `[C1]` measured live on 2025.2.1: a column declared `{"type":"timestamp","fractionalSecondsPrecision":9}` is reported by `SYS.EXA_ALL_COLUMNS` as `TIMESTAMP(9)`
* *AND* the resolver SHALL return the bare string `TIMESTAMP` on an engine whose leading component parses as an integer `< 2025`, clamping the nanosecond source to what that engine can emit, so a nanosecond column loses six fractional digits there, the same named Exasol 8.x target-type limitation the microsecond source already carries, widened by three digits
* *AND* the clamp MUST NOT be left to the engine's own silent downgrade, which `[C1]` measured on 8.29.13 for both precision 6 and precision 9, because that would make `SYS.EXA_ALL_COLUMNS` claim a precision the adapter never obtains
* *AND* the Delta/Unity producer SHALL NOT gain a nanosecond arm, because the Delta Lake protocol defines no nanosecond timestamp type and Unity Catalog's `ColumnTypeName` domain carries only `TIMESTAMP` and `TIMESTAMP_NTZ`, so the asymmetry between the two producers is a protocol fact rather than an oversight
* *AND* the nanosecond DIGITS SHALL be present in the data rather than zeroed upstream, established end to end rather than by reasoning about the reader: a `timestamp_ns` column seeded through the `format-version` table PROPERTY, carrying two instants that differ ONLY below the microsecond, was measured on `exasol/docker-db:2025.1.16` to be declared `TIMESTAMP(9)` with BOTH instants distinct, and on 8.29.13 to be declared bare `TIMESTAMP`, reported `TIMESTAMP(3)`, with the two collapsing to ONE
* *AND* the INT96 exception SHALL be named: `raw_scan.rs`'s `coerce_int96 = "us"` forces an INT96 PHYSICAL column to microseconds whatever its logical type, so a nanosecond-typed column stored as INT96 arrives at microsecond; a spec-conformant Iceberg `timestamp_ns` is written as INT64 `TIMESTAMP(NANOS)` and is unaffected

### Scenario: The Arrow-input type resolver stays outside the version gate

* *GIVEN* `arrow_to_exasol_type` and its private `compatible_exasol_type` — the ARROW-INPUT direction, whose `DataType::Timestamp(_, _)` arm returns the bare string `TIMESTAMP`
* *WHEN* the version gate is threaded through the catalog-declared producers
* *THEN* neither function SHALL gain a precision parameter, and both SHALL keep their recorded signature and their recorded answer for every input byte-identical, including `TIMESTAMP` for every `Timestamp(_, _)`
* *AND* the exclusion SHALL hold because no production path declares an Exasol type from an Arrow type: `datafusion-scan/type-mapping-module-structure` records that `arrow_to_exasol_type` has no call site anywhere in the crate, and the only production consumer of `compatible_exasol_type` is `needs_json_fallback`, whose answer for a `Timestamp(_, _)` is `false` at every precision
* *AND* `needs_json_fallback` SHALL keep its recorded `fn(&DataType) -> bool` signature and its answer for every input unchanged, so none of its call sites move
* *AND* the compatible-Arrow-types table's `Timestamp(_, _) | TIMESTAMP` row SHALL stay unamended (`datafusion-scan/type-mapping`), and a reader MUST NOT read it as governing the catalog-declared declaration this delta version-gates
* *AND* the exclusion SHALL be recorded rather than left silent, because issue #359's scope text names `arrow_to_exasol_type` as a gate target and an unexplained omission is indistinguishable from an oversight

### Scenario: An empty or unparseable database version declares the microsecond precision

* *GIVEN* a database version string that is EMPTY (the `UdfContext::database_version` default for a context carrying no handshake metadata) or whose leading dot-separated component does not parse as an integer (`v2025.2.1`, `unknown`, `.2.1`)
* *WHEN* the adapter resolves a catalog timestamp column's Exasol type for the `createVirtualSchema` declaration
* *THEN* the resolver SHALL resolve the ENGINE axis to the unclamped arm, the SAME answer it returns for a recognised `>= 2025` version, so a microsecond source is declared `TIMESTAMP(6)` and a nanosecond source `TIMESTAMP(9)`
* *AND* the EMPTY string and every UNPARSEABLE string SHALL take that one default arm and MUST NOT be distinguished from each other, because neither carries information the other lacks and a second arm would invite the two to drift
* *AND* the resolver MUST NOT error, panic, log a warning, or fall back to the bare `TIMESTAMP` declaration on either input, so an unrecognised engine gets the fidelity-preserving declaration and, if it rejects it, fails loudly at `createVirtualSchema` rather than silently truncating every timestamp value
* *AND* this default SHALL be recorded as a deliberate reversal of the conservative alternative, so a later reader does not "fix" it back to the bare declaration

### Scenario: Iceberg timestamptz maps to plain Exasol TIMESTAMP

* *GIVEN* an Iceberg `timestamptz` or `timestamptz_ns` column, whose values the Iceberg spec stores as UTC with no retained source time zone
* *WHEN* the adapter resolves the column's Exasol type for the `createVirtualSchema` declaration and the scan `EMITS` clause, and the scan coerces the column at the emit boundary
* *THEN* the resolver SHALL return an Exasol `TIMESTAMP` (bare, `TIMESTAMP(6)`, or `TIMESTAMP(9)` per the source width and the engine clamp this feature's declaration scenarios own), and MUST NOT return `TIMESTAMP WITH LOCAL TIME ZONE` at any precision, because Exasol rejects `TIMESTAMP WITH LOCAL TIME ZONE` as a UDF `EMITS` output type (`sqlCode 22002: Column type not supported`)
* *AND* the scan UDF SHALL register the column as the timezone-aware Arrow `Timestamp(_, Some("UTC"))`, so DataFusion timestamp comparisons, date-function evaluation, and predicate binding stay timezone-correct
* *AND* the emit-boundary coercion SHALL cast that column to `Timestamp(_, None)` at the unit the DECLARED precision names, preserving the underlying UTC-instant value, so the emitted `TIMESTAMP` is the UTC wall-clock instant and no value is shifted
* *AND* a `timestamptz_ns` column SHALL keep its nanosecond digits through that cast on an engine declaring `TIMESTAMP(9)`, because the zone flattening changes the timezone field only and MUST NOT change the unit
* *AND* the declared Exasol column type MUST NOT distinguish `timestamptz` from `timestamp` at the Exasol SQL surface, a deliberate, named target-type trade-off, not a change to any emitted value
* *AND* the zone-awareness trade-off above and the fractional-second PRECISION are two independent decisions: neither the version gate nor the source-width axis changes this zone-awareness trade-off, and neither MUST be read as narrowing or widening it

### Scenario: A declared TIMESTAMP(p) EMITS column maps back to the Arrow unit of that precision

* *GIVEN* an EMITS type of the form `TIMESTAMP(p)` for an integer precision `p` in 0-9, what the adapter declares on the unclamped engine arm and for a projected TIMESTAMP CAST expression whose `fractionalSecondsPrecision` `exasol_type_from_json` reads (`vs-adapter/pushdown-planning`), or a bare `TIMESTAMP`, what it declares on the clamped arm
* *WHEN* the scan resolves that column's Arrow coercion target at the emit boundary (`target_arrow_type`) from the `ExaType` that `UdfContext::output_column` reports for the column
* *THEN* the reported variant SHALL be `ExaType::Timestamp { precision }` for every such declaration
* *AND* `target_arrow_type` SHALL return `DataType::Timestamp(unit, None)` where `unit` is `Millisecond` for a reported `precision` of 3, `Microsecond` for 6, and `Nanosecond` for 9, read from the SAME owner that produces the declaration string so the two sides cannot disagree about what `TIMESTAMP(9)` means, and MUST NOT return a fixed unit at every precision, because a microsecond target under a `TIMESTAMP(9)` declaration destroys every nanosecond digit through the strict (`safe: false`) cast in `coerce_column`
* *AND* a reported `precision` outside `{3, 6, 9}` SHALL resolve to the COARSEST of those three units that is NOT COARSER than the declaration (`0`, `1` and `2` to `Millisecond`, `4` and `5` to `Microsecond`, `7` and above to `Nanosecond`), so a mapping error can only ever emit a value Exasol truncates, never one the scan has already destroyed
* *AND* the `Millisecond` floor for `p` below 3 SHALL be a deliberate trade-off: Arrow's `Second` unit would match a `TIMESTAMP(0)` declaration exactly, but no emit path in this repo has ever fed the SLC a second-unit Arrow column, and the only declaration reaching `p = 0` is the exotic projected `CAST(x AS TIMESTAMP(0))`, whose one Exasol-side truncation is the cheaper risk
* *AND* on the CATALOG-column path the declared precision SHALL equal the emitted resolution on BOTH engine arms, so the round trip is exact by construction and no component truncates after the scan
* *AND* the running engine SHALL accept a `Timestamp(Millisecond, None)`, a `Timestamp(Microsecond, None)` and a `Timestamp(Nanosecond, None)` Arrow column into the matching `TIMESTAMP(3)`, `TIMESTAMP(6)` and `TIMESTAMP(9)` declaration, each measured against the local Exasol Docker container rather than carried over from the SDK's upstream fixture, which exercises the `Value` emit path rather than this Arrow one; all three were measured on `exasol/docker-db:2025.1.16` and none was rejected, and the `Millisecond` unit was measured again on 8.29.13
* *AND* on Exasol 8.29.13, where the clamp declares every catalog timestamp column bare `TIMESTAMP`, the scan SHALL emit the `Millisecond` unit and the round trip SHALL be exact at three digits, so the 8.x limitation is a DECLARED narrowing rather than a reliance on the engine truncating a finer value
* *AND* the scan MUST NOT inspect the declaration's spelling to reach that arm: the SLC hands the UDF `ColumnInfo { typ: Timestamp { precision: 3 }, type_name: "TIMESTAMP(3)", … }` for a bare `TIMESTAMP`, so the engine normalises it to precision 3 before the scan sees it
* *AND* `target_arrow_type` MUST NOT route `ExaType::Timestamp` through the `Utf8` string path, which would stringify the value and violate the `TIMESTAMP(p)` EMITS declaration
* *AND* `target_arrow_type` SHALL carry no `ExaType::TimestampTz` arm, which changes no emitted value: this feature's "Iceberg timestamptz maps to plain Exasol TIMESTAMP" scenario already requires the emit boundary to cast a zoned column to `Timestamp(_, None)`, and Exasol rejects `TIMESTAMP WITH LOCAL TIME ZONE` as a UDF `EMITS` output type (`sqlCode 22002`), so no declaration ever reached that target
* *AND* `exasol_type_to_arrow` SHALL map a `TIMESTAMP(p)` type STRING to the same unit `target_arrow_type` resolves for the same `p`, because it is the single source of truth for the Arrow type the strict `emit_batch` feed accepts and a second, disagreeing answer in the same module is the drift its doc comment forbids
* *AND* `exasol_type_to_arrow` SHALL keep its `TIMESTAMP WITH LOCAL TIME ZONE` arm unchanged
