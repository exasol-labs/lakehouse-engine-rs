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
* Exasol's bare `TIMESTAMP` IS `TIMESTAMP(3)` — millisecond precision. The Iceberg spec
  defines `timestamp`/`timestamptz` as microsecond precision, so `TIMESTAMP(6)` is the
  spec-correct target. The version gate declares `TIMESTAMP(6)` on Exasol 2025.x+ and bare
  `TIMESTAMP` on 8.x, so 8.x silently truncates sub-millisecond digits — a named version
  limitation, not a defect.
* `TIMESTAMP(9)` is not the target because iceberg-rust truncates nanoseconds to
  microseconds (`timestamp_to_micros`) before any value reaches DataFusion. Once iceberg-rust
  preserves nanoseconds, raising the declaration is a one-line change with no emit-side work.
* Zone-awareness and precision are independent decisions. The `timestamptz`-to-plain-`TIMESTAMP`
  trade-off (Exasol rejects `TIMESTAMP WITH LOCAL TIME ZONE` as a UDF EMITS type, `sqlCode
  22002`) is unaffected by the precision gate.
* One `TimestampPrecision` owner in `types/mapping.rs` holds the version rule and both
  declaration strings. Both producers (`iceberg_primitive_to_exasol`,
  `unity_type_name_to_exasol`) read it. The owner takes a version `&str`, not a `UdfContext`,
  keeping the type-mapping module free of I/O; `vs-adapter/create-virtual-schema` owns the
  single `ctx.database_version()` read.
* Exasol version strings: `8.29.13` (8.x line) and `2025.2.1` (calendar-versioned line). A
  leading-component parse separates the two lines.
* The default on an unreadable or empty version is `TIMESTAMP(6)` — a deliberate reversal of
  the conservative choice. A loud failure on an unknown engine is preferred over silent data
  loss on every known one.
* `arrow_to_exasol_type` is not threaded through the version gate because no production path
  declares an Exasol type from an Arrow type. `needs_json_fallback`'s answer for every
  `Timestamp(_, _)` is `false` at any precision.
* `ExaType` carries no timestamp precision: every `TIMESTAMP(p)` in the `EMITS` clause
  reaches the scan as the single variant `ExaType::Timestamp`. The rule that `p` governs
  only Exasol's own type check, never the Arrow unit, therefore holds structurally.

## Scenarios

### Scenario: A catalog timestamp column is declared TIMESTAMP(6) on Exasol 2025.x and later

* *GIVEN* a column whose catalog-declared type is a timestamp — an Iceberg `timestamp`, `timestamptz`, `timestamp_ns`, or `timestamptz_ns`, or a Delta/Unity Catalog `TIMESTAMP` or `TIMESTAMP_NTZ`
* *AND* a database version string read from the running Exasol engine's UDF handshake metadata
* *WHEN* the adapter resolves that column's Exasol type for the `createVirtualSchema` declaration
* *THEN* the resolver SHALL return `TIMESTAMP(6)` when the version's leading dot-separated component parses as an integer `>= 2025`, and SHALL return the bare string `TIMESTAMP` when it parses as an integer `< 2025`, so `2025.2.1` yields `TIMESTAMP(6)` and `8.29.13` yields `TIMESTAMP`
* *AND* exactly ONE type in `crates/lakehouse-engine/src/types/mapping.rs` SHALL own that version RULE and the two returned declaration STRINGS, and BOTH producers — `iceberg_primitive_to_exasol` and `unity_type_name_to_exasol` — SHALL read their answer from it rather than each carrying its own copy of either literal, so an Iceberg `timestamp` and a Delta `timestamp` cannot be declared at different precisions
* *AND* that owner SHALL take the version as a STRING and MUST NOT take a `UdfContext`, so the type-mapping module reads no ambient state and performs no I/O; the single `ctx.database_version()` read and the threading of the resolved value belong to `vs-adapter/create-virtual-schema`
* *AND* the resolved precision SHALL be threaded as a plain value through `column_source_type_to_exasol` and `iceberg_type_to_exasol` to both producers, and the context MUST NOT be threaded into the type-mapping module in its place
* *AND* the resolver MUST NOT fail, return a `Result`, or abort the enumeration on any version string, keeping `column_source_type_to_exasol` and `build_listing_virtual_tables` infallible
* *AND* `TIMESTAMP(6)` MUST NOT be declared for any Iceberg or Delta type OTHER than the four Iceberg timestamp variants and the two Unity timestamp names — `date` stays `DATE` and Iceberg `time` stays `VARCHAR(2000000)`, both byte-identical
* *AND* every other declared type SHALL stay byte-identical on BOTH version arms, so this delta changes exactly one row of the declaration surface
* *AND* the emitted VALUE MUST NOT be altered on either arm: on `TIMESTAMP(6)` Exasol retains the microsecond digits the Iceberg spec stores, and on bare `TIMESTAMP` Exasol truncates them to milliseconds — a named 8.x version limitation, not a defect and not a tracked exception

### Scenario: An empty or unparseable database version declares the microsecond precision

* *GIVEN* a database version string that is EMPTY (the `UdfContext::database_version` default for a context carrying no handshake metadata) or whose leading dot-separated component does not parse as an integer (`v2025.2.1`, `unknown`, `.2.1`)
* *WHEN* the adapter resolves a catalog timestamp column's Exasol type for the `createVirtualSchema` declaration
* *THEN* the resolver SHALL return `TIMESTAMP(6)` — the SAME answer it returns for a recognised `>= 2025` version
* *AND* the EMPTY string and every UNPARSEABLE string SHALL take that one default arm and MUST NOT be distinguished from each other, because neither carries information the other lacks and a second arm would invite the two to drift
* *AND* the resolver MUST NOT error, panic, log a warning, or fall back to the bare `TIMESTAMP` declaration on either input, so an unrecognised engine gets the fidelity-preserving declaration and, if it rejects it, fails loudly at `createVirtualSchema` rather than silently truncating every timestamp value
* *AND* this default SHALL be recorded as a deliberate reversal of the conservative alternative, so a later reader does not "fix" it back to the bare declaration

### Scenario: The Arrow-input type resolver stays outside the version gate

* *GIVEN* `arrow_to_exasol_type` and its private `compatible_exasol_type` — the ARROW-INPUT direction, whose `DataType::Timestamp(_, _)` arm returns the bare string `TIMESTAMP`
* *WHEN* the version gate is threaded through the catalog-declared producers
* *THEN* neither function SHALL gain a precision parameter, and both SHALL keep their recorded signature and their recorded answer for every input byte-identical, including `TIMESTAMP` for every `Timestamp(_, _)`
* *AND* the exclusion SHALL hold because no production path declares an Exasol type from an Arrow type: `datafusion-scan/type-mapping-module-structure` records that `arrow_to_exasol_type` has no call site anywhere in the crate, and the only production consumer of `compatible_exasol_type` is `needs_json_fallback`, whose answer for a `Timestamp(_, _)` is `false` at every precision
* *AND* `needs_json_fallback` SHALL keep its recorded `fn(&DataType) -> bool` signature and its answer for every input unchanged, so none of its call sites move
* *AND* the compatible-Arrow-types table's `Timestamp(_, _) | TIMESTAMP` row SHALL stay unamended (`datafusion-scan/type-mapping`), and a reader MUST NOT read it as governing the catalog-declared declaration this delta version-gates
* *AND* the exclusion SHALL be recorded rather than left silent, because issue #359's scope text names `arrow_to_exasol_type` as a gate target and an unexplained omission is indistinguishable from an oversight

### Scenario: Iceberg timestamptz maps to plain Exasol TIMESTAMP

* *GIVEN* an Iceberg `timestamptz` or `timestamptz_ns` column, whose values the Iceberg spec stores as UTC with no retained source time zone
* *WHEN* the adapter resolves the column's Exasol type for the `createVirtualSchema` declaration and the scan `EMITS` clause, and the scan coerces the column at the emit boundary
* *THEN* the resolver SHALL return an Exasol `TIMESTAMP` — bare or `TIMESTAMP(6)` per the version gate this feature's "A catalog timestamp column is declared TIMESTAMP(6) on Exasol 2025.x and later" scenario owns — and MUST NOT return `TIMESTAMP WITH LOCAL TIME ZONE` at any precision, because Exasol rejects `TIMESTAMP WITH LOCAL TIME ZONE` as a UDF `EMITS` output type (`sqlCode 22002: Column type not supported`)
* *AND* the scan UDF SHALL register the column as the timezone-aware Arrow `Timestamp(_, Some("UTC"))`, so DataFusion timestamp comparisons, date-function evaluation, and predicate binding stay timezone-correct
* *AND* the emit-boundary coercion SHALL cast that column to `Timestamp(_, None)` preserving the underlying UTC-instant value, so the emitted `TIMESTAMP` is the UTC wall-clock instant and no value is shifted
* *AND* the declared Exasol column type MUST NOT distinguish `timestamptz` from `timestamp` at the Exasol SQL surface — a deliberate, named target-type trade-off, not a change to any emitted value
* *AND* the zone-awareness trade-off above and the fractional-second PRECISION are two independent decisions: the version gate changes only the precision, and MUST NOT be read as narrowing or widening this zone-awareness trade-off

### Scenario: A TIMESTAMP(p) EMITS string maps back to the microsecond Arrow timestamp

* *GIVEN* an EMITS type string of the form `TIMESTAMP(p)` for an integer precision `p` in 0-9 — the shape the adapter declares for a projected TIMESTAMP CAST expression once `exasol_type_from_json` (`vs-adapter/pushdown-planning`) reads `fractionalSecondsPrecision`
* *WHEN* the scan resolves that column's Arrow coercion target at the emit boundary (`target_arrow_type`) from the `ExaType` that `UdfContext::output_column` reports for the column
* *THEN* the reported variant SHALL be `ExaType::Timestamp` for every `TIMESTAMP(p)`, `p` in 0-9, and for a bare `TIMESTAMP`, because `ExaType` models no fractional-second precision
* *AND* `target_arrow_type` SHALL return `DataType::Timestamp(TimeUnit::Microsecond, None)` for `ExaType::Timestamp`, because Arrow's Microsecond unit is this project's fixed internal representation for every Exasol TIMESTAMP precision, and the declared `p` governs only Exasol's own type check
* *AND* `target_arrow_type` MUST NOT route `ExaType::Timestamp` through the `Utf8` string path, which would stringify the value and violate the `TIMESTAMP(p)` EMITS declaration
* *AND* `ExaType::TimestampTz` SHALL map to `DataType::Timestamp(TimeUnit::Microsecond, Some("UTC"))`, keeping the recorded `TIMESTAMP WITH LOCAL TIME ZONE` target unchanged
* *AND* `exasol_type_to_arrow` SHALL keep its recorded `TIMESTAMP(p)` and `TIMESTAMP WITH LOCAL TIME ZONE` arms and its recorded test coverage, because it stays the documented public inverse of `arrow_to_exasol_type` even though the emit path no longer calls it
