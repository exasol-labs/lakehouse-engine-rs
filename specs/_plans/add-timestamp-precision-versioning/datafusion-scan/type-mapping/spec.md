# Feature: DataFusion-to-Exasol Type Mapping

Defines the single authoritative mapping from DataFusion/Arrow column types to Exasol SQL types, and the companion Iceberg-to-Arrow mapping used to build the logical schema the scan registers, so that every column an Iceberg table exposes is queryable through Exasol.

## Background

* **This delta is issue #359.** It adds THREE scenarios and AMENDS ONE. The added scenarios record the
  version-gated Exasol TIMESTAMP precision, the default taken when the version string cannot be read,
  and the deliberate exclusion of the ARROW-INPUT resolver from that gate. The amended scenario is
  "Iceberg timestamptz maps to plain Exasol TIMESTAMP", which gains the precision qualifier and keeps
  every other clause byte-identical.
* **The bug this delta fixes is silent, unconditional truncation on every Exasol version.**
  `iceberg_primitive_to_exasol` maps `Timestamp`, `TimestampNs`, `Timestamptz`, and `TimestamptzNs` to
  the bare string `TIMESTAMP`, and `unity_type_name_to_exasol` maps `TIMESTAMP` and `TIMESTAMP_NTZ` the
  same way. Exasol's bare `TIMESTAMP` IS `TIMESTAMP(3)` — millisecond — so Exasol truncates the three
  sub-millisecond digits of every Iceberg and Delta timestamp value on receipt. Nothing failed and no
  test noticed, because no recorded scenario asserted a timestamp value below seconds resolution.
* **Apache Iceberg spec check — microsecond is the spec's own precision for these types, so
  `TIMESTAMP(3)` is a DEVIATION this plan FIXES, not a recorded trade-off.** The spec's Schemas and
  Data Types § Primitive Types table states `timestamp` as *"Timestamp, microsecond precision, without
  timezone"* and `timestamptz` as *"Timestamp, microsecond precision, with timezone"*. Appendix A
  (Parquet) pins the physical form: `timestamp` is *"`TIMESTAMP_MICROS` with `adjustToUtc=false`"* and
  *"Stores microseconds from 1970-01-01 00:00:00.000000."*; `timestamptz` is *"`TIMESTAMP_MICROS` with
  `adjustToUtc=true`"*. `TIMESTAMP(6)` is therefore the spec-correct target precision, and this plan
  closes the gap rather than filing a tracked exception for it.
* **`TIMESTAMP(9)` is explicitly NOT the target, and the reason is upstream, not Exasol.** The spec's
  v3 rows state `timestamp_ns` and `timestamptz_ns` as *"Timestamp, nanosecond precision"*, and Exasol
  accepts `TIMESTAMP(p)` for every `p` in 0-9. The ceiling is iceberg-rust: its `TimestampNs` handling
  calls `timestamp_to_micros`, truncating nanoseconds to microseconds before any value reaches
  DataFusion. So a nanosecond Iceberg column carries no sub-microsecond digit for a `TIMESTAMP(9)`
  declaration to preserve, and declaring 9 would advertise a precision the read path cannot deliver.
  Once iceberg-rust preserves nanoseconds, raising the declaration is a one-line change here with no
  emit-side work, because the UDF output path already carries nine fractional digits.
* **This delta does NOT touch the `timestamptz`-flattening trade-off, which is a separate, already-
  recorded Exasol target-type limitation.** The recorded scenario "Iceberg timestamptz maps to plain
  Exasol TIMESTAMP" stands: `timestamptz` still declares an Exasol type that cannot be distinguished
  from `timestamp` at the SQL surface, because Exasol rejects `TIMESTAMP WITH LOCAL TIME ZONE` as a UDF
  `EMITS` output type (`sqlCode 22002`). What changes is only the FRACTIONAL-SECOND precision of the
  declaration. Zone-awareness and precision are two independent decisions and MUST NOT be conflated.
* **The version gate needs ONE owner, because two producers already state the same declaration
  independently.** `iceberg_primitive_to_exasol` and `unity_type_name_to_exasol` each hardcode the
  literal `"TIMESTAMP"`, exactly the shape that let the catalog-decimal guard drift into four copies
  before issue #329 consolidated it. This delta introduces one `TimestampPrecision` decision in
  `crates/lakehouse-engine/src/types/mapping.rs` that owns both the version rule and the two
  declaration strings, and both producers read it.
* **The gate belongs in the type-mapping module, and the version STRING is its input rather than the
  UDF context.** `types/mapping.rs` already owns Exasol's own type domain, including the live-captured
  `DECIMAL` bounds in `exasol_representable_catalog_decimal`. Taking a `&str` keeps the module free of
  `UdfContext` — it reads no ambient state and performs no I/O — so the dependency still points from
  the adapter inward, exactly as `cluster_nodes_from_context` keeps `node_count` at the adapter edge.
  `vs-adapter/create-virtual-schema` owns the single `ctx.database_version()` read and the threading.
* **Exasol's version string shape is the same one the E2E Docker image tags carry**: `8.29.13` for the
  8.x line and `2025.2.1` for the calendar-versioned line. A leading-component parse therefore
  separates the two lines with no version-comparison machinery, and Exasol's move to calendar
  versioning after 8.x is what makes the single `>= 2025` threshold unambiguous.
* **The default on an unreadable version is the MODERN declaration, and that is a deliberate
  reversal of the conservative choice.** `UdfContext::database_version` returns `String::new()` on a
  context that does not populate handshake metadata, and no call site for it exists anywhere in this
  repo today. Defaulting to `TIMESTAMP(6)` means the fidelity-preserving declaration is what a new or
  unrecognised engine gets; the cost is that a hypothetical engine that rejects `TIMESTAMP(6)` fails
  loudly at `createVirtualSchema` rather than silently truncating. A loud failure on an unknown engine
  is preferred over silent data loss on every known one.
* **The pushdown and CAST halves of the precision surface already work and are NOT re-specified here.**
  `exasol_type_from_json` reads `fractionalSecondsPrecision` and renders `TIMESTAMP(p)`
  (`vs-adapter/pushdown-planning`), `render_cast_target` does the same for the CAST dialects
  (`sql-comprehension/vs-expression-translator-cast`), and this feature's recorded scenario "A
  TIMESTAMP(p) EMITS string maps back to the microsecond Arrow timestamp" already pins
  `exasol_type_to_arrow` to `Timestamp(Microsecond, None)` for every `p` in 0-9. So once the
  declaration carries the precision, Exasol echoes it into the pushdown request, the EMITS clause
  carries `TIMESTAMP(6)`, and the emit-boundary coercion is unchanged. No new code is needed on any of
  those three paths.
* **`arrow_to_exasol_type` is NOT threaded, and its exclusion is recorded rather than left silent**
  because issue #359's own scope text names it. `datafusion-scan/type-mapping-module-structure` already
  records that it *"has NO call site anywhere in the crate"*; the only production consumer of
  `compatible_exasol_type` is the `needs_json_fallback` boolean, whose answer for every
  `Timestamp(_, _)` is `false` at any precision. Threading a precision through a resolver no
  production declaration reaches would add a parameter that cannot change an observable answer.

## Scenarios

<!-- DELTA:NEW -->
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
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: An empty or unparseable database version declares the microsecond precision

* *GIVEN* a database version string that is EMPTY (the `UdfContext::database_version` default for a context carrying no handshake metadata) or whose leading dot-separated component does not parse as an integer (`v2025.2.1`, `unknown`, `.2.1`)
* *WHEN* the adapter resolves a catalog timestamp column's Exasol type for the `createVirtualSchema` declaration
* *THEN* the resolver SHALL return `TIMESTAMP(6)` — the SAME answer it returns for a recognised `>= 2025` version
* *AND* the EMPTY string and every UNPARSEABLE string SHALL take that one default arm and MUST NOT be distinguished from each other, because neither carries information the other lacks and a second arm would invite the two to drift
* *AND* the resolver MUST NOT error, panic, log a warning, or fall back to the bare `TIMESTAMP` declaration on either input, so an unrecognised engine gets the fidelity-preserving declaration and, if it rejects it, fails loudly at `createVirtualSchema` rather than silently truncating every timestamp value
* *AND* this default SHALL be recorded as a deliberate reversal of the conservative alternative, so a later reader does not "fix" it back to the bare declaration
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: The Arrow-input type resolver stays outside the version gate

* *GIVEN* `arrow_to_exasol_type` and its private `compatible_exasol_type` — the ARROW-INPUT direction, whose `DataType::Timestamp(_, _)` arm returns the bare string `TIMESTAMP`
* *WHEN* the version gate is threaded through the catalog-declared producers
* *THEN* neither function SHALL gain a precision parameter, and both SHALL keep their recorded signature and their recorded answer for every input byte-identical, including `TIMESTAMP` for every `Timestamp(_, _)`
* *AND* the exclusion SHALL hold because no production path declares an Exasol type from an Arrow type: `datafusion-scan/type-mapping-module-structure` records that `arrow_to_exasol_type` has no call site anywhere in the crate, and the only production consumer of `compatible_exasol_type` is `needs_json_fallback`, whose answer for a `Timestamp(_, _)` is `false` at every precision
* *AND* `needs_json_fallback` SHALL keep its recorded `fn(&DataType) -> bool` signature and its answer for every input unchanged, so none of its call sites move
* *AND* the compatible-Arrow-types table's `Timestamp(_, _) | TIMESTAMP` row SHALL stay unamended, and a reader MUST NOT read it as governing the catalog-declared declaration this delta version-gates
* *AND* the exclusion SHALL be recorded rather than left silent, because issue #359's scope text names `arrow_to_exasol_type` as a gate target and an unexplained omission is indistinguishable from an oversight
<!-- /DELTA:NEW -->

<!-- DELTA:CHANGED -->
### Scenario: Iceberg timestamptz maps to plain Exasol TIMESTAMP

* *GIVEN* an Iceberg `timestamptz` or `timestamptz_ns` column, whose values the Iceberg spec stores as UTC with no retained source time zone
* *WHEN* the adapter resolves the column's Exasol type for the `createVirtualSchema` declaration and the scan `EMITS` clause, and the scan coerces the column at the emit boundary
* *THEN* the resolver SHALL return an Exasol `TIMESTAMP` — bare or `TIMESTAMP(6)` per the version gate this feature's "A catalog timestamp column is declared TIMESTAMP(6) on Exasol 2025.x and later" scenario owns — and MUST NOT return `TIMESTAMP WITH LOCAL TIME ZONE` at any precision, because Exasol rejects `TIMESTAMP WITH LOCAL TIME ZONE` as a UDF `EMITS` output type (`sqlCode 22002: Column type not supported`)
* *AND* the scan UDF SHALL register the column as the timezone-aware Arrow `Timestamp(_, Some("UTC"))`, so DataFusion timestamp comparisons, date-function evaluation, and predicate binding stay timezone-correct
* *AND* the emit-boundary coercion SHALL cast that column to `Timestamp(_, None)` preserving the underlying UTC-instant value, so the emitted `TIMESTAMP` is the UTC wall-clock instant and no value is shifted
* *AND* the declared Exasol column type MUST NOT distinguish `timestamptz` from `timestamp` at the Exasol SQL surface — a deliberate, named target-type trade-off, not a change to any emitted value
* *AND* the zone-awareness trade-off above and the fractional-second PRECISION are two independent decisions: the version gate changes only the precision, and MUST NOT be read as narrowing or widening this zone-awareness trade-off
<!-- /DELTA:CHANGED -->
