# Feature: DataFusion-to-Exasol Type Mapping

Defines the single authoritative mapping from DataFusion/Arrow column types to Exasol SQL types, and the companion reverse mapping (`exasol_type_to_arrow`) the scan uses to coerce each result column to its declared Arrow target before emit.

## Background

* This delta adds one scenario governing the reverse mapping `exasol_type_to_arrow` (`crates/lakehouse-engine/src/types/mapping.rs`) for a `TIMESTAMP(p)` EMITS string; every other type-mapping scenario is unchanged.
* `exasol_type_to_arrow` maps an Exasol EMITS type string back to the Arrow target the emit-boundary coercion casts each result column to (`crates/lakehouse-engine/src/scan/emit.rs::target_arrow_type`, `coerce_batch_to_exa_types`). A `None` return routes the column through the VARCHAR/`Utf8` string path.
* The function matches most Exasol types by exact string compare and parses arguments only for `DECIMAL(p,s)` (`parse_decimal_args`). It has no parse arm for a parenthesised `TIMESTAMP(p)` — only the bare literal `"TIMESTAMP"`.
* This project's Arrow↔Exasol convention collapses every TIMESTAMP precision to a single Arrow representation `Timestamp(Microsecond, None)` on the way in (mission type table); the same single representation applies on the way back out. The declared Exasol precision `p` is what Exasol's own type-checker validates against; it never changes the Arrow unit used internally.

## Scenarios

<!-- DELTA:NEW -->
### Scenario: A TIMESTAMP(p) EMITS string maps back to the microsecond Arrow timestamp

* *GIVEN* an EMITS type string of the form `TIMESTAMP(p)` for an integer precision `p` in 0-9 — the shape the adapter now declares for a projected TIMESTAMP CAST expression once `exasol_type_from_json` (`vs-adapter/pushdown-planning`) reads `fractionalSecondsPrecision`
* *WHEN* the scan resolves that column's Arrow coercion target via `exasol_type_to_arrow` at the emit boundary (`target_arrow_type`)
* *THEN* `exasol_type_to_arrow` SHALL return `Some(DataType::Timestamp(TimeUnit::Microsecond, None))` for every `TIMESTAMP(p)`, `p` in 0-9, identical to the target it already returns for bare `TIMESTAMP` — because Arrow's Microsecond unit is this project's fixed internal representation for every Exasol TIMESTAMP precision, and the declared `p` only governs Exasol's own type check, never the Arrow unit
* *AND* the function MUST NOT return `None` for a `TIMESTAMP(p)` string, so the column stays a timestamp and is NOT routed through the `Utf8`/string path — which would stringify the value and violate the `TIMESTAMP(p)` EMITS declaration
* *AND* a bare `TIMESTAMP` string SHALL continue to map to `Some(DataType::Timestamp(TimeUnit::Microsecond, None))`, unchanged by this scenario
* *AND* `exasol_type_to_arrow` SHALL leave its `TIMESTAMP WITH LOCAL TIME ZONE` exact-match arm unchanged, because `exasol_type_from_json`'s WLTZ branch short-circuits before any precision logic (`vs-adapter/pushdown-planning`, decision [3]) and emits the bare literal `TIMESTAMP WITH LOCAL TIME ZONE` with no `(p)` suffix, so no precision-aware WLTZ arm is ever needed
<!-- /DELTA:NEW -->
