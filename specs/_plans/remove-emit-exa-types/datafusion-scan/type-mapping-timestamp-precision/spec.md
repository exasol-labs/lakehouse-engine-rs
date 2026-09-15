# Feature: Timestamp Precision in DataFusion-to-Exasol Type Mapping

Defines the version-gated fractional-second precision Exasol declares for a catalog timestamp
column (Iceberg `timestamp`/`timestamptz`/`timestamp_ns`/`timestamptz_ns`, Delta/Unity Catalog
`TIMESTAMP`/`TIMESTAMP_NTZ`), and the `timestamptz` zone-flattening trade-off that precision
change does not alter. Split out of `datafusion-scan/type-mapping` once that feature's scenario

## Background

<!-- DELTA:NEW -->
* **This delta is issue #399.** It restates ONE scenario against the new source of the declared emit
  type. It amends no other scenario, SUPERSEDES no Background bullet, and changes no declared
  precision, no version gate, and no zone-awareness trade-off.
* **The scan no longer parses the declared type string at the emit boundary.** Issue #399 removes
  `CommonScanSpec::emit_exa_types`. `target_arrow_type` now reads the `ExaType` the SDK reports
  through `UdfContext::output_column`, so `exasol_type_to_arrow` is no longer on the emit path.
* **`ExaType` carries no timestamp precision, which makes the recorded rule structural.** Every
  `TIMESTAMP(p)` in the `EMITS` clause reaches the scan as the single variant `ExaType::Timestamp`.
  The recorded rule that `p` governs only Exasol's own type check, never the Arrow unit, therefore
  holds by construction rather than by a parser arm that must be kept precision-tolerant.
* **The adapter side is untouched.** `exasol_type_from_json` still reads `fractionalSecondsPrecision`
  and still renders `TIMESTAMP(p)` into the `EMITS` clause and the `createVirtualSchema` declaration.
  Only the scan's reading of that clause changes.
<!-- /DELTA:NEW -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: A TIMESTAMP(p) EMITS string maps back to the microsecond Arrow timestamp

* *GIVEN* an EMITS type string of the form `TIMESTAMP(p)` for an integer precision `p` in 0-9 — the shape the adapter declares for a projected TIMESTAMP CAST expression once `exasol_type_from_json` (`vs-adapter/pushdown-planning`) reads `fractionalSecondsPrecision`
* *WHEN* the scan resolves that column's Arrow coercion target at the emit boundary (`target_arrow_type`) from the `ExaType` that `UdfContext::output_column` reports for the column
* *THEN* the reported variant SHALL be `ExaType::Timestamp` for every `TIMESTAMP(p)`, `p` in 0-9, and for a bare `TIMESTAMP`, because `ExaType` models no fractional-second precision
* *AND* `target_arrow_type` SHALL return `DataType::Timestamp(TimeUnit::Microsecond, None)` for `ExaType::Timestamp`, because Arrow's Microsecond unit is this project's fixed internal representation for every Exasol TIMESTAMP precision, and the declared `p` governs only Exasol's own type check
* *AND* `target_arrow_type` MUST NOT route `ExaType::Timestamp` through the `Utf8` string path, which would stringify the value and violate the `TIMESTAMP(p)` EMITS declaration
* *AND* `ExaType::TimestampTz` SHALL map to `DataType::Timestamp(TimeUnit::Microsecond, Some("UTC"))`, keeping the recorded `TIMESTAMP WITH LOCAL TIME ZONE` target unchanged
* *AND* `exasol_type_to_arrow` SHALL keep its recorded `TIMESTAMP(p)` and `TIMESTAMP WITH LOCAL TIME ZONE` arms and its recorded test coverage, because it stays the documented public inverse of `arrow_to_exasol_type` even though the emit path no longer calls it
<!-- /DELTA:CHANGED -->
