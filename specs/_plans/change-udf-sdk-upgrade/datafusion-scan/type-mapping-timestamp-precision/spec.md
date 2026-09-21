# Feature: Timestamp Precision in DataFusion-to-Exasol Type Mapping

Defines the version-gated fractional-second precision Exasol declares for a catalog timestamp
column (Iceberg `timestamp`/`timestamptz`/`timestamp_ns`/`timestamptz_ns`, Delta/Unity Catalog
`TIMESTAMP`/`TIMESTAMP_NTZ`), and the `timestamptz` zone-flattening trade-off that precision
change does not alter. Split out of `datafusion-scan/type-mapping` once that feature's scenario
count crossed this library's per-spec organization threshold; this feature owns every scenario
issue #359 added, and the parent feature keeps the general Arrow/Exasol type-compatibility surface.

## Background

<!-- DELTA:REMOVED -->
* `ExaType` carries no timestamp precision: every `TIMESTAMP(p)` in the `EMITS` clause
  reaches the scan as the single variant `ExaType::Timestamp`. The rule that `p` governs
  only Exasol's own type check, never the Arrow unit, therefore holds structurally.
<!-- /DELTA:REMOVED -->

<!-- DELTA:NEW -->
* `ExaType::Timestamp { precision }` carries the declared fractional-second precision since
  `exasol-udf-sdk` 0.28.1 (issue #405). The scan reads that precision and does not act on it. The
  fixed microsecond emit target is therefore a decision backed by a live measurement, not a
  property of the type.
* Arrow's `TimeUnit` offers Second, Millisecond, Microsecond and Nanosecond only, so a coercion
  target that follows the declared `p` is not expressible for `p` in {1, 2, 4, 5, 7, 8}.
* Which component truncates depends on the declaration path. For a CATALOG timestamp column the
  adapter declares bare `TIMESTAMP` or `TIMESTAMP(6)` with no CAST in the query, the scan emits the
  microsecond value unchanged, and EXASOL performs any remaining truncation. For a projected
  `CAST(x AS TIMESTAMP(p))` the truncation happens earlier and elsewhere: `vs-expression`'s
  `snap_timestamp_precision` renders the cast into the scan's own DataFusion SQL, so DATAFUSION
  truncates the value before it reaches the emit boundary. This feature's live-engine claim is
  measured on the catalog-column path only and rests on nothing the CAST path does.
<!-- /DELTA:NEW -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: A TIMESTAMP(p) EMITS string maps back to the microsecond Arrow timestamp

* *GIVEN* an EMITS type of the form `TIMESTAMP(p)` for an integer precision `p` in 0-9, or a bare `TIMESTAMP` — the first is what the adapter declares for a projected TIMESTAMP CAST expression once `exasol_type_from_json` (`vs-adapter/pushdown-planning`) reads `fractionalSecondsPrecision`, the second is what it declares for a catalog timestamp column on the pre-2025 version arm
* *WHEN* the scan resolves that column's Arrow coercion target at the emit boundary (`target_arrow_type`) from the `ExaType` that `UdfContext::output_column` reports for the column
* *THEN* the reported variant SHALL be `ExaType::Timestamp { precision }` for every such declaration, and the resolved emit target MUST NOT depend on which `precision` the handshake reports, so neither a declared `p` nor an SLC-side default for a bare `TIMESTAMP` can change any emitted value
* *AND* `target_arrow_type` SHALL return `DataType::Timestamp(TimeUnit::Microsecond, None)` for EVERY reported `precision`, MUST NOT vary the Arrow unit or the timezone with `precision`, and MUST NOT gain a precision-following target, because Arrow's `TimeUnit` has no unit for `p` in {1, 2, 4, 5, 7, 8}
* *AND* on Exasol 8.29.13, where the version gate declares a CATALOG timestamp column as a bare `TIMESTAMP` whose SLC-reported `precision` task 3.2 records, and no CAST stands between the scan and the emit boundary, the running engine SHALL accept that microsecond Arrow column into the BELOW-microsecond declaration and SHALL truncate each value to millisecond on its own side rather than failing the emit; on Exasol 2025.1.16, where the gate declares `TIMESTAMP(6)` and the emitted resolution equals the declaration, it SHALL retain every microsecond digit. Both arms SHALL be measured against the local Exasol Docker container rather than carried over from the SDK's upstream fixture, because the SLC's Arrow-IPC block feed is the component that gained the precision and this scan path is not the fixture upstream exercised
* *AND* the preceding clause MUST NOT be read as covering a declaration ABOVE microsecond resolution, which stays a named, tracked limitation ([#411](https://github.com/exasol-labs/lakehouse-engine-rs/issues/411)): the version gate declares only bare `TIMESTAMP` or `TIMESTAMP(6)` for a catalog column, so the only route to a higher `p` is a projected `CAST(x AS TIMESTAMP(p))` whose value DataFusion has already truncated before the emit boundary, and nothing measured here establishes what the SLC does with a genuinely finer value under such a declaration
* *AND* `target_arrow_type` MUST NOT route `ExaType::Timestamp` through the `Utf8` string path, which would stringify the value and violate the `TIMESTAMP(p)` EMITS declaration
* *AND* the removal of the `ExaType::TimestampTz` coercion target MUST NOT change any emitted value, because this feature's "Iceberg timestamptz maps to plain Exasol TIMESTAMP" scenario already requires the emit boundary to cast a zoned column to `Timestamp(_, None)`, and Exasol rejects `TIMESTAMP WITH LOCAL TIME ZONE` as a UDF `EMITS` output type (`sqlCode 22002`), so no declaration ever reached that target
* *AND* `exasol_type_to_arrow` SHALL keep its recorded `TIMESTAMP(p)` and `TIMESTAMP WITH LOCAL TIME ZONE` arms and its recorded test coverage, because it is keyed by TYPE STRING rather than by `ExaType` and the removed variant does not reach it
<!-- /DELTA:CHANGED -->
