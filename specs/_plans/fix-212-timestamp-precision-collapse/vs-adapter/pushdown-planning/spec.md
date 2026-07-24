# Feature: Pushdown Planning

Translates an Exasol `pushdown` request into a scan-driving SQL statement over the `LAKEHOUSE_SCAN` UDF: derives the scanned Iceberg table, resolves the file list once, pushes projection/filter/LIMIT into the per-shard scan spec, and declares the scalar scan UDF's EMITS column types.

## Background

* This delta adds one scenario governing the EMITS type of a projected TIMESTAMP CAST expression; every other pushdown-planning scenario is unchanged.
* The scalar scan UDF's EMITS column types are derived by `exasol_type_from_json` from each select-list item's `selectListDataTypes` descriptor. Exasol's `EXPLAIN VIRTUAL` type check validates the outer query's expected column types against these EMITS types.
* A TIMESTAMP dataType carries its fractional-seconds precision in `fractionalSecondsPrecision` (optional, default 3), and its timezone semantics in `withLocalTimeZone` (optional, default false).

## Scenarios

<!-- DELTA:NEW -->
### Scenario: Projected CAST expression preserves the declared TIMESTAMP fractional-seconds precision in its EMITS type

* *GIVEN* a row-scan `pushdown` request whose select list carries an expression item — for example `CAST(c_ts AS TIMESTAMP(6))` — whose parallel `selectListDataTypes` entry is `{"type":"TIMESTAMP","fractionalSecondsPrecision":6}`
* *WHEN* the adapter derives the scalar scan UDF's EMITS column type for that item via `exasol_type_from_json`
* *THEN* the derived EMITS type SHALL be `TIMESTAMP(6)`, reading the precision from the `fractionalSecondsPrecision` field — Exasol's documented data-type field for a TIMESTAMP's fractional-seconds precision (default 3), verified against Exasol's virtual-schema data-type API and the reference fixture `pushdown_request_alltypes.json`; the `DECIMAL` arm's `precision`/`scale` keys MUST NOT be read for a TIMESTAMP
* *AND* when `fractionalSecondsPrecision` is absent the derived EMITS type SHALL be bare `TIMESTAMP`, equivalent to Exasol's default `TIMESTAMP(3)`, preserving the current behavior asserted by `exasol_type_from_json_reads_with_local_time_zone_flag`
* *AND* a `withLocalTimeZone: true` timestamp dataType SHALL still map to `TIMESTAMP WITH LOCAL TIME ZONE` and SHALL take precedence over any precision rendering, leaving the WLTZ branch unchanged
* *AND* this EMITS-precision derivation and the vs-expression CAST-render precision fix (`sql-comprehension/vs-expression-translator-scalar-ops`) SHALL ship together, because Exasol's `EXPLAIN VIRTUAL` type check (`Data type mismatch ... Expected TIMESTAMP(6), but got TIMESTAMP(3)`, SQL error 04000) compares the outer query's expected column type against the EMITS-declared type, and fixing only one of the two collapse points still fails the check; this scenario governs the pushed-down CAST *expression's* declared target type and MUST NOT be conflated with `datafusion-scan/type-mapping`'s "Iceberg timestamptz maps to plain Exasol TIMESTAMP" scenario, which governs a raw column's `createVirtualSchema` schema declaration (always bare `TIMESTAMP`)
<!-- /DELTA:NEW -->
