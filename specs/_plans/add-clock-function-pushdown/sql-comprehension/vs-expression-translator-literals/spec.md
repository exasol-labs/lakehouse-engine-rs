# Feature: VS Expression Translator, Literals

Extends the VS expression translator (`sql-comprehension/vs-expression-translator`) with the literal-node rendering rules for string, numeric, boolean, null, date, and timestamp literals. The timestamp rules keep literal types aligned with the engine's microsecond-typed columns.

## Background

* The scan coerces every Iceberg timestamp column to `Timestamp(Microsecond, ...)`. The DataFusion dialect renders timestamp literals through `arrow_cast(<value>, 'Timestamp(Microsecond, ...)')`, because DataFusion parses a bare `TIMESTAMP '...'` as `Timestamp(Nanosecond)`.
* `arrow_cast` is DataFusion-only, and Exasol rejects it with `function or script ARROW_CAST not found` (SQL code 42000). The Exasol dialect renders the bare `TIMESTAMP '<value>'` literal instead, since Exasol re-parses a literal it sent itself.
* Exasol's wire node type for a UTC timestamp literal is `literal_timestamputc`. The capability name is `LITERAL_TIMESTAMP_UTC`.
* Exasol's `TIMESTAMP` literal has no offset field, so the Exasol dialect never appends `+00:00`. The wire value is already UTC-normalized, and Iceberg `timestamptz` maps to plain Exasol `TIMESTAMP`.
* Exasol reads the naive side of a `TIMESTAMP` versus `TIMESTAMP WITH LOCAL TIME ZONE` comparison as session-local. A bare UTC literal compared against a plain `TIMESTAMP` column therefore gives a wrong result, so the Exasol dialect converts it with `CONVERT_TZ` into the session zone.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Literal nodes translate to SQL literal forms

* *GIVEN* a VS expression node of type `literal_string`, `literal_exactnumeric`, `literal_double`, `literal_bool`, `literal_null`, `literal_date`, `literal_timestamp`, `literal_timestamp_utc` (an accepted alias), or `literal_timestamputc` (Exasol's wire spelling)
* *WHEN* the translator renders the node
* *THEN* the translator SHALL return the corresponding SQL literal:
  `literal_string` → single-quoted string with internal single-quotes escaped by doubling;
  `literal_exactnumeric` → bare numeric value;
  `literal_double` → bare numeric value in the Exasol dialect, and `CAST('<value>' AS DOUBLE)` in the DataFusion dialect, because the scan reads a bare numeric literal as DECIMAL (`scan-types/scan-numeric-typing`);
  `literal_bool` → `TRUE` or `FALSE`;
  `literal_null` → `NULL`;
  `literal_date` → `DATE 'YYYY-MM-DD'`
* *AND* in the DataFusion dialect `literal_timestamp` SHALL render as `arrow_cast('YYYY-MM-DD HH:MI:SS', 'Timestamp(Microsecond, None)')`, or with `Timestamp(Nanosecond, None)` when its value has more than 6 significant fractional digits (`pushdown-types/pushdown-planning-timestamp-literal-precision`), and `literal_timestamp_utc` SHALL render as `arrow_cast('YYYY-MM-DD HH:MI:SS+00:00', 'Timestamp(Microsecond, Some("UTC"))')`, never as a bare `TIMESTAMP '…'`, which DataFusion types as nanoseconds
* *AND* in the DataFusion dialect `literal_timestamputc` SHALL decline, because its comparison depends on the session zone; a comparison with it reaches the scan only as the plain timestamp literal that `pushdown/pushdown-planning-clock-functions` puts in its place
* *AND* in the Exasol dialect `literal_timestamp` SHALL render as bare `TIMESTAMP '<value>'`, and `literal_timestamp_utc` and `literal_timestamputc` SHALL render as `CAST(CONVERT_TZ(TIMESTAMP '<value>', 'UTC', SESSIONTIMEZONE) AS TIMESTAMP WITH LOCAL TIME ZONE)`, so Exasol shows the UTC value in the caller's session zone and a projected literal returns the session's native value, and MUST NOT render `arrow_cast`, which Exasol rejects
* *AND* a null timestamp value SHALL render as bare `NULL` in both dialects, because `TIMESTAMP NULL` is a syntax error on Exasol
* *AND* the translator SHALL single-quote the timestamp value and escape internal single-quotes by doubling, exactly as for `literal_string`, so no literal value produces an SQL injection vector
<!-- /DELTA:CHANGED -->
