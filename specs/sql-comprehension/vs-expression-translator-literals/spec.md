# Feature: VS Expression Translator, Literals

Extends the VS expression translator (`sql-comprehension/vs-expression-translator`) with the literal-node rendering rules for string, numeric, boolean, null, date, and timestamp literals. The timestamp rules keep literal types aligned with the engine's microsecond-typed columns.

## Background

* The scan coerces every Iceberg timestamp column to `Timestamp(Microsecond, ...)`. The DataFusion dialect renders timestamp literals through `arrow_cast(<value>, 'Timestamp(Microsecond, ...)')`, because DataFusion parses a bare `TIMESTAMP '...'` as `Timestamp(Nanosecond)`.
* `arrow_cast` is DataFusion-only, and Exasol rejects it with `function or script ARROW_CAST not found` (SQL code 42000). The Exasol dialect renders the bare `TIMESTAMP '<value>'` literal instead, since Exasol re-parses a literal it sent itself.
* Exasol's wire node type for a UTC timestamp literal is `literal_timestamputc`. The capability name is `LITERAL_TIMESTAMP_UTC`.
* Exasol's `TIMESTAMP` literal has no offset field, so the Exasol dialect never appends `+00:00`. The wire value is already UTC-normalized, and Iceberg `timestamptz` maps to plain Exasol `TIMESTAMP`.
* Exasol reads the naive side of a `TIMESTAMP` versus `TIMESTAMP WITH LOCAL TIME ZONE` comparison as session-local. A bare UTC literal compared against a plain `TIMESTAMP` column therefore gives a wrong result, so the Exasol dialect converts it with `CONVERT_TZ` into the session zone.

## Scenarios

### Scenario: Literal nodes translate to SQL literal forms

* *GIVEN* a VS expression node of type `literal_string`, `literal_exactnumeric`, `literal_double`, `literal_bool`, `literal_null`, `literal_date`, `literal_timestamp`, `literal_timestamp_utc` (a synthetic name, accepted as an alias), or `literal_timestamputc` (Exasol's real wire spelling)
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL return the corresponding SQL literal:
  `literal_string` → single-quoted string with internal single-quotes escaped by doubling;
  `literal_exactnumeric` → bare numeric value;
  `literal_double` → bare numeric value in the Exasol dialect, and `CAST('<value>' AS DOUBLE)` in the DataFusion dialect, because the scan reads a bare numeric literal as DECIMAL (`scan-types/scan-numeric-typing`);
  `literal_bool` → `TRUE` or `FALSE`;
  `literal_null` → `NULL`;
  `literal_date` → `DATE 'YYYY-MM-DD'`
* *AND* in the DataFusion dialect `literal_timestamp` SHALL render as `arrow_cast('YYYY-MM-DD HH:MI:SS', 'Timestamp(Microsecond, None)')`, or as `Timestamp(Nanosecond, None)` when its value has more than 6 significant fractional digits (`pushdown-types/pushdown-planning-timestamp-literal-precision`), and `literal_timestamp_utc` SHALL render as `arrow_cast('YYYY-MM-DD HH:MI:SS+00:00', 'Timestamp(Microsecond, Some("UTC"))')`, each carrying an explicit `Timestamp(Microsecond, …)` target type, never the bare `TIMESTAMP '…'` form, which DataFusion's SQL frontend parses as `Timestamp(Nanosecond)` by default; `literal_timestamputc` SHALL keep declining in the DataFusion dialect exactly as an unknown node type, returning no rendered fragment, so no TSTZ literal predicate reaches the scan filter as `Timestamp(Microsecond, Some("UTC"))` against a naive `timestamp_us` column, a coercion that is not verified
* *AND* in the EXASOL dialect `literal_timestamp` SHALL render as bare `TIMESTAMP '<value>'`; BOTH `literal_timestamp_utc` and `literal_timestamputc` SHALL instead render as `CAST(CONVERT_TZ(TIMESTAMP '<value>', 'UTC', SESSIONTIMEZONE) AS TIMESTAMP WITH LOCAL TIME ZONE)`, converting the UTC-normalized wire value into the CALLER's session zone (`SESSIONTIMEZONE` referenced SYMBOLICALLY, never resolved by the adapter) and re-declaring it TSTZ, so Exasol applies its OWN `TIMESTAMP`-vs-`TIMESTAMP WITH LOCAL TIME ZONE` coercion rule to whatever plain-`TIMESTAMP` value it is compared or matched against, this is the SAME rendering used identically whether the item appears in a projected SELECT-LIST position or a self-applied declined-WHERE-FILTER position, because both contexts compare or project this value against this project's plain-`TIMESTAMP`-mapped Iceberg columns and both need the SAME session-local-equivalent value; a bare (unconverted) rendering was verified live to produce a WRONG comparison result in the filter context (Exasol's coercion rule reads the naive column as session-local, not as a raw UTC-normalized value), MUST NOT emit `arrow_cast`, which is not an Exasol function and makes any wrapper SQL carrying it invalid; the asymmetry between the two dialects for `literal_timestamputc` (rendered in Exasol, declined in DataFusion) MUST be commented at the arm; a null/absent value stays bare `NULL` in both node types (no `CAST`/`CONVERT_TZ`), since a three-valued NULL comparison or projection needs no conversion and `TIMESTAMP NULL` is a syntax error on Exasol
* *AND* the translator SHALL single-quote the timestamp value and escape internal single-quotes by doubling, exactly as for `literal_string`, so no literal value produces an SQL injection vector

### Scenario: Plain timestamp literals render as bare Exasol TIMESTAMP literals in the Exasol dialect

* *GIVEN* a VS expression node of type `literal_timestamp` (the plain, non-TSTZ timestamp literal, `literal_timestamp_utc`/`literal_timestamputc` are TSTZ literals, covered by the preceding scenario's `CAST(CONVERT_TZ(...) AS TIMESTAMP WITH LOCAL TIME ZONE)` rendering, not by this one)
* *WHEN* `render_expression_exasol` processes the node
* *THEN* the translator SHALL return `TIMESTAMP '<value>'` with the value single-quoted and internal single-quotes escaped by doubling, exactly as for `literal_string`, and the fragment MUST NOT contain `arrow_cast`, which Exasol rejects with `function or script ARROW_CAST not found` (SQL code 42000)
* *AND* a node whose `value` is absent or JSON `null` SHALL render as the bare `NULL` keyword in the Exasol dialect, because `TIMESTAMP NULL` is an Exasol syntax error (`unexpected TIMESTAMP_`, SQL code 42000, verified on live Exasol 2025.2.1)
* *AND* the DataFusion dialect SHALL render that same absent-or-`null` `value` as `arrow_cast(NULL, 'Timestamp(Microsecond, None)')`, identical to the preceding scenario, so the microsecond-typing guarantee the node-local scan depends on holds

### Scenario: Far-future timestamp literals survive DataFusion optimization against a microsecond column

* *GIVEN* a `literal_timestamp` or `literal_timestamp_utc` node whose value is a far-future instant outside the `Timestamp(Nanosecond)` range, for example `9999-12-31 23:59:59`, above the nanosecond maximum `2262-04-11T23:47:16`
* *AND* a DataFusion table whose comparison column is typed `Timestamp(Microsecond, None)` for `literal_timestamp`, or `Timestamp(Microsecond, Some("UTC"))` for `literal_timestamp_utc`
* *WHEN* the fragment rendered by `render_expression` is placed in a predicate or CASE branch against that column and the query is driven through DataFusion's `simplify_expressions` optimizer rule
* *THEN* the optimizer SHALL complete without a `Cast error: Overflow converting … to Nanosecond`
* *AND* the rendered fragment MUST NOT introduce a `Timestamp(Nanosecond)` intermediate during type unification with the microsecond column
* *AND* this scenario SHALL remain scoped to the DataFusion dialect, because the overflow arises inside DataFusion's optimizer and no equivalent hazard exists for an Exasol-parsed `TIMESTAMP '<value>'` literal, whose range covers the value Exasol itself sent
