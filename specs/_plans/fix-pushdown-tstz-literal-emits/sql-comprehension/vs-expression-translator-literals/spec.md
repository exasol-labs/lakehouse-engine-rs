# Feature: vs-expression Translator — Literals

Translates Exasol Virtual Schema literal nodes into rendered SQL literal fragments for
the scan-side DataFusion dialect and the wrapper-side Exasol dialect.

## Background

* The two timestamp literal arms are currently dialect-INSENSITIVE: both render
  `arrow_cast(...)`, a DataFusion function that does not exist in Exasol. Any wrapper
  splicing such a fragment into Exasol SQL is invalid — a latent defect the row-scan
  routing in `vs-adapter/pushdown-planning-capability-extensions` would otherwise expose.
* **Exasol's wire node type is `literal_timestamputc`, not `literal_timestamp_utc`.** The
  translator matches the latter, so the arm never matches real traffic and every TSTZ
  literal is unrenderable. Verified from the captured request JSON on the live E2E
  container: `{"type":"literal_timestamputc","value":"2024-03-01 09:00:00.000"}`. Confirmed
  unmatched by its effect — the pushed scan spec for that predicate carries NO `filter`
  field and lists every data file, where a plain-timestamp predicate pushes
  `"filter":"(\"ID\" = 1)"`. Note the CAPABILITY name does carry the underscore
  (`LITERAL_TIMESTAMP_UTC`); only the node type does not.
* The wire value of that node is UTC-NORMALIZED, not session-local: `09:00:00.000` for a
  session-local `10:00:00` at `SESSIONTIMEZONE = EUROPE/BERLIN` — one hour earlier, the UTC
  representation. Exasol's own `filter_expr_string_for_debug` for the same request names the
  canonical repair shape, including its precision:
  `CAST(CONVERT_TZ(TIMESTAMP '2024-03-01 09:00:00.000', 'UTC', SESSIONTIMEZONE) AS TIMESTAMP(3) WITH LOCAL TIME ZONE)`.
* Exasol CONSTANT-FOLDS a cast of a timestamp literal into a bare literal node before the
  request reaches the adapter — verified for the emittable plain-TIMESTAMP analogue, whose
  select-list node is a bare `{"type":"literal_timestamp","value":"2024-03-01 10:00:00.000"}`
  with no `function_scalar_cast` wrapper. So a projected
  `CAST(TIMESTAMP '…' AS TIMESTAMP WITH LOCAL TIME ZONE)` is a LITERAL node, and this delta
  is what makes it renderable.
* That repair is value-exact and was verified live:
  `CAST(CONVERT_TZ(TIMESTAMP '2024-03-01 09:00:00', 'UTC', SESSIONTIMEZONE) AS TIMESTAMP WITH LOCAL TIME ZONE)`
  returns `2024-03-01 10:00:00`, identical to the native
  `CAST(TIMESTAMP '2024-03-01 10:00:00' AS TIMESTAMP WITH LOCAL TIME ZONE)`, and Exasol
  types the result `TIMESTAMP(3) WITH LOCAL TIME ZONE`.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Literal nodes translate to SQL literal forms

* *GIVEN* a VS expression node of type `literal_string`, `literal_exactnumeric`, `literal_double`, `literal_bool`, `literal_null`, `literal_date`, `literal_timestamp`, or `literal_timestamputc` (Exasol's wire spelling; the translator SHALL also accept the legacy `literal_timestamp_utc` alias its existing fixtures use)
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL return the corresponding SQL literal:
  `literal_string` → single-quoted string with internal single-quotes escaped by doubling;
  `literal_exactnumeric` / `literal_double` → bare numeric value;
  `literal_bool` → `TRUE` or `FALSE`;
  `literal_null` → `NULL`;
  `literal_date` → `DATE 'YYYY-MM-DD'`
* *AND* in the DataFusion dialect the two timestamp forms SHALL render as
  `literal_timestamp` → `arrow_cast('YYYY-MM-DD HH:MI:SS', 'Timestamp(Microsecond, None)')` and
  the timestamp-utc form under its legacy `literal_timestamp_utc` spelling → `arrow_cast('YYYY-MM-DD HH:MI:SS+00:00', 'Timestamp(Microsecond, Some("UTC"))')`, each carrying an explicit `Timestamp(Microsecond, …)` target type — never the bare `TIMESTAMP '…'` form, which DataFusion's SQL frontend parses as `Timestamp(Nanosecond)` by default
* *AND* in the EXASOL dialect the two timestamp forms SHALL instead render as
  `literal_timestamp` → `TIMESTAMP '<value>'` and
  the timestamp-utc form → `CAST(CONVERT_TZ(TIMESTAMP '<value>', 'UTC', SESSIONTIMEZONE) AS TIMESTAMP WITH LOCAL TIME ZONE)`, and MUST NOT emit `arrow_cast`, which is not an Exasol function and makes any wrapper SQL carrying it invalid
* *AND* the EXASOL dialect SHALL accept the wire node type `literal_timestamputc` while the DATAFUSION dialect SHALL keep declining it exactly as today, returning no rendered fragment; the asymmetry is deliberate and MUST be commented at the arm, because accepting the wire name in the DataFusion dialect would begin pushing TSTZ literal predicates into the scan filter as `Timestamp(Microsecond, Some("UTC"))` against a naive `timestamp_us` column, an unverified coercion whose failure mode is a silently wrong result rather than today's correct-but-unpruned scan — tracked as `(#242)`, together with the identically misspelled Iceberg `timestamptz` range-pruning arm
* *AND* the Exasol-dialect timestamp-utc rendering SHALL treat the node's value as the UTC representation and SHALL reference `SESSIONTIMEZONE` SYMBOLICALLY so Exasol resolves it in the CALLER's session — the adapter MUST NOT read, hardcode, or infer a time zone, and MUST NOT reach for connect-back, which opens an INDEPENDENT session — and its `CAST` target SHALL carry the fractional-seconds precision the request declares for the item when one is present (`TIMESTAMP(p) WITH LOCAL TIME ZONE`), so Exasol's positional select-list type check matches exactly
* *AND* the translator SHALL single-quote the timestamp value and escape internal single-quotes by doubling, exactly as for `literal_string`, so no literal value produces an SQL injection vector
<!-- /DELTA:CHANGED -->
