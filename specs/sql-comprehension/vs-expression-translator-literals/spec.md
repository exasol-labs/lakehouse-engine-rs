# Feature: VS Expression Translator — Literals

Extends the VS expression translator (`sql-comprehension/vs-expression-translator`) with the literal-node rendering rules — string, numeric, boolean, null, date, and timestamp literals — including the timestamp-precision handling that keeps literal types aligned with the engine's microsecond-typed columns. Split out of the core translator spec to keep that spec's scenario count manageable, mirroring the existing `-date-fns`/`-scalar-fns`/`-scalar-ops` split in this domain.

## Background

* This feature shares the six public entry points of `crates/vs-expression` — the DataFusion trio
  (`render_expression`, `render_expression_safe`, `render_df_filter_safe`) and the Exasol trio
  (`render_expression_exasol`, `render_expression_exasol_safe`, `render_df_filter_exasol_safe`);
  literal rendering is a set of arms inside the same recursive walker.
* The scan coerces every Iceberg timestamp column to `Timestamp(Microsecond, …)` (decisions 009 and
  007). In the DataFusion dialect, timestamp literals render through
  `arrow_cast(<value>, 'Timestamp(Microsecond, …)')` rather than the bare `TIMESTAMP '…'` form,
  which DataFusion's SQL frontend parses as `Timestamp(Nanosecond)` by default — the only
  SQL-surface form that pins an explicit arrow type.
* `arrow_cast` is a DataFusion-only function. In the Exasol dialect the fragment becomes wrapper SQL
  text parsed by Exasol's core engine, which rejects it with `function or script ARROW_CAST not
  found` (SQL code 42000, verified on live Exasol 2025.2.1, the image pinned in
  `docker-compose.yml`) — so a timestamp literal reaching any
  Exasol-parsed wrapper is a hard compilation error today (issue #209). The Exasol dialect renders
  the bare `TIMESTAMP '<value>'` literal instead: that is the form Exasol's own compiler sent, so
  Exasol re-parses its own literal and the `Timestamp(Nanosecond)` hazard that motivates
  `arrow_cast` does not exist on that path — it is a DataFusion type-unification concern only.
* The `+00:00` offset the DataFusion dialect appends for `literal_timestamp_utc` MUST NOT be
  appended in the Exasol dialect: Exasol's `TIMESTAMP` literal format is
  `YYYY-MM-DD HH24:MI:SS.FF9` with no offset field, and the offset form raises
  `data exception - invalid character value for cast` (SQL code 22018, verified on live Exasol
  2025.2.1). The value Exasol sends for a UTC timestamp literal is already UTC-normalised, and the
  project maps Iceberg `timestamptz` to plain Exasol `TIMESTAMP` (see `specs/mission.md` and the
  project data-type table), so the wrapper's counterpart column is a plain UTC `TIMESTAMP` and the
  offset carries no information Exasol needs.
* **Exasol's wire node type is `literal_timestamputc`, not `literal_timestamp_utc`.** The
  translator matched only the latter, so the arm never matched real traffic and every TSTZ
  literal was unrenderable in the Exasol dialect. Verified from the captured request JSON on
  the live E2E container: `{"type":"literal_timestamputc","value":"2024-03-01 09:00:00.000"}`.
  Confirmed unmatched by its effect — the pushed scan spec for that predicate carried NO
  `filter` field and listed every data file, where a plain-timestamp predicate pushes
  `"filter":"(\"ID\" = 1)"`. Note the CAPABILITY name does carry the underscore
  (`LITERAL_TIMESTAMP_UTC`); only the node type does not.
* **Reconciled against `main` at implementation time (2026-08-03), then corrected after
  adversarial review (round 4, `decision-log.md` of `fix-pushdown-tstz-literal-emits`).** The
  Exasol-dialect rendering for `literal_timestamp_utc` already existed before this fix (added by
  the independent, already-merged `fix-vs-expression-dialect` plan, PR #258) as a BARE
  `TIMESTAMP '<value>'` literal, with NO offset and NO `CONVERT_TZ` — the prior scenario below
  reflected that rendering. That rendering serves the DECLINED-WHERE-FILTER SELF-APPLY path
  (`_decision/045-fix-declined-filter-self-apply`), but the bare-wire-name defect (#242's
  wire-name half) had never made it reachable there before this fix, because the Exasol dialect
  didn't match the REAL wire name either. A first fix attempt kept the bare rendering unchanged
  and added a separate `CONVERT_TZ` wrap only at the select-list call site, reasoning that the
  filter and select-list contexts needed opposite renderings. Adversarial review verified LIVE
  (Exasol 2025.2.1) that this was wrong: comparing the bare UTC-normalized literal directly
  against this project's plain-`TIMESTAMP`-mapped Iceberg column (`007-fix-timestamptz-mapping`)
  disagrees with Exasol's own `TIMESTAMP`-vs-`TIMESTAMP WITH LOCAL TIME ZONE` coercion rule,
  which reads the naive side as SESSION-LOCAL, not as a raw value —
  `TIMESTAMP '09:30:00' > TIMESTAMP '09:00:00'` (the bare rendering's comparison) is `TRUE` while
  Exasol's native `TIMESTAMP '09:30:00' > CAST(TIMESTAMP '10:00:00' AS TSTZ)` is `FALSE`, a live
  wrong-result bug once the wire-name fix made the path reachable. `CAST(CONVERT_TZ(<value>,
  'UTC', SESSIONTIMEZONE) AS TIMESTAMP WITH LOCAL TIME ZONE)` — verified live to match Exasol's
  native comparison result — is correct for BOTH the filter self-apply context and the
  select-list projection context, so this rendering supersedes the bare `literal_timestamp_utc`
  Exasol-dialect rendering and applies identically to `literal_timestamputc`.
* Exasol CONSTANT-FOLDS a cast of a timestamp literal into a bare literal node before the
  request reaches the adapter — verified for the emittable plain-TIMESTAMP analogue, whose
  select-list node is a bare `{"type":"literal_timestamp","value":"2024-03-01 10:00:00.000"}`
  with no `function_scalar_cast` wrapper. So a projected
  `CAST(TIMESTAMP '…' AS TIMESTAMP WITH LOCAL TIME ZONE)` is a LITERAL node, and this rendering
  is what makes it renderable, and value-correct, in the Exasol dialect at all.
* A bare `CAST(x AS TIMESTAMP WITH LOCAL TIME ZONE)` (no explicit precision) was verified live
  to report `TIMESTAMP(3) WITH LOCAL TIME ZONE` via `SYS.EXA_ALL_COLUMNS` — matching Exasol's
  own default declared precision for a select-list TSTZ item exactly, so no separate
  precision-preservation logic is needed at any call site.

## Scenarios

### Scenario: Literal nodes translate to SQL literal forms

* *GIVEN* a VS expression node of type `literal_string`, `literal_exactnumeric`, `literal_double`, `literal_bool`, `literal_null`, `literal_date`, `literal_timestamp`, `literal_timestamp_utc` (the pre-existing synthetic name, kept as an accepted alias), or `literal_timestamputc` (Exasol's real wire spelling)
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL return the corresponding SQL literal:
  `literal_string` → single-quoted string with internal single-quotes escaped by doubling;
  `literal_exactnumeric` / `literal_double` → bare numeric value;
  `literal_bool` → `TRUE` or `FALSE`;
  `literal_null` → `NULL`;
  `literal_date` → `DATE 'YYYY-MM-DD'`
* *AND* in the DataFusion dialect `literal_timestamp` SHALL render as `arrow_cast('YYYY-MM-DD HH:MI:SS', 'Timestamp(Microsecond, None)')` and `literal_timestamp_utc` SHALL render as `arrow_cast('YYYY-MM-DD HH:MI:SS+00:00', 'Timestamp(Microsecond, Some("UTC"))')`, each carrying an explicit `Timestamp(Microsecond, …)` target type — never the bare `TIMESTAMP '…'` form, which DataFusion's SQL frontend parses as `Timestamp(Nanosecond)` by default; `literal_timestamputc` SHALL keep declining in the DataFusion dialect exactly as an unknown node type — returning no rendered fragment — so the pushed `ScanSpec.filter` stays byte-identical for every request (accepting it there would begin pushing TSTZ literal predicates into the scan filter as `Timestamp(Microsecond, Some("UTC"))` against a naive `timestamp_us` column, an unverified coercion tracked as `(#242)` together with the identically misspelled Iceberg `timestamptz` range-pruning arm)
* *AND* in the EXASOL dialect `literal_timestamp` SHALL render as bare `TIMESTAMP '<value>'`, unchanged; BOTH `literal_timestamp_utc` and `literal_timestamputc` SHALL instead render as `CAST(CONVERT_TZ(TIMESTAMP '<value>', 'UTC', SESSIONTIMEZONE) AS TIMESTAMP WITH LOCAL TIME ZONE)` — converting the UTC-normalized wire value into the CALLER's session zone (`SESSIONTIMEZONE` referenced SYMBOLICALLY, never resolved by the adapter) and re-declaring it TSTZ, so Exasol applies its OWN `TIMESTAMP`-vs-`TIMESTAMP WITH LOCAL TIME ZONE` coercion rule to whatever plain-`TIMESTAMP` value it is compared or matched against — this is the SAME rendering used identically whether the item appears in a projected SELECT-LIST position or a self-applied declined-WHERE-FILTER position, because both contexts compare or project this value against this project's plain-`TIMESTAMP`-mapped Iceberg columns and both need the SAME session-local-equivalent value; a bare (unconverted) rendering was verified live to produce a WRONG comparison result in the filter context (Exasol's coercion rule reads the naive column as session-local, not as a raw UTC-normalized value) — MUST NOT emit `arrow_cast`, which is not an Exasol function and makes any wrapper SQL carrying it invalid; the asymmetry between the two dialects for `literal_timestamputc` (rendered in Exasol, declined in DataFusion) MUST be commented at the arm; a null/absent value stays bare `NULL` in both node types (no `CAST`/`CONVERT_TZ`), since a three-valued NULL comparison or projection needs no conversion and `TIMESTAMP NULL` is a syntax error on Exasol
* *AND* the translator SHALL single-quote the timestamp value and escape internal single-quotes by doubling, exactly as for `literal_string`, so no literal value produces an SQL injection vector

### Scenario: Plain timestamp literals render as bare Exasol TIMESTAMP literals in the Exasol dialect

* *GIVEN* a VS expression node of type `literal_timestamp` (the plain, non-TSTZ timestamp literal — `literal_timestamp_utc`/`literal_timestamputc` are TSTZ literals, covered by the preceding scenario's `CAST(CONVERT_TZ(...) AS TIMESTAMP WITH LOCAL TIME ZONE)` rendering, not by this one)
* *WHEN* `render_expression_exasol` processes the node
* *THEN* the translator SHALL return `TIMESTAMP '<value>'` with the value single-quoted and internal single-quotes escaped by doubling, exactly as for `literal_string`, and the fragment MUST NOT contain `arrow_cast`, which Exasol rejects with `function or script ARROW_CAST not found` (SQL code 42000)
* *AND* a node whose `value` is absent or JSON `null` SHALL render as the bare `NULL` keyword in the Exasol dialect, because `TIMESTAMP NULL` is an Exasol syntax error (`unexpected TIMESTAMP_`, SQL code 42000, verified on live Exasol 2025.2.1)
* *AND* the DataFusion dialect SHALL keep its existing rendering of that same absent-or-`null` `value` unchanged — `arrow_cast(NULL, 'Timestamp(Microsecond, None)')` — and remain byte-identical to the preceding scenario, so the microsecond-typing guarantee the node-local scan depends on is unchanged

### Scenario: Far-future timestamp literals survive DataFusion optimization against a microsecond column

* *GIVEN* a `literal_timestamp` or `literal_timestamp_utc` node whose value is a far-future instant outside the `Timestamp(Nanosecond)` range — for example `9999-12-31 23:59:59`, above the nanosecond maximum `2262-04-11T23:47:16`
* *AND* a DataFusion table whose comparison column is typed `Timestamp(Microsecond, None)` for `literal_timestamp`, or `Timestamp(Microsecond, Some("UTC"))` for `literal_timestamp_utc`
* *WHEN* the fragment rendered by `render_expression` is placed in a predicate or CASE branch against that column and the query is driven through DataFusion's `simplify_expressions` optimizer rule
* *THEN* the optimizer SHALL complete without a `Cast error: Overflow converting … to Nanosecond`
* *AND* the rendered fragment MUST NOT introduce a `Timestamp(Nanosecond)` intermediate during type unification with the microsecond column
* *AND* this scenario SHALL remain scoped to the DataFusion dialect, because the overflow arises inside DataFusion's optimizer and no equivalent hazard exists for an Exasol-parsed `TIMESTAMP '<value>'` literal, whose range covers the value Exasol itself sent
