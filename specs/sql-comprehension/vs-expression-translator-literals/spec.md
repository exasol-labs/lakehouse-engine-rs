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

## Scenarios

### Scenario: Literal nodes translate to SQL literal forms

* *GIVEN* a VS expression node of type `literal_string`, `literal_exactnumeric`, `literal_double`, `literal_bool`, `literal_null`, `literal_date`, `literal_timestamp`, or `literal_timestamp_utc`
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL return the corresponding SQL literal:
  `literal_string` → single-quoted string with internal single-quotes escaped by doubling;
  `literal_exactnumeric` / `literal_double` → bare numeric value;
  `literal_bool` → `TRUE` or `FALSE`;
  `literal_null` → `NULL`;
  `literal_date` → `DATE 'YYYY-MM-DD'`;
  `literal_timestamp` → `arrow_cast('YYYY-MM-DD HH:MI:SS', 'Timestamp(Microsecond, None)')`;
  `literal_timestamp_utc` → `arrow_cast('YYYY-MM-DD HH:MI:SS+00:00', 'Timestamp(Microsecond, Some("UTC"))')`
* *AND* both timestamp forms SHALL carry an explicit `Timestamp(Microsecond, …)` target type — never the bare `TIMESTAMP '…'` form, which DataFusion's SQL frontend parses as `Timestamp(Nanosecond)` by default
* *AND* the translator SHALL single-quote the timestamp value and escape internal single-quotes by doubling, exactly as for `literal_string`, so no literal value produces an SQL injection vector
* *AND* the six non-timestamp literal forms SHALL render identically in both dialects, because each is already valid Exasol SQL

### Scenario: Timestamp literals render as bare Exasol TIMESTAMP literals in the Exasol dialect

* *GIVEN* a VS expression node of type `literal_timestamp` or `literal_timestamp_utc`
* *WHEN* `render_expression_exasol` processes the node
* *THEN* the translator SHALL return `TIMESTAMP '<value>'` with the value single-quoted and internal single-quotes escaped by doubling, exactly as for `literal_string`, and the fragment MUST NOT contain `arrow_cast`, which Exasol rejects with `function or script ARROW_CAST not found` (SQL code 42000)
* *AND* for `literal_timestamp_utc` the rendered value MUST be the value Exasol sent with NO `+00:00` offset appended, because Exasol's `TIMESTAMP` literal format has no offset field and the offset form raises `data exception - invalid character value for cast` (SQL code 22018)
* *AND* a node whose `value` is absent or JSON `null` SHALL render as the bare `NULL` keyword in the Exasol dialect, for both node types, because `TIMESTAMP NULL` is an Exasol syntax error (`unexpected TIMESTAMP_`, SQL code 42000, verified on live Exasol 2025.2.1)
* *AND* the DataFusion dialect SHALL keep its existing per-node-type rendering of that same absent-or-`null` `value` unchanged — `arrow_cast(NULL, 'Timestamp(Microsecond, None)')` for `literal_timestamp`, the bare `NULL` keyword for `literal_timestamp_utc` — an asymmetry that predates this change and is frozen by it, NOT aligned across the two node types
* *AND* the DataFusion-dialect rendering of both node types MUST remain byte-identical to the preceding scenario, so the microsecond-typing guarantee the node-local scan depends on is unchanged

### Scenario: Far-future timestamp literals survive DataFusion optimization against a microsecond column

* *GIVEN* a `literal_timestamp` or `literal_timestamp_utc` node whose value is a far-future instant outside the `Timestamp(Nanosecond)` range — for example `9999-12-31 23:59:59`, above the nanosecond maximum `2262-04-11T23:47:16`
* *AND* a DataFusion table whose comparison column is typed `Timestamp(Microsecond, None)` for `literal_timestamp`, or `Timestamp(Microsecond, Some("UTC"))` for `literal_timestamp_utc`
* *WHEN* the fragment rendered by `render_expression` is placed in a predicate or CASE branch against that column and the query is driven through DataFusion's `simplify_expressions` optimizer rule
* *THEN* the optimizer SHALL complete without a `Cast error: Overflow converting … to Nanosecond`
* *AND* the rendered fragment MUST NOT introduce a `Timestamp(Nanosecond)` intermediate during type unification with the microsecond column
* *AND* this scenario SHALL remain scoped to the DataFusion dialect, because the overflow arises inside DataFusion's optimizer and no equivalent hazard exists for an Exasol-parsed `TIMESTAMP '<value>'` literal, whose range covers the value Exasol itself sent
