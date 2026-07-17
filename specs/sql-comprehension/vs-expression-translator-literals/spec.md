# Feature: VS Expression Translator — Literals

Extends the VS expression translator (`sql-comprehension/vs-expression-translator`) with the literal-node rendering rules — string, numeric, boolean, null, date, and timestamp literals — including the timestamp-precision handling that keeps literal types aligned with the engine's microsecond-typed columns. Split out of the core translator spec to keep that spec's scenario count manageable, mirroring the existing `-date-fns`/`-scalar-fns`/`-scalar-ops` split in this domain.

## Background

* This feature shares the three public entry points of `crates/vs-expression`
  (`render_expression`, `render_expression_safe`, `render_df_filter_safe`); literal rendering is a
  set of arms inside the same recursive walker.
* The scan coerces every Iceberg timestamp column to `Timestamp(Microsecond, …)` (decisions 009 and
  007). Timestamp literals render through `arrow_cast(<value>, 'Timestamp(Microsecond, …)')` rather
  than the bare `TIMESTAMP '…'` form, which DataFusion's SQL frontend parses as
  `Timestamp(Nanosecond)` by default — the only SQL-surface form that pins an explicit arrow type.

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
  `literal_timestamp_utc` → `arrow_cast('YYYY-MM-DD HH:MI:SS', 'Timestamp(Microsecond, Some("+00:00"))')`
* *AND* both timestamp forms SHALL carry an explicit `Timestamp(Microsecond, …)` target type — never the bare `TIMESTAMP '…'` form, which DataFusion's SQL frontend parses as `Timestamp(Nanosecond)` by default
* *AND* the translator SHALL single-quote the timestamp value and escape internal single-quotes by doubling, exactly as for `literal_string`, so no literal value produces an SQL injection vector

### Scenario: Far-future timestamp literals survive DataFusion optimization against a microsecond column

* *GIVEN* a `literal_timestamp` or `literal_timestamp_utc` node whose value is a far-future instant outside the `Timestamp(Nanosecond)` range — for example `9999-12-31 23:59:59`, above the nanosecond maximum `2262-04-11T23:47:16`
* *AND* a DataFusion table whose comparison column is typed `Timestamp(Microsecond, None)` for `literal_timestamp`, or `Timestamp(Microsecond, Some("UTC"))` for `literal_timestamp_utc`
* *WHEN* the fragment rendered by `render_expression` is placed in a predicate or CASE branch against that column and the query is driven through DataFusion's `simplify_expressions` optimizer rule
* *THEN* the optimizer SHALL complete without a `Cast error: Overflow converting … to Nanosecond`
* *AND* the rendered fragment MUST NOT introduce a `Timestamp(Nanosecond)` intermediate during type unification with the microsecond column
