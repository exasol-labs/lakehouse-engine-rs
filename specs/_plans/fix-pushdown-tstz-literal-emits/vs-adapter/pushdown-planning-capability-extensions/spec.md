# Feature: Pushdown Planning — Capability Extensions

Extends pushdown planning (`vs-adapter/pushdown-planning`) with the newly advertised
capabilities: scalar select-list expression pushdown, HAVING clause pushdown, and
decomposable statistical aggregate pushdown via sufficient statistics. Each extends the
translator or aggregate planner with a shard-associative partial/merge path.

## Background

* This delta rewrites TWO scenarios and adds one. It changes the row-scan decline
  behavior for a select-list item the scan UDF cannot emit: from the full-base-row
  projection (an INVALID pushdown response) to the qualified single-table wrapper
  (a valid one). Every other capability-extensions scenario is unchanged.
* **The premise the previous text rested on is false.** The full-base-row fallback
  is NOT a "correct but unaccelerated" backstop for a request whose select list
  Exasol delegated. Exasol validates the pushdown response POSITIONALLY against the
  request's `selectList`, so a full-row response to an N-item select list is rejected
  with SQL state `04000` "Expected number of columns is N but pushdown query has M"
  and the user's query FAILS. Verified on the live E2E container (Exasol 2025.2.1):
  `SELECT CURRENT_TIMESTAMP FROM <vs_table> WHERE ID = 1` →
  "Expected number of columns is 1 but pushdown query has 5"; likewise
  `SELECT CAST(TIMESTAMP '2024-03-01 10:00:00' AS TIMESTAMP WITH LOCAL TIME ZONE) FROM <vs_table>`,
  and likewise at every item position (`SELECT ID, CURRENT_TIMESTAMP …` and
  `SELECT CURRENT_TIMESTAMP, ID …` both → "expected 2 … has 5"). The recorded
  literal-projection scenario already names this failure mode for issues #190/#205;
  its own closing clause and the decline scenario contradicted it.
* The repair mechanism already exists and is already specified. `pushdown-planning-grouped-agg`
  and `pushdown-planning-count-distinct` route their declines to the **qualified
  single-table wrapper** (`qualified_single_table_fallback_pushdown`, the join N-scan
  fallback at N = 1): `SELECT <select list rendered as Exasol SQL> FROM (<sharded raw
  fan-out narrowed to referenced columns>) AS "LHS_T0"`. Its contract is exactly what is
  needed here — "the result column count and per-column types match Exasol's positional
  `selectListDataTypes` validation, so this never emits the `04000`-triggering bare row
  scan". The row-scan path is the one decline path that never adopted it.
* The scalar scan UDF's EMITS column types come from `exasol_type_from_json` over each
  select-list item's `selectListDataTypes` descriptor. `withLocalTimeZone: true` yields
  `TIMESTAMP WITH LOCAL TIME ZONE`, which Exasol rejects as a UDF EMITS output type
  (sqlCode 22002). That rejection is real; only the response to it changes.
* Substituting plain `TIMESTAMP` for the declared type is not an alternative: Exasol
  returns a `TIMESTAMP WITH LOCAL TIME ZONE` value converted into the SESSION time zone
  while plain `TIMESTAMP` is returned verbatim, so the emitted UTC instant would surface
  as the UTC wall clock. Exasol's positional check also rejects the substituted type
  outright (verified for the analogous `CHAR`/`VARCHAR` case, `(#240)`).
* The scan UDF cannot evaluate a session-context-dependent expression correctly at all,
  whatever its declared type: it has no access to the user session's `SESSIONTIMEZONE`
  (connect-back opens an INDEPENDENT session). A projected `SYSTIMESTAMP` is declared
  plain `TIMESTAMP(3)`, passes the EMITS-type gate today, and therefore ships the UTC
  wall clock — verified `16:32:33.665` against a native `18:32:34.061` under CEST
  (`(#238)`, fixed by this delta). The filter-side instance of the same rendering arm is
  a separate, deliberately unfixed tracked exception, `(#239)`.
* `specs/parallelism/work-unit-sharding` and `specs/vs-adapter/pushdown-planning` forbid
  wrapping the outer scalar select in a `SELECT * FROM (...)` materialization boundary.
  That `MUST NOT` governs the streaming row-scan HAPPY path and is textually scoped to
  the star form; it does not reach a non-star decline wrapper, which three recorded specs
  already emit (`pushdown-planning-grouped-agg`, `pushdown-planning-count-distinct`,
  `pushdown-planning-single-group-agg`). This delta keeps the happy path star-free and
  wrapper-free.
* The routing decision is a REASON-based predicate over the request, not a comparison of
  the `selectList` length against the projection length. An arity comparison also fires on
  the absent, empty, and non-array `selectList` arms, where the full base row IS the correct
  response; a reason-based predicate cannot regress those.
* **Scope of this delta: the SINGLE-TABLE row-scan path only.** The broadcast-join fast path
  reaches `project_columns` through `extract_join_projection` and splices its projection
  into the broadcast join SQL with no equivalent check, so a join query with a
  non-emittable select-list item still emits the invalid response and still fails with
  `04000`. That is a known, deliberately unfixed gap, tracked as `(#231)`, which documents
  the identical mechanism and its fix shape (fall through to `build_n_scan_join_sql`) for
  the join path. This delta leaves `project_columns`' signature and behavior unchanged
  precisely so the join path keeps today's behavior rather than acquiring a half-migrated one.
* A select-list item the translator cannot render in the EXASOL dialect at all is NOT fixed
  by this delta. `CAST(<column> AS TIMESTAMP WITH LOCAL TIME ZONE)` is the live example:
  `render_cast_target` declines `withLocalTimeZone: true` in BOTH dialects, and
  `specs/sql-comprehension/vs-expression-translator-cast/spec.md` records that decline
  normatively. Such an item is still routed away from the invalid full-row response — its
  declared type is `TIMESTAMP WITH LOCAL TIME ZONE`, so the classifier catches it — and the
  wrapper's renderer then fails with a named hard error instead of a misleading column-count
  `04000`. A clearer failure, not a fix; the same underlying `04000` `(#218)` already covers.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Projected constant whose declared EMITS type Exasol rejects routes to the qualified wrapper

* *GIVEN* a row-scan `pushdown` request whose select list contains an item the scan UDF cannot emit — an item whose declared result type in `selectListDataTypes` is `TIMESTAMP WITH LOCAL TIME ZONE` (a `literal_timestamp_utc` constant, or an advertised `FN_CURRENT_TIMESTAMP` item, which Exasol types `TIMESTAMP(3) WITH LOCAL TIME ZONE`), OR an item whose value depends on the user session's time zone whatever its declared type (an advertised `FN_SYSTIMESTAMP`, `FN_CURRENT_DATE`, or `FN_SYSDATE` item, which Exasol types plain `TIMESTAMP(3)`/`DATE`)
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL classify that item as not emittable by the scan and SHALL route the whole request to the qualified single-table wrapper — the same `qualified_single_table_fallback_pushdown` shape the grouped-aggregate and multi-`COUNT(DISTINCT)` declines already use — so the response is `SELECT <every select-list item rendered as Exasol SQL> FROM (<sharded raw fan-out narrowed to the referenced columns>) AS "LHS_T0"`
* *AND* the adapter MUST NOT respond with the full-base-row projection for such a request, because Exasol validates the response positionally against the request's `selectList` and rejects a column-count mismatch with SQL state `04000` "Expected number of columns is N but pushdown query has M" — a hard query failure, NOT a correct-but-unaccelerated result
* *AND* the wrapper's select list SHALL be rendered in the EXASOL dialect and SHALL reproduce each item's declared type exactly, so Exasol's positional type check passes: an `FN_CURRENT_TIMESTAMP` item renders as `CURRENT_TIMESTAMP` (never DataFusion's `now()`), and a `literal_timestamp_utc` item renders as `CAST(CONVERT_TZ(TIMESTAMP '<utc value>', 'UTC', SESSIONTIMEZONE) AS TIMESTAMP WITH LOCAL TIME ZONE)` — `SESSIONTIMEZONE` referenced SYMBOLICALLY so Exasol evaluates it in the CALLER's session, which the adapter cannot read
* *AND* the returned value SHALL equal the value Exasol computes natively for the same expression in the same session; the adapter MUST NOT substitute plain `TIMESTAMP` for a declared `TIMESTAMP WITH LOCAL TIME ZONE` EMITS type, which would surface the UTC wall clock where Exasol natively surfaces the session-local wall clock (verified at `SESSIONTIMEZONE = EUROPE/BERLIN`: native `CAST(TIMESTAMP '2024-03-01 10:00:00' AS TIMESTAMP WITH LOCAL TIME ZONE)` is `2024-03-01 10:00:00`, its UTC representation `2024-03-01 09:00:00`). This decline SHALL NOT be conflated with `datafusion-scan/type-mapping`'s "Iceberg timestamptz maps to plain Exasol TIMESTAMP" scenario (decision `007-fix-timestamptz-mapping`): there the VS DECLARES the column plain `TIMESTAMP` at `createVirtualSchema`, so Exasol makes no localization promise and the UTC wall clock is the contract; here Exasol has ALREADY inferred the type for the select-list item independently of the adapter's schema, so a localization promise exists
* *AND* the SAME classification SHALL route the ZERO-FILE path: when file resolution prunes every file the adapter short-circuits to the empty-result response BEFORE the dispatcher runs, so that path SHALL emit a zero-row result whose columns are typed from `selectListDataTypes` in select-list order (`CAST(NULL AS <declared type>)`, the shape the grouped qualified-wrapper decline already emits) and MUST NOT emit the full-base-row empty projection, whose column count trips the same `04000` — verified live, `SELECT CURRENT_TIMESTAMP FROM <vs_table> WHERE ID = 999999` fails today for exactly this reason. `CAST(NULL AS TIMESTAMP WITH LOCAL TIME ZONE)` is valid Exasol SQL, so the routed empty shape is buildable for a TSTZ-declared item
* *AND* the classification SHALL NOT fire where the full base row is the CORRECT response — an absent, `null`, empty, or non-array `selectList` — and SHALL ignore a bare `column` select-list item, whose EMITS type comes from `involvedTables` rather than from `selectListDataTypes`
* *AND* an item the translator cannot render in the Exasol dialect at all SHALL remain a hard error from the wrapper's existing renderer, never a silent wrong value; a select-list item whose declared type is `CHAR(n)` remains a distinct, separately tracked positional TYPE-mismatch failure on the narrowed-projection path, `(#240)`
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: Projected literal or constant select-list item is pushed down as a positional projection

* *GIVEN* a row-scan `pushdown` request whose select list contains one or more literal/constant items — `literal_exactnumeric`, `literal_string`, `literal_bool`, `literal_double`, `literal_date`, `literal_timestamp`, or `literal_null` — each with a parallel `selectListDataTypes` entry
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL render each literal select-list item through the `crates/vs-expression` translator into a POSITIONAL `Expr` projection item — one projection item per select-list item, typed from the parallel top-level `selectListDataTypes` array — exactly as the `function_scalar` select-list branch already does, and MUST NOT trigger the full-base-row fallback that emits every base column and yields the column-count mismatch Exasol rejects ("Expected number of columns is 1 but pushdown query has N", issues #190 and #205)
* *AND* the emitted scan's column arity SHALL equal the query's select-list arity, so two structurally identical literal items — such as the two `1` items in `SELECT 1, name, 1` — SHALL each occupy their own projected position and MUST NOT be collapsed into one
* *AND* each projected literal SHALL be evaluated once per scanned source row, so `SELECT <literal> FROM t` returns one constant-valued row per source table row, and the synthesized `literal_null` item behind a LIMIT barrier SHALL emit one single-column row per admitted row so the outer `COUNT(*)` counts exactly the rows the inner LIMIT admits (issue #205)
* *AND* an item the translator cannot render, or one the scan cannot emit (an EMITS-invalid declared type or a session-context-dependent value), SHALL route the request to the qualified single-table wrapper so Exasol evaluates the select list over a narrowed materialized scan — and SHALL NOT fall back to the full base row, which is an INVALID pushdown response for a delegated select list, not a correctness backstop
<!-- /DELTA:CHANGED -->

<!-- DELTA:NEW -->
### Scenario: Routed session-dependent projection returns the value Exasol computes natively

* *GIVEN* a virtual-schema table
* *AND* an Exasol session whose `SESSIONTIMEZONE` UTC offset is non-zero
* *WHEN* the test reads `SESSIONTIMEZONE` before asserting any value
* *THEN* the test SHALL set the session time zone explicitly (`ALTER SESSION SET TIME_ZONE = 'EUROPE/BERLIN'`) and SHALL FAIL LOUDLY if the resulting UTC offset is zero, because a zero-offset session makes the value assertion pass whether or not the defect is present
* *AND* `SELECT CURRENT_TIMESTAMP FROM <vs_table> WHERE id = 1` and `SELECT SYSTIMESTAMP FROM <vs_table> WHERE id = 1` SHALL each succeed rather than fail with SQL state `04000`, and each value SHALL equal Exasol's native value for the same expression in the same session within a 60-second tolerance — `SYSTIMESTAMP` included because it is declared plain `TIMESTAMP(3)` and would otherwise pass the EMITS-type gate and ship the UTC wall clock (`(#238)`)
* *AND* each returned value's deviation from its native value SHALL be strictly smaller than the session's UTC offset in seconds, so the assertion FAILS if the adapter is ever changed to emit the UTC instant — the regression guard this scenario exists to provide
* *AND* a projected TSTZ LITERAL SHALL be asserted by EXACT value, which the two moving now-family values cannot provide: `SELECT CAST(TIMESTAMP '2024-03-01 10:00:00' AS TIMESTAMP WITH LOCAL TIME ZONE) FROM <vs_table> WHERE id = 1` SHALL return `2024-03-01 10:00:00`, equal to the same session's native `CAST(CONVERT_TZ(TIMESTAMP '2024-03-01 09:00:00', 'UTC', SESSIONTIMEZONE) AS TIMESTAMP WITH LOCAL TIME ZONE)` — Exasol constant-folds that projected cast into a bare `literal_timestamputc` item carrying the UTC-normalized value, so this asserts the literal path end to end
* *AND* the same queries under an ALL-PRUNING predicate (`WHERE id = 999999`) SHALL each succeed and return zero rows, never SQL state `04000`, so the zero-file short-circuit is covered rather than only the resolved-file dispatcher
* *AND* `EXPLAIN VIRTUAL` for both now-family queries SHALL show the qualified wrapper — a `FROM (…) AS "LHS_T0"` scan whose projection is narrowed to at most the referenced columns — and MUST NOT show a narrowed positional `_LH_PROJ_0` EMITS identifier for the routed item
<!-- /DELTA:NEW -->
