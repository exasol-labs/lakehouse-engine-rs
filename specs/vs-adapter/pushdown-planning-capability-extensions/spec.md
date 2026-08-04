# Feature: Pushdown Planning — Capability Extensions

Extends pushdown planning (`vs-adapter/pushdown-planning`) with the getCapabilities-level
capability advertisements for scalar and type-conversion functions the adapter has added
since the base feature: arithmetic operator scalar functions, CAST/unary-negation, and ISO
week — plus the capabilities that were considered and deliberately kept absent (regexp
scalar functions, bitwise operator functions). Each advertised capability is gated on a
`crates/vs-expression` translator arm that renders it faithfully; each absent capability
records why no faithful translation exists. Ordered-sort-key capability advertisement
(`ORDER_BY_COLUMN` / `ORDER_BY_EXPRESSION`) lives in its own sibling feature,
`vs-adapter/pushdown-planning-order-by-capability`. Related capability-driven extensions —
scalar select-list expression pushdown, HAVING pushdown, statistical aggregates, and literal
projection — live in their own sibling features too (see the "See also" note at the end of
the Background).

## Background

* A scalar-function capability is advertised only once a `crates/vs-expression` arm renders it and
  the DataFusion 54 result matches Exasol. `FN_CAST`, `FN_NEG`, and `FN_WEEK` meet this bar;
  `FN_DIV`, `FN_TO_CHAR`, `FN_TO_NUMBER`, the regexp scalar functions, the divergent date
  functions, and the bitwise operator functions do not and stay unadvertised.
* Credentials MUST NOT appear in any returned SQL or error message.
* Iceberg spec compliance: checked, not engaged. Verified against the Apache Iceberg table
  spec (https://iceberg.apache.org/spec/) rather than from memory: the normative sections
  that could bear on this change are the ones governing what a reader must resolve —
  schema/field-id resolution ("Schemas and Data Types", "Column Projection") and scan
  planning ("Scan Planning", manifest/partition filtering). This feature touches none of
  them: it changes only which scalar/type-conversion capabilities the adapter advertises,
  reading no manifest and resolving no snapshot, field id, delete, or type mapping. No
  normative requirement applies, so there is no deviation to fix and none to track.
* See also: ordered-sort-key capability advertisement (`ORDER_BY_COLUMN` /
  `ORDER_BY_EXPRESSION`) lives in `vs-adapter/pushdown-planning-order-by-capability`;
  scalar/boolean select-list expression pushdown and widened-projection routing live in
  `vs-adapter/pushdown-planning-selectlist-expressions`; HAVING pushdown and statistical
  aggregates live in `vs-adapter/pushdown-planning-aggregate-extensions`; literal/constant
  select-list projection lives in `vs-adapter/pushdown-planning-literal-projection`.
* **A capability is withdrawn when the scan cannot evaluate the function faithfully, not only when the translator cannot render it.** The four now-family names — `CURRENT_DATE`, `SYSDATE`, `CURRENT_TIMESTAMP`, `SYSTIMESTAMP` — render as valid SQL in both dialects today, yet the node-local scan cannot produce Exasol's value for any of them. Exasol's four names are three distinct semantics over one instant: `CURRENT_TIMESTAMP` interprets it in the session time zone (`TIMESTAMP(3) WITH LOCAL TIME ZONE`), `SYSTIMESTAMP` interprets the same instant in the database time zone (`TIMESTAMP(3)`), and `CURRENT_DATE`/`SYSDATE` are `TO_DATE` of each. Rendering that distinction needs `SESSIONTIMEZONE` and `DBTIMEZONE`. Neither value reaches the scan UDF: the pushdown request carries no zone, `CommonScanSpec` carries no temporal field, the scan script declares only the common blob and the per-file list, the scan opens no connect-back session, and the SDK's `UdfContext` exposes no clock and no zone. The scan therefore reads its own container clock in UTC. It also reads that clock once per shard — the fan-out builds and drops a `SessionContext` per invocation — so a pushed clock call is evaluated G times with no statement anchor, while Exasol's now-family is statement-constant. Withdrawal is the correctness fix: Exasol never delegates a capability the adapter does not advertise, so Exasol evaluates its own clock, once, in its own zones. All three claims were measured against live Exasol 2025.2.1 rather than inferred from the advertised capability set: `EXPLAIN VIRTUAL` over a select-list `SYSTIMESTAMP` pushes `"projection":[{"expr":"now()"}, …]` with `"emit_exa_types":["TIMESTAMP(3)", …]`, and a filter-position `CURRENT_TIMESTAMP` pushes `"filter":"(now() < \"EVENT_TS\")"`, so the node is genuinely delegated; the same select returned `15:02:02.716` through the virtual schema against `17:02:03.141` from Exasol in one session, with `DBTIMEZONE` and `SESSIONTIMEZONE` both `EUROPE/BERLIN` over a UTC container clock; and `GROUP BY SYSTIMESTAMP` over a two-file table returned two distinct timestamps against one statement-constant native value. A pure-constant predicate is not a valid probe, because Exasol constant-folds it before building the pushdown request.
* **Withdrawing a capability is the safe direction; advertising without a backing path is the unsafe one.** An unadvertised function is never delegated, so Exasol keeps it and evaluates it over the returned rows (`docs/capabilities.md` § Handled by Exasol). Advertising a capability the adapter cannot honour is what produces silent wrong answers — verified live for `ORDER_BY_EXPRESSION` with no backing path (see `vs-adapter/pushdown-planning-order-by-capability`). The now-family withdrawal moves these four names from the delegated side to the Exasol-evaluated side, so it cannot lose or mistranslate a clause.
* **Fixing `#218`/`#238` (row-scan decline routing).** A select-list item the scan UDF cannot
  emit — its declared `selectListDataTypes` type is `TIMESTAMP WITH LOCAL TIME ZONE`, which
  Exasol rejects as a UDF EMITS output type (sqlCode 22002) — used to decline to the
  full-base-row projection. That decline is an INVALID pushdown response, not a
  correct-but-unaccelerated one: Exasol validates the response POSITIONALLY against the
  request's `selectList`, so a full-row response to an N-item select list is rejected with SQL
  state `04000` "Expected number of columns is N but pushdown query has M" and the query FAILS.
  Verified on the live E2E container (Exasol 2025.2.1):
  `SELECT CURRENT_TIMESTAMP FROM <vs_table> WHERE ID = 1` →
  "Expected number of columns is 1 but pushdown query has 5"; likewise
  `SELECT CAST(TIMESTAMP '2024-03-01 10:00:00' AS TIMESTAMP WITH LOCAL TIME ZONE) FROM <vs_table>`,
  and likewise at every item position. The repair — routing to the qualified single-table
  wrapper (`qualified_single_table_fallback_pushdown`, the join N-scan fallback at N = 1)
  already used by `pushdown-planning-grouped-agg` and `pushdown-planning-count-distinct` —
  already exists and is already specified: `SELECT <select list rendered as Exasol SQL> FROM
  (<sharded raw fan-out narrowed to referenced columns>) AS "LHS_T0"`. The row-scan path was the
  one decline path that never adopted it; this fix adopts it there too. Substituting plain
  `TIMESTAMP` for the declared type is not an alternative: Exasol returns a
  `TIMESTAMP WITH LOCAL TIME ZONE` value converted into the SESSION time zone while plain
  `TIMESTAMP` is returned verbatim, so the emitted UTC instant would surface as the UTC wall
  clock.
* **Reconciled against `main` at implementation time (2026-08-03).** This fix was drafted
  assuming `FN_CURRENT_TIMESTAMP`/`FN_SYSTIMESTAMP`/`FN_CURRENT_DATE`/`FN_SYSDATE` were still
  advertised capabilities, reachable by the adapter as `function_scalar` items and requiring a
  session-context routing rule to catch. Between drafting and implementation, the independent
  `fix-vs-expression-dialect` plan (PR #258, see the "Now-family date/time capabilities are
  withdrawn" scenario above) withdrew all four capabilities from the advertised set entirely,
  so Exasol never delegates any of them — in a filter or a select list — and evaluates its own
  clock instead. That independently resolves the SYSTIMESTAMP-ships-UTC defect (`#238`) and the
  filter-side divergence (`#239`) as a side effect, verified live during this implementation.
  Both issues stay open only for bookkeeping — closed by this fix's PR alongside `#218`, with
  the withdrawal cited as the actual fix. This fix's own scope therefore narrows to the
  `TIMESTAMP WITH LOCAL TIME ZONE`-declared item (the literal/constant case, `#218` proper).
* `specs/parallelism/work-unit-sharding` and `specs/vs-adapter/pushdown-planning` forbid
  wrapping the outer scalar select in a `SELECT * FROM (...)` materialization boundary. That
  `MUST NOT` governs the streaming row-scan HAPPY path and is textually scoped to the star
  form; it does not reach a non-star decline wrapper, which three recorded specs already emit
  (`pushdown-planning-grouped-agg`, `pushdown-planning-count-distinct`,
  `pushdown-planning-single-group-agg`). This fix keeps the happy path star-free and
  wrapper-free.
* The routing decision is a REASON-based predicate over the request, not a comparison of the
  `selectList` length against the projection length. An arity comparison also fires on the
  absent, empty, and non-array `selectList` arms, where the full base row IS the correct
  response; a reason-based predicate cannot regress those.
* **Scope: the SINGLE-TABLE row-scan path only.** The broadcast-join fast path reaches
  `project_columns` through `extract_join_projection` and splices its projection into the
  broadcast join SQL with no equivalent check, so a join query with a non-emittable
  select-list item still emits the invalid response and still fails with `04000`. That is a
  known, deliberately unfixed gap, tracked as `(#231)`, which documents the identical
  mechanism and its fix shape (fall through to `build_n_scan_join_sql`) for the join path.
* A select-list item the translator cannot render in the EXASOL dialect at all is NOT fixed
  by this. `CAST(<column> AS TIMESTAMP WITH LOCAL TIME ZONE)` is the live example:
  `render_cast_target` declines `withLocalTimeZone: true` in BOTH dialects, and
  `specs/sql-comprehension/vs-expression-translator-cast/spec.md` records that decline
  normatively. Such an item is still routed away from the invalid full-row response — its
  declared type is `TIMESTAMP WITH LOCAL TIME ZONE`, so the classifier catches it — and the
  wrapper's renderer then fails with a named hard error instead of a misleading column-count
  `04000`. A clearer failure, not a fix; the same underlying `04000` `(#218)` already covers.

## Scenarios

### Scenario: Arithmetic operator scalar-function capabilities are advertised so arithmetic expression trees are pushed down

* *GIVEN* the adapter's advertised capability set
* *WHEN* Exasol requests `getCapabilities`
* *THEN* the response SHALL advertise the arithmetic binary-operator scalar-function capabilities for `+`, `-`, `*`, and `/` (the Exasol capability names `FN_ADD`, `FN_SUB`, `FN_MULT`, and `FN_FLOAT_DIV`), verified against the Exasol Virtual Schema capability vocabulary
* *AND* each advertised arithmetic operator SHALL be backed by a `crates/vs-expression` translator arm that renders it (so an advertised operator is never one the translator would decline), keeping the capability set coherent with the translator the same way GROUP BY tuple advertisement is gated on its backing path
* *AND* Cartesian-product capabilities SHALL remain absent, and only the inner equi-join capabilities (`JOIN`/`JOIN_TYPE_INNER`/`JOIN_CONDITION_EQUI`, see `vs-adapter/pushdown-planning-join`) SHALL be advertised — advertising arithmetic operators MUST NOT introduce any additional join or cross-join capability

### Scenario: An advertised arithmetic expression Exasol cannot see decomposed remains correctness-safe

* *GIVEN* the adapter advertises the arithmetic operator capabilities and Exasol pushes an arithmetic expression tree in a filter, select-list, group-key, or aggregate-argument position
* *WHEN* the `crates/vs-expression` translator cannot render a particular arithmetic node (e.g. an operator or operand shape it does not handle)
* *THEN* the adapter SHALL fall back on the affected clause exactly as for any other untranslatable expression — a filter is omitted and retained by Exasol, a select-list expression falls back to projecting underlying columns, and an aggregate over the unrenderable argument falls back to row scanning
* *AND* the adapter MUST NOT emit a scan spec that would compute a different result than single-node evaluation

### Scenario: Conversion and unary-negation capabilities are advertised so CAST and unary-minus expressions push down

* *GIVEN* the adapter's advertised capability set
* *WHEN* Exasol requests `getCapabilities`
* *THEN* the response SHALL advertise `FN_CAST` and `FN_NEG`, each backed by a `crates/vs-expression` translator arm (the CAST arm over its faithful target-type set and the unary-negation arm), so no advertised capability is one the translator would decline for a shape Exasol expects it to handle
* *AND* a CAST to an unsupported target type SHALL fall back — the adapter omits the CAST and Exasol evaluates it — rather than producing an incorrect result
* *AND* `FN_TO_CHAR`, `FN_TO_NUMBER`, and `FN_DIV` SHALL remain absent
* *AND* Cartesian-product capabilities SHALL remain absent and only the inner equi-join capabilities (`JOIN`/`JOIN_TYPE_INNER`/`JOIN_CONDITION_EQUI`, see `vs-adapter/pushdown-planning-join`) SHALL be advertised, so advertising `FN_CAST` and `FN_NEG` introduces no additional join or cross-join capability

### Scenario: ISO week capability is advertised so WEEK expressions push down

* *GIVEN* the adapter's advertised capability set
* *WHEN* Exasol requests `getCapabilities`
* *THEN* the response SHALL advertise `FN_WEEK`, backed by the `crates/vs-expression` `WEEK` arm rendering `date_part('week', …)`, whose ISO-8601 result matches Exasol `WEEK` (see `sql-comprehension/vs-expression-translator-date-fns`)
* *AND* `FN_ADD_DAYS`, `FN_ADD_HOURS`, `FN_ADD_MINUTES`, `FN_ADD_SECONDS`, `FN_ADD_WEEKS`, `FN_ADD_MONTHS`, `FN_ADD_YEARS`, `FN_DAYS_BETWEEN`, `FN_HOURS_BETWEEN`, `FN_MINUTES_BETWEEN`, `FN_SECONDS_BETWEEN`, `FN_MONTHS_BETWEEN`, `FN_YEARS_BETWEEN`, `FN_DAYOFWEEK`, `FN_LAST_DAY`, and `FN_CONVERT_TZ` SHALL remain absent
* *AND* Cartesian-product capabilities SHALL remain absent and only the inner equi-join capabilities (`JOIN`/`JOIN_TYPE_INNER`/`JOIN_CONDITION_EQUI`) SHALL be advertised, so advertising `FN_WEEK` introduces no additional join or cross-join capability

### Scenario: Regexp scalar function capabilities remain absent

* *GIVEN* the adapter's advertised capability set
* *WHEN* Exasol requests `getCapabilities`
* *THEN* the response SHALL NOT advertise `FN_REGEXP_REPLACE`, `FN_REGEXP_SUBSTR`, `FN_REGEXP_INSTR`, or `FN_REGEXP_COUNT`
* *AND* Exasol SHALL post-process regexp scalar functions rather than pushing them to the node-local scan, because at the pinned DataFusion 54.0.0 and `regex` 1.12.4 the Rust `regex` dialect rejects the backreferences and lookaround Exasol's PCRE dialect accepts, DataFusion has no `regexp_substr`, and its `regexp_replace`/`regexp_instr`/`regexp_count` argument shapes omit Exasol's position, occurrence, and return-option arguments — a compile-time literal-pattern check cannot certify semantic match parity, so no faithful translation exists (see issue #106 and `sql-comprehension/vs-expression-translator-scalar-fns`)
* *AND* the pre-existing `FN_PRED_REGEXP_LIKE` predicate advertisement SHALL remain unchanged

### Scenario: Bitwise operator function capabilities remain absent

* *GIVEN* the adapter's advertised capability set
* *WHEN* Exasol requests `getCapabilities`
* *THEN* the response SHALL NOT advertise `FN_BIT_AND`, `FN_BIT_OR`, `FN_BIT_XOR`, `FN_BIT_NOT`, `FN_BIT_LSHIFT`, `FN_BIT_RSHIFT`, `FN_BIT_LROTATE`, `FN_BIT_RROTATE`, `FN_BIT_CHECK`, `FN_BIT_SET`, or `FN_BIT_TO_NUM`
* *AND* Exasol SHALL post-process bitwise operator functions rather than pushing them to the node-local scan, because at pinned DataFusion 54.0.0 no faithful translation exists for any of the eleven over Exasol's bit-function domain (issue #108): Exasol bit functions operate on unsigned 64-bit integers (range `0`–`18446744073709551615`, result `DECIMAL(20,0)`), while DataFusion's `&`/`|`/`#`/`<<`/`>>` act on the operand's signed Arrow integer type — Iceberg sources carry only signed integers (`int` = 32-bit signed, `long` = 64-bit signed; the Iceberg spec defines no unsigned integer primitive), so a bit-63-set result is a large positive value in Exasol but negative under signed `Int64` and the `Int64` → `DECIMAL(20,0)` mapping carries the negative value; `BIT_RSHIFT` diverges unconditionally on any bit-63-set operand because DataFusion's signed `>>` is arithmetic (sign-extending) whereas Exasol's is logical (zero-fill); and DataFusion 54.0.0 provides no operator or scalar function at all for `BIT_NOT` (its SQL planner rejects unary `~`), `BIT_LROTATE`, `BIT_RROTATE`, `BIT_CHECK`, `BIT_SET`, or `BIT_TO_NUM`
* *AND* `FN_BIT_LENGTH` SHALL be treated as out of scope for this decision — it is an Exasol string function (bit count of a string), not a bitwise operator — and only the inner equi-join capabilities (`JOIN`/`JOIN_TYPE_INNER`/`JOIN_CONDITION_EQUI`) SHALL be advertised, so this decision introduces no additional join, cross-join, or string-function capability change

### Scenario: Now-family date/time capabilities are withdrawn so Exasol evaluates its own clock

* *GIVEN* the adapter's advertised capability set
* *WHEN* Exasol requests `getCapabilities`
* *THEN* the response SHALL NOT advertise `FN_CURRENT_DATE`, `FN_CURRENT_TIMESTAMP`, `FN_SYSDATE`, or `FN_SYSTIMESTAMP`
* *AND* Exasol SHALL evaluate the now-family natively rather than pushing it to the node-local scan, because no time zone, clock, or statement anchor reaches the scan UDF, so the scan can only read its own container clock in UTC, independently per shard — see the Background and `sql-comprehension/vs-expression-translator-date-fns`
* *AND* the four names SHALL be declined by the expression translator in BOTH dialects with the `unsupported scalar function: <name>` error, keeping the capability set and the translator coherent the same way the regexp, bitwise, and `ADD_*` date-arithmetic withdrawals do
* *AND* the withdrawal SHALL NOT alter any other advertised capability — `FN_DATE_TRUNC`, `FN_EXTRACT`, the field shortcuts (`FN_DAY`, `FN_HOUR`, `FN_MINUTE`, `FN_MONTH`, `FN_SECOND`, `FN_YEAR`, `FN_WEEK`), `FN_TO_DATE`, `FN_TO_TIMESTAMP`, and the `*_BETWEEN` family SHALL all remain advertised because each takes its datetime from its own arguments rather than from a clock — and Cartesian-product capabilities SHALL remain absent with only the inner equi-join capabilities (`JOIN`/`JOIN_TYPE_INNER`/`JOIN_CONDITION_EQUI`) advertised, so this withdrawal introduces no join or cross-join capability change
* *AND* `docs/capabilities.md` SHALL NOT list the four withdrawn capabilities in its pushed-down scalar-function table, so the operator-facing documentation cannot claim a pushdown the adapter no longer advertises

### Scenario: Projected constant whose declared EMITS type Exasol rejects routes to the qualified wrapper

* *GIVEN* a row-scan `pushdown` request whose select list contains an item the scan UDF cannot emit — an item whose declared result type in `selectListDataTypes` is `TIMESTAMP WITH LOCAL TIME ZONE` (a `literal_timestamputc`/`literal_timestamp_utc` constant, which Exasol types `TIMESTAMP(3) WITH LOCAL TIME ZONE`); the now-family session-dependent functions this scenario originally also named (`FN_CURRENT_TIMESTAMP`/`FN_SYSTIMESTAMP`/`FN_CURRENT_DATE`/`FN_SYSDATE`) are withdrawn from the advertised capability set by the independent `fix-vs-expression-dialect` plan and can no longer reach this classifier at all — see the Background note
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL classify that item as not emittable by the scan (already true today, via `project_columns`'s `needs_full_fallback`/`projection_widened` signal) and SHALL route the whole request to the qualified single-table wrapper — the same `qualified_single_table_fallback_pushdown` shape the grouped-aggregate and multi-`COUNT(DISTINCT)` declines already use — so the response is `SELECT <every select-list item rendered as Exasol SQL> FROM (<sharded raw fan-out narrowed to the referenced columns>) AS "LHS_T0"`
* *AND* the adapter MUST NOT respond with the full-base-row projection for such a request, because Exasol validates the response positionally against the request's `selectList` and rejects a column-count mismatch with SQL state `04000` "Expected number of columns is N but pushdown query has M" — a hard query failure, NOT a correct-but-unaccelerated result
* *AND* the wrapper's select list SHALL be rendered in the EXASOL dialect and SHALL reproduce each item's declared type exactly, so Exasol's positional type check passes: a `literal_timestamputc`/`literal_timestamp_utc` item declared `TIMESTAMP WITH LOCAL TIME ZONE` renders as `CAST(CONVERT_TZ(TIMESTAMP '<utc value>', 'UTC', SESSIONTIMEZONE) AS TIMESTAMP WITH LOCAL TIME ZONE)` — `SESSIONTIMEZONE` referenced SYMBOLICALLY so Exasol evaluates it in the CALLER's session, which the adapter cannot read; this rendering lives in the SHARED `vs-expression` literal-rendering arm (`render_exasol_tstz_literal`), used identically by both this SELECT-LIST context and the declined-WHERE-filter self-apply path (`render_self_applied_where`) — a live-verified correction from an initial attempt, which rendered the filter path bare and was found to disagree with Exasol's native `TIMESTAMP`-vs-`TSTZ` coercion rule (Exasol reads a naive comparand as session-local, not as a raw value); converting into the caller's session zone before comparison is what reproduces that native rule for the plain-`TIMESTAMP`-mapped Iceberg column on the other side of the comparison (`007-fix-timestamptz-mapping`)
* *AND* the returned value SHALL equal the value Exasol computes natively for the same expression in the same session; the adapter MUST NOT substitute plain `TIMESTAMP` for a declared `TIMESTAMP WITH LOCAL TIME ZONE` EMITS type, which would surface the UTC wall clock where Exasol natively surfaces the session-local wall clock (verified at `SESSIONTIMEZONE = EUROPE/BERLIN`: native `CAST(TIMESTAMP '2024-03-01 10:00:00' AS TIMESTAMP WITH LOCAL TIME ZONE)` is `2024-03-01 10:00:00`, its UTC representation `2024-03-01 09:00:00`). This decline SHALL NOT be conflated with `datafusion-scan/type-mapping`'s "Iceberg timestamptz maps to plain Exasol TIMESTAMP" scenario (decision `007-fix-timestamptz-mapping`): there the VS DECLARES the column plain `TIMESTAMP` at `createVirtualSchema`, so Exasol makes no localization promise and the UTC wall clock is the contract; here Exasol has ALREADY inferred the type for the select-list item independently of the adapter's schema, so a localization promise exists
* *AND* the SAME classification SHALL route the ZERO-FILE path: when file resolution prunes every file the adapter short-circuits to the empty-result response BEFORE the dispatcher runs, so that path SHALL emit a zero-row result whose columns are typed from `selectListDataTypes` in select-list order (`CAST(NULL AS <declared type>)`, the shape the grouped qualified-wrapper decline already emits) and MUST NOT emit the full-base-row empty projection, whose column count trips the same `04000` — this routing (`file_resolution.rs`'s `RequestShape::RowScan if projection_widened` arm calling `empty_select_list_typed_sql`) already reached `main` via the same widening mechanism as the non-empty path by implementation time; this scenario pins it as a regression guard rather than introducing it. `CAST(NULL AS TIMESTAMP WITH LOCAL TIME ZONE)` is valid Exasol SQL, so the routed empty shape is buildable for a TSTZ-declared item
* *AND* the classification SHALL NOT fire where the full base row is the CORRECT response — an absent, `null`, empty, or non-array `selectList` — and SHALL ignore a bare `column` select-list item, whose EMITS type comes from `involvedTables` rather than from `selectListDataTypes`
* *AND* an item the translator cannot render in the Exasol dialect at all SHALL remain a hard error from the wrapper's existing renderer, never a silent wrong value; a select-list item whose declared type is `CHAR(n)` remains a distinct, separately tracked positional TYPE-mismatch failure on the narrowed-projection path, `(#240)`

### Scenario: Projected literal or constant select-list item is pushed down as a positional projection

* *GIVEN* a row-scan `pushdown` request whose select list contains one or more literal/constant items — `literal_exactnumeric`, `literal_string`, `literal_bool`, `literal_double`, `literal_date`, `literal_timestamp`, or `literal_null` — each with a parallel `selectListDataTypes` entry
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL render each literal select-list item through the `crates/vs-expression` translator into a POSITIONAL `Expr` projection item — one projection item per select-list item, typed from the parallel top-level `selectListDataTypes` array — exactly as the `function_scalar` select-list branch already does, and MUST NOT trigger the full-base-row fallback that emits every base column and yields the column-count mismatch Exasol rejects ("Expected number of columns is 1 but pushdown query has N", issues #190 and #205)
* *AND* the emitted scan's column arity SHALL equal the query's select-list arity, so two structurally identical literal items — such as the two `1` items in `SELECT 1, name, 1` — SHALL each occupy their own projected position and MUST NOT be collapsed into one
* *AND* each projected literal SHALL be evaluated once per scanned source row, so `SELECT <literal> FROM t` returns one constant-valued row per source table row, and the synthesized `literal_null` item behind a LIMIT barrier SHALL emit one single-column row per admitted row so the outer `COUNT(*)` counts exactly the rows the inner LIMIT admits (issue #205)
* *AND* an item the translator cannot render, or one the scan cannot emit (an EMITS-invalid declared type, e.g. `TIMESTAMP WITH LOCAL TIME ZONE`), SHALL route the request to the qualified single-table wrapper so Exasol evaluates the select list over a narrowed materialized scan — and SHALL NOT fall back to the full base row, which is an INVALID pushdown response for a delegated select list, not a correctness backstop

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
