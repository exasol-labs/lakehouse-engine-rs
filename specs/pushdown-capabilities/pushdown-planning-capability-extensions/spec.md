# Feature: Pushdown Planning, Capability Extensions

The adapter advertises the arithmetic operator scalar functions and ISO week in `getCapabilities`, and leaves the bitwise operator functions and the now-family date/time functions unadvertised. It also routes a select-list item the scan cannot emit to the qualified single-table wrapper.

## Background

* A scalar-function capability is advertised only when a `crates/vs-expression` arm renders it and the DataFusion result matches Exasol. `FN_CAST`, `FN_NEG`, and `FN_WEEK` meet this bar. `FN_DIV`, `FN_TO_CHAR`, `FN_TO_NUMBER`, the divergent date functions, and the bitwise operator functions do not.
* Exasol never delegates an unadvertised capability and evaluates it over the returned rows. Advertising a capability the adapter cannot honour returns wrong rows.
* The now-family (`CURRENT_DATE`, `SYSDATE`, `CURRENT_TIMESTAMP`, `SYSTIMESTAMP`) is unadvertised, because the scan UDF receives no time zone, clock, or statement anchor. It would read its own container clock in UTC once per shard, while Exasol's now-family is constant per statement and uses the session or database time zone.
* A select-list item whose declared `selectListDataTypes` type is `TIMESTAMP WITH LOCAL TIME ZONE` cannot be a UDF EMITS type (`sqlCode 22002`). A full-base-row response to such a request fails Exasol's positional check with SQL state `04000` ("Expected number of columns is N but pushdown query has M"). The adapter routes the request to the qualified single-table wrapper (`qualified_single_table_fallback_pushdown`) instead: `SELECT <select list as Exasol SQL> FROM (<sharded raw fan-out narrowed to referenced columns>) AS "LHS_T0"`.
* The routing decision depends on the item's declared type, not on the `selectList` length. An absent, `null`, empty, or non-array `selectList` correctly receives the full base row.
* The scan cannot substitute plain `TIMESTAMP` for `TIMESTAMP WITH LOCAL TIME ZONE`, because Exasol returns the latter in the session time zone and the former verbatim. The `vs-expression` literal arm (`render_exasol_tstz_literal`) renders the value as `CAST(CONVERT_TZ(TIMESTAMP '<utc value>', 'UTC', SESSIONTIMEZONE) AS TIMESTAMP WITH LOCAL TIME ZONE)`, so Exasol evaluates `SESSIONTIMEZONE` in the caller's session.
* Exasol bit functions operate on unsigned 64-bit integers, while DataFusion's operators act on signed Arrow integers, and Iceberg has no unsigned integer type. DataFusion has no operator or function for `BIT_NOT`, `BIT_LROTATE`, `BIT_RROTATE`, `BIT_CHECK`, `BIT_SET`, or `BIT_TO_NUM`.
* The broadcast-join path does not apply this routing.

## Scenarios

### Scenario: Arithmetic operator scalar-function capabilities are advertised so arithmetic expression trees are pushed down

* *GIVEN* the adapter's advertised capability set
* *WHEN* Exasol requests `getCapabilities`
* *THEN* the response SHALL advertise the arithmetic binary-operator scalar-function capabilities for `+`, `-`, `*`, and `/` (the Exasol capability names `FN_ADD`, `FN_SUB`, `FN_MULT`, and `FN_FLOAT_DIV`), verified against the Exasol Virtual Schema capability vocabulary
* *AND* each advertised arithmetic operator SHALL be backed by a `crates/vs-expression` translator arm that renders it (so an advertised operator is never one the translator would decline), keeping the capability set coherent with the translator the same way GROUP BY tuple advertisement is gated on its backing path
* *AND* Cartesian-product capabilities SHALL remain absent, and only the inner equi-join capabilities (`JOIN`/`JOIN_TYPE_INNER`/`JOIN_CONDITION_EQUI`, see `pushdown-joins/pushdown-planning-join`) SHALL be advertised, advertising arithmetic operators MUST NOT introduce any additional join or cross-join capability

### Scenario: An advertised arithmetic expression Exasol cannot see decomposed remains correctness-safe

* *GIVEN* the adapter advertises the arithmetic operator capabilities and Exasol pushes an arithmetic expression tree in a filter, select-list, group-key, or aggregate-argument position
* *WHEN* the `crates/vs-expression` translator cannot render a particular arithmetic node (e.g. an operator or operand shape it does not handle)
* *THEN* the adapter SHALL route the request to the qualified single-table wrapper (`pushdown-aggregates/pushdown-planning-aggregate-extensions`), which renders the affected clause as ordinary Exasol SQL over materialized rows: an untranslatable filter renders as the wrapper's `WHERE`, a select-list expression renders in the wrapper's select list, and an aggregate over the unrenderable argument is computed by the wrapper
* *AND* the adapter MUST NOT omit an untranslatable filter from the returned SQL, because Exasol does not re-apply an advertised capability and omission would return wrong rows, and MUST NOT emit a scan spec that would compute a different result than single-node evaluation

### Scenario: ISO week capability is advertised so WEEK expressions push down

* *GIVEN* the adapter's advertised capability set
* *WHEN* Exasol requests `getCapabilities`
* *THEN* the response SHALL advertise `FN_WEEK`, backed by the `crates/vs-expression` `WEEK` arm rendering `date_part('week', …)`, whose ISO-8601 result matches Exasol `WEEK` (see `sql-functions/vs-expression-translator-date-fns`)
* *AND* `FN_ADD_DAYS`, `FN_ADD_HOURS`, `FN_ADD_MINUTES`, `FN_ADD_SECONDS`, `FN_ADD_WEEKS`, `FN_ADD_MONTHS`, `FN_ADD_YEARS`, `FN_DAYS_BETWEEN`, `FN_HOURS_BETWEEN`, `FN_MINUTES_BETWEEN`, `FN_SECONDS_BETWEEN`, `FN_MONTHS_BETWEEN`, `FN_YEARS_BETWEEN`, `FN_DAYOFWEEK`, `FN_LAST_DAY`, and `FN_CONVERT_TZ` SHALL remain absent
* *AND* Cartesian-product capabilities SHALL remain absent and only the inner equi-join capabilities (`JOIN`/`JOIN_TYPE_INNER`/`JOIN_CONDITION_EQUI`) SHALL be advertised, so advertising `FN_WEEK` introduces no additional join or cross-join capability

### Scenario: Bitwise operator function capabilities remain absent

* *GIVEN* the adapter's advertised capability set
* *WHEN* Exasol requests `getCapabilities`
* *THEN* the response SHALL NOT advertise `FN_BIT_AND`, `FN_BIT_OR`, `FN_BIT_XOR`, `FN_BIT_NOT`, `FN_BIT_LSHIFT`, `FN_BIT_RSHIFT`, `FN_BIT_LROTATE`, `FN_BIT_RROTATE`, `FN_BIT_CHECK`, `FN_BIT_SET`, or `FN_BIT_TO_NUM`
* *AND* Exasol SHALL post-process bitwise operator functions rather than pushing them to the node-local scan, because no faithful translation exists for any of the eleven over Exasol's bit-function domain: Exasol bit functions operate on unsigned 64-bit integers (range `0`–`18446744073709551615`, result `DECIMAL(20,0)`), while DataFusion's `&`/`|`/`#`/`<<`/`>>` act on the operand's signed Arrow integer type, Iceberg sources carry only signed integers (`int` = 32-bit signed, `long` = 64-bit signed; the Iceberg spec defines no unsigned integer primitive), so a bit-63-set result is a large positive value in Exasol but negative under signed `Int64` and the `Int64` → `DECIMAL(20,0)` mapping carries the negative value; `BIT_RSHIFT` diverges unconditionally on any bit-63-set operand because DataFusion's signed `>>` is arithmetic (sign-extending) whereas Exasol's is logical (zero-fill); and DataFusion provides no operator or scalar function at all for `BIT_NOT` (its SQL planner rejects unary `~`), `BIT_LROTATE`, `BIT_RROTATE`, `BIT_CHECK`, `BIT_SET`, or `BIT_TO_NUM`
* *AND* `FN_BIT_LENGTH` SHALL be treated as out of scope for this decision, it is an Exasol string function (bit count of a string), not a bitwise operator, and only the inner equi-join capabilities (`JOIN`/`JOIN_TYPE_INNER`/`JOIN_CONDITION_EQUI`) SHALL be advertised, so this decision introduces no additional join, cross-join, or string-function capability change

### Scenario: Now-family date/time capabilities are withdrawn so Exasol evaluates its own clock

* *GIVEN* the adapter's advertised capability set
* *WHEN* Exasol requests `getCapabilities`
* *THEN* the response SHALL NOT advertise `FN_CURRENT_DATE`, `FN_CURRENT_TIMESTAMP`, `FN_SYSDATE`, or `FN_SYSTIMESTAMP`
* *AND* Exasol SHALL evaluate the now-family natively rather than pushing it to the node-local scan, because no time zone, clock, or statement anchor reaches the scan UDF, so the scan can only read its own container clock in UTC, independently per shard, see the Background and `sql-functions/vs-expression-translator-date-fns`
* *AND* the four names SHALL be declined by the expression translator in BOTH dialects with the `unsupported scalar function: <name>` error, keeping the capability set and the translator coherent the same way the regexp, bitwise, and `ADD_*` date-arithmetic withdrawals do
* *AND* the withdrawal SHALL NOT alter any other advertised capability, `FN_DATE_TRUNC`, `FN_EXTRACT`, the field shortcuts (`FN_DAY`, `FN_HOUR`, `FN_MINUTE`, `FN_MONTH`, `FN_SECOND`, `FN_YEAR`, `FN_WEEK`), `FN_TO_DATE`, `FN_TO_TIMESTAMP`, and the `*_BETWEEN` family SHALL all remain advertised because each takes its datetime from its own arguments rather than from a clock, and Cartesian-product capabilities SHALL remain absent with only the inner equi-join capabilities (`JOIN`/`JOIN_TYPE_INNER`/`JOIN_CONDITION_EQUI`) advertised, so this withdrawal introduces no join or cross-join capability change
* *AND* `docs/capabilities.md` SHALL NOT list the four withdrawn capabilities in its pushed-down scalar-function table, so the operator-facing documentation cannot claim a pushdown the adapter no longer advertises

### Scenario: Projected constant whose declared EMITS type Exasol rejects routes to the qualified wrapper

* *GIVEN* a row-scan `pushdown` request whose select list contains an item the scan UDF cannot emit, an item whose declared result type in `selectListDataTypes` is `TIMESTAMP WITH LOCAL TIME ZONE` (a `literal_timestamputc`/`literal_timestamp_utc` constant, which Exasol types `TIMESTAMP(3) WITH LOCAL TIME ZONE`); the now-family functions are unadvertised and cannot reach this classifier
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL classify that item as not emittable by the scan, through the projection-widening signal of the projection derivation, and SHALL route the whole request to the qualified single-table wrapper, the same `qualified_single_table_fallback_pushdown` shape the grouped-aggregate and multi-`COUNT(DISTINCT)` declines use, so the response is `SELECT <every select-list item rendered as Exasol SQL> FROM (<sharded raw fan-out narrowed to the referenced columns>) AS "LHS_T0"`
* *AND* the adapter MUST NOT respond with the full-base-row projection for such a request, because Exasol validates the response positionally against the request's `selectList` and rejects a column-count mismatch with SQL state `04000` "Expected number of columns is N but pushdown query has M", a hard query failure, NOT a correct-but-unaccelerated result
* *AND* the wrapper's select list SHALL be rendered in the EXASOL dialect and SHALL reproduce each item's declared type exactly, so Exasol's positional type check passes: a `literal_timestamputc`/`literal_timestamp_utc` item declared `TIMESTAMP WITH LOCAL TIME ZONE` renders as `CAST(CONVERT_TZ(TIMESTAMP '<utc value>', 'UTC', SESSIONTIMEZONE) AS TIMESTAMP WITH LOCAL TIME ZONE)`, with `SESSIONTIMEZONE` referenced SYMBOLICALLY so Exasol evaluates it in the CALLER's session, which the adapter cannot read; the shared `vs-expression` literal-rendering arm (`render_exasol_tstz_literal`) produces this rendering identically for the select-list context and the declined-WHERE-filter self-apply path, because converting into the caller's session zone before comparison reproduces Exasol's native `TIMESTAMP`-vs-`TIMESTAMP WITH LOCAL TIME ZONE` coercion for the plain-`TIMESTAMP`-mapped Iceberg column on the other side of the comparison
* *AND* the returned value SHALL equal the value Exasol computes natively for the same expression in the same session; the adapter MUST NOT substitute plain `TIMESTAMP` for a declared `TIMESTAMP WITH LOCAL TIME ZONE` EMITS type, which would surface the UTC wall clock where Exasol natively surfaces the session-local wall clock (at `SESSIONTIMEZONE = EUROPE/BERLIN`, native `CAST(TIMESTAMP '2024-03-01 10:00:00' AS TIMESTAMP WITH LOCAL TIME ZONE)` is `2024-03-01 10:00:00`, its UTC representation `2024-03-01 09:00:00`). This decline differs from the `scan-types/type-mapping` scenario "Iceberg timestamptz maps to plain Exasol TIMESTAMP": there the VS DECLARES the column plain `TIMESTAMP` at `createVirtualSchema`, so Exasol makes no localization promise, while here Exasol has already inferred the type for the select-list item independently of the adapter's schema, so a localization promise exists
* *AND* the SAME classification SHALL route the ZERO-FILE path: when file resolution prunes every file the adapter short-circuits to the empty-result response BEFORE the dispatcher runs, so that path SHALL emit a zero-row result whose columns are typed from `selectListDataTypes` in select-list order (`CAST(NULL AS <declared type>)`) and MUST NOT emit the full-base-row empty projection, whose column count trips the same `04000`; `CAST(NULL AS TIMESTAMP WITH LOCAL TIME ZONE)` is valid Exasol SQL, so the routed empty shape is buildable for a declared-TSTZ item
* *AND* the classification SHALL NOT fire where the full base row is the CORRECT response, an absent, `null`, empty, or non-array `selectList`, and SHALL ignore a bare `column` select-list item, whose EMITS type comes from `involvedTables` rather than from `selectListDataTypes`
* *AND* an item the translator cannot render in the Exasol dialect at all SHALL remain a hard error from the wrapper's renderer, never a silent wrong value

### Scenario: Projected literal or constant select-list item is pushed down as a positional projection

* *GIVEN* a row-scan `pushdown` request whose select list contains one or more literal/constant items, `literal_exactnumeric`, `literal_string`, `literal_bool`, `literal_double`, `literal_date`, `literal_timestamp`, or `literal_null`, each with a parallel `selectListDataTypes` entry
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL render each literal select-list item through the `crates/vs-expression` translator into a POSITIONAL `Expr` projection item, one projection item per select-list item, typed from the parallel top-level `selectListDataTypes` array, exactly as the `function_scalar` select-list branch already does, and MUST NOT trigger the full-base-row fallback that emits every base column and yields the column-count mismatch Exasol rejects ("Expected number of columns is 1 but pushdown query has N")
* *AND* the emitted scan's column arity SHALL equal the query's select-list arity, so two structurally identical literal items, such as the two `1` items in `SELECT 1, name, 1`, SHALL each occupy their own projected position and MUST NOT be collapsed into one
* *AND* each projected literal SHALL be evaluated once per scanned source row, so `SELECT <literal> FROM t` returns one constant-valued row per source table row, and the synthesized `literal_null` item behind a LIMIT barrier SHALL emit one single-column row per admitted row so the outer `COUNT(*)` counts exactly the rows the inner LIMIT admits
* *AND* an item the translator cannot render, or one the scan cannot emit (an EMITS-invalid declared type, e.g. `TIMESTAMP WITH LOCAL TIME ZONE`), SHALL route the request to the qualified single-table wrapper so Exasol evaluates the select list over a narrowed materialized scan, and SHALL NOT fall back to the full base row, which is an INVALID pushdown response for a delegated select list, not a correctness backstop

### Scenario: Routed session-dependent projection returns the value Exasol computes natively

* *GIVEN* a virtual-schema table
* *AND* an Exasol session whose `SESSIONTIMEZONE` UTC offset is non-zero
* *WHEN* the session runs `SELECT CURRENT_TIMESTAMP FROM <vs_table> WHERE id = 1` and `SELECT SYSTIMESTAMP FROM <vs_table> WHERE id = 1`
* *THEN* each query SHALL succeed rather than fail with SQL state `04000`, and each value SHALL equal Exasol's native value for the same expression in the same session, apart from the time between the two evaluations, and SHALL NOT be the UTC instant, `SYSTIMESTAMP` included because it is declared plain `TIMESTAMP(3)` and would otherwise pass the EMITS-type gate and ship the UTC wall clock
* *AND* `SELECT CAST(TIMESTAMP '2024-03-01 10:00:00' AS TIMESTAMP WITH LOCAL TIME ZONE) FROM <vs_table> WHERE id = 1` SHALL return exactly `2024-03-01 10:00:00`, equal to the same session's native `CAST(CONVERT_TZ(TIMESTAMP '2024-03-01 09:00:00', 'UTC', SESSIONTIMEZONE) AS TIMESTAMP WITH LOCAL TIME ZONE)`, because Exasol constant-folds that projected cast into a bare `literal_timestamputc` item carrying the UTC-normalized value
* *AND* the same queries under an ALL-PRUNING predicate (`WHERE id = 999999`) SHALL each succeed and return zero rows, never SQL state `04000`, so the zero-file short-circuit is covered rather than only the resolved-file dispatcher
* *AND* `EXPLAIN VIRTUAL` for both now-family queries SHALL show the qualified wrapper, a `FROM (…) AS "LHS_T0"` scan whose projection is narrowed to at most the referenced columns, and MUST NOT show a narrowed positional `_LH_PROJ_0` EMITS identifier for the routed item
