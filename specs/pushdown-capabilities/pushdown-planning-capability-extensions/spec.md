# Feature: Pushdown Planning, Capability Extensions

The adapter advertises the arithmetic operator scalar functions and ISO week in `getCapabilities`, and leaves the bitwise operator functions unadvertised. `pushdown/pushdown-planning-clock-functions` owns the clock functions. The adapter also routes a select-list item the scan cannot emit to the qualified single-table wrapper.

## Background

* Exasol delegates an advertised capability fully and never re-checks it. It evaluates an unadvertised capability itself over the rows the scan returns.
* The scan UDF receives no session time zone, clock, or statement anchor.
* A scan UDF cannot emit a `TIMESTAMP WITH LOCAL TIME ZONE`, `INTERVAL`, `GEOMETRY`, or `HASHTYPE` column.
* Exasol bit functions operate on unsigned 64-bit integers, DataFusion's integer operators are signed, and Iceberg has no unsigned integer type.

## Scenarios

### Scenario: Arithmetic operator scalar-function capabilities are advertised so arithmetic expression trees are pushed down

* *GIVEN* the adapter's advertised capability set
* *WHEN* Exasol requests `getCapabilities`
* *THEN* the response SHALL advertise the arithmetic scalar-function capabilities for `+`, `-`, `*`, `/`, integer division `DIV`, and unary `-` (the Exasol capability names `FN_ADD`, `FN_SUB`, `FN_MULT`, `FN_FLOAT_DIV`, `FN_DIV`, and `FN_NEG`), verified against the Exasol Virtual Schema capability vocabulary
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
* *AND* `FN_ADD_SECONDS`, `FN_ADD_MONTHS`, `FN_ADD_YEARS`, `FN_MONTHS_BETWEEN`, `FN_YEARS_BETWEEN`, `FN_DAYOFWEEK`, `FN_LAST_DAY`, and `FN_CONVERT_TZ` SHALL remain absent, while `FN_ADD_DAYS`, `FN_ADD_WEEKS`, `FN_ADD_HOURS`, and `FN_ADD_MINUTES` (`sql-functions/vs-expression-translator-date-add-fns`) and the `*_BETWEEN` family (`sql-functions/vs-expression-translator-date-diff-fns`) are advertised
* *AND* Cartesian-product capabilities SHALL remain absent and only the inner equi-join capabilities (`JOIN`/`JOIN_TYPE_INNER`/`JOIN_CONDITION_EQUI`) SHALL be advertised, so advertising `FN_WEEK` introduces no additional join or cross-join capability

### Scenario: Bitwise operator function capabilities remain absent

* *GIVEN* the adapter's advertised capability set
* *WHEN* Exasol requests `getCapabilities`
* *THEN* the response SHALL NOT advertise `FN_BIT_AND`, `FN_BIT_OR`, `FN_BIT_XOR`, `FN_BIT_NOT`, `FN_BIT_LSHIFT`, `FN_BIT_RSHIFT`, `FN_BIT_LROTATE`, `FN_BIT_RROTATE`, `FN_BIT_CHECK`, `FN_BIT_SET`, or `FN_BIT_TO_NUM`
* *AND* Exasol SHALL post-process bitwise operator functions rather than pushing them to the node-local scan, because no faithful translation exists for any of the eleven over Exasol's bit-function domain: Exasol bit functions operate on unsigned 64-bit integers (range `0`–`18446744073709551615`, result `DECIMAL(20,0)`), while DataFusion's `&`/`|`/`#`/`<<`/`>>` act on the operand's signed Arrow integer type, Iceberg sources carry only signed integers (`int` = 32-bit signed, `long` = 64-bit signed; the Iceberg spec defines no unsigned integer primitive), so a bit-63-set result is a large positive value in Exasol but negative under signed `Int64` and the `Int64` → `DECIMAL(20,0)` mapping carries the negative value; `BIT_RSHIFT` diverges unconditionally on any bit-63-set operand because DataFusion's signed `>>` is arithmetic (sign-extending) whereas Exasol's is logical (zero-fill); and DataFusion provides no operator or scalar function at all for `BIT_NOT` (its SQL planner rejects unary `~`), `BIT_LROTATE`, `BIT_RROTATE`, `BIT_CHECK`, `BIT_SET`, or `BIT_TO_NUM`
* *AND* `FN_BIT_LENGTH` SHALL be treated as out of scope for this decision, it is an Exasol string function (bit count of a string), not a bitwise operator, and only the inner equi-join capabilities (`JOIN`/`JOIN_TYPE_INNER`/`JOIN_CONDITION_EQUI`) SHALL be advertised, so this decision introduces no additional join, cross-join, or string-function capability change

### Scenario: Projected constant whose declared EMITS type Exasol rejects routes to the qualified wrapper

* *GIVEN* a row-scan `pushdown` request whose select list holds an item that `selectListDataTypes` declares `TIMESTAMP WITH LOCAL TIME ZONE`, such as a `literal_timestamputc` constant, a `CURRENT_TIMESTAMP` call, or a `LOCALTIMESTAMP` call on Exasol 8 (`pushdown/pushdown-planning-clock-functions`)
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL route the whole request to the qualified single-table wrapper, so the response is `SELECT <every select-list item rendered as Exasol SQL> FROM (<sharded raw fan-out narrowed to the referenced columns>) AS "LHS_T0"`
* *AND* the adapter MUST NOT respond with the full base row, because Exasol checks the response positionally against the request's `selectList` and rejects a column-count mismatch with SQL state `04000` "Expected number of columns is N but pushdown query has M"
* *AND* the wrapper's select list SHALL reproduce each item's declared type exactly, so Exasol's positional type check passes: a `literal_timestamputc` item renders as `CAST(CONVERT_TZ(TIMESTAMP '<utc value>', 'UTC', SESSIONTIMEZONE) AS TIMESTAMP WITH LOCAL TIME ZONE)`, with `SESSIONTIMEZONE` left for Exasol to evaluate in the caller's session, and a `CURRENT_TIMESTAMP` or `LOCALTIMESTAMP` item renders the same way with the statement's UTC instant
* *AND* a `literal_timestamputc` item SHALL return the value Exasol computes natively in the same session. At `SESSIONTIMEZONE = EUROPE/BERLIN`, native `CAST(TIMESTAMP '2024-03-01 10:00:00' AS TIMESTAMP WITH LOCAL TIME ZONE)` is `2024-03-01 10:00:00`, so the adapter MUST NOT return the UTC wall clock `2024-03-01 09:00:00`
* *AND* when file resolution prunes every file, the response SHALL be a zero-row result whose columns are typed from `selectListDataTypes` in select-list order (`CAST(NULL AS <declared type>)`), never the full-base-row empty projection
* *AND* the routing SHALL NOT apply to an absent or `null` `selectList`, where the full base row is the correct response, to an empty `selectList`, where one column is, or to a bare `column` item, whose type comes from `involvedTables`

### Scenario: Projected literal or constant select-list item is pushed down as a positional projection

* *GIVEN* a row-scan `pushdown` request whose select list contains one or more literal/constant items, `literal_exactnumeric`, `literal_string`, `literal_bool`, `literal_double`, `literal_date`, `literal_timestamp`, or `literal_null`, each with a parallel `selectListDataTypes` entry
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL render each literal select-list item through the `crates/vs-expression` translator into a POSITIONAL `Expr` projection item, one projection item per select-list item, typed from the parallel top-level `selectListDataTypes` array, exactly as the `function_scalar` select-list branch already does, and MUST NOT trigger the full-base-row fallback that emits every base column and yields the column-count mismatch Exasol rejects ("Expected number of columns is 1 but pushdown query has N")
* *AND* the emitted scan's column arity SHALL equal the query's select-list arity, so two structurally identical literal items, such as the two `1` items in `SELECT 1, name, 1`, SHALL each occupy their own projected position and MUST NOT be collapsed into one
* *AND* each projected literal SHALL be evaluated once per scanned source row, so `SELECT <literal> FROM t` returns one constant-valued row per source table row, and the synthesized `literal_null` item behind a LIMIT barrier SHALL emit one single-column row per admitted row so the outer `COUNT(*)` counts exactly the rows the inner LIMIT admits
* *AND* an item the translator cannot render, or one the scan cannot emit (an EMITS-invalid declared type, e.g. `TIMESTAMP WITH LOCAL TIME ZONE`), SHALL route the request to the qualified single-table wrapper so Exasol evaluates the select list over a narrowed materialized scan, and SHALL NOT fall back to the full base row, which is an INVALID pushdown response for a delegated select list, not a correctness backstop

### Scenario: Routed session-dependent projection returns the value Exasol computes natively

* *GIVEN* a virtual-schema table, on a virtual schema without `SESSION_TIME_ZONE`
* *AND* an Exasol session whose `SESSIONTIMEZONE` equals `DBTIMEZONE` and has a non-zero UTC offset
* *WHEN* the session runs `SELECT CURRENT_TIMESTAMP FROM <vs_table> WHERE id = 1` and `SELECT SYSTIMESTAMP FROM <vs_table> WHERE id = 1`
* *THEN* each query SHALL succeed rather than fail with SQL state `04000`, and each value SHALL equal Exasol's native value for the same expression in the same session, apart from the time between the two evaluations, and SHALL NOT be the UTC instant
* *AND* `SELECT CAST(TIMESTAMP '2024-03-01 10:00:00' AS TIMESTAMP WITH LOCAL TIME ZONE) FROM <vs_table> WHERE id = 1` SHALL return exactly `2024-03-01 10:00:00`, equal to the same session's native `CAST(CONVERT_TZ(TIMESTAMP '2024-03-01 09:00:00', 'UTC', SESSIONTIMEZONE) AS TIMESTAMP WITH LOCAL TIME ZONE)`, because Exasol constant-folds that projected cast into a bare `literal_timestamputc` item carrying the UTC-normalized value
* *AND* the same queries under an all-pruning predicate (`WHERE id = 999999`) SHALL each succeed and return zero rows, never SQL state `04000`
* *AND* `EXPLAIN VIRTUAL` of the `CURRENT_TIMESTAMP` query SHALL show the qualified wrapper, a `FROM (…) AS "LHS_T0"` scan whose projection is narrowed to at most the referenced columns, and MUST NOT show a positional `_LH_PROJ_0` EMITS column for the routed item
* *AND* `EXPLAIN VIRTUAL` of the `SYSTIMESTAMP` query SHALL show its folded `TIMESTAMP` literal as a column the scan emits, because a plain `TIMESTAMP` needs no wrapper

### Scenario: An INTERVAL, GEOMETRY, or HASHTYPE select-list item, or a TIMESTAMP WITH LOCAL TIME ZONE aggregate or GROUP BY key, is computed by Exasol over the sharded scan

* *GIVEN* a single-table or join `pushdown` request whose select list carries an item of one of the two cases below
* *AND* case 1: an item that `selectListDataTypes` declares `INTERVAL YEAR TO MONTH`, `INTERVAL DAY TO SECOND`, `GEOMETRY`, or `HASHTYPE`, whether a projected expression such as `ts - ts` or `CAST(c AS HASHTYPE)`, an aggregate such as `MIN(ts - ts)`, or a GROUP BY key such as `GROUP BY ts - ts`
* *AND* case 2: an aggregate or GROUP BY key that `selectListDataTypes` declares `TIMESTAMP WITH LOCAL TIME ZONE`
* *WHEN* the adapter plans the pushdown
* *THEN* a single-table request SHALL route to the qualified single-table wrapper, and a join SHALL lose broadcast eligibility and take the N-scan join fallback, so Exasol SQL computes every select-list item over the sharded raw scan
* *AND* the adapter MUST NOT declare such an item as a scan `EMITS` column, because the scan has no value of these types to emit and Exasol rejects `TIMESTAMP WITH LOCAL TIME ZONE` as an `EMITS` type, and MUST NOT declare it as `VARCHAR(2000000)`, which Exasol rejects with `Data type mismatch`
* *AND* the query SHALL return the same rows as native Exasol
* *AND* the sharded fan-out, the referenced-column projection narrowing, and the scan-side WHERE predicate SHALL be retained
