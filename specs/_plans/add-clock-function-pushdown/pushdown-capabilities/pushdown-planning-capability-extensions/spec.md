<!-- DELTA:CHANGED -->
# Feature: Pushdown Planning, Capability Extensions

The adapter advertises the arithmetic operator scalar functions and ISO week in `getCapabilities`, and leaves the bitwise operator functions unadvertised. `pushdown/pushdown-planning-clock-functions` owns the clock functions. The adapter also routes a select-list item the scan cannot emit to the qualified single-table wrapper.
<!-- /DELTA:CHANGED -->

## Background

* Exasol delegates an advertised capability fully and never re-checks it. It evaluates an unadvertised capability itself over the rows the scan returns.
* The scan UDF receives no session time zone, clock, or statement anchor.
* A scan UDF cannot emit a `TIMESTAMP WITH LOCAL TIME ZONE`, `INTERVAL`, `GEOMETRY`, or `HASHTYPE` column.
* Exasol bit functions operate on unsigned 64-bit integers, DataFusion's integer operators are signed, and Iceberg has no unsigned integer type.

## Scenarios

<!-- DELTA:REMOVED -->
### Scenario: Now-family date/time capabilities are withdrawn so Exasol evaluates its own clock

* *GIVEN* the adapter's advertised capability set and a clock function call
* *WHEN* Exasol plans a query that uses the call
* *THEN* this behavior SHALL be removed; replaced by "The five clock functions are advertised" in `pushdown/pushdown-planning-clock-functions`
<!-- /DELTA:REMOVED -->

<!-- DELTA:CHANGED -->
### Scenario: Projected constant whose declared EMITS type Exasol rejects routes to the qualified wrapper

* *GIVEN* a row-scan `pushdown` request whose select list holds an item that `selectListDataTypes` declares `TIMESTAMP WITH LOCAL TIME ZONE`, such as a `literal_timestamputc` constant, a `CURRENT_TIMESTAMP` call, or a `LOCALTIMESTAMP` call on Exasol 8 (`pushdown/pushdown-planning-clock-functions`)
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL route the whole request to the qualified single-table wrapper, so the response is `SELECT <every select-list item rendered as Exasol SQL> FROM (<sharded raw fan-out narrowed to the referenced columns>) AS "LHS_T0"`
* *AND* the adapter MUST NOT respond with the full base row, because Exasol checks the response positionally against the request's `selectList` and rejects a column-count mismatch with SQL state `04000` "Expected number of columns is N but pushdown query has M"
* *AND* the wrapper's select list SHALL reproduce each item's declared type exactly, so Exasol's positional type check passes: a `literal_timestamputc` item renders as `CAST(CONVERT_TZ(TIMESTAMP '<utc value>', 'UTC', SESSIONTIMEZONE) AS TIMESTAMP WITH LOCAL TIME ZONE)`, with `SESSIONTIMEZONE` left for Exasol to evaluate in the caller's session, and a `CURRENT_TIMESTAMP` or `LOCALTIMESTAMP` item renders the same way with the statement's UTC instant
* *AND* a `literal_timestamputc` item SHALL return the value Exasol computes natively in the same session. At `SESSIONTIMEZONE = EUROPE/BERLIN`, native `CAST(TIMESTAMP '2024-03-01 10:00:00' AS TIMESTAMP WITH LOCAL TIME ZONE)` is `2024-03-01 10:00:00`, so the adapter MUST NOT return the UTC wall clock `2024-03-01 09:00:00`
* *AND* when file resolution prunes every file, the response SHALL be a zero-row result whose columns are typed from `selectListDataTypes` in select-list order (`CAST(NULL AS <declared type>)`), never the full-base-row empty projection
* *AND* the routing SHALL NOT apply to an absent or `null` `selectList`, where the full base row is the correct response, to an empty `selectList`, where one column is, or to a bare `column` item, whose type comes from `involvedTables`
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: Routed session-dependent projection returns the value Exasol computes natively

* *GIVEN* a virtual-schema table, on a virtual schema without `SESSION_TIME_ZONE`
* *AND* an Exasol session whose `SESSIONTIMEZONE` equals `DBTIMEZONE` and has a non-zero UTC offset
* *WHEN* the session runs `SELECT CURRENT_TIMESTAMP FROM <vs_table> WHERE id = 1` and `SELECT SYSTIMESTAMP FROM <vs_table> WHERE id = 1`
* *THEN* each query SHALL succeed rather than fail with SQL state `04000`, and each value SHALL equal Exasol's native value for the same expression in the same session, apart from the time between the two evaluations, and SHALL NOT be the UTC instant
* *AND* `SELECT CAST(TIMESTAMP '2024-03-01 10:00:00' AS TIMESTAMP WITH LOCAL TIME ZONE) FROM <vs_table> WHERE id = 1` SHALL return exactly `2024-03-01 10:00:00`, equal to the same session's native `CAST(CONVERT_TZ(TIMESTAMP '2024-03-01 09:00:00', 'UTC', SESSIONTIMEZONE) AS TIMESTAMP WITH LOCAL TIME ZONE)`, because Exasol constant-folds that projected cast into a bare `literal_timestamputc` item carrying the UTC-normalized value
* *AND* the same queries under an all-pruning predicate (`WHERE id = 999999`) SHALL each succeed and return zero rows, never SQL state `04000`
* *AND* `EXPLAIN VIRTUAL` of the `CURRENT_TIMESTAMP` query SHALL show the qualified wrapper, a `FROM (…) AS "LHS_T0"` scan whose projection is narrowed to at most the referenced columns, and MUST NOT show a positional `_LH_PROJ_0` EMITS column for the routed item
* *AND* `EXPLAIN VIRTUAL` of the `SYSTIMESTAMP` query SHALL show its folded `TIMESTAMP` literal as a column the scan emits, because a plain `TIMESTAMP` needs no wrapper
<!-- /DELTA:CHANGED -->
