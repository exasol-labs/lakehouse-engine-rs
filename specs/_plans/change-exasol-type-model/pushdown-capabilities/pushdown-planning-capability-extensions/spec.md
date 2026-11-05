# Feature: Pushdown Planning, Capability Extensions

The adapter advertises the arithmetic operator scalar functions and ISO week in `getCapabilities`, and leaves the bitwise operator functions and the now-family date/time functions unadvertised. It also routes a select-list item the scan cannot emit to the qualified single-table wrapper.

<!-- DELTA:CHANGED -->
## Background

* A scalar-function capability is advertised only when a `crates/vs-expression` arm renders it and the DataFusion result matches Exasol. `FN_NEG` and `FN_WEEK` meet this bar. `FN_CAST` meets it except for the text-target deviation that `sql-comprehension/vs-expression-translator-cast` records and (#587) removes. `FN_DIV`, `FN_TO_CHAR`, `FN_TO_NUMBER`, the divergent date functions, and the bitwise operator functions do not. Exasol never delegates an unadvertised capability and evaluates it over the returned rows, so advertising a capability the adapter cannot honour returns wrong rows.
* The now-family (`CURRENT_DATE`, `SYSDATE`, `CURRENT_TIMESTAMP`, `SYSTIMESTAMP`) is unadvertised, because the scan UDF receives no time zone, clock, or statement anchor. It would read its own container clock in UTC once per shard, while Exasol's now-family is constant per statement and uses the session or database time zone.
* A select-list item whose declared `selectListDataTypes` type is `TIMESTAMP WITH LOCAL TIME ZONE` cannot be a UDF EMITS type (`sqlCode 22002`). A full-base-row response to such a request fails Exasol's positional check with SQL state `04000` ("Expected number of columns is N but pushdown query has M"). The adapter routes the request to the qualified single-table wrapper instead: `SELECT <select list as Exasol SQL> FROM (<sharded raw fan-out narrowed to referenced columns>) AS "LHS_T0"`. The routing decision depends on the item's declared type, not on the `selectList` length, and an absent, `null`, empty, or non-array `selectList` receives the full base row.
* The scan cannot substitute plain `TIMESTAMP` for `TIMESTAMP WITH LOCAL TIME ZONE`, because Exasol returns the latter in the session time zone and the former verbatim. The `vs-expression` literal arm renders the value as `CAST(CONVERT_TZ(TIMESTAMP '<utc value>', 'UTC', SESSIONTIMEZONE) AS TIMESTAMP WITH LOCAL TIME ZONE)`, so Exasol evaluates `SESSIONTIMEZONE` in the caller's session.
* Exasol bit functions operate on unsigned 64-bit integers, while DataFusion's operators act on signed Arrow integers, and Iceberg has no unsigned integer type. DataFusion has no operator or function for `BIT_NOT`, `BIT_LROTATE`, `BIT_RROTATE`, `BIT_CHECK`, `BIT_SET`, or `BIT_TO_NUM`.
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:NEW -->
### Scenario: An INTERVAL, GEOMETRY, or HASHTYPE select-list item, or a TIMESTAMP WITH LOCAL TIME ZONE aggregate or GROUP BY key, is computed by Exasol over the sharded scan

* *GIVEN* a single-table or join `pushdown` request whose select list carries an item of one of the two cases below
* *AND* case 1: an item that `selectListDataTypes` declares `INTERVAL YEAR TO MONTH`, `INTERVAL DAY TO SECOND`, `GEOMETRY`, or `HASHTYPE`, whether a projected expression such as `ts - ts` or `CAST(c AS HASHTYPE)`, an aggregate such as `MIN(ts - ts)`, or a GROUP BY key such as `GROUP BY ts - ts`
* *AND* case 2: an aggregate or GROUP BY key that `selectListDataTypes` declares `TIMESTAMP WITH LOCAL TIME ZONE`
* *WHEN* the adapter plans the pushdown
* *THEN* a single-table request SHALL route to the qualified single-table wrapper, and a join SHALL lose broadcast eligibility and take the N-scan join fallback, so Exasol SQL computes every select-list item over the sharded raw scan
* *AND* the adapter MUST NOT declare such an item as a scan `EMITS` column, because the scan has no value of these types to emit and Exasol rejects `TIMESTAMP WITH LOCAL TIME ZONE` as an `EMITS` type, and MUST NOT declare it as `VARCHAR(2000000)`, which Exasol rejects with `Data type mismatch`
* *AND* the query SHALL return the same rows as native Exasol
* *AND* the sharded fan-out, the referenced-column projection narrowing, and the scan-side WHERE predicate SHALL be retained
<!-- /DELTA:NEW -->
