# Feature: Pushdown Planning, Capability Extensions

The adapter advertises the arithmetic operator scalar functions and ISO week in `getCapabilities`, and leaves the bitwise operator functions and the now-family date/time functions unadvertised. It also routes a select-list item the scan cannot emit to the qualified single-table wrapper.

## Background

* A scalar-function capability is advertised only when a `crates/vs-expression` arm renders it and the DataFusion result matches Exasol. `FN_NEG` and `FN_WEEK` meet this bar. `FN_CAST` meets it except for the text-target deviation that `sql-comprehension/vs-expression-translator-cast` records and (#587) removes. `FN_DIV`, `FN_TO_CHAR`, `FN_TO_NUMBER`, the divergent date functions, and the bitwise operator functions do not. Exasol never delegates an unadvertised capability and evaluates it over the returned rows, so advertising a capability the adapter cannot honour returns wrong rows.
* The now-family (`CURRENT_DATE`, `SYSDATE`, `CURRENT_TIMESTAMP`, `SYSTIMESTAMP`) is unadvertised, because the scan UDF receives no time zone, clock, or statement anchor. It would read its own container clock in UTC once per shard, while Exasol's now-family is constant per statement and uses the session or database time zone.
* A select-list item whose declared `selectListDataTypes` type is `TIMESTAMP WITH LOCAL TIME ZONE` cannot be a UDF EMITS type (`sqlCode 22002`). A full-base-row response to such a request fails Exasol's positional check with SQL state `04000` ("Expected number of columns is N but pushdown query has M"). The adapter routes the request to the qualified single-table wrapper instead: `SELECT <select list as Exasol SQL> FROM (<sharded raw fan-out narrowed to referenced columns>) AS "LHS_T0"`. The routing decision depends on the item's declared type, not on the `selectList` length, and an absent, `null`, empty, or non-array `selectList` receives the full base row.
* The scan cannot substitute plain `TIMESTAMP` for `TIMESTAMP WITH LOCAL TIME ZONE`, because Exasol returns the latter in the session time zone and the former verbatim. The `vs-expression` literal arm renders the value as `CAST(CONVERT_TZ(TIMESTAMP '<utc value>', 'UTC', SESSIONTIMEZONE) AS TIMESTAMP WITH LOCAL TIME ZONE)`, so Exasol evaluates `SESSIONTIMEZONE` in the caller's session.
* Exasol bit functions operate on unsigned 64-bit integers, while DataFusion's operators act on signed Arrow integers, and Iceberg has no unsigned integer type. DataFusion has no operator or function for `BIT_NOT`, `BIT_LROTATE`, `BIT_RROTATE`, `BIT_CHECK`, `BIT_SET`, or `BIT_TO_NUM`.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: ISO week capability is advertised so WEEK expressions push down

* *GIVEN* the adapter's advertised capability set
* *WHEN* Exasol requests `getCapabilities`
* *THEN* the response SHALL advertise `FN_WEEK`, backed by the `crates/vs-expression` `WEEK` arm rendering `date_part('week', …)`, whose ISO-8601 result matches Exasol `WEEK` (see `sql-functions/vs-expression-translator-date-fns`)
* *AND* `FN_ADD_SECONDS`, `FN_ADD_MONTHS`, `FN_ADD_YEARS`, `FN_MONTHS_BETWEEN`, `FN_YEARS_BETWEEN`, `FN_DAYOFWEEK`, `FN_LAST_DAY`, and `FN_CONVERT_TZ` SHALL remain absent, while `FN_ADD_DAYS`, `FN_ADD_WEEKS`, `FN_ADD_HOURS`, and `FN_ADD_MINUTES` (`sql-functions/vs-expression-translator-date-add-fns`) and the `*_BETWEEN` family (`sql-functions/vs-expression-translator-date-diff-fns`) are advertised
* *AND* Cartesian-product capabilities SHALL remain absent and only the inner equi-join capabilities (`JOIN`/`JOIN_TYPE_INNER`/`JOIN_CONDITION_EQUI`) SHALL be advertised, so advertising `FN_WEEK` introduces no additional join or cross-join capability
<!-- /DELTA:CHANGED -->
