# Feature: Scan Execution — Date/Time Truncation and Rounding

The scan session's truncation, rounding, and date-trunc UDFs compute Exasol's `TRUNC`, `ROUND`,
and `DATE_TRUNC` results for DATE and TIMESTAMP arguments directly on the Arrow value, so a pushed
date/time truncation returns native Exasol's value across Exasol's full DATE range (#201). The
scenarios name each exception. The
translator side that renders these calls is `sql-comprehension/vs-expression-translator-datetime-trunc`.

## Background

* `<trunc-fn>` and `<round-fn>` (#431) and `<date-trunc-fn>` are registered once per scan session.
  Each dispatches on its first argument's Arrow type: `Date32`, or `Timestamp(unit, zone)` for any
  unit and zone. The numeric branches are #431's. The format
  argument is a scalar string token from the vocabulary declared in `crates/vs-expression`, with no
  format meaning `day`.
* Exasol semantics, measured on the Docker Exasol container (`exasol/docker-db:2025.1.16`,
  `NLS_FIRST_DAY_OF_WEEK = 7`). `d` is a date in calendar year `Y`. `S(Y)` is the Monday of the
  ISO week that contains January 4 of `Y`. A TIMESTAMP is compared against each ROUND threshold as
  a full value. The calendar thresholds fall at midnight, so
  `ROUND(TIMESTAMP '2024-05-15 23:59:59.999', 'MM')` is `2024-05-01`.

| Unit | TRUNC result | ROUND moves to the next unit start when | TRUNC of `DATE '2024-05-15'` |
|---|---|---|---|
| century | January 1 of year `⌊(Y−1)/100⌋·100 + 1` | `(Y−1) mod 100 ≥ 50` | `2001-01-01` |
| year | January 1 of `Y` | month ≥ 7 | `2024-01-01` |
| ISO year | `S(Y)` if `S(Y) ≤ d`, else `S(Y−1)` | not a move: ROUND is `S(Y+1)` if month ≥ 7, else `S(Y)` | `2024-01-01` |
| quarter | first day of the quarter | on or after the 16th of the quarter's second month | `2024-04-01` |
| month | first day of the month | day ≥ 16 | `2024-05-01` |
| week of year | January 1 plus `7·⌊(day of year − 1)/7⌋` days | at least 3 days 12 hours past the TRUNC result, moving to it plus 7 days | `2024-05-13` |
| ISO week | the Monday on or before `d` | as week of year | `2024-05-13` |
| week of month | the 1st plus `7·⌊(day of month − 1)/7⌋` days | as week of year | `2024-05-15` |
| day | midnight | time of day ≥ 12:00 | `2024-05-15` |
| hour | start of the hour | minute ≥ 30 | fails, see below |
| minute | start of the minute | second ≥ 30 | fails, see below |
| second | start of the second, fraction dropped | fraction ≥ 0.5 s | fails, see below |

* Measured values the unit tests assert, beyond the table's last column:
  * TRUNC: `TIMESTAMP '2024-05-15 10:37:12.789'` gives the table's DATE value at `00:00:00` for
    every unit from century to day, `2024-05-15 00:00:00` with no format, `10:00:00` (`HH`),
    `10:37:00` (`MI`), and `10:37:12.000` (`SS`). `DATE '2021-01-02'` gives `2019-12-30` and
    `DATE '2019-12-30'` gives `2018-12-31` (`IYYY`). `DATE '2025-05-15'` gives `2025-05-14`
    (`WW`).
  * DATE_TRUNC: `DATE '1996-03-13'` gives `1996-01-01` (`year`, `quarter`), `1996-03-01`
    (`month`), and `1996-03-13` (`day`, `hour`, `minute`, `second`).
    `TIMESTAMP '1996-03-13 10:11:12.789'` gives `1996-03-13 00:00:00` (`day`), `10:00:00`
    (`hour`), `10:11:00` (`minute`), and `10:11:12.000` (`second`).
  * ROUND of `DATE '2024-05-15'` equals its TRUNC for every unit, because the date lies before
    each threshold. ROUND of `TIMESTAMP '2024-05-15 10:37:12.789'` gives that date at `00:00:00`
    for every unit from century to day, `11:00:00` (`HH`), `10:37:00` (`MI`), and `10:37:13`
    (`SS`).
  * ROUND: `CC` `2050-12-31`→`2001-01-01`, `2051-01-01`→`2101-01-01`. `YYYY`
    `2024-06-30`→`2024-01-01`, `2024-07-01`→`2025-01-01`. `IYYY` `2021-01-02`→`2021-01-04`,
    `2024-07-01`→`2024-12-30`, `2019-12-30`→`2019-12-30`. `Q` `2024-05-15`→`2024-04-01`,
    `2024-05-16`→`2024-07-01`. `MM` `2024-05-15`→`2024-05-01`, `2024-05-16`→`2024-06-01`. `IW`
    `2024-05-16`→`2024-05-13`, `2024-05-17`→`2024-05-20`, `TIMESTAMP '2024-05-16 11:59:59.999'`→
    `2024-05-13`, `TIMESTAMP '2024-05-16 12:00:00'`→`2024-05-20`. `W` `2023-02-26`→`2023-03-01`.
    `DD` `11:59:59.999`→same day, `12:00:00`→next day. `HH` `10:29:59.999`→`10:00:00`,
    `10:30:00`→`11:00:00`. `SS` `.499`→`10:37:12`, `.500`→`10:37:13`,
    `TIMESTAMP '2024-05-15 23:59:59.500'`→`2024-05-16 00:00:00`.
  * Range edges: TRUNC of `DATE '9999-12-31'` gives `9901-01-01` (`CC`), `9999-01-01` (`YYYY`),
    `9999-01-04` (`IYYY`), `9999-10-01` (`Q`), `9999-12-01` (`MM`), `9999-12-31` (`WW`),
    `9999-12-27` (`IW`), `9999-12-29` (`W`). ROUND of `DATE '9999-12-31'` with `CC`, `YYYY`,
    `IYYY`, `Q`, `MM`, or `IW`, and ROUND of `TIMESTAMP '9999-12-31 23:59:59.999'` with `DD`, `HH`,
    `MI`, or `SS`, fail with `data exception - datetime field overflow` (SQL state `22008`). ROUND
    of `DATE '9999-12-31'` gives `9999-12-31` (`WW`), `9999-12-29` (`W`), `9999-12-31` (`DD`).
  * An hour, minute, or second token on a DATE: `TRUNC` fails with `data exception - unsupported
    format in date trunc` (`22769`) and `ROUND` with `data exception - unsupported format in date
    round` (`22770`). Exasol still delegates the call: `EXPLAIN VIRTUAL` over
    `TRUNC(EVENT_DATE, 'HH')` pushes it to the scan. `DATE_TRUNC('hour' | 'minute' | 'second',
    <date>)` returns the date unchanged.
  * Result types (`CREATE TABLE AS` read back from `EXA_ALL_COLUMNS`): DATE stays DATE,
    `TIMESTAMP(3)` stays `TIMESTAMP(3)`, and `TIMESTAMP(6)` stays `TIMESTAMP(6)`, for `TRUNC`,
    `ROUND`, and `DATE_TRUNC`, `DATE_TRUNC('hour', <date>)` included. A NULL argument gives NULL.
* Exasol uses the Julian calendar before the Gregorian reform. Measured: `DATE '1582-10-10'`
  normalizes to `1582-10-15`, `DATE '0100-02-29'` is valid, `TO_CHAR(DATE '0001-01-01', 'DY')` is
  `SAT`, and `DAYS_BETWEEN(DATE '1970-01-01', DATE '0001-01-01')` is `719164`. The scan reads a
  `Date32` as a proleptic Gregorian day count through chrono (`scan/convert.rs`), in which
  `0001-01-01` is a Monday and the same difference is 719162 days. The two calendars agree from
  `1582-10-15` on.
* Iceberg/Delta compliance. Iceberg § Primitive Types: `date` is "Calendar date without timezone or
  time", and note 2 says "Timestamp values _with time zone_ represent a point in time: values are
  stored as UTC and do not retain a source time zone". Iceberg § Parquet: `date` "Stores days from
  1970-01-01." Delta § Primitive Types: `date` is "A calendar date, represented as a year-month-day
  triple without a timezone.", and `timestamp` is "Microsecond precision timestamp elapsed since
  the Unix epoch, 1970-01-01 00:00:00 UTC." Iceberg § Primitive Types: `timestamp_ns` is
  "Timestamp, nanosecond precision, without timezone". Iceberg § Parquet stores it as `int64`
  `TIMESTAMP_NANOS`, which "Stores nanoseconds from 1970-01-01 00:00:00.000000000." A nanosecond
  value therefore lies between `1677-09-21 00:12:43.145224192` and `2262-04-11 23:47:16.854775807`.
  Neither spec names a calendar system. The scan emits a
  zoned timestamp as the UTC instant's wall clock (`datafusion-scan/type-mapping-timestamp-precision`),
  so that wall clock is the value Exasol sees. The pre-reform divergence is a calendar mismatch
  between Exasol and the proleptic Gregorian day count, not a table-format spec deviation, and it is
  a tracked exception (scenario below).

## Scenarios

### Scenario: TRUNC on a DATE or TIMESTAMP returns Exasol's value for every non-session-week format

* *GIVEN* a `Date32` argument or a `Timestamp` argument in seconds, milliseconds, microseconds, or nanoseconds, and no format or a vocabulary token other than `D`, `DAY`, or `DY`, excluding an hour, minute, or second token on a `Date32`, and a result the argument's time unit can represent
* *WHEN* `<trunc-fn>` evaluates it
* *THEN* it SHALL return the Background table's TRUNC result, equal to every measured TRUNC value in the Background
* *AND* it SHALL compute on the calendar date and the time of day in the argument's own unit, never through a nanosecond timestamp, so the range-edge TRUNC values of `DATE '9999-12-31'` hold

### Scenario: ROUND on a DATE or TIMESTAMP rounds at Exasol's thresholds

* *GIVEN* the argument and format classes of the preceding scenario
* *WHEN* `<round-fn>` evaluates it
* *THEN* it SHALL return the TRUNC result when the argument lies before the Background table's ROUND threshold and the next unit start when it lies at or after it, with the ISO-year row's own rule
* *AND* the result SHALL equal every measured ROUND value in the Background

### Scenario: The result keeps the argument's Arrow type and DataFusion plans every pushdown position

* *GIVEN* a DataFusion session with `<trunc-fn>`, `<round-fn>`, and `<date-trunc-fn>` registered and a table with `Date32`, `Timestamp(Microsecond, None)`, `Timestamp(Microsecond, "UTC")`, and `Timestamp(Nanosecond, None)` columns
* *WHEN* a query applies each UDF to each column in a projection, in a filter comparison against a literal, as a `GROUP BY` key, and inside an aggregate argument
* *THEN* planning SHALL succeed and each result column SHALL have its argument's exact Arrow type, zone included
* *AND* a NULL argument SHALL yield NULL, and each UDF SHALL be `Immutable`, so DataFusion MAY fold a literal call and evaluate the UDF inside the Parquet row filter

### Scenario: An hour, minute, or second format on a DATE fails TRUNC and ROUND but not DATE_TRUNC

* *GIVEN* a `Date32` argument and an `HH`, `HH12`, `HH24`, `MI`, or `SS` token
* *WHEN* `<trunc-fn>`, `<round-fn>`, or `<date-trunc-fn>` evaluates it
* *THEN* `<trunc-fn>` SHALL fail the query with a message containing `unsupported format in date trunc`, and `<round-fn>` with one containing `unsupported format in date round`, as native Exasol does
* *AND* `<date-trunc-fn>` SHALL return the date unchanged, as `DATE_TRUNC('hour', <date>)` does natively
* *AND* for every other argument and token, `<date-trunc-fn>` SHALL return `<trunc-fn>`'s result
* *AND* `<trunc-fn>` and `<round-fn>` SHALL raise at DataFusion planning time from the literal format, so a shard with no rows also fails, matching native `TRUNC`'s compile-time `22769`. A scan the adapter prunes to zero files runs no UDF and returns no rows, an accepted divergence

### Scenario: A date/time format token on a numeric argument fails the query

* *GIVEN* a numeric first argument and a vocabulary token as the second argument, for example `TRUNC(1.2345, 'MM')`
* *WHEN* `<trunc-fn>` or `<round-fn>` evaluates it
* *THEN* the query SHALL fail, as Exasol's cast of the token to a number does (SQL state `22018`), and the UDF MUST NOT return a numeric result

### Scenario: A result outside Exasol's date range or the argument's time unit fails the query

* *GIVEN* an argument whose `<trunc-fn>`, `<round-fn>`, or `<date-trunc-fn>` result lies after `9999-12-31`, or outside the range of the argument's Arrow time unit (for a nanosecond timestamp, the Background's `1677-09-21` to `2262-04-11` range)
* *WHEN* the UDF evaluates it
* *THEN* the query SHALL fail with a message containing `datetime field overflow`, as the measured `22008` failures do
* *AND* a nanosecond result outside the unit's range SHALL be an explicit exception against native Exasol, which returns a value: `TRUNC` with `CC` of a nanosecond timestamp in `1690` is `1601-01-01` natively and fails in the scan. The reason is the input-type rule: the result keeps its argument's Arrow type, and `Timestamp(Nanosecond)` cannot hold `1601-01-01`
* *AND* the UDF MUST NOT return a value outside either range, because a filter consumes the value inside DataFusion and never reaches the emit-boundary range check

### Scenario: A date/time UDF failure reaches the user with Exasol's message on every route

* *GIVEN* a failure that another scenario of this feature requires from `<trunc-fn>`, `<round-fn>`, or `<date-trunc-fn>`, raised at planning time or while evaluating a projection, a filter DataFusion pushed into the Parquet row filter, a group key, or an aggregate argument
* *WHEN* the scan surfaces the failure to Exasol
* *THEN* the message SHALL contain the Exasol text the failing scenario names, where it names one, and MUST NOT contain `scan failed: assigned data could not be read`, `DataFusion SQL error`, or `partial aggregate SQL error`
* *AND* the UDFs SHALL carry the failure as a typed error and record it on the scan session, and the scan dispatcher SHALL reframe from that record by the rule of `datafusion-scan/scan-execution-expression-pushdown` "A checked float division raises rather than producing a non-finite value": the record replaces the surfaced failure and leads a memory exhaustion

### Scenario: A zoned timestamp truncates on the UTC wall clock the scan emits

* *GIVEN* a `Timestamp(unit, Some(zone))` argument
* *WHEN* `<trunc-fn>`, `<round-fn>`, or `<date-trunc-fn>` evaluates it
* *THEN* it SHALL compute on the stored UTC instant read as a wall-clock value, ignoring the zone, and SHALL return the same zone annotation
* *AND* the result's stored value SHALL equal the UDF's result for the same stored value without a zone

### Scenario: A date before the Gregorian reform follows the proleptic Gregorian calendar as a tracked exception

* *GIVEN* an argument whose date lies before `1582-10-15`
* *WHEN* `<trunc-fn>`, `<round-fn>`, or `<date-trunc-fn>` evaluates it
* *THEN* it SHALL compute in the proleptic Gregorian calendar the scan reads `Date32` in, and the result MAY differ from native Exasol's Julian-calendar result, recorded as a tracked exception (#TBD)
* *AND* the measured divergences SHALL be pinned: `TRUNC(DATE '0001-01-01', 'IW')` and `TRUNC(DATE '0001-01-01', 'IYYY')` fail natively (`22104`, `22008`) and return `0001-01-01` from the UDF, and `TRUNC(DATE '0100-03-10', 'WW')` is `0100-03-04` natively and `0100-03-05` from the UDF
* *AND* TRUNC with a century, year, quarter, month, week-of-month, or day token SHALL still match Exasol outside October 1582, because both calendars give those results the same label: TRUNC of `DATE '0001-01-01'` with `CC`, `YYYY`, `Q`, `MM`, `W`, or `DD` is `0001-01-01` in both
