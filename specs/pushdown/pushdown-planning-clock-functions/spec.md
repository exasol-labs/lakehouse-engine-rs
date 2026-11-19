# Feature: Pushdown Planning, Clock Functions and Time Zone Literals

The adapter pushes down `CURRENT_DATE`, `CURRENT_TIMESTAMP`, `LOCALTIMESTAMP`, `SYSDATE`, and `SYSTIMESTAMP` by replacing each call with a date or timestamp literal in the pushed SQL. It also replaces a `TIMESTAMP WITH LOCAL TIME ZONE` value that the query compares or computes with by the plain timestamp it stands for: its UTC date and time beside an instant operand, and its wall-clock time otherwise. A filter over either then prunes files and runs in the scan like a filter over a plain literal. `vs-properties/session-time-zone` owns the zone each wall-clock value folds in.

## Background

* Exasol evaluates `SYSDATE` and `SYSTIMESTAMP` in `DBTIMEZONE`, and `CURRENT_DATE`, `CURRENT_TIMESTAMP`, and `LOCALTIMESTAMP` in `SESSIONTIMEZONE`. `CURRENT_DATE` and `SYSDATE` are a `DATE`, `SYSTIMESTAMP` is a `TIMESTAMP`, and `CURRENT_TIMESTAMP` is a `TIMESTAMP WITH LOCAL TIME ZONE`. `LOCALTIMESTAMP` is a `TIMESTAMP WITH LOCAL TIME ZONE` on Exasol 8 and a `TIMESTAMP` on Exasol 2025.
* On Exasol 2025 the three timestamp functions take a fractional-seconds precision, 3 unless the call names one, and the value has that many fractional digits. On Exasol 8 they have three.
* Exasol folds an expression over a clock function, such as `CURRENT_DATE - 7` or `ADD_HOURS(CURRENT_TIMESTAMP, -1)`, into a literal before it sends a pushdown request, so only a bare call reaches the adapter. It sends a `TIMESTAMP WITH LOCAL TIME ZONE` value as a `literal_timestamputc` that holds the UTC instant, and it compares a plain `TIMESTAMP` with such a value by reading the plain side as session-local.
* `SYSDATE` and `SYSTIMESTAMP` fold in the `TZ` zone, and the other values in the folding zone, as `vs-properties/session-time-zone` defines them. `docs/semantic-differences.md` lists where the folded values and the comparisons with an instant column differ from native Exasol.
* An instant column holds UTC instants: an Iceberg `timestamptz` or `timestamptz_ns`, a Delta or Unity Catalog `timestamp`, or a direct-storage Parquet timestamp that is `isAdjustedToUTC` or INT96. The virtual schema declares it a plain `TIMESTAMP` that holds each instant's UTC date and time (`scan-types/type-mapping-timestamp-precision`). An instant operand is an instant column, or an expression over one that native Exasol types `TIMESTAMP WITH LOCAL TIME ZONE` when the column is one, such as `MAX(tz)`, `ADD_HOURS(tz, 1)`, or `COALESCE(tz, ...)`. A value stands beside it when both are operands of one comparison, `BETWEEN`, `IN`, or function call.

## Scenarios

### Scenario: The five clock functions are advertised

* *GIVEN* a virtual schema without `EXCLUDED_CAPABILITIES`
* *WHEN* Exasol requests `getCapabilities`
* *THEN* the response SHALL advertise `FN_CURRENT_DATE`, `FN_CURRENT_TIMESTAMP`, `FN_LOCALTIMESTAMP`, `FN_SYSDATE`, and `FN_SYSTIMESTAMP`
* *AND* the response SHALL NOT advertise `FN_DBTIMEZONE` or `FN_SESSIONTIMEZONE`
* *AND* `EXCLUDED_CAPABILITIES` SHALL withhold each of the five, so Exasol evaluates a withheld one itself (`vs-properties/excluded-capabilities`)

### Scenario: A clock function reaches the pushed SQL as a literal

* *GIVEN* a virtual table `t` with a `DATE` column `d` and a `TIMESTAMP` column `ts` that is no instant column
* *WHEN* the user runs `EXPLAIN VIRTUAL` of `SELECT * FROM t WHERE d <= CURRENT_DATE`, `WHERE d <= SYSDATE`, `WHERE ts < LOCALTIMESTAMP`, `WHERE ts < SYSTIMESTAMP`, and `WHERE ts < CURRENT_TIMESTAMP`, each in its own statement
* *THEN* the scan's filter SHALL hold a `DATE` literal in place of `CURRENT_DATE` and `SYSDATE`, and a `TIMESTAMP` literal with the function's fractional digits in place of the three timestamp functions
* *AND* the pushed SQL MUST NOT name any of the five functions, so the scan never reads a clock and every value of the statement comes from the adapter
* *AND* each query SHALL return the rows that native Exasol returns for the same filter over the same data in a session whose `SESSIONTIMEZONE` equals the folding zone

### Scenario: A TIMESTAMP WITH LOCAL TIME ZONE literal in a comparison runs in the scan

* *GIVEN* a virtual table `t` with a `TIMESTAMP` column `ts` that is no instant column, and a session whose `SESSIONTIMEZONE` equals the folding zone `EUROPE/BERLIN`
* *WHEN* the user runs `SELECT * FROM t WHERE ts > CAST(TIMESTAMP '2024-03-01 10:00:00' AS TIMESTAMP WITH LOCAL TIME ZONE)` and `SELECT * FROM t WHERE ts > ADD_HOURS(CURRENT_TIMESTAMP, -1)`
* *THEN* `EXPLAIN VIRTUAL` SHALL show the scan's filter comparing with the plain `TIMESTAMP` literal of the instant's wall-clock time in the folding zone, `2024-03-01 10:00:00.000` for the first query, and MUST NOT show `SESSIONTIMEZONE` in that filter
* *AND* each query SHALL return the rows that native Exasol returns in that session

### Scenario: The clock functions fold in the zone that Exasol uses when the session zone matches

* *GIVEN* a virtual schema without `SESSION_TIME_ZONE`, and a session whose `SESSIONTIMEZONE` equals `DBTIMEZONE`, which is not UTC
* *WHEN* the session selects each of the five functions from a virtual table, and compares each with a `DATE` column and with a `TIMESTAMP` column that is no instant column
* *THEN* each function SHALL fold to the date or wall-clock time in the `TZ` zone
* *AND* each projected value SHALL be within 60 seconds of the same session's native value, and each comparison SHALL return the rows that native Exasol returns
* *AND* `SELECT CURRENT_TIMESTAMP FROM t` SHALL return a `TIMESTAMP WITH LOCAL TIME ZONE` column, the type native Exasol returns
* *AND* a projected `CURRENT_TIMESTAMP(6)` or `TIMESTAMP WITH LOCAL TIME ZONE` literal SHALL keep every fractional digit that native Exasol shows

### Scenario: A projected clock function keeps the aggregate split

* *GIVEN* a virtual table `t` with a column `region`
* *WHEN* the user runs `SELECT CURRENT_TIMESTAMP AS run_at, region, COUNT(*) FROM t GROUP BY region` and `SELECT SYSDATE, COUNT(*) FROM t`
* *THEN* `EXPLAIN VIRTUAL` SHALL show the per-shard partial aggregation that the same queries without the clock column show
* *AND* each query SHALL return native Exasol's groups and counts, with a clock value within 60 seconds of the native one

### Scenario: Every clock function of a statement folds from one reading

* *GIVEN* two virtual tables `t1` and `t2` of one virtual schema
* *WHEN* the user runs `EXPLAIN VIRTUAL` of a statement that joins `t1` and `t2` and filters each on `SYSTIMESTAMP`, and of a statement `SELECT SYSTIMESTAMP FROM t1 UNION ALL SELECT SYSTIMESTAMP FROM t2`
* *THEN* every folded literal of one statement SHALL hold the same instant, across its tables, its pushdown requests, and its shards
* *AND* every row of `SELECT SYSTIMESTAMP FROM t1 UNION ALL SELECT SYSTIMESTAMP FROM t2` SHALL hold the same value

### Scenario: A filter on a clock function or a time zone literal prunes files at plan time

* *GIVEN* a virtual table whose data files hold disjoint ranges of a `DATE` column `d` and a `TIMESTAMP` column `ts` that is no instant column, all before today, and a virtual table identity-partitioned on a `DATE` column `p`
* *WHEN* the user runs `EXPLAIN VIRTUAL` of `WHERE d >= CURRENT_DATE`, of `WHERE p = CURRENT_DATE`, and of `WHERE ts < CAST(TIMESTAMP '<t>' AS TIMESTAMP WITH LOCAL TIME ZONE)`, with `<t>` chosen so that only some files hold a matching row
* *THEN* the pushed SQL SHALL list no file for the first two queries, and for the third only the files whose range of `ts` can hold a matching row
* *AND* each query SHALL return the rows native Exasol returns in a session whose `SESSIONTIMEZONE` equals the folding zone

### Scenario: A TIMESTAMP WITH LOCAL TIME ZONE value beside an instant operand compares the instants

* *GIVEN* a virtual table `t` with a column `region` and an instant column `tz` whose data files hold disjoint ranges of `tz`, a virtual schema whose folding zone is `EUROPE/BERLIN`, and a session with `ALTER SESSION SET TIME_ZONE = 'PACIFIC/KIRITIMATI'`
* *WHEN* the user runs `SELECT * FROM t WHERE tz > CAST(TIMESTAMP '2024-03-01 10:00:00' AS TIMESTAMP WITH LOCAL TIME ZONE)`, `WHERE tz > ADD_HOURS(CURRENT_TIMESTAMP, -1)`, `WHERE tz BETWEEN CAST(TIMESTAMP '2024-03-01 10:00:00' AS TIMESTAMP WITH LOCAL TIME ZONE) AND CURRENT_TIMESTAMP`, `WHERE tz IN (CAST(TIMESTAMP '2024-03-01 10:00:00' AS TIMESTAMP WITH LOCAL TIME ZONE))`, `SELECT CASE WHEN tz < CURRENT_TIMESTAMP THEN 1 ELSE 0 END FROM t`, and, with `v` the value `CAST(TIMESTAMP '2024-03-01 23:30:00' AS TIMESTAMP WITH LOCAL TIME ZONE)`, `WHERE ADD_HOURS(tz, 1) > v`, `SELECT HOURS_BETWEEN(v, tz) FROM t`, and `SELECT region FROM t GROUP BY region HAVING MAX(tz) >= v`
* *THEN* `EXPLAIN VIRTUAL` SHALL show the scan comparing `tz` with the plain `TIMESTAMP` literal of each instant's UTC date and time, `2024-02-29 20:00:00.000` for the first query, and MUST NOT show `SESSIONTIMEZONE` in the scan's filter
* *AND* each query SHALL return the rows and values that native Exasol returns in that session over a native table whose `TIMESTAMP WITH LOCAL TIME ZONE` column holds the same instants, and the same rows with `SESSION_TIME_ZONE = 'ASIA/TOKYO'` and in a session whose zone is UTC, because no zone takes part in the comparison
* *AND* the pushed SQL of the first query SHALL list only the files whose range of `tz` can hold a matching row
* *AND* on Exasol 8, where `LOCALTIMESTAMP` is a `TIMESTAMP WITH LOCAL TIME ZONE`, `WHERE tz < LOCALTIMESTAMP` SHALL compare the instants as `WHERE tz < CURRENT_TIMESTAMP` does

### Scenario: A plain value compared with an instant column compares with its UTC date and time

* *GIVEN* a virtual table `t` with an instant column `tz`, and a session whose `SESSIONTIMEZONE` equals the folding zone, which is not UTC
* *WHEN* the user runs `SELECT * FROM t WHERE tz > TIMESTAMP '2024-03-01 10:00:00'`, `WHERE tz >= DATE '2024-03-01'`, `WHERE tz <= SYSTIMESTAMP`, and `WHERE tz >= CURRENT_DATE`
* *THEN* the scan SHALL compare the UTC date and time of `tz` with `2024-03-01 10:00:00`, with `2024-03-01 00:00:00`, with the wall-clock time of `SYSTIMESTAMP` in the `TZ` zone, and with the start of today in the folding zone
* *AND* each query SHALL return the rows that native Exasol returns in that session over a native `TIMESTAMP` column that holds the UTC dates and times of the same instants
* *AND* on Exasol 2025, where `LOCALTIMESTAMP` is a `TIMESTAMP`, `WHERE tz <= LOCALTIMESTAMP` SHALL compare with the wall-clock time in the folding zone
