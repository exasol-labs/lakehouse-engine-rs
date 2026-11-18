<!-- DELTA:CHANGED -->
# Feature: Session Time Zone

The `SESSION_TIME_ZONE` virtual schema property names the zone that stands in for the session time zone when the adapter folds a clock function or a compared `TIMESTAMP WITH LOCAL TIME ZONE` literal (`pushdown/pushdown-planning-clock-functions`). `SYSDATE` and `SYSTIMESTAMP` fold in the zone of the adapter VM's `TZ` variable. A `TIMESTAMP WITH LOCAL TIME ZONE` value compared with an instant column folds in no zone.
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
## Background

* No adapter or UDF request carries `SESSIONTIMEZONE`. Exasol sets the `TZ` environment variable of the adapter VM to `DBTIMEZONE`, as a zone name or, for a numeric zone, expectedly as a POSIX `UTC±N` string with the sign inverted. A `%env TZ=<zone>;` line in the adapter script source overrides it.
* The folding zone is the `SESSION_TIME_ZONE` zone when the property is set, and the `TZ` zone otherwise. It applies to `CURRENT_DATE`, `CURRENT_TIMESTAMP`, `LOCALTIMESTAMP`, and every compared `TIMESTAMP WITH LOCAL TIME ZONE` literal, except a `TIMESTAMP WITH LOCAL TIME ZONE` value compared with an instant column, which folds to its UTC date and time.
* Exasol parses `ALTER VIRTUAL SCHEMA ... SET TIME_ZONE` as its own syntax and refuses a quoted property name, so the property is not named `TIME_ZONE`.
* `docs/semantic-differences.md` lists where the folded values differ from native Exasol.
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: A session zone that differs from the folding zone gets the folding zone's values

* *GIVEN* a virtual schema without `SESSION_TIME_ZONE`, a virtual table `t` whose `TIMESTAMP` column `ts` is no instant column, and a session with `ALTER SESSION SET TIME_ZONE = 'ASIA/TOKYO'` while `DBTIMEZONE` is not `ASIA/TOKYO`
* *WHEN* the session runs `SELECT CURRENT_DATE, LOCALTIMESTAMP FROM t`, `SELECT CURRENT_TIMESTAMP FROM t`, and `SELECT COUNT(*) FROM t WHERE ts > CAST(TIMESTAMP '<x>' AS TIMESTAMP WITH LOCAL TIME ZONE)`
* *THEN* `CURRENT_DATE` SHALL be the date in the `TZ` zone, not in `ASIA/TOKYO`, and on Exasol 2025 `LOCALTIMESTAMP` SHALL be the wall-clock time in the `TZ` zone, as `docs/semantic-differences.md` documents
* *AND* the comparison SHALL compare `ts` with the instant's wall-clock time in the `TZ` zone, not with `<x>`
* *AND* the projected `CURRENT_TIMESTAMP`, and on Exasol 8 the projected `LOCALTIMESTAMP`, SHALL be the same session's native value, because a projected `TIMESTAMP WITH LOCAL TIME ZONE` value is shown in the session zone
* *AND* `ALTER VIRTUAL SCHEMA ... SET SESSION_TIME_ZONE = 'ASIA/TOKYO'` SHALL make the next query of the same session return the `ASIA/TOKYO` date and compare with `<x>`, without a `REFRESH`
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: An unreadable TZ fails only a query that needs it

* *GIVEN* an adapter script whose source holds `%env TZ=NOT/A_ZONE;`, and a virtual schema on it without `SESSION_TIME_ZONE`
* *WHEN* the user runs a query over a virtual table that uses `SYSDATE`, a query that compares a `TIMESTAMP` column that is no instant column with a `TIMESTAMP WITH LOCAL TIME ZONE` literal, a query that compares an instant column with that literal, and a query that uses neither a clock function nor such a literal
* *THEN* the first two queries SHALL fail with an error that names the `TZ` value `NOT/A_ZONE`, and MUST NOT fold in UTC or in any other zone
* *AND* the query over the instant column and the query that uses neither SHALL succeed, because neither reads `TZ`
* *AND* the same failure SHALL occur when `TZ` is empty or unset
* *AND* after `ALTER VIRTUAL SCHEMA ... SET SESSION_TIME_ZONE = 'EUROPE/BERLIN'`, the query with the `TIMESTAMP WITH LOCAL TIME ZONE` literal and a query that uses only `CURRENT_DATE`, `CURRENT_TIMESTAMP`, or `LOCALTIMESTAMP` SHALL succeed, because they then do not read `TZ`
<!-- /DELTA:CHANGED -->
