# Feature: Pushdown Planning, Scan Text Conversion

The scan computes an expression only when DataFusion returns what native Exasol returns under every session setting. A conversion between text and a type that Exasol formats or parses by session settings runs in Exasol.

<!-- DELTA:CHANGED -->
## Background

* Exasol writes a DATE by `NLS_DATE_FORMAT`, a TIMESTAMP by `NLS_TIMESTAMP_FORMAT`, and a DOUBLE or a DECIMAL with a scale above 0 by `NLS_NUMERIC_CHARACTERS`, and reads text into these types by the same settings. The pushdown request carries none of them. DataFusion always uses ISO dates, a `T` timestamp separator, and a `.` decimal point.
* An integer (`DECIMAL(p,0)`) has the same text in every session. A BOOLEAN is `TRUE` or `FALSE` in every session.
* Exasol converts text implicitly in a comparison, `IN`, `BETWEEN`, `CASE`, `COALESCE`, `NVL`, `GREATEST`, `LEAST`, `NULLIF`, and the argument of a non-text function or operator. It takes text without conversion in `LIKE`, `||`, string-function text arguments, text-to-text comparisons and CASTs, and a format or unit parameter.
* The rule holds for every expression the scan computes: the WHERE filter, the select list, GROUP BY keys, aggregates and their arguments, `COUNT(DISTINCT expr)`, the broadcast join condition and filter, and each N-scan leg filter. An aggregate is typed as a whole, so a bare-column argument meets the same checks as an expression.
* A declined expression takes the path's existing fallback: a WHERE conjunct runs in the qualified wrapper's `WHERE` (`pushdown/pushdown-declined-filter-self-apply`), a select item widens the projection to the full base row, an aggregate or group key routes to the qualified wrapper, and a join condition or filter loses the broadcast plan.
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Text the session format converts runs in Exasol

* *GIVEN* an expression that converts a DATE, TIMESTAMP, DOUBLE, or DECIMAL with a scale above 0 to text, or text to one of these types, explicitly through a CAST or `TO_DATE`/`TO_TIMESTAMP` without a format, or implicitly, such as `date_col LIKE '2024%'`, `ts_col = '2024-01-01 10:00:00'`, `date_col = varchar_col`, or `qty + '1'`
* *AND* the expression is not a text literal compared with a DATE, which "Exasol converts a text literal compared with a DATE before the scan" covers
* *WHEN* the adapter plans the request
* *THEN* the scan SHALL NOT compute the expression, and the path's fallback SHALL apply it in Exasol
* *AND* the result SHALL equal native Exasol under any `NLS_DATE_FORMAT`, `NLS_TIMESTAMP_FORMAT`, and `NLS_NUMERIC_CHARACTERS`, including the native error for text that does not parse
<!-- /DELTA:CHANGED -->

<!-- DELTA:NEW -->
### Scenario: Exasol converts a text literal compared with a DATE before the scan

* *GIVEN* a comparison (`=`, `<>`, `<`, `<=`, `>`, `>=`), `BETWEEN`, `IN`, or simple `CASE` of a DATE operand, such as a DATE column, a CAST to DATE, or `ADD_DAYS` over a DATE, with one or more text literals, such as `d = '01.02.2024'`, `d BETWEEN '01.01.2024' AND '31.01.2024'`, or `d IN ('01.01.2024', '02.01.2024')` under `NLS_DATE_FORMAT = 'DD.MM.YYYY'`
* *WHEN* the adapter plans the request
* *THEN* the scan SHALL compute the expression, and the dispatch SQL SHALL make Exasol convert each such literal to a DATE by the session's `NLS_DATE_FORMAT` and pass the scan the converted value, with the literal's request text only inside an Exasol string literal
* *AND* the scan SHALL compare the DATE operand with the converted value as with the same DATE literal, and with NULL where the literal converts to NULL
* *AND* the result SHALL equal native Exasol under any `NLS_DATE_FORMAT`, in the WHERE filter, a select item, a GROUP BY key, an aggregate argument, and a broadcast join condition or filter, which keeps its broadcast plan
* *AND* a literal that does not parse SHALL fail the query with Exasol's conversion error whenever a shard scans, also when no row would reach the comparison, as `docs/semantic-differences.md` documents
* *AND* the adapter's file pruning SHALL derive no bound from the literal, because the adapter does not know the converted value
<!-- /DELTA:NEW -->
