# Feature: Pushdown Planning, Scan Text Conversion

The scan computes an expression only when DataFusion returns what native Exasol returns under every session setting. A conversion between text and a type that Exasol formats or parses by session settings runs in Exasol.

<!-- DELTA:CHANGED -->
## Background

* Exasol writes a DATE by `NLS_DATE_FORMAT`, a TIMESTAMP by `NLS_TIMESTAMP_FORMAT`, and a DOUBLE or a DECIMAL with a scale above 0 by `NLS_NUMERIC_CHARACTERS`, and reads text into these types by the same settings. The pushdown request carries none of them. DataFusion always uses ISO dates, a `T` timestamp separator, and a `.` decimal point.
* An integer (`DECIMAL(p,0)`) has the same text in every session. A BOOLEAN is `TRUE` or `FALSE` in every session.
* Exasol converts text implicitly in a comparison, `IN`, `BETWEEN`, `CASE`, `COALESCE`, `NVL`, `GREATEST`, `LEAST`, `NULLIF`, and the argument of a non-text function or operator. It takes text without conversion in `LIKE`, `||`, string-function text arguments, text-to-text comparisons and CASTs, and a format or unit parameter.
* The rule holds for every expression the scan computes: the WHERE filter, the select list, GROUP BY keys, aggregates and their arguments, `COUNT(DISTINCT expr)`, the broadcast join condition and filter, and each N-scan leg filter. An aggregate is typed as a whole, so a bare-column argument meets the same checks as an expression. A declined expression takes the path's existing fallback: a WHERE filter runs in the qualified wrapper (`pushdown/pushdown-declined-filter-self-apply`), a select item widens the projection to the full base row, an aggregate or group key routes to the qualified wrapper, a join filter runs in Exasol's `WHERE` over the broadcast fan-out, and a join key the scan declines keeps its pair off the broadcast.
* `SUM`, `AVG`, `STDDEV`, and `VARIANCE` with their `_POP` and `_SAMP` forms convert text to a number by `NLS_NUMERIC_CHARACTERS` and a BOOLEAN to 1 or 0. DataFusion computes them only over numbers.
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:REMOVED -->
### Scenario: A join condition or filter that converts a date to text takes the N-scan fallback

* *GIVEN* an inner equi-join whose filter converts a DATE to text
* *WHEN* the adapter renders the join
* *THEN* this behavior SHALL be removed; per "A join filter that converts a date to text runs in Exasol, and such a join key keeps the N-scan"
<!-- /DELTA:REMOVED -->

<!-- DELTA:NEW -->
### Scenario: A join filter that converts a date to text runs in Exasol, and such a join key keeps the N-scan

* *GIVEN* an inner equi-join whose WHERE filter or `ON` filter converts a DATE to text, such as `o_orderdate LIKE '2024-01-0%'`, or whose join key does, such as `ON TO_CHAR(o.o_orderdate) = c.c_label`
* *WHEN* the adapter renders the join
* *THEN* a filter that converts a DATE to text SHALL keep the broadcast plan and run in Exasol's `WHERE` over the broadcast fan-out (`pushdown/pushdown-declined-filter-self-apply`)
* *AND* a join key that converts a DATE to text SHALL keep its pair off the broadcast, so a two-leg join takes the N-scan fallback, whose `ON` applies the conversion in Exasol, and a join of three or more legs broadcasts the next eligible pair (`pushdown-joins/pushdown-planning-join`)
* *AND* the rows SHALL equal native Exasol under any `NLS_DATE_FORMAT`
<!-- /DELTA:NEW -->
