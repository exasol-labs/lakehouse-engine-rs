# Feature: Pushdown Planning, Scan Text Conversion

The scan computes an expression only when DataFusion returns what native Exasol returns under every session setting. A conversion between text and a type that Exasol formats or parses by session settings runs in Exasol.

## Background

* Exasol writes a DATE by `NLS_DATE_FORMAT`, a TIMESTAMP by `NLS_TIMESTAMP_FORMAT`, and a DOUBLE or a DECIMAL with a scale above 0 by `NLS_NUMERIC_CHARACTERS`, and reads text into these types by the same settings. The pushdown request carries none of them. DataFusion always uses ISO dates, a `T` timestamp separator, and a `.` decimal point.
* An integer (`DECIMAL(p,0)`) has the same text in every session. A BOOLEAN is `TRUE` or `FALSE` in every session.
* Exasol converts text implicitly in a comparison, `IN`, `BETWEEN`, `CASE`, `COALESCE`, `NVL`, `GREATEST`, `LEAST`, `NULLIF`, and the argument of a non-text function or operator. It takes text without conversion in `LIKE`, `||`, string-function text arguments, text-to-text comparisons and CASTs, and a format or unit parameter.
* The rule holds for every expression the scan computes: the WHERE filter, the select list, GROUP BY keys, aggregates and their arguments, `COUNT(DISTINCT expr)`, the broadcast join condition and filter, and each N-scan leg filter. An aggregate is typed as a whole, so a bare-column argument meets the same checks as an expression.
* `SUM`, `AVG`, `STDDEV`, and `VARIANCE` with their `_POP` and `_SAMP` forms convert text to a number by `NLS_NUMERIC_CHARACTERS` and a BOOLEAN to 1 or 0. DataFusion computes them only over numbers.
* A declined expression takes the path's existing fallback: a WHERE filter runs in the qualified wrapper (`pushdown/pushdown-declined-filter-self-apply`), a select item widens the projection to the full base row, an aggregate or group key routes to the qualified wrapper, and a join condition or filter loses the broadcast plan.

## Scenarios

### Scenario: Text the session format converts runs in Exasol

* *GIVEN* an expression that converts a DATE, TIMESTAMP, DOUBLE, or DECIMAL with a scale above 0 to text, or text to one of these types, explicitly through a CAST or `TO_DATE`/`TO_TIMESTAMP` without a format, or implicitly, such as `date_col LIKE '2024%'`, `date_col = '2024-01-01'`, or `qty + '1'`
* *WHEN* the adapter plans the request
* *THEN* the scan SHALL NOT compute the expression, and the path's fallback SHALL apply it in Exasol
* *AND* the result SHALL equal native Exasol under any `NLS_DATE_FORMAT`, `NLS_TIMESTAMP_FORMAT`, and `NLS_NUMERIC_CHARACTERS`, including the native error for text that does not parse

### Scenario: Integer and boolean text runs in the scan

* *GIVEN* an expression that converts an integer or a BOOLEAN to text, such as `CAST(qty AS VARCHAR(20)) LIKE '1%'`, `UPPER(qty)`, or `CAST(flag AS VARCHAR(5)) = 'TRUE'`
* *WHEN* the adapter plans the request
* *THEN* the scan SHALL compute it, writing an integer in plain digits and a BOOLEAN as `TRUE` or `FALSE`
* *AND* a CAST to a text type too short for the source's widest text, such as an integer of precision 9 into `VARCHAR(5)` or a BOOLEAN into `VARCHAR(4)`, SHALL run in Exasol

### Scenario: A digits-only literal compares with an integer as a number

* *GIVEN* a comparison, `IN`, or `BETWEEN` of integer operands with a text literal of 1 to 18 digits and an optional minus sign, and no more digits than any operand's precision, such as `int_col = '2147483648'` against `DECIMAL(10,0)`
* *WHEN* the adapter renders it for the scan
* *THEN* the scan SHALL compare the literal as an integer literal, so a value outside the column's range matches no row instead of failing the scan

### Scenario: A digits-only literal wider than the integer's precision runs in Exasol

* *GIVEN* a comparison, `IN`, or `BETWEEN` of an integer operand of precision p with a digits-only text literal of more than p digits, such as `qty IN ('7', '99999999999')` against `DECIMAL(10,0)`
* *WHEN* the adapter plans the request
* *THEN* the scan SHALL NOT compute the expression, and the path's fallback SHALL apply it in Exasol
* *AND* the result SHALL equal native Exasol, including the numeric out-of-range error Exasol raises when it converts the literal to the operand's type

### Scenario: A CAST of text to CHAR runs in Exasol

* *GIVEN* a CAST of a text value to `CHAR(n)`
* *WHEN* the adapter plans the request
* *THEN* the scan SHALL NOT compute it, because the scan does not blank-pad, and the result SHALL equal native Exasol

### Scenario: A CAST of text to a shorter VARCHAR runs unchecked in the scan

* *GIVEN* a CAST of a text value to a `VARCHAR(n)` shorter than its source, or to an `ASCII` target
* *WHEN* the scan computes it
* *THEN* the scan SHALL keep the whole value, with no length or ASCII check
* *AND* a value that does not fit SHALL return a result in a filter, where native Exasol fails
* *AND* trailing spaces beyond `n`, which native Exasol cuts, SHALL stay
* *AND* a select item over a value that does not fit SHALL fail at the emit

### Scenario: A numeric aggregate over text or BOOLEAN runs in Exasol

* *GIVEN* a `SUM`, `AVG`, `STDDEV`, or `VARIANCE` aggregate, or one of their `_POP` and `_SAMP` forms, whose argument is text, a BOOLEAN, a DATE, a TIMESTAMP, or an expression of one of these types, such as `AVG(flag)` or `SUM(CAST(qty AS VARCHAR(20)))`
* *WHEN* the adapter plans the request
* *THEN* the aggregate SHALL NOT split into scan partials, and the qualified wrapper SHALL compute it in Exasol
* *AND* the result SHALL equal native Exasol under any `NLS_NUMERIC_CHARACTERS`, including the native error for text that does not parse
* *AND* the same aggregate over a DOUBLE, a DECIMAL, or an arithmetic expression over them SHALL keep the split

### Scenario: A numeric aggregate over a bare NULL runs in Exasol

* *GIVEN* a `SUM`, `AVG`, `STDDEV`, or `VARIANCE` aggregate, or one of their `_POP` and `_SAMP` forms, whose argument is a NULL literal, such as `SUM(NULL)` or `SUM(CAST(NULL AS DECIMAL(10,0)))`, which Exasol sends as a bare NULL
* *WHEN* the adapter plans the request, with or without GROUP BY
* *THEN* the aggregate SHALL NOT split into scan partials, and the qualified wrapper SHALL compute it in Exasol
* *AND* the result SHALL equal native Exasol

### Scenario: A BOOLEAN converts to 1 or 0 where Exasol converts it to a number

* *GIVEN* a CAST of a BOOLEAN to `DECIMAL(p,s)`, or a comparison, `BETWEEN`, or simple `CASE` of a BOOLEAN with a number, such as `CAST(flag AS INTEGER)` or `flag = 2`, which DataFusion rejects
* *WHEN* the adapter plans the request
* *THEN* the scan SHALL compute it with `TRUE` as 1 and `FALSE` as 0, and the result SHALL equal native Exasol
* *AND* a CAST to a `DECIMAL(p,s)` whose p equals s, which cannot hold 1, SHALL run in Exasol
* *AND* an `IN` list of numbers over a BOOLEAN SHALL run in Exasol, because Exasol converts the numbers to BOOLEAN and fails on any but 0 and 1

### Scenario: A broadcast join keeps its plan for an integer LIKE

* *GIVEN* a broadcast-eligible inner equi-join whose WHERE filter holds `LIKE` over an integer column of one leg
* *WHEN* the adapter renders the join
* *THEN* the join SHALL keep the broadcast plan, and the scan SHALL apply the filter
* *AND* the rows SHALL equal the same filter applied to that leg alone, then joined

### Scenario: A join condition or filter that converts a date to text takes the N-scan fallback

* *GIVEN* an inner equi-join whose condition or WHERE filter converts a DATE to text, such as `o_orderdate LIKE '2024-01-0%'`
* *WHEN* the adapter renders the join
* *THEN* the join SHALL lose the broadcast plan and take the N-scan fallback, whose outer WHERE applies the conversion in Exasol
* *AND* the rows SHALL equal native Exasol under any `NLS_DATE_FORMAT`
