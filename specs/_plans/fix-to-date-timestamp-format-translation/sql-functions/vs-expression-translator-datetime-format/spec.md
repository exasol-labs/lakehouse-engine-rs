# Feature: VS Expression Translator — Datetime Format Models

Translates the format model of a pushed `TO_DATE` or `TO_TIMESTAMP` call into the strftime pattern
that the node-local scan parses, so the conversion runs in the scan and returns the value native
Exasol returns. A format with no exact strftime equivalent is declined, and Exasol evaluates that
call itself.

## Background

* Exasol pushes `TO_DATE(<source>, <format>)` and `TO_TIMESTAMP(<source>, <format>)` with the
  Exasol format model it received, such as `'YYYY-MM-DD HH24:MI:SS'`. The scan's `to_date` and
  `to_timestamp` parse strftime patterns, such as `'%Y-%m-%d %H:%M:%S'`.
* **Translated elements.** `YYYY` → `%Y`, `MM` → `%m`, `DD` → `%d`, `HH24` → `%H`, `MI` → `%M`,
  `SS` → `%S`, and `FF3`, `FF6`, `FF9` → a fraction of exactly 3, 6, or 9 digits (`%3f`, `%6f`,
  `%9f`). A `.` directly before `FFn` joins it as `%.3f`, `%.6f`, or `%.9f`, which also accepts a
  value without a fraction, as Exasol does. Elements match in any letter case, as in Exasol. The
  separators `-`, `/`, `.`, `,`, `;`, `:`, and space are copied unchanged.
* **Structural rules.** A translatable format names each of `YYYY`, `MM`, and `DD` exactly once and
  names no element twice. A `TO_DATE` format holds no time element. The time elements of a
  `TO_TIMESTAMP` format are none, `HH24` and `MI`, those two and `SS`, or those three and one
  `FFn`.
* **Measured parse differences.** For a translated format, the following inputs give a different
  outcome than native Exasol.
  * The scan raises an error where Exasol returns a value: extra spaces (trailing, or before a
    separator), a time after a `TO_DATE` date, a value that ends before its format, a fraction
    with fewer digits than `FFn` or with more digits up to nine, an empty string (Exasol reads it
    as NULL), and a `TO_TIMESTAMP` value outside 1677-09-21 to 2262-04-11.
  * The scan returns a value where Exasol raises an error: a `+` before the year; a `-` before the
    year or a year of 0 under `TO_DATE`, which the scan evaluates in a filter and rejects only when
    it emits the date; a tab, line break, or other whitespace that is not a space before any
    numeric field; a tab or no whitespace where the format has a space; and a seconds value of
    `60`, which the scan reads as the next minute.

## Scenarios

### Scenario: A string-literal format translates to a strftime pattern

* *GIVEN* a `function_scalar` node named `TO_DATE` or `TO_TIMESTAMP` with a source argument and a `literal_string` format that uses only the Background's translated elements and separators and meets its structural rules
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL render `to_date(<source_sql>, '<pattern>')` or `to_timestamp(<source_sql>, '<pattern>')`, where `<pattern>` replaces each element with its strftime form, so `'YYYY-MM-DD'` renders as `'%Y-%m-%d'` and `'yyyy-mm-dd hh24:mi:ss.ff3'` as `'%Y-%m-%d %H:%M:%S%.3f'`
* *AND* a query over a virtual schema table that pushes such a call into the scan SHALL return the value native Exasol returns for a string that is not one of the Background's measured parse differences, so the strings `1996-03-13` and `1996-03-13 10:11:12` under `YYYY-MM-DD` and `YYYY-MM-DD HH24:MI:SS` yield the DATE `1996-03-13` and the TIMESTAMP `1996-03-13 10:11:12`

### Scenario: A format the translation cannot express declines in the DataFusion dialect

* *GIVEN* a `function_scalar` node named `TO_DATE` or `TO_TIMESTAMP` with more than two arguments, or whose format argument is not a `literal_string`, holds an element or character outside the Background's translated set (such as `MON`, `MONTH`, `DY`, `DAY`, `IW`, `IYYY`, `RR`, `HH12`, `FF1`, or quoted text), or breaks the Background's structural rules
* *WHEN* the node is rendered in each dialect
* *THEN* the DataFusion dialect SHALL return an error in raising mode and `None` in the safe variants
* *AND* the Exasol dialect SHALL render the call verbatim, with the name, arguments, and format model Exasol sent
* *AND* the caller SHALL treat that asymmetry as a decline to self-apply (`pushdown/pushdown-declined-filter-self-apply`), because `FN_TO_DATE` and `FN_TO_TIMESTAMP` stay advertised and Exasol delegates every pushed call

### Scenario: A declined format returns the native result on every pushdown path

* *GIVEN* a query over virtual schema tables with a `TO_DATE` or `TO_TIMESTAMP` call whose format the DataFusion dialect declines, such as `'DD-MON-YYYY'` or a format built from a column
* *WHEN* the call is in the select list, a `WHERE` filter, a `GROUP BY` key, an aggregate argument, or a join filter, with join pushdown on or off
* *THEN* the query SHALL return the rows the same query returns on native Exasol tables
* *AND* the generated pushdown SQL SHALL evaluate the call in Exasol, outside the scan UDF

### Scenario: A string Exasol rejects for a translated format fails the query

* *GIVEN* a query over a virtual schema table that pushes a `TO_DATE` or `TO_TIMESTAMP` call with a translated format into the scan
* *WHEN* the scan reads a string that native Exasol rejects for that format, such as `event-01` or `2024-02-30` for `YYYY-MM-DD`, and that is not one of the Background's measured parse differences
* *THEN* the query SHALL fail with an error, in the select list and in a filter, as it does on a native Exasol table
* *AND* the scan MUST NOT return NULL for that string or silently drop its row
