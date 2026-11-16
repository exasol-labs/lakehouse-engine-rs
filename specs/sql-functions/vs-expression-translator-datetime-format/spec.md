# Feature: VS Expression Translator, Datetime Format Models

Translates the format model of a pushed `TO_DATE` or `TO_TIMESTAMP` call into the strftime pattern
that the node-local scan parses, so the conversion runs in the scan and returns the value native
Exasol returns. A format with no exact strftime equivalent is declined, and Exasol evaluates that
call itself.

## Background

* **Supported formats.** A string-literal format built from `YYYY`, `MM`, and `DD` once each, in
  any letter case. For `TO_TIMESTAMP` only, it may also hold `HH24` and `MI`, then optionally
  `SS`, then optionally one `.FF3`, `.FF6`, or `.FF9`, which joins as `%.f` and reads one or more digits.
  Separators are `-` `/` `.` `,` `;` `:` `_` and space.
* **Everything else** runs in Exasol.

The inputs on which the scan and Exasol differ are documented in `docs/semantic-differences.md`.

## Scenarios

### Scenario: A string-literal format translates to a strftime pattern

* *GIVEN* a `function_scalar` node named `TO_DATE` or `TO_TIMESTAMP` with a source argument and a `literal_string` format that is a supported format
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL render `to_date(nullif(<source_sql>, ''), '<pattern>')` or `vs_to_timestamp(nullif(<source_sql>, ''), '<pattern>', <digits>)`, so an empty string yields NULL as in Exasol, where `<pattern>` replaces each element with its strftime form, so `'YYYY-MM-DD'` renders as `'%Y-%m-%d'` and `'yyyy-mm-dd hh24:mi:ss.ff3'` as `'%Y-%m-%d %H:%M:%S%.f'`
* *AND* `<digits>` SHALL be the fractional digits the typed pass derives for the call (`sql-functions/vs-expression-translator-date-add-fns`), else the `n` of the format's `FFn`, else 0
* *AND* a query over a virtual schema table that pushes such a call into the scan SHALL return the value native Exasol returns for a string that is not one of the documented parse differences, so the strings `1996-03-13` and `1996-03-13 10:11:12` under `YYYY-MM-DD` and `YYYY-MM-DD HH24:MI:SS` yield the DATE `1996-03-13` and the TIMESTAMP `1996-03-13 10:11:12`

### Scenario: The scan reads a TO_TIMESTAMP string as Exasol does

* *GIVEN* a query over a virtual schema table that pushes a `TO_TIMESTAMP` call with a translated format into the scan, alone, inside a date function or a CASE, in a filter, or as a split-aggregate argument or group key
* *WHEN* the scan reads a string for that format
* *THEN* the scan SHALL return the value native Exasol returns for any date from 0001-01-01 to 9999-12-31, such as `9999-12-31` and `1600-01-01`
* *AND* a string that ends after the date but before the format, such as `2024-01-31` or `2024-01-31 10` under `YYYY-MM-DD HH24:MI:SS`, SHALL read its missing time fields as 0, while a string that ends inside the date SHALL fail
* *AND* a fraction under `FFn` SHALL read one or more digits and keep the first `n`, and on Exasol 8 the first 3
* *AND* a second of 60 SHALL fail, as in Exasol
* *AND* the scan SHALL compute the value in microseconds, and in nanoseconds only under `FF9` on Exasol 2025, where a value outside 1677-09-21 to 2262-04-11 fails (`docs/semantic-differences.md`)

### Scenario: A format the translation cannot express declines in the DataFusion dialect

* *GIVEN* a `function_scalar` node named `TO_DATE` or `TO_TIMESTAMP` with more than two arguments, or a format that is not a supported format
* *WHEN* the node is rendered in each dialect
* *THEN* the DataFusion dialect SHALL return an error
* *AND* the Exasol dialect SHALL render the call verbatim, with the name, arguments, and format model Exasol sent
* *AND* the caller SHALL treat that asymmetry as a decline to self-apply (`pushdown/pushdown-declined-filter-self-apply`), because `FN_TO_DATE` and `FN_TO_TIMESTAMP` stay advertised and Exasol delegates every pushed call

### Scenario: A declined format returns the native result in the select list and in a filter

* *GIVEN* a query over virtual schema tables with a `TO_DATE` call in the select list or in a `WHERE` filter whose format the DataFusion dialect declines
* *WHEN* the query runs
* *THEN* the query SHALL return the rows the same query returns on native Exasol tables
* *AND* the generated pushdown SQL SHALL evaluate the call in Exasol, outside the scan UDF

### Scenario: A string Exasol rejects for a translated format fails the query

* *GIVEN* a query over a virtual schema table that pushes a `TO_DATE` or `TO_TIMESTAMP` call with a translated format into the scan
* *WHEN* the scan reads a string that native Exasol rejects for that format, such as `event-01` or `2024-02-30` for `YYYY-MM-DD`, and that is not one of the documented parse differences
* *THEN* the query SHALL fail with an error, in the select list and in a filter, as it does on a native Exasol table
* *AND* the scan MUST NOT return NULL for that string or silently drop its row
