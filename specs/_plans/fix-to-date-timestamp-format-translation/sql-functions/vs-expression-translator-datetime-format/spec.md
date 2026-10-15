# Feature: VS Expression Translator — Datetime Format Models

Translates the format model of a pushed `TO_DATE` or `TO_TIMESTAMP` call into the strftime pattern
that the node-local scan parses, so the conversion runs in the scan and returns the value native
Exasol returns. A format with no exact strftime equivalent is declined, and Exasol evaluates that
call itself.

## Background

* **Supported formats.** A string-literal format built from `YYYY`, `MM`, and `DD` once each, in
  any letter case. For `TO_TIMESTAMP` only, it may also hold `HH24` and `MI`, then optionally
  `SS`, then optionally one `FF3`, `FF6`, or `FF9` (`.FFn` joins as `%.3f`, `%.6f`, `%.9f`).
  Separators are `-` `/` `.` `,` `;` `:` `_` and space.
* **Everything else** runs in Exasol.

The inputs on which the scan and Exasol differ are listed in `docs/semantic-differences.md`.

## Scenarios

### Scenario: A string-literal format translates to a strftime pattern

* *GIVEN* a `function_scalar` node named `TO_DATE` or `TO_TIMESTAMP` with a source argument and a `literal_string` format that is a supported format
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL render `to_date(nullif(<source_sql>, ''), '<pattern>')` or `to_timestamp(nullif(<source_sql>, ''), '<pattern>')`, so an empty string yields NULL as in Exasol, where `<pattern>` replaces each element with its strftime form, so `'YYYY-MM-DD'` renders as `'%Y-%m-%d'` and `'yyyy-mm-dd hh24:mi:ss.ff3'` as `'%Y-%m-%d %H:%M:%S%.3f'`
* *AND* a query over a virtual schema table that pushes such a call into the scan SHALL return the value native Exasol returns for a string that is not one of the listed parse differences, so the strings `1996-03-13` and `1996-03-13 10:11:12` under `YYYY-MM-DD` and `YYYY-MM-DD HH24:MI:SS` yield the DATE `1996-03-13` and the TIMESTAMP `1996-03-13 10:11:12`

### Scenario: A format the translation cannot express declines in the DataFusion dialect

* *GIVEN* a `function_scalar` node named `TO_DATE` or `TO_TIMESTAMP` with more than two arguments, or a format that is not a supported format
* *WHEN* the node is rendered in each dialect
* *THEN* the DataFusion dialect SHALL return an error in raising mode and `None` in the safe variants
* *AND* the Exasol dialect SHALL render the call verbatim, with the name, arguments, and format model Exasol sent
* *AND* the caller SHALL treat that asymmetry as a decline to self-apply (`pushdown/pushdown-declined-filter-self-apply`), because `FN_TO_DATE` and `FN_TO_TIMESTAMP` stay advertised and Exasol delegates every pushed call

### Scenario: A declined format returns the native result on every pushdown path

* *GIVEN* a query over virtual schema tables with a `TO_DATE` call in a `WHERE` filter whose format the DataFusion dialect declines
* *WHEN* the query runs
* *THEN* the query SHALL return the rows the same query returns on native Exasol tables
* *AND* the generated pushdown SQL SHALL evaluate the call in Exasol, outside the scan UDF

### Scenario: A string Exasol rejects for a translated format fails the query

* *GIVEN* a query over a virtual schema table that pushes a `TO_DATE` or `TO_TIMESTAMP` call with a translated format into the scan
* *WHEN* the scan reads a string that native Exasol rejects for that format, such as `event-01` or `2024-02-30` for `YYYY-MM-DD`, and that is not one of the listed parse differences
* *THEN* the query SHALL fail with an error, in the select list and in a filter, as it does on a native Exasol table
* *AND* the scan MUST NOT return NULL for that string or silently drop its row
