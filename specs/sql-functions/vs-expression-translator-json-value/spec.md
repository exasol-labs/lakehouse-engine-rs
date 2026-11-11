# Feature: VS Expression Translator, JSON_VALUE

The adapter advertises `FN_JSON_VALUE`. The scan evaluates the `JSON_VALUE` forms that need no session setting, with native Exasol's results, and Exasol evaluates every other form over the document column.

## Background

* Exasol sends `JSON_VALUE` as a `function_scalar_json_value` node with the document and a path literal as `arguments`, a `returningDataType` (`VARCHAR(2000000)` without a `RETURNING` clause), and an `emptyBehavior` and `errorBehavior` of `NULL`, `ERROR`, or `DEFAULT` with an `expression`.
* Exasol converts the extracted text to `DECIMAL`, `DOUBLE`, `DATE`, and `TIMESTAMP` by the session's NLS settings, which the pushdown request does not carry.
* Exasol casts a `DEFAULT` to the `RETURNING` type when it compiles the statement, and fails the statement when the value does not convert or fit.

## Scenarios

### Scenario: JSON_VALUE with a session-free form runs in the scan

* *GIVEN* a `JSON_VALUE` call whose document is text and whose path is `$` followed by `.name`, `."name"` without a backslash, and `[n]` steps with `n` at most 2147483647, with no whitespace
* *AND* whose `RETURNING` type is `VARCHAR(n)` in UTF8 or `BOOLEAN`
* *AND* whose `ON EMPTY` and `ON ERROR` behaviors are `NULL`, or `DEFAULT` with a NULL literal or a literal of the `RETURNING` type that fits it
* *WHEN* the call appears in the select list, the WHERE clause, a GROUP BY key, or an aggregate argument
* *THEN* the scan SHALL evaluate it, and the scan SHALL NOT project the document column for it
* *AND* a GROUP BY over it, or an aggregate over it, SHALL keep the split aggregate, typed by its `RETURNING` type

### Scenario: JSON_VALUE in the scan reads the document and the path as Exasol does

* *GIVEN* a `JSON_VALUE` call that runs in the scan
* *WHEN* the scan evaluates it on a document
* *THEN* a NULL or empty document SHALL yield NULL, and a document that is not strict RFC 8259 JSON SHALL take the `ON ERROR` behavior
* *AND* the path SHALL run in lax mode over item sequences: a member step selects the member of that name in an object or in each object of an array, and an index step treats a non-array as a one-item array
* *AND* no item SHALL take the `ON EMPTY` behavior, and several items or an object or array item SHALL take the `ON ERROR` behavior

### Scenario: JSON_VALUE in the scan differs from Exasol on number text, duplicate keys, and deep nesting

* *GIVEN* a `JSON_VALUE` call that runs in the scan
* *WHEN* the selected item is a number other than an integer in the 64-bit range, the path meets a duplicate key, or the document nests deeper than 128 levels
* *THEN* a number SHALL yield the shortest text that reads back as the same DOUBLE, such as `1.5` for `1.50` and `100.0` for `1e2`, where native Exasol yields the number's source text
* *AND* a duplicate key SHALL select the last member of that name, where native Exasol takes the `ON ERROR` behavior
* *AND* a document nested deeper than 128 levels SHALL take the `ON ERROR` behavior, where native Exasol reads it

### Scenario: JSON_VALUE in the scan converts the item as Exasol does

* *GIVEN* a `JSON_VALUE` call that runs in the scan and a path that selects one scalar item
* *WHEN* the scan converts the item to the `RETURNING` type
* *THEN* a JSON null SHALL yield NULL, and the item's text SHALL be a string's unescaped text, a number's text, or `true` or `false`
* *AND* for `VARCHAR(n)`, an empty text SHALL yield NULL, and a text longer than `n` characters SHALL take the `ON ERROR` behavior
* *AND* for `BOOLEAN`, the text SHALL convert as Exasol casts text to BOOLEAN: spaces trimmed, case ignored, `TRUE`, `T`, `YES`, `Y`, and the number 1 true, `FALSE`, `F`, `NO`, `N`, and the number 0 false, and any other text SHALL take the `ON ERROR` behavior

### Scenario: A JSON_VALUE the scan cannot compute runs in Exasol over the document column

* *GIVEN* a `JSON_VALUE` call outside the forms the scan evaluates, such as `RETURNING DECIMAL`, `DOUBLE`, `DATE`, `TIMESTAMP`, `CHAR`, or an ASCII `VARCHAR`, an `ERROR` behavior, a `DEFAULT` that is not a fitting literal of the `RETURNING` type, or a path with a wildcard
* *WHEN* the call appears in the select list or the WHERE clause
* *THEN* the adapter SHALL render the call in Exasol SQL with every clause Exasol sent, so Exasol returns its own result or error
* *AND* the scan SHALL project only the columns the query reads
