# Feature: VS Expression Translator — String Conversion

Makes the DataFusion dialect reproduce Exasol's implicit conversion of a value to text. Exasol
converts a non-string argument to VARCHAR before it applies a string function or a string CAST.
DataFusion does not, so the DataFusion dialect routes every such argument through the scan session
function `exa_to_varchar`, which picks the conversion from the argument's Arrow type
(`datafusion-scan/scan-execution-exa-to-varchar`). The Exasol dialect adds no conversion, because
Exasol performs it itself (issue #227).

## Background

* An argument is **string-converted** when Exasol converts it to text before it evaluates the
  parent node. The parent is a string function or a string CAST.
* String-converted argument indices, per function name and arity:

| Function | String-converted argument indices |
|----------|-----------------------------------|
| `CONCAT`, `TRIM`, `LTRIM`, `RTRIM`, `REPLACE`, `TRANSLATE` | every argument |
| `LOWER`, `UPPER`, `ASCII`, `INITCAP`, `REVERSE`, `LENGTH`, `OCTET_LENGTH`, `UNICODE`, `SUBSTR`, `REPEAT`, `LEFT`, `RIGHT` | 0 |
| `LPAD`, `RPAD` | 0, and 2 when a third argument is present |
| `INSTR`, `LOCATE` | 0 and 1 |
| `CHR`, `UNICODECHR`, every other function | none |

* A string CAST is a `function_scalar_cast` node, or the defensive `function_scalar` node named
  `CAST`, whose `dataType.type` is `VARCHAR` or `CHAR` in any letter case. Its source argument is
  string-converted.
* The table has one owner, `crates/vs-expression`. The renderer reads it to decide which arguments
  to wrap. The adapter reads it through `string_converted_args` to decide which arguments its
  decline check inspects (`vs-adapter/pushdown-planning-string-fn-type-coercion`).
* The crate carries no column-type context. The wrapping is syntactic, so one node renders
  to one SQL text on every call. Grouped planning matches group keys, aggregate arguments, HAVING
  references, and ORDER BY keys by that text.
* The scan session leaves `datafusion.sql_parser.parse_float_as_decimal` at its default `false`
  (`scan::session_config_for_spec`), so DataFusion parses a numeric literal that carries a
  fractional part as `Float64`. `exa_to_varchar` rejects `Float64`.
* A DataFusion-dialect render error routes every pushdown surface to native Exasol
  evaluation (`sql-comprehension/vs-expression-translator-cast` Background): the WHERE filter
  self-applies, the select list widens to the base row, a GROUP BY key or an aggregate argument
  declines decomposition, and a join conjunct becomes residual. This feature uses that route for
  every shape it cannot convert faithfully.

## Scenarios

### Scenario: String-converted function arguments render through exa_to_varchar in the DataFusion dialect

* *GIVEN* a `function_scalar` node named in the string-converted argument table, for example `UPPER(c_custkey)`, `SUBSTR(c_name, 2, 3)`, or `LPAD(c_name, 10, c_pad)`
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL render every string-converted argument that is not boolean-producing as `exa_to_varchar(<arg>)`, for example `upper(exa_to_varchar("C_CUSTKEY"))`, `substr(exa_to_varchar("C_NAME"), 2, 3)`, `lpad(exa_to_varchar("C_NAME"), 10, exa_to_varchar("C_PAD"))`, and, for `INSTR` and `LOCATE`, both operands in the `strpos(string, substring)` order of `sql-comprehension/vs-expression-translator-scalar-fns`
* *AND* a boolean-producing string-converted argument (`literal_bool` or a predicate node) SHALL render as `(CASE <arg> WHEN TRUE THEN 'TRUE' WHEN FALSE THEN 'FALSE' ELSE NULL END)` (issue #200) with no `exa_to_varchar` wrapper in every arm: string functions, `INSTR`/`LOCATE`, `CONCAT`, and string CAST, so `UPPER(TRUE)` and `UPPER(c_a > 1)` render `upper((CASE ... END))`
* *AND* the translator SHALL render every other argument unwrapped, including the numeric length, offset, and count arguments and the codepoint argument of `CHR` and `UNICODECHR`
* *AND* rendering the same node twice SHALL yield byte-identical text

### Scenario: A string CAST renders as a cast of exa_to_varchar in the DataFusion dialect

* *GIVEN* a string CAST, for example `CAST(c_acctbal AS VARCHAR(20))`, `CAST(c_custkey AS CHAR(10))`, or `CAST(c_a > 1 AS VARCHAR(5))`
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL render `CAST(exa_to_varchar(<source>) AS VARCHAR)` for a source that is not boolean-producing, keeping the bare, length-less `VARCHAR` target of `sql-comprehension/vs-expression-translator-cast`
* *AND* a CAST to any non-string target SHALL render with no wrapper
* *AND* a boolean-producing source SHALL render as the CASE form of the first scenario alone, with no `exa_to_varchar` wrapper and no outer CAST, because that form yields Exasol-cased text

### Scenario: The Exasol dialect never renders exa_to_varchar

* *GIVEN* any expression tree that carries a string-converted argument
* *WHEN* the tree is rendered through `render_expression_exasol`, `render_expression_exasol_safe`, or `render_df_filter_exasol_safe`
* *THEN* the output MUST NOT contain `exa_to_varchar`, and string conversion SHALL leave the Exasol-dialect rendering of every node unaffected, because Exasol converts the argument itself and its parser does not know the function
* *AND* this SHALL hold for every Exasol-dialect caller: the qualified single-table wrapper, the N-scan join wrapper, and the grouped merge wrapper's HAVING and scalar-over-aggregate rendering

### Scenario: INSTR and LOCATE beyond two arguments are a DataFusion-dialect render error

* *GIVEN* `INSTR(a, b, start)`, `INSTR(a, b, start, occurrence)`, or `LOCATE(a, b, start)`, over arguments of any type
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL return an error in raising mode and `None` in the safe variants, because the two-argument `strpos` takes no start position or occurrence (issue #228, step 1)
* *AND* the Exasol dialect SHALL render the call verbatim with every argument
* *AND* faithful three- and four-argument DataFusion rendering SHALL remain the tracked exception #228

### Scenario: A string-converted literal DataFusion cannot convert faithfully is a DataFusion-dialect render error

* *GIVEN* a string-converted argument that is a `literal_double`, a `literal_exactnumeric` whose value text carries a decimal point or an exponent, or a `literal_timestamp` / `literal_timestamp_utc` / `literal_timestamputc` node, for example `CONCAT(c_name, 1.5)`
* *WHEN* `render_expression` processes the parent node
* *THEN* the translator SHALL return an error in raising mode and `None` in the safe variants, so the surface falls back to native Exasol evaluation instead of reaching `exa_to_varchar` with a `Float64` or `Timestamp` argument
* *AND* a string-converted `literal_string`, `literal_null`, `literal_date`, or integer `literal_exactnumeric` SHALL render inside the `exa_to_varchar` wrapper, and a string-converted `literal_bool` SHALL render as the CASE form of the first scenario with no wrapper

### Scenario: The string-converted argument table is one query shared with the adapter

* *GIVEN* the public function `string_converted_args(node: &Json) -> Vec<&Json>`
* *WHEN* it is called with any expression node
* *THEN* it SHALL return exactly the argument nodes that the DataFusion dialect converts for that node, either wrapped or CASE-rendered, in argument order, per the Background table and the string CAST rule, omitting every index at or beyond the node's argument count
* *AND* it SHALL return an empty list for `CHR`, `UNICODECHR`, every other function, a CAST to a non-string target, and every other node type
* *AND* the renderer SHALL decide what it converts from the same table, so the adapter's decline check and the renderer MUST NOT disagree on which arguments are converted
