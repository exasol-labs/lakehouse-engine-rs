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
* The table has one owner and one reader, the `crates/vs-expression` renderer. The adapter makes no
  string-conversion decision (`vs-adapter/pushdown-planning-string-fn-type-coercion`).
* `exa_to_varchar` converts every Arrow type the scan can produce, so the renderer wraps an argument
  without knowing its type. A boolean-producing argument needs no special form in this dialect.
* The crate carries no column-type context. The wrapping is syntactic, so one node renders to one
  SQL text on every call. Grouped planning matches group keys, aggregate arguments, HAVING
  references, and ORDER BY keys by that text.
* A DataFusion-dialect render error routes every pushdown surface to native Exasol evaluation
  (`sql-comprehension/vs-expression-translator-cast` Background): the WHERE filter self-applies, the
  select list widens to the base row, a GROUP BY key or an aggregate argument declines
  decomposition, and a join conjunct becomes residual. `INSTR` and `LOCATE` beyond two arguments
  use that route.

## Scenarios

### Scenario: String-converted function arguments render through exa_to_varchar in the DataFusion dialect

* *GIVEN* a `function_scalar` node named in the string-converted argument table, for example `UPPER(c_custkey)`, `SUBSTR(c_name, 2, 3)`, `LPAD(c_name, 10, c_pad)`, `LPAD(c_name, 10)`, `UPPER(TRUE)`, or `CONCAT(c_acctbal > 0, '')`
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL render every string-converted argument, boolean-producing ones included, as `exa_to_varchar(<arg>)`, for example `upper(exa_to_varchar("C_CUSTKEY"))`, `lpad(exa_to_varchar("C_NAME"), 10, exa_to_varchar("C_PAD"))`, and `upper(exa_to_varchar(true))`, and, for `INSTR` and `LOCATE`, both operands in the `strpos(string, substring)` order of `sql-comprehension/vs-expression-translator-scalar-fns`
* *AND* the translator SHALL render unwrapped every other argument, every table index at or beyond the node's argument count, and every value outside a string-converted position, so `LPAD(c_name, 10)` wraps index 0 only and neither `CHR`'s codepoint nor a comparison operand is wrapped
* *AND* rendering the same node twice SHALL yield byte-identical text

### Scenario: A string CAST renders as a cast of exa_to_varchar in the DataFusion dialect

* *GIVEN* a string CAST, for example `CAST(c_acctbal AS VARCHAR(20))`, `CAST(c_custkey AS CHAR(10))`, or `CAST(c_a > 1 AS VARCHAR(5))`
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL render `CAST(exa_to_varchar(<source>) AS VARCHAR)` for every source, boolean-producing ones included, keeping the bare, length-less `VARCHAR` target of `sql-comprehension/vs-expression-translator-cast`
* *AND* a CAST to any non-string target SHALL render with no wrapper

### Scenario: The Exasol dialect never renders exa_to_varchar

* *GIVEN* any expression tree that carries a string-converted argument
* *WHEN* the tree is rendered through `render_expression_exasol`, `render_expression_exasol_safe`, or `render_df_filter_exasol_safe`
* *THEN* the output MUST NOT contain `exa_to_varchar`, and string conversion SHALL leave the Exasol-dialect rendering of every node unaffected, because Exasol converts the argument itself and its parser does not know the function
* *AND* this SHALL hold for every Exasol-dialect caller: the qualified single-table wrapper, the N-scan join wrapper, and the grouped merge wrapper's HAVING and scalar-over-aggregate rendering

### Scenario: INSTR and LOCATE beyond two arguments are a DataFusion-dialect render error

* *GIVEN* `INSTR(a, b, start)`, `INSTR(a, b, start, occurrence)`, or `LOCATE(a, b, start)`, over arguments of any type
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL return an error in raising mode and `None` in the safe variants, because the two-argument `strpos` takes no start position or occurrence (issue #228, step 1)
* *AND* a two-argument `INSTR` or `LOCATE`, which Exasol sends with exactly two arguments, SHALL keep its `strpos` rendering, and the Exasol dialect SHALL render every call verbatim with every argument
* *AND* faithful three- and four-argument DataFusion rendering SHALL remain the tracked exception #228

### Scenario: One node renders one text on every DataFusion surface

* *GIVEN* issue #227's repros over an integer column `c_custkey`: `UPPER(c_custkey)` as a GROUP BY key with and without its select item, as a grouped ORDER BY key, as the argument of `MAX`, `COUNT`, and `COUNT(DISTINCT)`, in `HAVING MAX(UPPER(c_custkey)) > '5'`, and inside the scalar over an aggregate `LENGTH(MAX(UPPER(c_custkey)))`
* *WHEN* the adapter plans each request, once with files remaining and once with every file pruned
* *THEN* every occurrence SHALL render `upper(exa_to_varchar("C_CUSTKEY"))`, so each select item, HAVING reference, and ORDER BY key matches its group key or aggregate plan by text, and each request decomposes into its grouped or single-group partial/merge scan
* *AND* the empty-result path SHALL resolve the same shape without a panic, and the merge wrapper SQL MUST NOT contain `exa_to_varchar`
* *AND* the returned rows SHALL equal native Exasol evaluation, where before this change the grouped repros failed with `F-UDF-CL-RUST-9001` (SQL state `22002`)
