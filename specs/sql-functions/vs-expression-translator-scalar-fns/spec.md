# Feature: VS Expression Translator, Scalar Functions

Extends the VS expression translator (`sql-comprehension/vs-expression-translator`) with named scalar function translation: math functions, the modulo operator, string functions, CASE expressions, and the NULLIF and COALESCE shorthands. `vs-expression-translator-scalar-ops` covers the arithmetic operators and CAST. `sql-functions/vs-expression-translator-greatest-least` covers `GREATEST` and `LEAST`, and `sql-functions/vs-expression-translator-concat` covers the `CONCAT` NULL rendering.

## Background

The translator serves two SQL parsers through one recursive walker. The DataFusion entry points (`render_expression`, `render_expression_safe`, `render_df_filter_safe`) feed DataFusion's SQL frontend inside the scan UDF. The Exasol entry points (`render_expression_exasol`, `render_expression_exasol_safe`, `render_df_filter_exasol_safe`) feed outer wrapper SQL that Exasol's own engine parses.

In the DataFusion dialect, most Exasol `FN_*` names lower-case to the DataFusion function of the same name. These need explicit aliasing: `SIGN`→`signum`, `LENGTH`→`character_length`, `MOD`→`%` operator, `INSTR`/`LOCATE`→`strpos` (with operand reorder), `UNICODE`→`ascii`, `UNICODECHR`→`chr`, `NULLIFZERO`→`nullif(x,0)`, `ZEROIFNULL`→`coalesce(x,0)`.

In the Exasol dialect every one of those aliases is wrong, because Exasol has no function of the aliased name: `SIGNUM` and `STRPOS` return `function or script <NAME> not found` (SQL code 42000), and Exasol's parser rejects `%`. An Exasol scalar function therefore renders verbatim, with its original name, argument order and argument count, because Exasol's compiler emitted the call. The crate declares every translated `function_scalar` name once, with its Exasol-dialect form. That declaration gates the dispatch and drives the enforcing sweep test (see `sql-comprehension/vs-expression-translator`). A name with a DataFusion arm but no declaration is not translated. `GREATEST` and `LEAST` keep the `ExasolForm::VerbatimCall` declaration, and `CONCAT` keeps `ExasolForm::Shaped`.

These constructs are excluded from the verbatim rule and keep a dedicated rendering, because verbatim is impossible or wrong for them:

| Construct | Why it is not rendered verbatim |
|---|---|
| `ADD`, `SUB`, `MULT`, `NEG` | Wire names for operators, not Exasol function names. Both dialects render `(<l> + <r>)` and the rest. |
| `FLOAT_DIV` | An operator wire name whose rendering differs by dialect: `(CAST(<l> AS DOUBLE) / <r>)` in the DataFusion dialect, `(<l> / <r>)` in the Exasol dialect. Exasol's `/` is always float division, while DataFusion's `/` truncates integer and decimal operands. Specified in `sql-comprehension/vs-expression-translator-float-div`. |
| `MOD` | Exasol requires `MOD(a, b)`, and DataFusion offers only `%`. Its arm branches on dialect and validates arity. |
| `CONCAT` | Also the wire encoding of Exasol's `\|\|`, with a different rendering per dialect. Specified in `sql-functions/vs-expression-translator-concat`. |
| `CAST` | The target type differs: an Exasol character target needs an explicit length, and an Exasol `TIMESTAMP` target needs an explicit precision. Specified in `sql-comprehension/vs-expression-translator-cast`. |
| `function_scalar` named `CASE` | Exasol's interleaved-argument CASE encoding. Both dialects render `CASE WHEN … THEN … END`. It is distinct from the `function_scalar_case` node type. |

## Scenarios

### Scenario: Math scalar functions translate to DataFusion math calls

* *GIVEN* a VS expression node of type `function_scalar` whose `name` is one of the supported Exasol math functions: `ABS`, `ROUND`, `FLOOR`, `CEIL`, `SQRT`, `POWER`, `EXP`, `LN`, `LOG`, `SIGN`, `TRUNC`, `SIN`, `COS`, `TAN`, `ASIN`, `ACOS`, `ATAN`, `ATAN2`, `SINH`, `COSH`, `TANH`, `COT`, `DEGREES`, or `RADIANS`
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL render the node as the corresponding DataFusion SQL function applied to its rendered arguments in order, using these name mappings: `SIGN`→`signum`, `CEIL`→`ceil`, `POWER`→`power`, and all other listed names lower-cased to their identically-named DataFusion function
* *AND* each argument SHALL be rendered recursively by the translator
* *AND* a node whose argument count does not match the function arity SHALL return an error in raising mode and `None` in the safe variants

### Scenario: Math scalar functions render verbatim in the Exasol dialect

* *GIVEN* a VS expression node of type `function_scalar` named with one of the math functions listed in the preceding scenario
* *WHEN* `render_expression_exasol` processes the node
* *THEN* the translator SHALL return `<NAME>(<arg1_sql>[, <arg2_sql>])` using the function's own uppercased Exasol name and its arguments in the order received, each rendered recursively in the Exasol dialect
* *AND* `SIGN` in particular MUST render as `SIGN(<arg_sql>)` and MUST NOT render as `signum(...)`, which Exasol rejects with `function or script SIGNUM not found` (SQL code 42000) when the fragment reaches an Exasol-parsed wrapper (issue #209)
* *AND* the DataFusion-dialect rendering of every listed name MUST remain byte-identical to the preceding scenario
* *AND* the Exasol dialect SHALL NOT impose its own arity check, because Exasol's compiler emitted a call its own engine accepts — the same rule the string-function family already follows for Exasol's three-argument `INSTR`

### Scenario: MOD translates to the modulo operator

* *GIVEN* a VS expression node of type `function_scalar` named `MOD` with two arguments
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL return `(<left> % <right>)`, because DataFusion exposes modulo as the `%` operator rather than a `mod()` function
* *AND* both operands SHALL be rendered recursively
* *AND* `render_expression_exasol` SHALL return `MOD(<left>, <right>)` instead, because Exasol's parser rejects `%` (issue #197)
* *AND* a node whose argument count is not exactly two SHALL return an error in raising mode and `None` in the safe variants, in BOTH dialects

### Scenario: String scalar functions translate to DataFusion string calls

* *GIVEN* a VS expression node of type `function_scalar` whose `name` is one of the supported Exasol string functions: `CONCAT`, `LENGTH`, `LOWER`, `UPPER`, `SUBSTR`, `TRIM`, `LTRIM`, `RTRIM`, `REPLACE`, `REPEAT`, `REVERSE`, `LPAD`, `RPAD`, `ASCII`, `CHR`, `INITCAP`, `LEFT`, `RIGHT`, `TRANSLATE`, `INSTR`, `LOCATE`, `OCTET_LENGTH`, `UNICODE`, or `UNICODECHR`
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL render the node as the corresponding DataFusion SQL function applied to its rendered arguments in order, using these name mappings: `SUBSTR`→`substr`, `LENGTH`→`character_length`, `OCTET_LENGTH`→`octet_length`, `INSTR`/`LOCATE`→`strpos` (with operands ordered string-then-substring per DataFusion `strpos(string, substring)`), `UNICODE`→`ascii`, `UNICODECHR`→`chr`, and all other listed names lower-cased to their identically-named DataFusion function
* *AND* each argument SHALL be rendered recursively by the translator
* *AND* `LOCATE`/`INSTR` argument reordering MUST preserve the Exasol semantics of "position of substring within string"
* *AND* `CONCAT` SHALL be rendered by its own dedicated per-dialect rule, specified in `sql-functions/vs-expression-translator-concat`, not by this name-mapping table

### Scenario: String scalar functions render verbatim in the Exasol dialect

* *GIVEN* a VS expression node of type `function_scalar` named with one of the string functions listed in the preceding scenario, `CONCAT` excepted
* *WHEN* `render_expression_exasol` processes the node
* *THEN* the translator SHALL return `<NAME>(<args_sql>)` using the function's own uppercased Exasol name and its arguments in the order received, with no name mapping and no operand reordering
* *AND* `INSTR` and `LOCATE` in particular MUST keep Exasol's own argument order and MUST NOT render as `strpos(...)`, which Exasol rejects with `function or script STRPOS not found` (SQL code 42000)
* *AND* an `INSTR` or `LOCATE` node carrying Exasol's optional start-position or occurrence argument SHALL render with that argument forwarded unchanged, because Exasol's native function accepts it — this is what lets the arity decline in `pushdown-types/pushdown-planning-string-fn-type-coercion` still evaluate correctly on the Exasol side
* *AND* the DataFusion-dialect rendering of every listed name MUST remain byte-identical to the preceding scenario

### Scenario: CASE expression translates to SQL CASE WHEN

* *GIVEN* a VS expression node of type `function_scalar_case` (the Exasol node type for both simple and searched CASE, including the expansion of `NULLIF(...)`) carrying `arguments` (the WHEN test conditions) and `results` (the THEN values, with the optional ELSE as the last element when `results` has one more entry than `arguments`)
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL return a `CASE WHEN <cond1> THEN <res1> [WHEN <condN> THEN <resN>]... [ELSE <else>] END` expression with each condition and result rendered recursively
* *AND* a `function_scalar_case` node with no WHEN branch SHALL return an error in raising mode and `None` in the safe variants

### Scenario: NULLIFZERO and ZEROIFNULL translate to NULLIF and COALESCE

* *GIVEN* a VS expression node of type `function_scalar` named `NULLIFZERO` or `ZEROIFNULL` with a single argument
* *WHEN* `render_expression` processes the node
* *THEN* `NULLIFZERO` SHALL render as `nullif(<arg>, 0)`
* *AND* `ZEROIFNULL` SHALL render as `coalesce(<arg>, 0)`
* *AND* `render_expression_exasol` SHALL render `NULLIFZERO(<arg>)` / `ZEROIFNULL(<arg>)` verbatim, because both are native Exasol functions and the rewrite exists only to reach a DataFusion equivalent

### Scenario: NULLIF translates to the DataFusion nullif call

* *GIVEN* a VS expression node of type `function_scalar` named `NULLIF` with two arguments, distinct from the `function_scalar_case` node type that carries Exasol's own expansion of `NULLIF(...)` and whose rendering the CASE scenario above specifies
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL return `nullif(<a>, <b>)` over the recursively rendered arguments
* *AND* `render_expression_exasol` SHALL return `NULLIF(<a>, <b>)` under the same verbatim rule as the other Exasol scalar functions, because `NULLIF` is a native Exasol function and the lower-cased call is a DataFusion form
* *AND* an argument count other than two SHALL return an error in raising mode and `None` in the safe variants in the DataFusion dialect
* *AND* the Exasol dialect SHALL NOT impose that arity check, for the reason given in the math-function scenario above: Exasol's compiler emitted a call its own engine accepts

### Scenario: Regexp scalar functions are deliberately not translated

* *GIVEN* a VS expression node of type `function_scalar` named `REGEXP_LIKE`, `REGEXP_REPLACE`, `REGEXP_SUBSTR`, `REGEXP_INSTR`, or `REGEXP_COUNT`
* *WHEN* `render_expression` processes the node in raising mode
* *THEN* the translator SHALL return the error `unsupported scalar function: <NAME>`
* *AND* `render_expression_safe` SHALL return `None` for the same node without panicking, and `render_expression_exasol` / `render_expression_exasol_safe` SHALL decline these five names identically — one capability set serves both dialects, so the Exasol dialect's verbatim rule MUST NOT widen the translated set
* *AND* the adapter SHALL NOT advertise `FN_PRED_REGEXP_LIKE`, `FN_REGEXP_REPLACE`, `FN_REGEXP_SUBSTR`, `FN_REGEXP_INSTR`, or `FN_REGEXP_COUNT`, so Exasol evaluates each of them
