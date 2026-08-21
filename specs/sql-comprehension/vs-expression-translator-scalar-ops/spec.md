# Feature: VS Expression Translator — Scalar Operations

Extends the VS expression translator (`sql-comprehension/vs-expression-translator`) with arithmetic operators and the safe/fallback entry points. CAST target-type rendering is covered in `sql-comprehension/vs-expression-translator-cast`. Named math/string/conditional scalar functions are covered in `sql-comprehension/vs-expression-translator-scalar-fns`; date/time functions in `sql-comprehension/vs-expression-translator-date-fns`. Floating-point division (`FLOAT_DIV`) is split out into its own dedicated feature, `sql-comprehension/vs-expression-translator-float-div`, because its rendering diverges by dialect — every other arithmetic operator here still renders byte-identically in both dialects.

## Background

The `crates/vs-expression` crate exposes six public entry points in two dialect trios. The DataFusion trio feeds DataFusion's SQL frontend inside the scan UDF:
- `render_expression` — raising mode, returns `Err` for unsupported nodes
- `render_expression_safe` — returns `None` for unsupported nodes, never panics
- `render_df_filter_safe` — same as `render_expression_safe` but also returns `None` for trivially-true results (e.g. `TRUE`, `NULL`) so the adapter can omit no-op filters from the scan spec

The Exasol trio — `render_expression_exasol`, `render_expression_exasol_safe`, `render_df_filter_exasol_safe` — carries the same three contracts for fragments Exasol's own core engine parses.

The arithmetic operator nodes and every decline in this feature behave identically in both dialects with ONE exception: `+`, `-`, `*` and unary `-` are the same syntax in both parsers, and a declined function is declined in both because the adapter advertises one capability set for both dialects — but `/` (`FLOAT_DIV`) diverges by dialect, specified in `sql-comprehension/vs-expression-translator-float-div`.

A conversion or operator node is translated only when its DataFusion 54 result matches Exasol. Exasol `DIV` returns the integer quotient by truncating toward zero — verified live: `DIV(-7,2) = -3` and `DIV(15.7,6.2) = 2` — and raises a division-by-zero error (SQL state 22012). DataFusion 54 has no `div` builtin; its `/` truncates only integer operands and divides non-integer operands fractionally. No single rendering reproduces `DIV` across every operand type, so `DIV` stays unsupported — the disqualifier is that a wrong rendering would be wrong on EVERY row for non-integer operands, the per-row problem rather than the zero-divisor one (see the `FLOAT_DIV` feature for why that same type-blindness does not disqualify `FLOAT_DIV`). DataFusion 54 `to_char` uses strftime masks rather than Exasol's Oracle-style format models and rejects numeric formatting, and DataFusion 54 has no `to_number`. These three functions are therefore left unsupported and fall back to Exasol. The bitwise operator functions (`BIT_AND`, `BIT_OR`, `BIT_XOR`, `BIT_NOT`, `BIT_LSHIFT`, `BIT_RSHIFT`, `BIT_LROTATE`, `BIT_RROTATE`, `BIT_CHECK`, `BIT_SET`, `BIT_TO_NUM`) are likewise unsupported: Exasol defines them over an unsigned 64-bit integer domain that DataFusion's signed-integer operators and the `Int64` → `DECIMAL(20,0)` mapping do not reproduce, and six of the eleven have no DataFusion builtin at all (issue #108).

The `crates/vs-expression` crate stays a pure, stateless, sibling-shared JSON-to-SQL translator with no column-type context. The adapter-synthesized node type `decimal_to_varchar_exasol` and the crate-visible pure helper `format_decimal_exasol_style` let an adapter that has already resolved a column as DECIMAL inject an Exasol-faithful DECIMAL→string trim without the translator inspecting types (see `vs-adapter/pushdown-planning-decimal-string-format`).

## Scenarios

### Scenario: Arithmetic operators translate to binary SQL expressions

* *GIVEN* a VS expression node of type `function_scalar` whose `name` is the Exasol scalar-function name for addition, subtraction, or multiplication, or for unary negation
* *AND* the exact `name` strings Exasol emits for these operators have been verified against live `EXPLAIN VIRTUAL` output for an arithmetic pushdown (so the translator matches what Exasol actually sends, e.g. `MULT` for `*`, not an assumed `MUL`)
* *WHEN* `render_expression` or `render_expression_exasol` processes the node
* *THEN* the `ADD`, `SUB`, and `MULT` nodes SHALL return `(<left> <op> <right>)` where the operators are `+`, `-`, `*` respectively, for operands that are themselves any renderable expression (including two bare column references, e.g. `(L_EXTENDEDPRICE * L_DISCOUNT)`), byte-identically in BOTH dialects — the operator syntax is shared by both parsers, and these wire names are NOT Exasol function names (Exasol has no function called `ADD`), so the Exasol dialect's verbatim rule for named functions MUST NOT be applied to them
* *AND* unary negation SHALL return `(-<operand>)` and SHALL compose inside an aggregate argument (e.g. `SUM(-<operand>)`) so it flows through the arithmetic-aggregate decomposition path
* *AND* floating-point division (`FLOAT_DIV`) SHALL NOT be rendered by this shape — it is the one arithmetic operator whose rendering diverges by dialect, specified in `sql-comprehension/vs-expression-translator-float-div` (issue #186); this scenario's "byte-identically in BOTH dialects" claim covers `ADD`, `SUB`, `MULT`, and `NEG` only
* *AND* the set of arithmetic `name` strings the translator matches SHALL correspond exactly to the arithmetic operator capabilities the adapter advertises (`vs-adapter/pushdown-planning-capability-extensions`) — `FN_ADD`, `FN_SUB`, `FN_MULT`, `FN_FLOAT_DIV`, and `FN_NEG` — so no advertised operator is left unrenderable and no rendered operator is left unadvertised
* *AND* Exasol integer division (`DIV`) SHALL NOT be matched here and `FN_DIV` SHALL NOT be advertised

### Scenario: Unsupported node type returns error in raising mode

* *GIVEN* a VS expression node with a `type` value not handled by the translator
* *WHEN* `render_expression` is called (raising mode)
* *THEN* the function SHALL return an error containing the unrecognised node type name
* *AND* the error MUST NOT contain any credential or secret-looking string from the node payload

### Scenario: Safe variant returns None for unsupported nodes

* *GIVEN* a VS expression node with a `type` value not handled by the translator
* *WHEN* `render_expression_safe` is called
* *THEN* the function SHALL return `None`
* *AND* the function MUST NOT panic

### Scenario: Trivially-true filter suppressed in safe variant

* *GIVEN* a call to `render_df_filter_safe` with an expression that renders to `TRUE` or `NULL`
* *WHEN* the safe entry-point evaluates the result
* *THEN* it SHALL return `None` so the adapter omits the redundant filter from the scan spec
* *AND* `render_df_filter_exasol_safe` SHALL suppress the same two trivially-true results, so the outer WHERE residual of the N-scan join wrapper omits a no-op conjunct on the Exasol path too

### Scenario: Integer division DIV is deliberately not translated

* *GIVEN* a VS expression node of type `function_scalar` named `DIV` — Exasol integer-quotient division, which truncates toward zero (`DIV(-7,2) = -3`, verified live) and raises a division-by-zero error
* *WHEN* `render_expression` processes the node in raising mode
* *THEN* the translator SHALL return an error naming `DIV` as unsupported
* *AND* `render_expression_safe` SHALL return `None` for the same node without panicking
* *AND* the adapter SHALL omit the expression and let Exasol evaluate `DIV`, because DataFusion 54 has no `div` builtin and, unlike `FLOAT_DIV`, no single type-blind rendering reproduces it: `DIV` needs TRUNCATION, which DataFusion's `/` delivers for integer operands and not for any other kind, and unlike CAST's explicit `dataType` field, DIV's operand types are not carried in the expression node, so the translator cannot identify and selectively render only the safe integer-operand case
* *AND* this decline SHALL NOT be read as resting on the division-by-zero divergence that `FLOAT_DIV` now knowingly accepts (see `sql-comprehension/vs-expression-translator-float-div`): for `x/0` the query fails either way, and for `0/0` the divergence belongs to the tracked NaN-at-emit gap `(#246)` rather than to the rendering. `DIV`'s disqualifying defect is that a wrong rendering would be wrong on EVERY row for non-integer operands, not only when a divisor is zero — that is the difference between the two operators, and a future `TRUNC(m/n)` emulation would have to answer the per-row problem, not the zero-divisor one

### Scenario: Conversion format functions TO_CHAR and TO_NUMBER are deliberately not translated

* *GIVEN* a VS expression node of type `function_scalar` named `TO_CHAR` or `TO_NUMBER`
* *WHEN* `render_expression` processes the node in raising mode
* *THEN* the translator SHALL return an error naming the function as unsupported
* *AND* `render_expression_safe` SHALL return `None` for the same node without panicking
* *AND* the adapter SHALL omit the expression and let Exasol evaluate it, because DataFusion 54 has no matching format model or `to_number`; a no-format string-to-number conversion remains reachable through `FN_CAST`

### Scenario: Bitwise operator functions are deliberately not translated

* *GIVEN* a VS expression node of type `function_scalar` whose `name` is one of `BIT_AND`, `BIT_OR`, `BIT_XOR`, `BIT_NOT`, `BIT_LSHIFT`, `BIT_RSHIFT`, `BIT_LROTATE`, `BIT_RROTATE`, `BIT_CHECK`, `BIT_SET`, or `BIT_TO_NUM` — the eleven bitwise operator functions Exasol names `FN_BIT_*` (the `function_scalar` name equals the capability name with `FN_` stripped)
* *WHEN* `render_expression` processes the node in raising mode
* *THEN* the translator SHALL return an error naming the function as unsupported, and `render_expression_safe` SHALL return `None` for the same node without panicking
* *AND* for `BIT_AND`, `BIT_OR`, `BIT_XOR`, `BIT_LSHIFT`, and `BIT_RSHIFT` — which map to DataFusion's `&`, `|`, `#`, `<<`, and `>>` operators — the translator MUST NOT render them and the adapter SHALL let Exasol evaluate the function, because Exasol defines them over unsigned 64-bit integers (`0`–`18446744073709551615`, result `DECIMAL(20,0)`) while DataFusion's operators act on the operand's signed Arrow integer type (Iceberg carries only signed `int`/`long`, no unsigned primitive) — a bit-63-set result reads as a large positive value in Exasol but negative under signed `Int64`, `BIT_RSHIFT`'s signed `>>` is arithmetic (sign-extending) versus Exasol's logical (zero-fill), and the value/type-blind translator cannot restrict rendering to the safe non-negative, bit-63-clear operand subset because operand types and values are not carried in the node (the same limitation the `DIV` decline records)
* *AND* for `BIT_NOT`, `BIT_LROTATE`, `BIT_RROTATE`, `BIT_CHECK`, `BIT_SET`, and `BIT_TO_NUM` the translator MUST NOT render them because DataFusion 54.0.0 provides no matching operator or scalar function: its SQL planner (`parse_sql_unary_op`) supports only logical `NOT`, unary `+`, and unary `-`, rejecting unary `~` with `not_impl_err`, and `datafusion-functions` 54.0.0 registers no bit-rotate, bit-test, bit-set, or bits-to-number scalar function (its only `bit`-named function is the string `bit_length`, out of scope here)

### Scenario: Decimal-to-VARCHAR node renders Exasol-trimmed string

* *GIVEN* a VS expression node of type `decimal_to_varchar_exasol` carrying a single `arguments` entry, an adapter-synthesized node the `crates/lakehouse-engine` pushdown layer injects in place of a confirmed-DECIMAL-typed stringification point (never emitted by Exasol on the wire; see `vs-adapter/pushdown-planning-decimal-string-format`)
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL render the single argument recursively, then wrap the rendered SQL fragment with the crate-visible `format_decimal_exasol_style` helper, so the emitted DataFusion SQL reproduces Exasol's shortest-form DECIMAL→string conversion (trailing scale zeros trimmed)
* *AND* a `decimal_to_varchar_exasol` node whose argument count is not exactly one SHALL return an error in raising mode and `None` in the safe variants
* *AND* the translator SHALL apply neither column-type inspection nor any type decision of its own for this node — the caller has already confirmed the wrapped argument is DECIMAL-typed, keeping `vs-expression` a pure, stateless, sibling-shared translator

### Scenario: format_decimal_exasol_style reproduces Exasol shortest-form decimal formatting

* *GIVEN* the crate-visible pure helper `format_decimal_exasol_style(expr_sql: &str) -> String`, which takes an already-rendered SQL fragment for a confirmed-DECIMAL-typed expression and carries no type information of its own
* *WHEN* the helper is called with a rendered fragment `<f>`
* *THEN* it SHALL return a DataFusion SQL string expression that casts `<f>` to text and trims trailing scale zeros — reproducing Exasol's DECIMAL→string conversion — using `regexp_replace(regexp_replace(CAST(<f> AS VARCHAR), '(\.[0-9]*[1-9])0+$', '\1'), '\.0+$', '')`, whose two POSIX-backreference replacements DataFusion 54 accepts
* *AND* the emitted expression SHALL trim a fractional part to its shortest form, including for negatives, and drop the decimal point entirely when the fraction is all zeros, verified for `2912.00`→`2912`, `-272.60`→`-272.6`, `868.90`→`868.9`, `0.00`→`0`, `100.00`→`100`, and `12.350`→`12.35`
* *AND* the emitted expression SHALL leave unchanged a value with no trailing scale zero (`40.99`→`40.99`) and a scale-0 integer DECIMAL (`100`→`100`, `-7`→`-7`), and SHALL pass a NULL DECIMAL through as NULL (both `regexp_replace` calls return NULL on a NULL input)
* *AND* the column the emitted expression produces under DataFusion 54 is Arrow `Utf8View`, which the emit boundary SHALL coerce to `Utf8` for a VARCHAR-declared column (see `datafusion-scan/scan-execution-expression-pushdown`), so a projected `decimal_to_varchar_exasol` column crosses the UDF boundary without a `Utf8View` emit rejection
