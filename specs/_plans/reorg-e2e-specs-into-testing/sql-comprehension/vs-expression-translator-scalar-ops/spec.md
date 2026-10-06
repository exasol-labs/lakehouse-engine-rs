<!-- DELTA:CHANGED -->
# Feature: VS Expression Translator — Scalar Operations

Extends the VS expression translator (`sql-comprehension/vs-expression-translator`) with arithmetic operators and the safe/fallback entry points. CAST target-type rendering is covered in `sql-comprehension/vs-expression-translator-cast`. Named math/string/conditional scalar functions are covered in `sql-functions/vs-expression-translator-scalar-fns`; date/time functions in `sql-functions/vs-expression-translator-date-fns`. Floating-point division (`FLOAT_DIV`) is split out into its own dedicated feature, `sql-comprehension/vs-expression-translator-float-div`, because its rendering diverges by dialect — every other arithmetic operator here still renders byte-identically in both dialects.
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
## Background

The `crates/vs-expression` crate exposes six public entry points in two dialect trios. The DataFusion trio feeds DataFusion's SQL frontend inside the scan UDF:
- `render_expression` — raising mode, returns `Err` for unsupported nodes
- `render_expression_safe` — returns `None` for unsupported nodes, never panics
- `render_df_filter_safe` — same as `render_expression_safe` but also returns `None` for trivially-true results (e.g. `TRUE`, `NULL`) so the adapter can omit no-op filters from the scan spec

The Exasol trio — `render_expression_exasol`, `render_expression_exasol_safe`, `render_df_filter_exasol_safe` — carries the same three contracts for fragments Exasol's own core engine parses.

The arithmetic operator nodes and every decline in this feature behave identically in both dialects with ONE exception: `+`, `-`, `*` and unary `-` are the same syntax in both parsers, and a declined function is declined in both because the adapter advertises one capability set for both dialects — but `/` (`FLOAT_DIV`) diverges by dialect, specified in `sql-comprehension/vs-expression-translator-float-div`.

A conversion or operator node is translated only when its DataFusion 54 result matches Exasol. Exasol `DIV` returns the integer quotient by truncating toward zero — verified live: `DIV(-7,2) = -3` and `DIV(15.7,6.2) = 2` — and raises a division-by-zero error (SQL state 22012). DataFusion 54 has no `div` builtin; its `/` truncates only integer operands and divides non-integer operands fractionally. No single rendering reproduces `DIV` across every operand type, so `DIV` stays unsupported — the disqualifier is that a wrong rendering would be wrong on EVERY row for non-integer operands, the per-row problem rather than the zero-divisor one (see the `FLOAT_DIV` feature for why that same type-blindness does not disqualify `FLOAT_DIV`). DataFusion 54 `to_char` uses strftime masks rather than Exasol's Oracle-style format models and rejects numeric formatting, and DataFusion 54 has no `to_number`. These three functions are therefore left unsupported and fall back to Exasol. The bitwise operator functions (`BIT_AND`, `BIT_OR`, `BIT_XOR`, `BIT_NOT`, `BIT_LSHIFT`, `BIT_RSHIFT`, `BIT_LROTATE`, `BIT_RROTATE`, `BIT_CHECK`, `BIT_SET`, `BIT_TO_NUM`) are likewise unsupported: Exasol defines them over an unsigned 64-bit integer domain that DataFusion's signed-integer operators and the `Int64` → `DECIMAL(20,0)` mapping do not reproduce, and six of the eleven have no DataFusion builtin at all (issue #108).

The `crates/vs-expression` crate stays a pure, stateless JSON-to-SQL translator with no column-type context. The adapter-synthesized node type `decimal_to_varchar_exasol` and the crate-visible pure helper `format_decimal_exasol_style` let an adapter that has already resolved a column as DECIMAL inject an Exasol-faithful DECIMAL→string trim without the translator inspecting types (see `pushdown-types/pushdown-planning-decimal-string-format`).
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Arithmetic operators translate to binary SQL expressions

* *GIVEN* a VS expression node of type `function_scalar` whose `name` is the Exasol scalar-function name for addition, subtraction, or multiplication, or for unary negation
* *AND* the exact `name` strings Exasol emits for these operators have been verified against live `EXPLAIN VIRTUAL` output for an arithmetic pushdown (so the translator matches what Exasol actually sends, e.g. `MULT` for `*`, not an assumed `MUL`)
* *WHEN* `render_expression` or `render_expression_exasol` processes the node
* *THEN* the `ADD`, `SUB`, and `MULT` nodes SHALL return `(<left> <op> <right>)` where the operators are `+`, `-`, `*` respectively, for operands that are themselves any renderable expression (including two bare column references, e.g. `(L_EXTENDEDPRICE * L_DISCOUNT)`), byte-identically in BOTH dialects — the operator syntax is shared by both parsers, and these wire names are NOT Exasol function names (Exasol has no function called `ADD`), so the Exasol dialect's verbatim rule for named functions MUST NOT be applied to them
* *AND* unary negation SHALL return `(-<operand>)` and SHALL compose inside an aggregate argument (e.g. `SUM(-<operand>)`) so it flows through the arithmetic-aggregate decomposition path
* *AND* floating-point division (`FLOAT_DIV`) SHALL NOT be rendered by this shape — it is the one arithmetic operator whose rendering diverges by dialect, specified in `sql-comprehension/vs-expression-translator-float-div` (issue #186); this scenario's "byte-identically in BOTH dialects" claim covers `ADD`, `SUB`, `MULT`, and `NEG` only
* *AND* the set of arithmetic `name` strings the translator matches SHALL correspond exactly to the arithmetic operator capabilities the adapter advertises (`pushdown-capabilities/pushdown-planning-capability-extensions`) — `FN_ADD`, `FN_SUB`, `FN_MULT`, `FN_FLOAT_DIV`, and `FN_NEG` — so no advertised operator is left unrenderable and no rendered operator is left unadvertised
* *AND* Exasol integer division (`DIV`) SHALL NOT be matched here and `FN_DIV` SHALL NOT be advertised
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: Decimal-to-VARCHAR node renders Exasol-trimmed string

* *GIVEN* a VS expression node of type `decimal_to_varchar_exasol` carrying a single `arguments` entry, an adapter-synthesized node the `crates/lakehouse-engine` pushdown layer injects in place of a confirmed-DECIMAL-typed stringification point (never emitted by Exasol on the wire; see `pushdown-types/pushdown-planning-decimal-string-format`)
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL render the single argument recursively, then wrap the rendered SQL fragment with the crate-visible `format_decimal_exasol_style` helper, so the emitted DataFusion SQL reproduces Exasol's shortest-form DECIMAL→string conversion (trailing scale zeros trimmed)
* *AND* a `decimal_to_varchar_exasol` node whose argument count is not exactly one SHALL return an error in raising mode and `None` in the safe variants
* *AND* the translator SHALL apply neither column-type inspection nor any type decision of its own for this node — the caller has already confirmed the wrapped argument is DECIMAL-typed, keeping `vs-expression` a pure, stateless translator
<!-- /DELTA:CHANGED -->
