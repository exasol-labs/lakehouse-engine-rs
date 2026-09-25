# Feature: VS Expression Translator — Scalar Operations

Unchanged by this plan, and reproduced here only because the delta validator requires a description. The recorded text stands as written in `specs/sql-comprehension/vs-expression-translator-scalar-ops/spec.md`.

<!-- DELTA:CHANGED -->
## Background

The `crates/vs-expression` crate exposes six public entry points in two dialect trios. The DataFusion trio feeds DataFusion's SQL frontend inside the scan UDF:
- `render_expression` — raising mode, returns `Err` for unsupported nodes
- `render_expression_safe` — returns `None` for unsupported nodes, never panics
- `render_df_filter_safe` — same as `render_expression_safe` but also returns `None` for trivially-true results (e.g. `TRUE`, `NULL`) so the adapter can omit no-op filters from the scan spec

The Exasol trio — `render_expression_exasol`, `render_expression_exasol_safe`, `render_df_filter_exasol_safe` — carries the same three contracts for fragments Exasol's own core engine parses.

The arithmetic operator nodes and every decline in this feature behave identically in both dialects with ONE exception: `+`, `-`, `*` and unary `-` are the same syntax in both parsers, and a declined function is declined in both because the adapter advertises one capability set for both dialects — but `/` (`FLOAT_DIV`) diverges by dialect, specified in `sql-comprehension/vs-expression-translator-float-div`.

A conversion or operator node is translated only when its DataFusion 54 result matches Exasol. Exasol `DIV` returns the integer quotient by truncating toward zero — verified live: `DIV(-7,2) = -3` and `DIV(15.7,6.2) = 2` — and raises a division-by-zero error (SQL state 22012). DataFusion 54 has no `div` builtin; its `/` truncates only integer operands and divides non-integer operands fractionally. No single rendering reproduces `DIV` across every operand type, so `DIV` stays unsupported — the disqualifier is that a wrong rendering would be wrong on EVERY row for non-integer operands, the per-row problem rather than the zero-divisor one (see the `FLOAT_DIV` feature for why that same type-blindness does not disqualify `FLOAT_DIV`). DataFusion 54 `to_char` uses strftime masks rather than Exasol's Oracle-style format models and rejects numeric formatting, and DataFusion 54 has no `to_number`. These three functions are therefore left unsupported and fall back to Exasol. The bitwise operator functions (`BIT_AND`, `BIT_OR`, `BIT_XOR`, `BIT_NOT`, `BIT_LSHIFT`, `BIT_RSHIFT`, `BIT_LROTATE`, `BIT_RROTATE`, `BIT_CHECK`, `BIT_SET`, `BIT_TO_NUM`) are likewise unsupported: Exasol defines them over an unsigned 64-bit integer domain that DataFusion's signed-integer operators and the `Int64` → `DECIMAL(20,0)` mapping do not reproduce, and six of the eleven have no DataFusion builtin at all (issue #108).

The `crates/vs-expression` crate stays a pure, stateless, sibling-shared JSON-to-SQL translator with no column-type context. `sql-comprehension/vs-expression-translator-string-conversion` specifies how a string-function or string-CAST argument converts to text.
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:REMOVED -->
### Scenario: Decimal-to-VARCHAR node renders Exasol-trimmed string

* *GIVEN* a VS expression node of type `decimal_to_varchar_exasol` carrying a single `arguments` entry, an adapter-synthesized node the `crates/lakehouse-engine` pushdown layer injects in place of a confirmed-DECIMAL-typed stringification point (never emitted by Exasol on the wire; see `vs-adapter/pushdown-planning-decimal-string-format`)
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL render the single argument recursively, then wrap the rendered SQL fragment with the crate-visible `format_decimal_exasol_style` helper, so the emitted DataFusion SQL reproduces Exasol's shortest-form DECIMAL→string conversion (trailing scale zeros trimmed)
* *AND* a `decimal_to_varchar_exasol` node whose argument count is not exactly one SHALL return an error in raising mode and `None` in the safe variants
* *AND* the translator SHALL apply neither column-type inspection nor any type decision of its own for this node — the caller has already confirmed the wrapped argument is DECIMAL-typed, keeping `vs-expression` a pure, stateless, sibling-shared translator
<!-- /DELTA:REMOVED -->

<!-- DELTA:REMOVED -->
### Scenario: format_decimal_exasol_style reproduces Exasol shortest-form decimal formatting

* *GIVEN* the crate-visible pure helper `format_decimal_exasol_style(expr_sql: &str) -> String`, which takes an already-rendered SQL fragment for a confirmed-DECIMAL-typed expression and carries no type information of its own
* *WHEN* the helper is called with a rendered fragment `<f>`
* *THEN* it SHALL return a DataFusion SQL string expression that casts `<f>` to text and trims trailing scale zeros — reproducing Exasol's DECIMAL→string conversion — using `regexp_replace(regexp_replace(CAST(<f> AS VARCHAR), '(\.[0-9]*[1-9])0+$', '\1'), '\.0+$', '')`, whose two POSIX-backreference replacements DataFusion 54 accepts
* *AND* the emitted expression SHALL trim a fractional part to its shortest form, including for negatives, and drop the decimal point entirely when the fraction is all zeros, verified for `2912.00`→`2912`, `-272.60`→`-272.6`, `868.90`→`868.9`, `0.00`→`0`, `100.00`→`100`, and `12.350`→`12.35`
* *AND* the emitted expression SHALL leave unchanged a value with no trailing scale zero (`40.99`→`40.99`) and a scale-0 integer DECIMAL (`100`→`100`, `-7`→`-7`), and SHALL pass a NULL DECIMAL through as NULL (both `regexp_replace` calls return NULL on a NULL input)
* *AND* the column the emitted expression produces under DataFusion 54 is Arrow `Utf8View`, which the emit boundary SHALL coerce to `Utf8` for a VARCHAR-declared column (see `datafusion-scan/scan-execution-expression-pushdown`), so a projected `decimal_to_varchar_exasol` column crosses the UDF boundary without a `Utf8View` emit rejection
<!-- /DELTA:REMOVED -->
