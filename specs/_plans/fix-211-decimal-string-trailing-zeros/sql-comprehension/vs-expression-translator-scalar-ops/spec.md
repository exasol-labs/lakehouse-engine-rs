# Feature: VS Expression Translator — Scalar Operations

Extends the VS expression translator (`sql-comprehension/vs-expression-translator`) with arithmetic operators, CAST, and the safe/fallback entry points. Named math/string/conditional scalar functions are covered in `sql-comprehension/vs-expression-translator-scalar-fns`; date/time functions in `sql-comprehension/vs-expression-translator-date-fns`.

## Background

The `crates/vs-expression` crate stays a pure, stateless, sibling-shared JSON-to-SQL translator with no column-type context. This change adds one adapter-synthesized node type, `decimal_to_varchar_exasol`, and one crate-visible pure helper, `format_decimal_exasol_style`, so an adapter that has already resolved a column as DECIMAL can inject an Exasol-faithful DECIMAL→string trim without the translator inspecting types (see `vs-adapter/pushdown-planning-decimal-string-format`).

## Scenarios

<!-- DELTA:NEW -->
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
<!-- /DELTA:NEW -->
