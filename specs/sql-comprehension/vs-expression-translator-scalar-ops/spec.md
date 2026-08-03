# Feature: VS Expression Translator — Scalar Operations

Extends the VS expression translator (`sql-comprehension/vs-expression-translator`) with arithmetic operators, CAST, and the safe/fallback entry points. Named math/string/conditional scalar functions are covered in `sql-comprehension/vs-expression-translator-scalar-fns`; date/time functions in `sql-comprehension/vs-expression-translator-date-fns`.

## Background

The `crates/vs-expression` crate exposes three public entry points:
- `render_expression` — raising mode, returns `Err` for unsupported nodes
- `render_expression_safe` — returns `None` for unsupported nodes, never panics
- `render_df_filter_safe` — same as `render_expression_safe` but also returns `None` for trivially-true results (e.g. `TRUE`, `NULL`) so the adapter can omit no-op filters from the scan spec

A conversion or operator node is translated only when its DataFusion 54 result matches Exasol. Exasol `DIV` returns the integer quotient by truncating toward zero — verified live: `DIV(-7,2) = -3` and `DIV(15.7,6.2) = 2` — and raises a division-by-zero error (SQL state 22012). DataFusion 54 has no `div` builtin; its `/` truncates only integer operands and divides non-integer operands fractionally, and float division by zero yields infinity instead of an error. No single rendering reproduces `DIV` across every operand type, so `DIV` stays unsupported. DataFusion 54 `to_char` uses strftime masks rather than Exasol's Oracle-style format models and rejects numeric formatting, and DataFusion 54 has no `to_number`. These three functions are therefore left unsupported and fall back to Exasol. The bitwise operator functions (`BIT_AND`, `BIT_OR`, `BIT_XOR`, `BIT_NOT`, `BIT_LSHIFT`, `BIT_RSHIFT`, `BIT_LROTATE`, `BIT_RROTATE`, `BIT_CHECK`, `BIT_SET`, `BIT_TO_NUM`) are likewise unsupported: Exasol defines them over an unsigned 64-bit integer domain that DataFusion's signed-integer operators and the `Int64` → `DECIMAL(20,0)` mapping do not reproduce, and six of the eleven have no DataFusion builtin at all (issue #108).

Exasol emits CAST as its own top-level node type, `function_scalar_cast` — not nested inside a generic `function_scalar` node — matching the same family pattern as `function_scalar_case` and `function_scalar_extract`. The translator also retains a defensive nested `function_scalar`+`name=CAST` arm for a legacy/alternate encoding, sharing the same rendering logic, but `function_scalar_cast` is the node type Exasol's live engine actually sends.

`render_cast_target` has TWO dialect arms, threaded through the shared recursive translator by a private `Dialect` parameter (`specs/_decision/011-fix-count-distinct-shard-cap.md`, follow-up "Exasol-dialect CAST for the qualified wrapper"). The `DataFusion` arm feeds fragments embedded in a `ScanSpec` (`filter`/`projection`/`group_keys`) that datafusion-sql parses inside the scan UDF; the `Exasol` arm feeds wrapper SQL text that Exasol's own core engine parses. On a `CHAR` target, the DataFusion arm renders a bare, length-less `VARCHAR` — Arrow has only `Utf8` and datafusion-sql rejects a length-qualified character target without `support_varchar_with_length`, which this project does not enable. The Exasol arm renders `CHAR({size})`, plus ` ASCII` when the node's `dataType.characterSet` is `ASCII` case-insensitively, matching the width and character set Exasol validates positionally against `selectListDataTypes` — rendering bare `CHAR({size})` for an ASCII-declared target would trade a `VARCHAR(n) ASCII` mismatch for a `CHAR(n) UTF8` one. `CAST(<expr> AS CHAR(n) ASCII)` is valid Exasol CAST syntax, verified live on Exasol 2025.2.1. Three Exasol-parsed wrapper paths reach the Exasol arm: `joins/sql_builders.rs`'s `n_scan_join_select_items` (the N-scan unaccelerated join wrapper) and `build_qualified_single_table_fallback_sql` (the qualified single-table aggregate fallback), both via `render_selectlist_item_qualified` → `render_expression_exasol_safe`; and `grouped_agg.rs`'s `render_scalar_over_merge` (the grouped-merge scalar-over-aggregate wrapper, reached from `build_grouped_aggregate_scan_sql`'s `ScalarOverAggregate` arm), via `render_expression_exasol` directly. The suffix rule mirrors the adapter's `exasol_type_from_json` CHAR rule so the two independent seams cannot disagree on a CHAR target — a claim scoped to CHAR deliberately: on a VARCHAR target the two seams already differ in suffix handling (the adapter's VARCHAR arm appends ` ASCII` for an ASCII `characterSet`; this crate's Exasol-dialect VARCHAR rendering emits `VARCHAR({size})` with no suffix), a pre-existing asymmetry this feature leaves untouched. This crate is shared with a sibling VS-adapter project (`specs/mission.md`), so the CHAR case is a narrowly additive dialect arm that leaves the `Dialect::DataFusion` behavior and the Exasol `VARCHAR` rendering byte-identical.

## Scenarios

### Scenario: Arithmetic operators translate to binary SQL expressions

* *GIVEN* a VS expression node of type `function_scalar` whose `name` is the Exasol scalar-function name for a binary arithmetic operator — addition, subtraction, multiplication, or floating-point division — or for unary negation
* *AND* the exact `name` strings Exasol emits for these operators have been verified against live `EXPLAIN VIRTUAL` output for an arithmetic pushdown (so the translator matches what Exasol actually sends, e.g. `MULT` for `*`, not an assumed `MUL`)
* *WHEN* `render_expression` processes the node
* *THEN* the binary arithmetic nodes SHALL return `(<left> <op> <right>)` where the operators are `+`, `-`, `*`, `/` respectively, for operands that are themselves any renderable expression (including two bare column references, e.g. `(L_EXTENDEDPRICE * L_DISCOUNT)`)
* *AND* unary negation SHALL return `(-<operand>)` and SHALL compose inside an aggregate argument (e.g. `SUM(-<operand>)`) so it flows through the arithmetic-aggregate decomposition path
* *AND* the set of arithmetic `name` strings the translator matches SHALL correspond exactly to the arithmetic operator capabilities the adapter advertises (`vs-adapter/pushdown-planning-capability-extensions`) — `FN_ADD`, `FN_SUB`, `FN_MULT`, `FN_FLOAT_DIV`, and `FN_NEG` — so no advertised operator is left unrenderable and no rendered operator is left unadvertised
* *AND* Exasol integer division (`DIV`) SHALL NOT be matched here and `FN_DIV` SHALL NOT be advertised

### Scenario: CAST translates to DataFusion CAST syntax

* *GIVEN* a VS expression node of type `function_scalar_cast` with `name` equal to `CAST` — the top-level node type Exasol's engine serializer emits for CAST (verified against the Exasol engine source; `function_scalar`+`name=CAST` is retained only as a defensive nested/alternate encoding, not the primary wire shape)
* *AND* the node carries a `dataType` field with at minimum a `type` string (e.g., `"VARCHAR"`, `"CHAR"`, `"DECIMAL"`, `"DOUBLE"`, `"BOOLEAN"`, `"DATE"`, `"TIMESTAMP"`)
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL return `CAST(<expr> AS <target_type>)` where `<target_type>` maps the VS data-type descriptor to an equivalent DataFusion type name, rendering `DECIMAL(p,s)` as `DECIMAL(p,s)`; both `VARCHAR` and `CHAR` as a bare, length-less `VARCHAR` — a DataFusion-dialect-specific rendering, because datafusion-sql rejects a length-qualified character target without `support_varchar_with_length` and Arrow has only `Utf8`, with no CHAR type for a fixed-width target to map to; `DOUBLE` as `DOUBLE`; `BOOLEAN` as `BOOLEAN`; `DATE` as `DATE`; `TIMESTAMP` as `TIMESTAMP`
* *AND* a `dataType` whose `type` is an Exasol target with no faithful DataFusion mapping — `INTERVAL YEAR TO MONTH`, `INTERVAL DAY TO SECOND`, `GEOMETRY`, `HASHTYPE`, or `TIMESTAMP WITH LOCAL TIME ZONE` — SHALL return an error in raising mode and `None` in the safe variants, so the adapter omits the CAST and Exasol evaluates it as a correctness backstop
* *AND* the set of CAST target types the translator renders SHALL be exactly the set whose DataFusion result matches Exasol's CAST result, so `FN_CAST` (advertised per `vs-adapter/pushdown-planning-capability-extensions`) is never advertised for a target the translator would render divergently

### Scenario: The Exasol dialect renders a CHAR CAST target as CHAR, not VARCHAR

* *GIVEN* a `function_scalar_cast` node whose `dataType` is `{"type":"CHAR","size":20,"characterSet":"ASCII"}`
* *WHEN* the node is rendered through the Exasol-dialect entry points (`render_expression_exasol`, `render_expression_exasol_safe`, `render_df_filter_exasol_safe`) — the ones whose output Exasol's own core engine parses in the qualified single-table wrapper, the N-scan join wrapper, and the grouped-merge wrapper
* *THEN* the CAST target SHALL render as `CHAR(20) ASCII`, appending the ` ASCII` suffix exactly when `characterSet` equals `ASCII` case-insensitively and no suffix otherwise
* *AND* the target MUST NOT render as `VARCHAR({size})`, which Exasol rejects as `Data type mismatch ... Expected CHAR(20) ASCII` and which also strips the value's blank padding, nor as a bare length-less `CHAR` or `VARCHAR`, which Exasol's parser rejects outright (`sqlCode 04000`, "unexpected ')', expecting '('") — the regression this dialect split was introduced to fix
* *AND* the Exasol dialect SHALL keep rendering a `VARCHAR` target as `VARCHAR({size})` with no character-set suffix, unchanged
* *AND* the Exasol dialect SHALL keep trusting the `size` Exasol sent without clamping it, unchanged — the defensive 2,000 CHAR cap belongs to the adapter's `exasol_type_from_json`, the seam that synthesizes a declared type rather than echoing one Exasol just sent
* *AND* a NESTED CHAR CAST — `CAST(CAST(<agg> AS CHAR(20) ASCII) AS CHAR(20) ASCII)`, the shape the grouped-merge wrapper can produce — SHALL render `CHAR(20) ASCII` at BOTH levels, because the renderer recurses into itself and the CHAR case therefore applies at every level
* *AND* the two dialects SHALL still DIVERGE on the same CHAR node — bare `VARCHAR` in the DataFusion dialect, `CHAR({size})` plus any suffix in the Exasol dialect — so the existing divergence guard remains a guard, its Exasol-side expectation retargeted rather than removed

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

### Scenario: Integer division DIV is deliberately not translated

* *GIVEN* a VS expression node of type `function_scalar` named `DIV` — Exasol integer-quotient division, which truncates toward zero (`DIV(-7,2) = -3`, verified live) and raises a division-by-zero error
* *WHEN* `render_expression` processes the node in raising mode
* *THEN* the translator SHALL return an error naming `DIV` as unsupported
* *AND* `render_expression_safe` SHALL return `None` for the same node without panicking
* *AND* the adapter SHALL omit the expression and let Exasol evaluate `DIV`, because DataFusion 54 has no `div` builtin and a `TRUNC(m/n)` emulation diverges from Exasol for DOUBLE operands on division by zero — Exasol raises SQL state 22012, DataFusion float division yields infinity — and unlike CAST's explicit `dataType` field, DIV's operand types are not carried in the expression node, so the translator cannot identify and selectively render only the safe integer-operand case

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
