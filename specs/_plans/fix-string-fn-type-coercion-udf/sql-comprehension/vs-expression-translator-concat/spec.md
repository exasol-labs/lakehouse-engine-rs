# Feature: VS Expression Translator — CONCAT NULL Semantics

Unchanged by this plan, and reproduced here only because the delta validator requires a description. The recorded text stands as written in `specs/sql-comprehension/vs-expression-translator-concat/spec.md`.

<!-- DELTA:CHANGED -->
## Background

**Exasol's `||` treats a NULL operand as the empty string; DataFusion's `||` propagates NULL
(issue #374).** Both halves were captured rather than recalled. On the Exasol 2025.2.1 container
pinned in `docker-compose.yml`, `SELECT NULL || 'abc', 'x' || NULL, CONCAT(NULL, 'abc') FROM
dual` returned `abc`, `x`, and `abc` — never NULL. Against the pinned DataFusion 54.1.0,
`SELECT "NAME" || '-suffix'` over a row whose `NAME` is NULL returned NULL, while
`concat("NAME", '-suffix')` returned `-suffix`; DataFusion documents the difference as "NULL
arguments are ignored" (`datafusion-functions-54.1.0/src/string/concat.rs:106`). Issue #374
captured the end-to-end consequence against staging with TPC-H `CUSTOMER` data: `C_NAME ||
NULLIF(C_MKTSEGMENT, C_MKTSEGMENT) || '-suffix'` returned `Customer#000000001-suffix` natively
and NULL through the virtual schema. Reproduced on this repository's own Docker container against
the 20-row seed fixture: `SELECT id, name || NULLIF(name, name) || '-suffix' FROM
MY_LAKEHOUSE.EVENTS WHERE id <= 3` returned NULL in all three rows, and `SELECT COUNT(*) ... WHERE
(name || NULLIF(name, name)) = name` returned `0` of 20.

**Exasol's VARCHAR domain has no empty string, so an all-NULL concatenation is NULL — which is why
`concat()` alone is not the fix.** Captured live on the same container: `'' IS NULL` is TRUE,
`LENGTH('')` is NULL, and `CONCAT(NULL, NULL) IS NULL` is TRUE. DataFusion's `concat()` returns the
non-NULL empty string instead: against the pinned 54.1.0, `concat(nullif("NAME","NAME"),
nullif("NAME","NAME"))` returned `''`, and `WHERE concat(...) IS NULL` matched 0 rows where Exasol
matches all of them. Wrapping the call as `nullif(concat(...), '')` reproduces Exasol exactly —
the same expression returned NULL and the same filter matched every row. The wrapper is load-bearing
in the FILTER and GROUP-BY-key positions, which DataFusion evaluates before any value crosses the
EMITS boundary; in the VALUE position it is unobservable, because Exasol's VARCHAR domain cannot
represent the `''` that would otherwise be emitted.

**The wrapper also preserves issue #200's NULL-boolean contract, which a bare `concat()` would
break.** A boolean-producing operand is rewritten to `(CASE <expr> WHEN TRUE THEN 'TRUE' WHEN FALSE
THEN 'FALSE' ELSE NULL END)` before assembly, so a NULL boolean becomes NULL rather than lowercase
`true`/`false` or a coerced `'FALSE'`. Under a bare `concat()` that NULL is skipped and the whole
expression becomes `''`; under `nullif(concat(...), '')` it stays NULL. Exasol agrees with the
latter: `(CAST(NULL AS DOUBLE) > 0) || ''` IS NULL and `(100.0 > 0) || ''` is `TRUE`, both
captured live. `crates/lakehouse-engine/tests/boolean_to_string_casing_test.rs` asserts exactly
that NULL label through a real DataFusion `SessionContext`, so it is the structural proof that the
wrapper is required rather than decorative.

**Exasol sends `a || b || c` as NESTED `CONCAT` nodes, and the rendering composes under
nesting.** `vs-adapter/pushdown-planning-decimal-string-format` records the wire shape:
`id || '-' || c_decimal_a` arrives as `CONCAT("ID", CONCAT('-', "C_DECIMAL_A"))`. Each level
renders its own `nullif(concat(...), '')`, and the composition is correct because an inner level
that collapses to NULL is then treated as an ignorable NULL operand by the outer level — exactly
what Exasol does. Verified against the pinned DataFusion 54.1.0: the nested rendering of
`name || NULLIF(name, name) || '-suffix'` returned the full concatenation, and the all-NULL
nesting returned NULL.

**Each argument reaches `concat()` as text.** Every `CONCAT` argument is string-converted (`sql-comprehension/vs-expression-translator-string-conversion`), so a non-boolean argument renders as `exa_to_varchar(<arg>)` inside the `concat(...)` call and a boolean-producing argument renders through the CASE rewrite above. `exa_to_varchar` converts an integer, a decimal, and a date to Exasol's text, which `concat()`'s own coercion does not: against the pinned DataFusion 54.1.0 `concat()` accepted an `Int64` column (`concat("ID", '-x')` returned `1-x`) but rendered a DECIMAL at its full declared scale. Which argument TYPES may reach this rendering is owned by `vs-adapter/pushdown-planning-string-fn-type-coercion`, which declines a bare `DOUBLE`, `BOOLEAN`, or `TIMESTAMP` column argument to native Exasol evaluation.
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: CONCAT translates to a NULL-skipping DataFusion concat call

* *GIVEN* a VS expression node of type `function_scalar` named `CONCAT` with one or more arguments — the wire encoding of Exasol's `||` operator
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL return `nullif(concat(<a1>, <a2>, ...), '')`, where each `<aN>` renders per `sql-comprehension/vs-expression-translator-string-conversion`, and SHALL NOT render DataFusion's `||` operator, which propagates NULL where Exasol treats a NULL operand as the empty string (issue #374)
* *AND* the `concat(...)` call SHALL carry EVERY argument, in the order received, comma-separated, because DataFusion's `concat` ignores a NULL argument — matching Exasol's `||`, which treats a NULL operand as the empty string
* *AND* the `nullif(..., '')` wrapper SHALL be emitted for EVERY argument list, because Exasol's VARCHAR domain contains no empty string (`'' IS NULL` is TRUE and `CONCAT(NULL, NULL) IS NULL` is TRUE, both captured live) while DataFusion's `concat` returns a non-NULL `''` for an all-NULL argument list — without the wrapper, `WHERE <concat> IS NULL` matches no row where Exasol matches every row
* *AND* the wrapper MUST NOT be omitted on the grounds that the VALUE position cannot observe it: the FILTER and GROUP-BY-key positions are evaluated inside DataFusion before any value crosses the EMITS boundary, and issue #200's NULL-boolean group label depends on it
* *AND* a boolean-producing argument SHALL still be rewritten to `(CASE <arg> WHEN TRUE THEN 'TRUE' WHEN FALSE THEN 'FALSE' ELSE NULL END)` before assembly, BYTE-IDENTICAL to its pre-delta form, so Exasol's `TRUE`/`FALSE` casing survives and a NULL boolean still yields NULL rather than lowercase `true`/`false`, the string `'NULL'`, or a coerced `'FALSE'` (issue #200)
* *AND* each argument SHALL be rendered exactly ONCE and referenced exactly once, so no sub-expression is walked or evaluated twice
* *AND* a nested `CONCAT` argument SHALL render its own `nullif(concat(...), '')`, because Exasol sends `a || b || c` as nested `CONCAT` nodes (`vs-adapter/pushdown-planning-decimal-string-format`), so that every level reproduces Exasol's own per-level semantics
* *AND* a single-argument call SHALL render `nullif(concat(<a1>), '')`, which is Exasol's own single-argument behavior — `CONCAT('a')` is `'a'` and a single NULL argument yields NULL, both captured live
* *AND* a node whose `arguments` key is absent SHALL return an error, and an EMPTY argument list SHALL return an error in raising mode and `None` in the safe variants, in BOTH dialects — REPLACING the recorded absence of any arity floor, which rendered the syntactically invalid `()` in the Exasol dialect and `concat()` in the DataFusion dialect
* *AND* `render_expression_exasol` SHALL render `(<a1> || <a2> ...)` — chained `||`, parenthesized, with the same per-argument boolean rewrite — BYTE-IDENTICAL to its pre-delta output and carrying NO `nullif`/`concat` wrapper, because Exasol's own `||` already has these semantics; the `ExasolForm::Shaped` declaration is unchanged, so the declaration-driven verbatim sweep test keeps its `("A" || "B")` expectation and stays green with no edit
* *AND* `capabilities.rs` SHALL keep advertising `FN_CONCAT` unchanged, so the fix restores the semantics at the rendering site rather than withdrawing the pushdown
<!-- /DELTA:CHANGED -->
