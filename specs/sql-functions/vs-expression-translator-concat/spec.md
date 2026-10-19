# Feature: VS Expression Translator, CONCAT NULL Semantics

Renders `CONCAT`, the wire encoding of Exasol's `||` operator, so the DataFusion dialect reproduces Exasol's NULL handling. The Exasol dialect keeps its `ExasolForm::Shaped` declaration, and `sql-functions/vs-expression-translator-scalar-fns` lists `CONCAT` in its verbatim exclusions.

## Background

* Exasol's `||` treats a NULL operand as the empty string. DataFusion's `||` propagates NULL. DataFusion's `concat()` ignores NULL arguments.
* Exasol's VARCHAR domain has no empty string: `'' IS NULL` is TRUE, and `CONCAT(NULL, NULL) IS NULL` is TRUE. DataFusion's `concat()` returns a non-NULL `''` for an all-NULL argument list. The `nullif(concat(...), '')` wrapper reproduces Exasol. DataFusion evaluates filters and GROUP BY keys before any value crosses the EMITS boundary, so the wrapper matters in those positions, while it is unobservable in the value position.
* The translator rewrites a boolean-producing operand to `(CASE <expr> WHEN TRUE THEN 'TRUE' WHEN FALSE THEN 'FALSE' ELSE NULL END)` before assembly, so a NULL boolean stays NULL. A bare `concat()` would skip that NULL, while `nullif(concat(...), '')` preserves it.
* Exasol sends `a || b || c` as nested `CONCAT` nodes: `id || '-' || c_decimal_a` arrives as `CONCAT("ID", CONCAT('-', "C_DECIMAL_A"))` (see `pushdown-types/pushdown-planning-decimal-string-format`). Each level renders its own `nullif(concat(...), '')`. An inner level that collapses to NULL is an ignorable NULL operand for the outer level, as in Exasol.
* `pushdown-types/pushdown-planning-string-fn-type-coercion` and `pushdown-types/pushdown-planning-decimal-string-format` own which argument types reach this rendering.

## Scenarios

### Scenario: CONCAT translates to a NULL-skipping DataFusion concat call

* *GIVEN* a VS expression node of type `function_scalar` named `CONCAT` with one or more arguments, the wire encoding of Exasol's `||` operator
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL return `nullif(concat(<a1>, <a2>, ...), '')`, REPLACING the recorded `(<a1> || <a2> ...)` chained-operator rendering, which propagates NULL in DataFusion where Exasol treats a NULL operand as the empty string, and so silently returns NULL values and wrong row sets (issue #374)
* *AND* the `concat(...)` call SHALL carry EVERY argument, in the order received, comma-separated, because DataFusion's `concat` ignores a NULL argument, matching Exasol's `||`, which treats a NULL operand as the empty string
* *AND* the `nullif(..., '')` wrapper SHALL be emitted for EVERY argument list, because Exasol's VARCHAR domain contains no empty string (`'' IS NULL` is TRUE and `CONCAT(NULL, NULL) IS NULL` is TRUE, both captured live) while DataFusion's `concat` returns a non-NULL `''` for an all-NULL argument list, without the wrapper, `WHERE <concat> IS NULL` matches no row where Exasol matches every row
* *AND* the wrapper MUST NOT be omitted on the grounds that the VALUE position cannot observe it: the FILTER and GROUP-BY-key positions are evaluated inside DataFusion before any value crosses the EMITS boundary, and issue #200's NULL-boolean group label depends on it
* *AND* a boolean-producing argument SHALL still be rewritten to `(CASE <arg> WHEN TRUE THEN 'TRUE' WHEN FALSE THEN 'FALSE' ELSE NULL END)` before assembly, BYTE-IDENTICAL to its pre-delta form, so Exasol's `TRUE`/`FALSE` casing survives and a NULL boolean still yields NULL rather than lowercase `true`/`false`, the string `'NULL'`, or a coerced `'FALSE'` (issue #200)
* *AND* each argument SHALL be rendered exactly ONCE and referenced exactly once, so no sub-expression is walked or evaluated twice
* *AND* a nested `CONCAT` argument SHALL render its own `nullif(concat(...), '')`, because Exasol sends `a || b || c` as nested `CONCAT` nodes (`pushdown-types/pushdown-planning-decimal-string-format`), so that every level reproduces Exasol's own per-level semantics
* *AND* a single-argument call SHALL render `nullif(concat(<a1>), '')`, which is Exasol's own single-argument behavior, `CONCAT('a')` is `'a'` and a single NULL argument yields NULL, both captured live
* *AND* a node whose `arguments` key is absent SHALL return an error, and an EMPTY argument list SHALL return an error in raising mode and `None` in the safe variants, in BOTH dialects, REPLACING the recorded absence of any arity floor, which rendered the syntactically invalid `()` in the Exasol dialect and `concat()` in the DataFusion dialect
* *AND* `render_expression_exasol` SHALL render `(<a1> || <a2> ...)`, chained `||`, parenthesized, with the same per-argument boolean rewrite, BYTE-IDENTICAL to its pre-delta output and carrying NO `nullif`/`concat` wrapper, because Exasol's own `||` already has these semantics; the `ExasolForm::Shaped` declaration is unchanged, so the declaration-driven verbatim sweep test keeps its `("A" || "B")` expectation and stays green with no edit
* *AND* `capabilities.rs` SHALL keep advertising `FN_CONCAT` unchanged, so the fix restores the semantics at the rendering site rather than withdrawing the pushdown

### Scenario: A pushed-down CONCAT over a NULL operand concatenates the non-NULL parts on the cluster

* *GIVEN* a virtual schema over the seeded 20-row fixture whose `id` runs 1..20 and whose `name` is `event-01`..`event-20`, and a query concatenating `name` with `NULLIF(name, name)`, NULL for every row, and the same shape issue #374 reproduced against TPC-H `CUSTOMER`
* *WHEN* Exasol pushes the query down and the scan UDF evaluates the expression in DataFusion
* *THEN* `SELECT id, name || NULLIF(name, name) || '-suffix' ... WHERE id <= 3 ORDER BY id` SHALL return `event-01-suffix`, `event-02-suffix`, and `event-03-suffix`, NOT the NULL that chained `||` returns, captured pre-fix on this repository's own container, so the fixture discriminates correct from buggy behavior in the VALUE position
* *AND* `SELECT COUNT(*) ... WHERE (name || NULLIF(name, name)) = name` SHALL return `20`, NOT the `0` captured pre-fix, so the fixture discriminates in the FILTER position
* *AND* `SELECT COUNT(*) ... WHERE (NULLIF(name, name) || NULLIF(name, name)) IS NULL` SHALL return `20`, so the fixture discriminates a `nullif`-wrapped rendering from a bare `concat(...)` one, the bare call returns `''`, which is not NULL, and would match `0` rows
