# Feature: VS Expression Translator

A standalone workspace crate (`crates/vs-expression`) that translates Exasol Virtual Schema pushdown expression-JSON nodes into SQL fragments for DataFusion and for Exasol. This feature owns the shared entry points, the dialect dispatch, column references, and aggregate-node splicing. `sql-comprehension/vs-expression-translator-predicates` specifies the filter-predicate operator set.

## Background

* Exasol sends expression trees as serde_json `Value` objects: column references, literals, comparison predicates, logical operators, scalar functions, arithmetic operators, CAST, and aggregate function nodes (`function_aggregate`). The crate uses only serde_json as its IR.
* An aggregate node is not a translated function. Its name is spliced verbatim and its arguments are rendered by recursion, so a scalar expression that wraps aggregates renders in full.
* The crate exposes six entry points in two dialect trios. The DataFusion trio is `render_expression` (returns `Err` for unsupported nodes), `render_expression_safe` (returns `None` for unsupported nodes), and `render_df_filter_safe` (as safe, and also suppresses trivially-true results). Its fragments are parsed by DataFusion inside the scan UDF. The Exasol trio (`render_expression_exasol`, `render_expression_exasol_safe`, `render_df_filter_exasol_safe`) has the same three contracts, and its fragments are spliced into outer wrapper SQL that Exasol parses.
* `render_expression_safe` does not suppress a trivially-true result, so it returns `None` for exactly the declined case.
* The dialect is threaded through every node, and a node branches on it where the two parsers disagree. The Exasol dialect renders what Exasol sent (verbatim name, argument order, and argument count). A construct that is not an Exasol call form has its own per-name arm: an operator wire name, `MOD`, `CONCAT`, a CAST target, and `CASE`.
* An Exasol-dialect fragment reaches Exasol's parser as a select item, a GROUP BY key, a HAVING operand, an ORDER BY element, an N-scan cross-side residual, or a declined single-table WHERE predicate (rendered through `render_df_filter_qualified`). An acceptance test for Exasol-dialect rendering uses one of those positions.
* Every translated `function_scalar` name is declared once in the crate, as `VerbatimCall` (`<NAME>(<rendered args>)`) or `Shaped` (rendered by its own per-name arm). The declaration gates dispatch, so an undeclared name is declined in both dialects with `unsupported scalar function: <name>` before any per-name arm is reached.

## Scenarios

### Scenario: A declined single-table WHERE predicate is an Exasol-dialect wrapper position

* *GIVEN* a single-table WHERE predicate whose DataFusion-dialect render declines while its Exasol-dialect render succeeds
* *WHEN* the adapter renders that predicate for the qualified single-table wrapper's outer `WHERE`
* *THEN* the Exasol trio SHALL be the renderer used, reached through `render_df_filter_qualified`, so the fragment is parsed by Exasol's core engine and carries length-qualified CAST targets
* *AND* the recorded set of wrapper positions an Exasol-dialect node can reach SHALL include this one, so an acceptance test for Exasol-dialect rendering MAY use a declined single-table WHERE predicate
* *AND* no translator entry point SHALL be added, because `render_expression_safe` already returns `None` for exactly the declined case and the trivially-true rule stays owned by this crate

### Scenario: Bare column reference translates to quoted identifier

* *GIVEN* a VS expression node of `type: "column"` with a `name` field
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL return the column name uppercased and double-quoted as a DataFusion identifier
* *AND* any embedded double-quote characters in the name MUST be escaped by doubling
* *AND* when the `column` node ALSO carries a non-empty `tableAlias`, the translator SHALL render the reference table-qualified as `"ALIAS"."NAME"`, the multi-relation form `pushdown-joins/pushdown-planning-join-fallback` depends on
* *AND* the translator MUST NOT drop a `tableAlias` on its own; removing a `tableAlias` so a single-relation scan target resolves bare names is the CALLER's responsibility (`pushdown/pushdown-planning`), NOT the translator's

### Scenario: An undeclared scalar function name is not translated in either dialect

* *GIVEN* a VS expression node of type `function_scalar` whose `name` is absent from the crate's one declaration of translated `function_scalar` names (for example `SUBSTRING`), whether or not a per-name rendering arm exists for that name
* *WHEN* `render_expression` or `render_expression_exasol` processes the node in raising mode
* *THEN* the translator SHALL return an error reading `unsupported scalar function: <NAME>` in both dialects, the same error an unrecognised name raises
* *AND* `render_expression_safe` and `render_expression_exasol_safe` SHALL return `None` for the same node without panicking
* *AND* the declaration lookup MUST happen before any per-name rendering arm is reached, so an arm added without a declaration entry is unreachable and cannot emit DataFusion SQL on the Exasol path
* *AND* the adapter SHALL omit the expression and let Exasol evaluate it, exactly as it does for any other untranslated name

### Scenario: Aggregate function nodes render with the aggregate name spliced verbatim

* *GIVEN* a VS expression node of type `function_aggregate`, either standalone (e.g. `SUM(col)`, `COUNT(*)`, `COUNT(DISTINCT col)`, `AVG(col)`) or nested inside a scalar function (e.g. the `SUM(CASE WHEN … END)` and `COUNT(*)` inside `ROUND(100.0 * SUM(CASE WHEN … END) / COUNT(*), 2)`)
* *WHEN* `render_expression` processes the node (directly, or by recursion from an enclosing `function_scalar`/arithmetic node)
* *THEN* the translator SHALL splice the aggregate `name` verbatim (uppercased, it is NOT mapped to a DataFusion function alias the way scalar functions are), rendering `<NAME>(<rendered args>)`
* *AND* a node with empty `arguments` or a star argument SHALL render as `COUNT(*)`
* *AND* a node carrying `distinct: true` SHALL render as `<NAME>(DISTINCT <rendered arg>)`
* *AND* each argument SHALL be rendered recursively by the translator (so `CASE`, arithmetic, and column-reference arguments render correctly), and a column argument carrying a `tableAlias` SHALL render table-qualified as `"ALIAS"."COL"`
* *AND* the translator MUST NOT fall through to the unsupported-node catch-all for a `function_aggregate` node (which returns an error in raising mode and `None` in the safe variants and would wrongly decline a scalar-over-aggregate select item)
* *AND* an aggregate node whose argument cannot be rendered SHALL return an error in raising mode and `None` in the safe variants, consistent with every other node type

### Scenario: The Exasol dialect never contains a DataFusion-only function name

* *GIVEN* every translated expression shape in the Exasol dialect
* *WHEN* the translator renders it
* *THEN* the output SHALL contain none of the DataFusion-only function names the translator can emit, including the checked-division function
* *AND* the banned-name list SHALL NOT name `CAST`, which is valid Exasol SQL
