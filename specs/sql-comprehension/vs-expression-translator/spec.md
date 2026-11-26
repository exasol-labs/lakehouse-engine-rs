# Feature: VS Expression Translator

A standalone workspace crate (`crates/vs-expression`) that translates Exasol Virtual Schema pushdown expression-JSON nodes into SQL fragments for DataFusion and for Exasol. This feature owns the shared entry points, the dialect dispatch, column references, and aggregate-node splicing. `sql-comprehension/vs-expression-translator-predicates` specifies the filter-predicate operator set.

## Background

* Exasol sends expression trees as serde_json `Value` objects: column references, literals, comparison predicates, logical operators, scalar functions, arithmetic operators, CAST, and aggregate function nodes (`function_aggregate`). The crate reads the whole adapter request of a pushdown through one entry point, `parse_request`, which returns the involved tables and the clauses with each expression tree parsed once into the crate's own typed expression tree. No other entry point takes request JSON, and every writer and walker works on the typed tree.
* An aggregate node is not a translated function. Its name is spliced verbatim and its arguments are rendered by recursion, so a scalar expression that wraps aggregates renders in full.
* The crate's DataFusion entry points are `ScanTyping::render_expression`, which returns `Err` for an unsupported node and for one the scan must not compute (`pushdown-types/pushdown-planning-scan-text-conversion`), and `ScanTyping::render_filter`, which also renders a `NULL` result as `FALSE`. Their fragments are parsed by DataFusion inside the scan UDF. The Exasol entry points (`render_expression_exasol`, `ScanTyping::exasol_fragment`) always return a fragment, spliced into outer wrapper SQL that Exasol parses, because Exasol pushes only the capabilities the adapter advertises and the Exasol dialect renders every one of them.
* `ScanTyping::render_expression` returns `Err` for exactly the declined case.
* Each parser has its own writer. The Exasol writer renders what Exasol sent (verbatim name, argument order, and argument count). The typed scan pass writes each node's DataFusion SQL as it types the node, so no DataFusion fragment exists for an expression that was not typed. The two writers share the forms both parsers read alike. A construct that is not an Exasol call form has its own per-name arm: an operator wire name, `MOD`, `CONCAT`, a CAST target, and `CASE`.
* An Exasol-dialect fragment reaches Exasol's parser as a select item, a GROUP BY key, a HAVING operand, an ORDER BY element, an N-scan cross-side residual, or a declined single-table WHERE predicate. An acceptance test for Exasol-dialect rendering uses one of those positions.
* Every translated `function_scalar` name is declared once in the crate, as `VerbatimCall` (`<NAME>(<rendered args>)`) or `Shaped` (rendered by its own per-name arm). The declaration gates dispatch, so the DataFusion dialect declines an undeclared name with `unsupported scalar function: <name>` before any per-name arm is reached.

## Scenarios

### Scenario: A declined single-table WHERE predicate is an Exasol-dialect wrapper position

* *GIVEN* a single-table WHERE predicate whose DataFusion-dialect render declines while its Exasol-dialect render succeeds
* *WHEN* the adapter renders that predicate for the qualified single-table wrapper's outer `WHERE`
* *THEN* the Exasol pair SHALL be the renderer used, so the fragment is parsed by Exasol's core engine and carries length-qualified CAST targets
* *AND* the recorded set of wrapper positions an Exasol-dialect node can reach SHALL include this one, so an acceptance test for Exasol-dialect rendering MAY use a declined single-table WHERE predicate
* *AND* no translator entry point SHALL be added, because `ScanTyping::render_expression` already returns `Err` for exactly the declined case

### Scenario: Bare column reference translates to quoted identifier

* *GIVEN* a VS expression node of `type: "column"` with a `name` field
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL return the column name uppercased and double-quoted as a DataFusion identifier
* *AND* any embedded double-quote characters in the name MUST be escaped by doubling
* *AND* when the `column` node ALSO carries a non-empty `tableAlias`, the translator SHALL render the reference table-qualified as `"ALIAS"."NAME"`, the multi-relation form `pushdown-joins/pushdown-planning-join-fallback` depends on
* *AND* the translator MUST NOT drop a `tableAlias` on its own; removing a `tableAlias` so a single-relation scan target resolves bare names is the CALLER's responsibility (`pushdown/pushdown-planning`), NOT the translator's

### Scenario: An undeclared scalar function name is not translated

* *GIVEN* a VS expression node of type `function_scalar` whose `name` is absent from the crate's one declaration of translated `function_scalar` names (for example `SUBSTRING`), whether or not a per-name rendering arm exists for that name
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL return an error reading `unsupported scalar function: <NAME>`, the same error an unrecognised name raises
* *AND* the scan dialect SHALL return an error for the same node without panicking
* *AND* a name absent from the declaration SHALL NOT render as DataFusion SQL on the Exasol path, even when a per-name rendering exists for it
* *AND* the adapter SHALL fail a pushdown request that carries such a node before any translator renders it (`A function call the adapter does not translate fails the request`)

### Scenario: A function call the adapter does not translate fails the request

* *GIVEN* a `function_scalar` node whose `name` is neither a translated scalar function nor a clock function the adapter folds into a literal, or a `function_aggregate` node whose `name` is not an aggregate the adapter advertises, or a call whose `arguments` hold a count its function does not take, where a missing `arguments` counts as none
* *WHEN* the adapter parses the pushdown request
* *THEN* the adapter SHALL fail the query with an error that names the node's `name` field and the function for an untranslated name, or the node's `arguments` field, the function, and the argument counts it takes for a wrong argument count
* *AND* the adapter MUST NOT render the call, so no name or argument list it did not check reaches the Exasol or the scan SQL
* *AND* only `COUNT`, for `COUNT(*)`, and the clock functions `CURRENT_DATE`, `SYSDATE`, `CURRENT_TIMESTAMP`, `LOCALTIMESTAMP`, and `SYSTIMESTAMP` SHALL take no arguments
* *AND* this state SHALL be unreachable for a request Exasol sends, because Exasol sends only the functions the adapter advertises, with argument counts Exasol itself accepts

### Scenario: Aggregate function nodes render with the aggregate name spliced verbatim

* *GIVEN* a VS expression node of type `function_aggregate`, either standalone (e.g. `SUM(col)`, `COUNT(*)`, `COUNT(DISTINCT col)`, `AVG(col)`) or nested inside a scalar function (e.g. the `SUM(CASE WHEN … END)` and `COUNT(*)` inside `ROUND(100.0 * SUM(CASE WHEN … END) / COUNT(*), 2)`)
* *WHEN* `render_expression` processes the node (directly, or by recursion from an enclosing `function_scalar`/arithmetic node)
* *THEN* the translator SHALL splice the aggregate `name` verbatim (uppercased, it is NOT mapped to a DataFusion function alias the way scalar functions are), rendering `<NAME>(<rendered args>)`
* *AND* a node with empty `arguments` or a star argument SHALL render as `COUNT(*)`
* *AND* a node carrying `distinct: true` SHALL render as `<NAME>(DISTINCT <rendered arg>)`
* *AND* each argument SHALL be rendered recursively by the translator (so `CASE`, arithmetic, and column-reference arguments render correctly), and a column argument carrying a `tableAlias` SHALL render table-qualified as `"ALIAS"."COL"`
* *AND* the translator MUST NOT fall through to the unsupported-node catch-all for a `function_aggregate` node (which would wrongly decline a scalar-over-aggregate select item)
* *AND* an aggregate node whose argument cannot be rendered SHALL return an error, consistent with every other node type

### Scenario: The Exasol dialect never contains a DataFusion-only function name

* *GIVEN* every translated expression shape in the Exasol dialect
* *WHEN* the translator renders it
* *THEN* the output SHALL contain none of the DataFusion-only function names the translator can emit, including the checked-division function
* *AND* the banned-name list SHALL NOT name `CAST`, which is valid Exasol SQL
