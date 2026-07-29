# Feature: VS Expression Translator

A standalone workspace crate (`crates/vs-expression`) that translates Exasol Virtual Schema pushdown expression-JSON nodes into DataFusion SQL fragments. Generalises the expression walker that lived in `adapter/predicate.rs`, adding scalar functions, arithmetic, CAST, and the full filter-predicate operator set so it can serve both filter pushdown and GROUP BY key rendering from a single shared library. This feature owns the shared entry points, the dialect-dispatch mechanism, column references, and aggregate-node splicing; the full filter-predicate operator set (comparison, logical, IS NULL, IN, BETWEEN, LIKE, REGEXP_LIKE) is specified in the sibling feature `sql-comprehension/vs-expression-translator-predicates`, split out to keep this feature's scenario count within the domain's convention.

## Background

Exasol sends pushdown requests with expression trees expressed as serde_json `Value` objects. Node types include column references, literals, comparison predicates, logical operators, scalar functions, arithmetic operators, CAST, and aggregate function nodes (`function_aggregate`). The crate must translate these trees to DataFusion SQL strings usable in WHERE clauses, GROUP BY clauses, and — via recursion through a scalar function that wraps aggregates — join select-list items, without adding a SQL-parser dependency; only serde_json is used as the IR. An aggregate node is not a translated function: its aggregate name (`SUM`, `COUNT`, `AVG`, `MIN`, `MAX`, and the STDDEV/VARIANCE family) is spliced verbatim, and its argument(s) are rendered by recursion — so a scalar expression that wraps aggregates renders in full rather than failing when recursion reaches the nested aggregate.

The crate is a standalone workspace member with no knowledge of lakehouse-engine internals. It exposes six public entry points in two dialect trios. The DataFusion trio — `render_expression` (raising, returns `Err` for unsupported nodes), `render_expression_safe` (returns `None` for unsupported nodes), and `render_df_filter_safe` (same as safe but also suppresses trivially-true results so the adapter can omit no-op filters) — produces fragments parsed by DataFusion's SQL frontend inside the scan UDF. The Exasol trio — `render_expression_exasol`, `render_expression_exasol_safe`, and `render_df_filter_exasol_safe` — carries the same three contracts but produces fragments spliced into outer wrapper SQL that Exasol's own core engine parses.

The dialect is threaded through every node of the recursive walk, and a node MUST branch on it whenever the two parsers disagree. Four consumer sites depend on the Exasol dialect producing SQL Exasol can compile:

| Consumer | Wrapper SQL it builds |
|---|---|
| `render_scalar_over_merge` (`adapter/pushdown/grouped_agg.rs`) | outer grouped-aggregate merge wrapper — scalar-over-aggregate select items and HAVING operands |
| `render_expression_qualified` (`adapter/pushdown/joins/rendering.rs`) | every table-qualified fragment of the N-scan join wrapper and of the qualified single-table fallback wrapper (the `COUNT(DISTINCT …)` shape) — select items, JOIN ON conditions, GROUP BY, HAVING, ORDER BY |
| `render_df_filter_qualified` (`adapter/pushdown/joins/rendering.rs`) | the outer WHERE residual of the N-scan join wrapper |
| `parse_declined_sort_key` (`adapter/pushdown/topn.rs`) | an expression ORDER BY element of the declined-ORDER-BY row-scan wrapper |

Every one of those four sites reads raw pushdown-request JSON. A WHERE-clause predicate on a single-table scan is NOT among them: `build_qualified_single_table_fallback_sql` (`adapter/pushdown/joins/sql_builders.rs`) applies that filter inside the scan through `fan_out_spec.filter`, which the DataFusion trio renders. An Exasol-dialect node therefore reaches Exasol's parser only as a select item, a GROUP BY key, a HAVING operand, an ORDER BY element, or an N-scan cross-side residual, and an acceptance test for any Exasol-dialect rendering MUST use one of those positions.

Because Exasol's compiler emitted the tree in the first place, the Exasol dialect's default is to render what Exasol sent — verbatim name, argument order, and argument count. A construct that is not an Exasol call form is rendered by its own per-name arm instead: an operator wire name, `MOD`, `CONCAT`, a CAST target, the `REGEXP_LIKE` predicate (whose Exasol form is infix), and `CASE` (whose Exasol form is `CASE WHEN … END`). The per-node rules live with each node's own feature: `-scalar-fns` for math, string, and conditional functions, `-date-fns` for date/time functions, `-literals` for timestamp literals, and `-cast` for CAST targets.

Every `function_scalar` name the translator translates MUST be declared exactly once in the crate, each carrying its Exasol-dialect form: `VerbatimCall` (`<NAME>(<rendered args>)`) or `Shaped` (rendered by its own per-name arm, which owns both dialects). That one declaration MUST gate the `function_scalar` dispatch: a name absent from it SHALL be declined in both dialects with the `unsupported scalar function: <name>` error, before any per-name arm is reached. Withdrawing a name from the declaration is therefore the mechanism that retires a translation, and it MUST be paired with withdrawing the matching capability so the advertised set never exceeds the translated set. A per-name arm added without a declaration entry is therefore unreachable rather than silently DataFusion-only, which is the failure mode that produced issue #209: because the Exasol branch precedes the DataFusion arms, a name present in a DataFusion arm but absent from the Exasol side would otherwise fall through to the DataFusion rendering with no error.

The enforcing sweep test MUST read the same declaration rather than a parallel hand-written list. It SHALL iterate the declared names, look each up in its fixture map, and FAIL naming any declared name that has no fixture and any fixture whose name is not declared. Per row: a `VerbatimCall` name's Exasol-dialect rendering SHALL equal `<NAME>(<rendered args>)` built from the node's own uppercased `name`; a `Shaped` name's SHALL equal the expected string its fixture declares. Every node type outside `function_scalar` SHALL equal its per-dialect expected string and is covered by its own explicit sweep row, not by the declaration.

Together these make Exasol-dialect coverage structural rather than reviewed, with the two links stated precisely: an undeclared name cannot be translated at all (dispatch-enforced), and a declared name cannot lack a sweep row (test-enforced). A `VerbatimCall` name's Exasol rendering is produced by the declaration's own branch, which no per-name arm can reach, so it cannot diverge from the name Exasol sent. A `Shaped` name's Exasol rendering still lives in its arm, so its correctness rests on the sweep row the test forces it to have.

The full filter-predicate operator set — comparison, logical connectives, IS NULL / IS NOT NULL, IN over a constant list (including the NULL-stripping rule Exasol's IN-list semantics require), BETWEEN, LIKE, and REGEXP_LIKE — is specified in `sql-comprehension/vs-expression-translator-predicates`.

## Scenarios

### Scenario: Bare column reference translates to quoted identifier

* *GIVEN* a VS expression node of `type: "column"` with a `name` field
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL return the column name uppercased and double-quoted as a DataFusion identifier
* *AND* any embedded double-quote characters in the name MUST be escaped by doubling
* *AND* when the `column` node ALSO carries a non-empty `tableAlias`, the translator SHALL render the reference table-qualified as `"ALIAS"."NAME"` — the multi-relation form `vs-adapter/pushdown-planning-join-fallback` depends on
* *AND* the translator MUST NOT drop a `tableAlias` on its own; removing a `tableAlias` so a single-relation scan target resolves bare names is the CALLER's responsibility (`vs-adapter/pushdown-planning`), NOT the translator's

### Scenario: An undeclared scalar function name is not translated in either dialect

* *GIVEN* a VS expression node of type `function_scalar` whose `name` is absent from the crate's one declaration of translated `function_scalar` names (for example `SUBSTRING`), whether or not a per-name rendering arm exists for that name
* *WHEN* `render_expression` or `render_expression_exasol` processes the node in raising mode
* *THEN* the translator SHALL return an error reading `unsupported scalar function: <NAME>` in both dialects, which is the same error an unrecognised name raises today
* *AND* `render_expression_safe` and `render_expression_exasol_safe` SHALL return `None` for the same node without panicking
* *AND* the declaration lookup MUST happen before any per-name rendering arm is reached, so an arm added without a declaration entry is unreachable and cannot emit DataFusion SQL on the Exasol path
* *AND* the adapter SHALL omit the expression and let Exasol evaluate it, exactly as it does for any other untranslated name, so the gate changes no capability advertisement

### Scenario: Aggregate function nodes render with the aggregate name spliced verbatim

* *GIVEN* a VS expression node of type `function_aggregate` — either standalone (e.g. `SUM(col)`, `COUNT(*)`, `COUNT(DISTINCT col)`, `AVG(col)`) or nested inside a scalar function (e.g. the `SUM(CASE WHEN … END)` and `COUNT(*)` inside `ROUND(100.0 * SUM(CASE WHEN … END) / COUNT(*), 2)`)
* *WHEN* `render_expression` processes the node (directly, or by recursion from an enclosing `function_scalar`/arithmetic node)
* *THEN* the translator SHALL splice the aggregate `name` verbatim (uppercased — it is NOT mapped to a DataFusion function alias the way scalar functions are), rendering `<NAME>(<rendered args>)`
* *AND* a node with empty `arguments` or a star argument SHALL render as `COUNT(*)`
* *AND* a node carrying `distinct: true` SHALL render as `<NAME>(DISTINCT <rendered arg>)`
* *AND* each argument SHALL be rendered recursively by the translator (so `CASE`, arithmetic, and column-reference arguments render correctly), and a column argument carrying a `tableAlias` SHALL render table-qualified as `"ALIAS"."COL"`
* *AND* the translator MUST NOT fall through to the unsupported-node catch-all for a `function_aggregate` node (which previously returned an error in raising mode and `None` in the safe variants, causing a scalar-over-aggregate select item to be wrongly declined)
* *AND* an aggregate node whose argument cannot be rendered SHALL return an error in raising mode and `None` in the safe variants, consistent with every other node type
