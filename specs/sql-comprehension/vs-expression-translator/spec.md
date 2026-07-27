# Feature: VS Expression Translator

A standalone workspace crate (`crates/vs-expression`) that translates Exasol Virtual Schema pushdown expression-JSON nodes into DataFusion SQL fragments. Generalises the expression walker that lived in `adapter/predicate.rs`, adding scalar functions, arithmetic, CAST, and the full filter-predicate operator set so it can serve both filter pushdown and GROUP BY key rendering from a single shared library.

## Background

Exasol sends pushdown requests with expression trees expressed as serde_json `Value` objects. Node types include column references, literals, comparison predicates, logical operators, scalar functions, arithmetic operators, CAST, and aggregate function nodes (`function_aggregate`). The crate must translate these trees to DataFusion SQL strings usable in WHERE clauses, GROUP BY clauses, and — via recursion through a scalar function that wraps aggregates — join select-list items, without adding a SQL-parser dependency; only serde_json is used as the IR. An aggregate node is not a translated function: its aggregate name (`SUM`, `COUNT`, `AVG`, `MIN`, `MAX`, and the STDDEV/VARIANCE family) is spliced verbatim, and its argument(s) are rendered by recursion — so a scalar expression that wraps aggregates renders in full rather than failing when recursion reaches the nested aggregate.

The crate is a standalone workspace member with no knowledge of lakehouse-engine internals. It exposes three public entry points: `render_expression` (raising, returns `Err` for unsupported nodes), `render_expression_safe` (returns `None` for unsupported nodes), and `render_df_filter_safe` (same as safe but also suppresses trivially-true results so the adapter can omit no-op filters).

Exasol and DataFusion diverge on NULL handling inside an IN list: Exasol ignores NULL entries under both `IN` and `NOT IN`, while DataFusion three-valued logic filters non-matching rows for `NOT IN` when the list contains NULL, so a NULL entry must be stripped from the rendered list. NULL entries reach the const list as several distinct node shapes — a `literal_null` node, or any typed literal (`literal_date`, `literal_timestamp`, `literal_exactnumeric`, etc.) carrying a null `value` — and these render to divergent strings (`NULL`, `DATE NULL`, `arrow_cast(NULL, ...)`), so stripping must key on the argument node's null-ness before rendering, not on the rendered string.

## Scenarios

### Scenario: Bare column reference translates to quoted identifier

* *GIVEN* a VS expression node of `type: "column"` with a `name` field
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL return the column name uppercased and double-quoted as a DataFusion identifier
* *AND* any embedded double-quote characters in the name MUST be escaped by doubling
* *AND* when the `column` node ALSO carries a non-empty `tableAlias`, the translator SHALL render the reference table-qualified as `"ALIAS"."NAME"` — the multi-relation form `vs-adapter/pushdown-planning-join-fallback` depends on
* *AND* the translator MUST NOT drop a `tableAlias` on its own; removing a `tableAlias` so a single-relation scan target resolves bare names is the CALLER's responsibility (`vs-adapter/pushdown-planning`), NOT the translator's

### Scenario: Comparison predicates translate to binary operator expressions

* *GIVEN* a VS expression node of type `predicate_equal`, `predicate_notequal`, `predicate_less`, `predicate_lessequal`, `predicate_greater`, or `predicate_greaterequal`
* *AND* each node carries `left` and `right` child expression nodes
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL return `(<left_sql> <op> <right_sql>)` where `<op>` is `=`, `<>`, `<`, `<=`, `>`, or `>=` respectively
* *AND* the translator SHALL recursively render both operands

### Scenario: Logical connectives translate to AND/OR/NOT with parentheses

* *GIVEN* a VS expression node of type `predicate_and`, `predicate_or`, or `predicate_not`
* *AND* `predicate_and` and `predicate_or` carry an `expressions` array of child nodes
* *WHEN* `render_expression` processes the node
* *THEN* `predicate_and` SHALL return `(<c1> AND <c2> AND ...)` for two or more children, or the single child without wrapping for exactly one child
* *AND* `predicate_or` SHALL return `(<c1> OR <c2> OR ...)` under the same rules
* *AND* `predicate_not` SHALL return `(NOT <inner>)`
* *AND* an empty `expressions` array for AND SHALL return `TRUE`; for OR SHALL return `FALSE`

### Scenario: IS NULL and IS NOT NULL predicates translate correctly

* *GIVEN* a VS expression node of type `predicate_is_null` or `predicate_is_not_null` with an `expression` child
* *WHEN* `render_expression` processes the node
* *THEN* `predicate_is_null` SHALL return `(<inner> IS NULL)`
* *AND* `predicate_is_not_null` SHALL return `(<inner> IS NOT NULL)`

### Scenario: IN constant list translates to SQL IN expression

* *GIVEN* a VS expression node of type `predicate_in_constlist` with an `expression` target and an `arguments` array of literal nodes
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL omit — keyed on the argument node before rendering, not on the rendered output string — any argument whose node is a NULL-valued literal (a `literal_null` node, OR any `literal_*` node whose `value` field is JSON `null` or absent, regardless of the literal's type, so a typed null such as `DATE NULL` or `arrow_cast(NULL, ...)` is stripped as reliably as a bare `literal_null`) and SHALL render each surviving argument recursively, because Exasol ignores NULL entries in an IN list under both `IN` and `NOT IN` polarities while DataFusion three-valued logic would filter every non-matching row for `NOT IN`
* *AND* for a non-empty list of surviving (non-NULL) arguments the translator SHALL return `(<target> IN (<v1>, <v2>, ...))` over the surviving arguments only
* *AND* an `arguments` array that is empty SHALL return `FALSE` (IN over empty set is always false)
* *AND* an `arguments` array whose entries are all NULL-valued literals (of any type) SHALL return `FALSE`, matching the empty-list result after NULL stripping
* *AND* when the `predicate_in_constlist` node is wrapped in a `predicate_not`, the same node-level stripping SHALL apply, so the rendered `(NOT (<target> IN (...)))` carries only the surviving non-NULL arguments

### Scenario: BETWEEN predicate translates correctly

* *GIVEN* a VS expression node of type `predicate_between` with `expression`, `left`, and `right` children
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL return `(<expr> BETWEEN <low> AND <high>)`

### Scenario: LIKE predicate translates with optional escape character

* *GIVEN* a VS expression node of type `predicate_like` with `expression` and `pattern` children
* *WHEN* `render_expression` processes the node
* *AND* an `escape_char` field is absent or empty
* *THEN* the translator SHALL return `(<expr> LIKE <pattern>)`
* *AND* when `escape_char` is present and non-empty the translator SHALL return `(<expr> LIKE <pattern> ESCAPE '<ch>')`

### Scenario: REGEXP_LIKE predicate translates to a DataFusion regexp_like call

* *GIVEN* a VS expression node of type `predicate_like_regexp` (the Exasol node type for the infix `<str> REGEXP_LIKE <pat>` predicate) with an `expression` operand and a `pattern` operand
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL return `regexp_like(<expression_sql>, <pattern_sql>)`
* *AND* the translator SHALL recursively render both operands
* *AND* a missing operand SHALL cause `render_expression` to return an error in raising mode and `None` in the safe variants

### Scenario: Aggregate function nodes render with the aggregate name spliced verbatim

* *GIVEN* a VS expression node of type `function_aggregate` — either standalone (e.g. `SUM(col)`, `COUNT(*)`, `COUNT(DISTINCT col)`, `AVG(col)`) or nested inside a scalar function (e.g. the `SUM(CASE WHEN … END)` and `COUNT(*)` inside `ROUND(100.0 * SUM(CASE WHEN … END) / COUNT(*), 2)`)
* *WHEN* `render_expression` processes the node (directly, or by recursion from an enclosing `function_scalar`/arithmetic node)
* *THEN* the translator SHALL splice the aggregate `name` verbatim (uppercased — it is NOT mapped to a DataFusion function alias the way scalar functions are), rendering `<NAME>(<rendered args>)`
* *AND* a node with empty `arguments` or a star argument SHALL render as `COUNT(*)`
* *AND* a node carrying `distinct: true` SHALL render as `<NAME>(DISTINCT <rendered arg>)`
* *AND* each argument SHALL be rendered recursively by the translator (so `CASE`, arithmetic, and column-reference arguments render correctly), and a column argument carrying a `tableAlias` SHALL render table-qualified as `"ALIAS"."COL"`
* *AND* the translator MUST NOT fall through to the unsupported-node catch-all for a `function_aggregate` node (which previously returned an error in raising mode and `None` in the safe variants, causing a scalar-over-aggregate select item to be wrongly declined)
* *AND* an aggregate node whose argument cannot be rendered SHALL return an error in raising mode and `None` in the safe variants, consistent with every other node type
