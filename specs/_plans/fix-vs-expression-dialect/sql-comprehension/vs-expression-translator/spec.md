# Feature: VS Expression Translator

A standalone workspace crate (`crates/vs-expression`) that translates Exasol Virtual Schema pushdown expression-JSON nodes into DataFusion SQL fragments. Generalises the expression walker that lived in `adapter/predicate.rs`, adding scalar functions, arithmetic, CAST, and the full filter-predicate operator set so it can serve both filter pushdown and GROUP BY key rendering from a single shared library.

## Background

Exasol sends pushdown requests with expression trees expressed as serde_json `Value` objects. Node types include column references, literals, comparison predicates, logical operators, scalar functions, arithmetic operators, CAST, and aggregate function nodes (`function_aggregate`). The crate must translate these trees to DataFusion SQL strings usable in WHERE clauses, GROUP BY clauses, and — via recursion through a scalar function that wraps aggregates — join select-list items, without adding a SQL-parser dependency; only serde_json is used as the IR. An aggregate node is not a translated function: its aggregate name (`SUM`, `COUNT`, `AVG`, `MIN`, `MAX`, and the STDDEV/VARIANCE family) is spliced verbatim, and its argument(s) are rendered by recursion — so a scalar expression that wraps aggregates renders in full rather than failing when recursion reaches the nested aggregate.

<!-- DELTA:CHANGED -->
The crate is a standalone workspace member with no knowledge of lakehouse-engine internals. It exposes six public entry points in two dialect trios. The DataFusion trio — `render_expression` (raising, returns `Err` for unsupported nodes), `render_expression_safe` (returns `None` for unsupported nodes), and `render_df_filter_safe` (same as safe but also suppresses trivially-true results so the adapter can omit no-op filters) — produces fragments parsed by DataFusion's SQL frontend inside the scan UDF. The Exasol trio — `render_expression_exasol`, `render_expression_exasol_safe`, and `render_df_filter_exasol_safe` — carries the same three contracts but produces fragments spliced into outer wrapper SQL that Exasol's own core engine parses.
<!-- /DELTA:CHANGED -->

<!-- DELTA:NEW -->
The dialect is threaded through every node of the recursive walk, and a node MUST branch on it whenever the two parsers disagree. Five consumer sites depend on the Exasol dialect producing SQL Exasol can compile:

| Consumer | Wrapper SQL it builds |
|---|---|
| `render_scalar_over_merge` (`adapter/pushdown/grouped_agg.rs`) | outer grouped-aggregate merge wrapper — scalar-over-aggregate select items and HAVING operands |
| `render_expression_qualified` (`adapter/pushdown/joins/rendering.rs`) | every table-qualified fragment of the N-scan join wrapper and of the qualified single-table fallback wrapper (the `COUNT(DISTINCT …)` shape) — select items, JOIN ON conditions, GROUP BY, HAVING, ORDER BY |
| `render_df_filter_qualified` (`adapter/pushdown/joins/rendering.rs`) | the outer WHERE residual of the N-scan join wrapper |
| `parse_declined_sort_key` (`adapter/pushdown/topn.rs`) | an expression ORDER BY element of the declined-ORDER-BY row-scan wrapper |

Because Exasol's compiler emitted the tree in the first place, the Exasol dialect's default is to render what Exasol sent — verbatim name, argument order, and argument count — which makes parity on that path structural rather than tested. Only where a construct is not an Exasol source form (an operator wire name, an adapter-synthesized node) does the Exasol dialect render something other than the original. The per-node rules live with each node's own feature: `-scalar-fns` for math and string functions, `-date-fns` for date/time functions, `-literals` for timestamp literals, `-cast` for CAST targets, `-scalar-ops` for the adapter-synthesized decimal node.
<!-- /DELTA:NEW -->

Exasol and DataFusion diverge on NULL handling inside an IN list: Exasol ignores NULL entries under both `IN` and `NOT IN`, while DataFusion three-valued logic filters non-matching rows for `NOT IN` when the list contains NULL, so a NULL entry must be stripped from the rendered list. NULL entries reach the const list as several distinct node shapes — a `literal_null` node, or any typed literal (`literal_date`, `literal_timestamp`, `literal_exactnumeric`, etc.) carrying a null `value` — and these render to divergent strings (`NULL`, `DATE NULL`, `arrow_cast(NULL, ...)`), so stripping must key on the argument node's null-ness before rendering, not on the rendered string.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: REGEXP_LIKE predicate translates to a DataFusion regexp_like call

* *GIVEN* a VS expression node of type `predicate_like_regexp` (the Exasol node type for the infix `<str> REGEXP_LIKE <pat>` predicate) with an `expression` operand and a `pattern` operand
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL return `regexp_like(<expression_sql>, <pattern_sql>)`
* *AND* the translator SHALL recursively render both operands
* *AND* a missing operand SHALL cause `render_expression` to return an error in raising mode and `None` in the safe variants
<!-- /DELTA:CHANGED -->

<!-- DELTA:NEW -->
### Scenario: REGEXP_LIKE predicate renders Exasol's infix form in the Exasol dialect

* *GIVEN* the same `predicate_like_regexp` node as the preceding scenario
* *WHEN* `render_expression_exasol` processes the node
* *THEN* the translator SHALL return `(<expression_sql> REGEXP_LIKE <pattern_sql>)` — Exasol's infix predicate form — with both operands rendered recursively in the Exasol dialect and wrapped in outer parentheses so the predicate composes safely as an operand
* *AND* the rendered fragment MUST NOT use the function-call form `regexp_like(...)`, which is not a function in Exasol: the fragment fails to parse with `syntax error, unexpected REGEXP_LIKE_` (SQL code 42000, verified on live Exasol 2025.1.x), so a pushed `REGEXP_LIKE` reaching any Exasol-parsed wrapper is a hard compilation error today (issue #209)
* *AND* a missing operand SHALL return an error in raising mode and `None` in the safe variants, matching the DataFusion dialect
* *AND* the DataFusion-dialect rendering of the same node MUST remain byte-identical to the preceding scenario, and `FN_PRED_REGEXP_LIKE` SHALL stay advertised because both dialects can now render the predicate
<!-- /DELTA:NEW -->
