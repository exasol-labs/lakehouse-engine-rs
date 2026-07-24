# Feature: VS Expression Translator

A standalone workspace crate (`crates/vs-expression`) that translates Exasol Virtual Schema pushdown expression-JSON nodes into DataFusion SQL fragments. Generalises the expression walker that lived in `adapter/predicate.rs`, adding scalar functions, arithmetic, CAST, and the full filter-predicate operator set so it can serve both filter pushdown and GROUP BY key rendering from a single shared library.

## Background

Exasol sends pushdown requests with expression trees expressed as serde_json `Value` objects. Node types include column references, literals, comparison predicates, logical operators, scalar functions, arithmetic operators, CAST, and aggregate function nodes (`function_aggregate`). The crate must translate these trees to DataFusion SQL strings usable in WHERE clauses, GROUP BY clauses, and — via recursion through a scalar function that wraps aggregates — join select-list items, without adding a SQL-parser dependency; only serde_json is used as the IR. Exasol and DataFusion diverge on NULL handling inside an IN list: Exasol ignores NULL entries under both `IN` and `NOT IN`, while DataFusion three-valued logic filters non-matching rows for `NOT IN` when the list contains NULL, so a NULL entry must be stripped from the rendered list. NULL entries reach the const list as several distinct node shapes — a `literal_null` node, or any typed literal (`literal_date`, `literal_timestamp`, `literal_exactnumeric`, etc.) carrying a null `value` — and these render to divergent strings (`NULL`, `DATE NULL`, `arrow_cast(NULL, ...)`), so stripping must key on the argument node's null-ness before rendering, not on the rendered string.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: IN constant list translates to SQL IN expression

* *GIVEN* a VS expression node of type `predicate_in_constlist` with an `expression` target and an `arguments` array of literal nodes
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL omit — keyed on the argument node before rendering, not on the rendered output string — any argument whose node is a NULL-valued literal (a `literal_null` node, OR any `literal_*` node whose `value` field is JSON `null` or absent, regardless of the literal's type, so a typed null such as `DATE NULL` or `arrow_cast(NULL, ...)` is stripped as reliably as a bare `literal_null`) and SHALL render each surviving argument recursively, because Exasol ignores NULL entries in an IN list under both `IN` and `NOT IN` polarities while DataFusion three-valued logic would filter every non-matching row for `NOT IN`
* *AND* for a non-empty list of surviving (non-NULL) arguments the translator SHALL return `(<target> IN (<v1>, <v2>, ...))` over the surviving arguments only
* *AND* an `arguments` array that is empty SHALL return `FALSE` (IN over empty set is always false)
* *AND* an `arguments` array whose entries are all NULL-valued literals (of any type) SHALL return `FALSE`, matching the empty-list result after NULL stripping
* *AND* when the `predicate_in_constlist` node is wrapped in a `predicate_not`, the same node-level stripping SHALL apply, so the rendered `(NOT (<target> IN (...)))` carries only the surviving non-NULL arguments
<!-- /DELTA:CHANGED -->
