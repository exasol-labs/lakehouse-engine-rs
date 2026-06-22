# Feature: VS Expression Translator

A standalone workspace crate (`crates/vs-expression`) that translates Exasol Virtual Schema pushdown expression-JSON nodes into DataFusion SQL fragments. Generalises the expression walker that lived in `adapter/predicate.rs`, adding scalar functions, arithmetic, CAST, and the full filter-predicate operator set so it can serve both filter pushdown and GROUP BY key rendering from a single shared library.

## Background

Exasol sends pushdown requests with expression trees expressed as serde_json `Value` objects. Node types include column references, literals, comparison predicates, logical operators, scalar functions, arithmetic operators, and CAST. The crate must translate these trees to DataFusion SQL strings usable in WHERE clauses and GROUP BY clauses without adding a SQL-parser dependency — only serde_json is used as the IR.

The crate is a standalone workspace member with no knowledge of lakehouse-engine internals. It exposes a raising variant (for tests and explicit handling) and `_safe` None-returning variants (for adapter fallback paths).

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Literal nodes translate to SQL literal forms

* *GIVEN* a VS expression node of type `literal_string`, `literal_exactnumeric`, `literal_double`, `literal_bool`, `literal_null`, `literal_date`, `literal_timestamp`, or `literal_timestamp_utc`
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL return the corresponding SQL literal:
  `literal_string` → single-quoted string with internal single-quotes escaped by doubling;
  `literal_exactnumeric` / `literal_double` → bare numeric value;
  `literal_bool` → `TRUE` or `FALSE`;
  `literal_null` → `NULL`;
  `literal_date` → `DATE 'YYYY-MM-DD'`;
  `literal_timestamp` → `TIMESTAMP 'YYYY-MM-DD HH:MI:SS'`;
  `literal_timestamp_utc` → a timestamp-with-timezone literal whose value DataFusion parses as UTC (`TIMESTAMP 'YYYY-MM-DD HH:MI:SS+00:00'` or the equivalent `arrow_cast` to `Timestamp(_, "UTC")`)
* *AND* the translator MUST NOT produce any SQL injection vector from string literal values
<!-- /DELTA:CHANGED -->

<!-- DELTA:NEW -->
### Scenario: REGEXP_LIKE predicate translates to a DataFusion regexp_like call

* *GIVEN* a VS expression node of type `predicate_regexp_like` (or `function_scalar` named `REGEXP_LIKE`) with a `expression` operand and a `pattern` operand
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL return `regexp_like(<expression_sql>, <pattern_sql>)`
* *AND* the translator SHALL recursively render both operands
* *AND* a missing operand SHALL cause `render_expression` to return an error in raising mode and `None` in the safe variants
<!-- /DELTA:NEW -->
