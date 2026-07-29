# Feature: VS Expression Translator — Predicates

Extends the VS expression translator (`sql-comprehension/vs-expression-translator`) with the full
filter-predicate operator set — comparison, logical connectives, IS NULL / IS NOT NULL, IN over a
constant list, BETWEEN, LIKE, and REGEXP_LIKE. Split out of the core translator spec to keep that
spec's scenario count within the domain's convention (mirroring the existing
`-scalar-fns`/`-date-fns`/`-scalar-ops`/`-literals` splits).

## Background

* This feature shares the six public entry points of `crates/vs-expression` — the DataFusion trio
  (`render_expression`, `render_expression_safe`, `render_df_filter_safe`) and the Exasol trio
  (`render_expression_exasol`, `render_expression_exasol_safe`, `render_df_filter_exasol_safe`);
  predicate rendering is a set of arms inside the same recursive walker.
* Comparison predicates, logical connectives, IS NULL / IS NOT NULL, IN over a constant list,
  BETWEEN, and LIKE all render byte-identically in both dialects, because their operator syntax is
  shared by both parsers — none needs the per-dialect branching `sql-comprehension/vs-expression-translator`
  describes for named scalar functions. `REGEXP_LIKE` is the one predicate in this feature that
  does branch: Exasol's own form is the infix `<str> REGEXP_LIKE <pat>` predicate, not a function
  call, so it is one of the exclusions from the verbatim-call rule that feature's Background
  enumerates.
* Exasol and DataFusion diverge on NULL handling inside an IN list: Exasol ignores NULL entries
  under both `IN` and `NOT IN`, while DataFusion three-valued logic filters non-matching rows for
  `NOT IN` when the list contains NULL, so a NULL entry must be stripped from the rendered list.
  NULL entries reach the const list as several distinct node shapes — a `literal_null` node, or any
  typed literal (`literal_date`, `literal_timestamp`, `literal_exactnumeric`, etc.) carrying a null
  `value` — and these render to divergent strings (`NULL`, `DATE NULL`, `arrow_cast(NULL, ...)`),
  so stripping must key on the argument node's null-ness before rendering, not on the rendered
  string.

## Scenarios

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

* *GIVEN* a VS expression node of type `predicate_like_regexp` (the Exasol node type for the infix `<str> REGEXP_LIKE <pat>` predicate) with an `expression` operand and a `pattern` operand, or of type `function_scalar` named `REGEXP_LIKE` with two `arguments` (the alternate encoding the translator also accepts)
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL return `regexp_like(<expression_sql>, <pattern_sql>)`
* *AND* the translator SHALL recursively render both operands
* *AND* both encodings SHALL render byte-identically within the DataFusion dialect, so the encoding a caller happens to build cannot change the emitted SQL
* *AND* a missing operand SHALL cause `render_expression` to return an error in raising mode and `None` in the safe variants, and a `function_scalar` `REGEXP_LIKE` node carrying fewer than two arguments SHALL do the same

### Scenario: REGEXP_LIKE predicate renders Exasol's infix form in the Exasol dialect

* *GIVEN* either encoding of the preceding scenario: a node of type `predicate_like_regexp`, or of type `function_scalar` named `REGEXP_LIKE` (the alternate encoding)
* *WHEN* `render_expression_exasol` processes the node
* *THEN* the translator SHALL return `(<expression_sql> REGEXP_LIKE <pattern_sql>)` — Exasol's infix predicate form — with both operands rendered recursively in the Exasol dialect, wrapped in outer parentheses so the predicate composes safely as an operand, and byte-identically from both encodings, so branching one encoding and not the other cannot leave a rejected form reachable
* *AND* the rendered fragment MUST NOT use the function-call form `regexp_like(...)`, which is not a function in Exasol: the fragment fails to parse with `syntax error, unexpected REGEXP_LIKE_` (SQL code 42000, verified on live Exasol 2025.2.1, the image pinned in `docker-compose.yml`), so a pushed `REGEXP_LIKE` reaching any Exasol-parsed wrapper is a hard compilation error today (issue #209)
* *AND* a missing operand SHALL return an error in raising mode and `None` in the safe variants, matching the DataFusion dialect
* *AND* the DataFusion-dialect rendering of both encodings MUST remain byte-identical to the preceding scenario, and `FN_PRED_REGEXP_LIKE` SHALL stay advertised because both dialects can now render the predicate
