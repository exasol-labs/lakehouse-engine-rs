<!-- DELTA:CHANGED -->
# Feature: VS Expression Translator — Predicates

Extends the VS expression translator (`sql-comprehension/vs-expression-translator`) with the
filter-predicate operator set — comparison, logical connectives, IS NULL / IS NOT NULL, IN over a
constant list, BETWEEN, and LIKE. `REGEXP_LIKE` is not translated, and Exasol evaluates it. Split
out of the core translator spec to keep that spec's scenario count within the domain's convention
(mirroring the existing `-scalar-fns`/`-date-fns`/`-scalar-ops`/`-literals` splits).
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
## Background

* This feature shares the six public entry points of `crates/vs-expression` — the DataFusion trio
  (`render_expression`, `render_expression_safe`, `render_df_filter_safe`) and the Exasol trio
  (`render_expression_exasol`, `render_expression_exasol_safe`, `render_df_filter_exasol_safe`);
  predicate rendering is a set of arms inside the same recursive walker.
* Comparison predicates, logical connectives, IS NULL / IS NOT NULL, IN over a constant list,
  BETWEEN, and LIKE all render byte-identically in both dialects, because their operator syntax is
  shared by both parsers — none needs the per-dialect branching `sql-comprehension/vs-expression-translator`
  describes for named scalar functions.
* `REGEXP_LIKE` is declined in both dialects. DataFusion's regex matching cannot reproduce
  Exasol's, so the adapter does not advertise `FN_PRED_REGEXP_LIKE` and Exasol evaluates the
  predicate itself (see `sql-functions/vs-expression-translator-scalar-fns`).
* Exasol and DataFusion diverge on NULL handling inside an IN list: Exasol ignores NULL entries
  under both `IN` and `NOT IN`, while DataFusion three-valued logic filters non-matching rows for
  `NOT IN` when the list contains NULL, so a NULL entry must be stripped from the rendered list.
  NULL entries reach the const list as several distinct node shapes — a `literal_null` node, or any
  typed literal (`literal_date`, `literal_timestamp`, `literal_exactnumeric`, etc.) carrying a null
  `value` — and these render to divergent strings (`NULL`, `DATE NULL`, `arrow_cast(NULL, ...)`),
  so stripping must key on the argument node's null-ness before rendering, not on the rendered
  string.
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:REMOVED -->
### Scenario: REGEXP_LIKE predicate translates to a DataFusion regexp_like call

* *GIVEN* a VS expression node of type `predicate_like_regexp` (the Exasol node type for the infix `<str> REGEXP_LIKE <pat>` predicate) with an `expression` operand and a `pattern` operand, or of type `function_scalar` named `REGEXP_LIKE` with two `arguments` (the alternate encoding the translator also accepts)
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL return `regexp_like(<expression_sql>, <pattern_sql>)`
* *AND* the translator SHALL recursively render both operands
* *AND* both encodings SHALL render byte-identically within the DataFusion dialect, so the encoding a caller happens to build cannot change the emitted SQL
* *AND* a missing operand SHALL cause `render_expression` to return an error in raising mode and `None` in the safe variants, and a `function_scalar` `REGEXP_LIKE` node carrying fewer than two arguments SHALL do the same
<!-- /DELTA:REMOVED -->

<!-- DELTA:REMOVED -->
### Scenario: REGEXP_LIKE predicate renders Exasol's infix form in the Exasol dialect

* *GIVEN* either encoding of the preceding scenario: a node of type `predicate_like_regexp`, or of type `function_scalar` named `REGEXP_LIKE` (the alternate encoding)
* *WHEN* `render_expression_exasol` processes the node
* *THEN* the translator SHALL return `(<expression_sql> REGEXP_LIKE <pattern_sql>)` — Exasol's infix predicate form — with both operands rendered recursively in the Exasol dialect, wrapped in outer parentheses so the predicate composes safely as an operand, and byte-identically from both encodings, so branching one encoding and not the other cannot leave a rejected form reachable
* *AND* the rendered fragment MUST NOT use the function-call form `regexp_like(...)`, which is not a function in Exasol: the fragment fails to parse with `syntax error, unexpected REGEXP_LIKE_` (SQL code 42000, verified on live Exasol 2025.2.1, the image pinned in `docker-compose.yml`), so a pushed `REGEXP_LIKE` reaching any Exasol-parsed wrapper is a hard compilation error today (issue #209)
* *AND* a missing operand SHALL return an error in raising mode and `None` in the safe variants, matching the DataFusion dialect
* *AND* the DataFusion-dialect rendering of both encodings MUST remain byte-identical to the preceding scenario, and `FN_PRED_REGEXP_LIKE` SHALL stay advertised because both dialects can now render the predicate
<!-- /DELTA:REMOVED -->

<!-- DELTA:NEW -->
### Scenario: REGEXP_LIKE predicate is not translated in either dialect

* *GIVEN* a VS expression node of type `predicate_like_regexp`, Exasol's encoding of the infix `<str> REGEXP_LIKE <pat>` predicate
* *WHEN* `render_expression` or `render_expression_exasol` processes the node in raising mode
* *THEN* the translator SHALL return the error `unsupported expression node type: predicate_like_regexp` in both dialects
* *AND* `render_expression_safe`, `render_df_filter_safe`, `render_expression_exasol_safe`, and `render_df_filter_exasol_safe` SHALL return `None` for the same node without panicking
<!-- /DELTA:NEW -->
