# Feature: VS Expression Translator

A standalone workspace crate (`crates/vs-expression`) that translates Exasol Virtual Schema pushdown expression-JSON nodes into DataFusion SQL fragments. Generalises the expression walker that lived in `adapter/predicate.rs`, adding scalar functions, arithmetic, CAST, and the full filter-predicate operator set so it can serve both filter pushdown and GROUP BY key rendering from a single shared library.

## Background

Exasol sends pushdown requests with expression trees expressed as serde_json `Value` objects. Node types include column references, literals, comparison predicates, logical operators, scalar functions, arithmetic operators, CAST, and aggregate function nodes (`function_aggregate`). The crate must translate these trees to DataFusion SQL strings usable in WHERE clauses, GROUP BY clauses, and — via recursion through a scalar function that wraps aggregates — join select-list items, without adding a SQL-parser dependency; only serde_json is used as the IR. An aggregate node is not a translated function: its aggregate name (`SUM`, `COUNT`, `AVG`, `MIN`, `MAX`, and the STDDEV/VARIANCE family) is spliced verbatim, and its argument(s) are rendered by recursion — so a scalar expression that wraps aggregates renders in full rather than failing when recursion reaches the nested aggregate.

<!-- DELTA:CHANGED -->
The crate is a standalone workspace member with no knowledge of lakehouse-engine internals. It exposes six public entry points in two dialect trios. The DataFusion trio — `render_expression` (raising, returns `Err` for unsupported nodes), `render_expression_safe` (returns `None` for unsupported nodes), and `render_df_filter_safe` (same as safe but also suppresses trivially-true results so the adapter can omit no-op filters) — produces fragments parsed by DataFusion's SQL frontend inside the scan UDF. The Exasol trio — `render_expression_exasol`, `render_expression_exasol_safe`, and `render_df_filter_exasol_safe` — carries the same three contracts but produces fragments spliced into outer wrapper SQL that Exasol's own core engine parses.
<!-- /DELTA:CHANGED -->

<!-- DELTA:NEW -->
The dialect is threaded through every node of the recursive walk, and a node MUST branch on it whenever the two parsers disagree. Four consumer sites depend on the Exasol dialect producing SQL Exasol can compile:

| Consumer | Wrapper SQL it builds |
|---|---|
| `render_scalar_over_merge` (`adapter/pushdown/grouped_agg.rs`) | outer grouped-aggregate merge wrapper — scalar-over-aggregate select items and HAVING operands |
| `render_expression_qualified` (`adapter/pushdown/joins/rendering.rs`) | every table-qualified fragment of the N-scan join wrapper and of the qualified single-table fallback wrapper (the `COUNT(DISTINCT …)` shape) — select items, JOIN ON conditions, GROUP BY, HAVING, ORDER BY |
| `render_df_filter_qualified` (`adapter/pushdown/joins/rendering.rs`) | the outer WHERE residual of the N-scan join wrapper |
| `parse_declined_sort_key` (`adapter/pushdown/topn.rs`) | an expression ORDER BY element of the declined-ORDER-BY row-scan wrapper |

Every one of those four sites reads raw pushdown-request JSON. A WHERE-clause predicate on a single-table scan is NOT among them: `build_qualified_single_table_fallback_sql` (`adapter/pushdown/joins/sql_builders.rs`) applies that filter inside the scan through `fan_out_spec.filter`, which the DataFusion trio renders. An Exasol-dialect node therefore reaches Exasol's parser only as a select item, a GROUP BY key, a HAVING operand, an ORDER BY element, or an N-scan cross-side residual, and an acceptance test for any Exasol-dialect rendering MUST use one of those positions.

Because Exasol's compiler emitted the tree in the first place, the Exasol dialect's default is to render what Exasol sent — verbatim name, argument order, and argument count. A construct that is not an Exasol call form is rendered by its own per-name arm instead: an operator wire name, `MOD`, `CONCAT`, a CAST target, the `REGEXP_LIKE` predicate (whose Exasol form is infix), and `CASE` (whose Exasol form is `CASE WHEN … END`). The four now-family names are a third case: their Exasol form is the original name as a bare keyword, with no parentheses. The per-node rules live with each node's own feature: `-scalar-fns` for math, string, and conditional functions, `-date-fns` for date/time functions, `-literals` for timestamp literals, and `-cast` for CAST targets.

Every `function_scalar` name the translator translates MUST be declared exactly once in the crate, each carrying its Exasol-dialect form: `VerbatimCall` (`<NAME>(<rendered args>)`), `BareKeyword` (the bare `<NAME>`), or `Shaped` (rendered by its own per-name arm, which owns both dialects). That one declaration MUST gate the `function_scalar` dispatch: a name absent from it SHALL be declined in both dialects with the `unsupported scalar function: <name>` error, before any per-name arm is reached. A per-name arm added without a declaration entry is therefore unreachable rather than silently DataFusion-only, which is the failure mode that produced issue #209: because the Exasol branch precedes the DataFusion arms, a name present in a DataFusion arm but absent from the Exasol side would otherwise fall through to the DataFusion rendering with no error.

The enforcing sweep test MUST read the same declaration rather than a parallel hand-written list. It SHALL iterate the declared names, look each up in its fixture map, and FAIL naming any declared name that has no fixture and any fixture whose name is not declared. Per row: a `VerbatimCall` name's Exasol-dialect rendering SHALL equal `<NAME>(<rendered args>)` built from the node's own uppercased `name`; a `BareKeyword` name's SHALL equal the bare `<NAME>`; a `Shaped` name's SHALL equal the expected string its fixture declares. Every node type outside `function_scalar` SHALL equal its per-dialect expected string and is covered by its own explicit sweep row, not by the declaration.

Together these make Exasol-dialect coverage structural rather than reviewed, with the two links stated precisely: an undeclared name cannot be translated at all (dispatch-enforced), and a declared name cannot lack a sweep row (test-enforced). A `VerbatimCall` name's Exasol rendering is produced by the declaration's own branch, which no per-name arm can reach, so it cannot diverge from the name Exasol sent. A `Shaped` name's Exasol rendering still lives in its arm, so its correctness rests on the sweep row the test forces it to have.
<!-- /DELTA:NEW -->

Exasol and DataFusion diverge on NULL handling inside an IN list: Exasol ignores NULL entries under both `IN` and `NOT IN`, while DataFusion three-valued logic filters non-matching rows for `NOT IN` when the list contains NULL, so a NULL entry must be stripped from the rendered list. NULL entries reach the const list as several distinct node shapes — a `literal_null` node, or any typed literal (`literal_date`, `literal_timestamp`, `literal_exactnumeric`, etc.) carrying a null `value` — and these render to divergent strings (`NULL`, `DATE NULL`, `arrow_cast(NULL, ...)`), so stripping must key on the argument node's null-ness before rendering, not on the rendered string.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: REGEXP_LIKE predicate translates to a DataFusion regexp_like call

* *GIVEN* a VS expression node of type `predicate_like_regexp` (the Exasol node type for the infix `<str> REGEXP_LIKE <pat>` predicate) with an `expression` operand and a `pattern` operand, or of type `function_scalar` named `REGEXP_LIKE` with two `arguments` (the alternate encoding the translator also accepts)
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL return `regexp_like(<expression_sql>, <pattern_sql>)`
* *AND* the translator SHALL recursively render both operands
* *AND* both encodings SHALL render byte-identically within the DataFusion dialect, so the encoding a caller happens to build cannot change the emitted SQL
* *AND* a missing operand SHALL cause `render_expression` to return an error in raising mode and `None` in the safe variants, and a `function_scalar` `REGEXP_LIKE` node carrying fewer than two arguments SHALL do the same
<!-- /DELTA:CHANGED -->

<!-- DELTA:NEW -->
### Scenario: REGEXP_LIKE predicate renders Exasol's infix form in the Exasol dialect

* *GIVEN* either encoding of the preceding scenario: a node of type `predicate_like_regexp`, or of type `function_scalar` named `REGEXP_LIKE` (the alternate encoding)
* *WHEN* `render_expression_exasol` processes the node
* *THEN* the translator SHALL return `(<expression_sql> REGEXP_LIKE <pattern_sql>)` — Exasol's infix predicate form — with both operands rendered recursively in the Exasol dialect, wrapped in outer parentheses so the predicate composes safely as an operand, and byte-identically from both encodings, so branching one encoding and not the other cannot leave a rejected form reachable
* *AND* the rendered fragment MUST NOT use the function-call form `regexp_like(...)`, which is not a function in Exasol: the fragment fails to parse with `syntax error, unexpected REGEXP_LIKE_` (SQL code 42000, verified on live Exasol 2025.2.1, the image pinned in `docker-compose.yml`), so a pushed `REGEXP_LIKE` reaching any Exasol-parsed wrapper is a hard compilation error today (issue #209)
* *AND* a missing operand SHALL return an error in raising mode and `None` in the safe variants, matching the DataFusion dialect
* *AND* the DataFusion-dialect rendering of both encodings MUST remain byte-identical to the preceding scenario, and `FN_PRED_REGEXP_LIKE` SHALL stay advertised because both dialects can now render the predicate
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: An undeclared scalar function name is not translated in either dialect

* *GIVEN* a VS expression node of type `function_scalar` whose `name` is absent from the crate's one declaration of translated `function_scalar` names (for example `SUBSTRING`), whether or not a per-name rendering arm exists for that name
* *WHEN* `render_expression` or `render_expression_exasol` processes the node in raising mode
* *THEN* the translator SHALL return an error reading `unsupported scalar function: <NAME>` in both dialects, which is the same error an unrecognised name raises today
* *AND* `render_expression_safe` and `render_expression_exasol_safe` SHALL return `None` for the same node without panicking
* *AND* the declaration lookup MUST happen before any per-name rendering arm is reached, so an arm added without a declaration entry is unreachable and cannot emit DataFusion SQL on the Exasol path
* *AND* the adapter SHALL omit the expression and let Exasol evaluate it, exactly as it does for any other untranslated name, so the gate changes no capability advertisement
<!-- /DELTA:NEW -->
