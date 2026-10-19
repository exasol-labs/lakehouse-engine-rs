# Feature: Pushdown Aggregate SQL Consolidation

The adapter builds aggregate-pushdown SQL with one owner each for the STDDEV/VARIANCE merge expressions, the declared-type CAST rule, and the aggregate-function-name to `AggKind` mapping. The generated SQL is byte-identical to the output before consolidation, except for the scan-spec `storage` value.

## Background

* The three consolidations are independent of each other.
* The statistical merge combines `(cnt, mean, m2)` partials with Chan's parallel formula (`pushdown-aggregates/pushdown-planning-aggregate-extensions`).
* The CAST rule wraps an expression in `CAST(<expr> AS <ty>)` unless `<ty>` is `VARCHAR(2000000)`. The `GroupKey` and `Aggregate` arms of `empty_grouped_sql` keep an unconditional `CAST(NULL AS <ty>)`, because a bare `NULL` has no `VARCHAR` type and fails the positional `selectListDataTypes` check.
* `COUNT` is the only aggregate name whose `AggKind` depends on its argument.
* The `storage` value of the scan-spec literal is the tagged wrapper owned by `storage-access/scan-spec-credential-reference`. The other `dispatch_golden` fixtures stay byte-identical, including the six `empty_*` fixtures, which carry no `storage` value.

## Scenarios

### Scenario: The statistical merge has one owner

* *GIVEN* the six statistical aggregate kinds `VAR_POP`, `VAR_SAMP`, `VARIANCE`, `STDDEV_POP`, `STDDEV_SAMP`, and `STDDEV`
* *WHEN* the adapter builds the outer merge expression for any of them
* *THEN* one function SHALL build the merged count `N`, the merged `M2`, and the three variance forms once per aggregate ordinal, and each standard deviation SHALL be `SQRT` of its matching variance
* *AND* the partial column names the merge references SHALL come from the single name owner recorded in `scan-aggregation/scan-partial-agg-column-contract`, so `merge_select_items` retains NO `PARTIAL_` string literal of its own

### Scenario: The declared-type CAST rule has one owner

* *GIVEN* six sites that each re-derive "wrap in `CAST(<expr> AS <ty>)` unless `<ty>` is the `VARCHAR(2000000)` default" — `constant_projection_sql`, the `gk_select` closure, `cast_merge_items`, and `cast_to_declared_type` in `grouped_agg.rs`, plus `empty_agg_sql` and the `ScalarOverAggregate` arm of `empty_grouped_sql` in `file_resolution.rs` — one of which documents itself as being that shared rule
* *WHEN* any of the six decides whether to cast an outer-wrapper expression to its Exasol-declared type
* *THEN* exactly ONE helper SHALL own that decision, taking the rendered expression and an `Option<&str>` declared type and returning the expression cast or uncast, and each of the six sites SHALL retain NO `VARCHAR(2000000)` comparison and NO `CAST(` format of its own
* *AND* the helper SHALL take an `Option<&str>` rather than a `&str`, because five of the six sites read their declared type from a `.get(i)` on a possibly-shorter list and already treat absence as "no cast"; the sixth passes `Some(declared_type)` and its behavior is unchanged
* *AND* the helper SHALL be declared `pub(super)` in `adapter/pushdown/support.rs`, the submodule that already owns the pushdown tree's shared primitives, because its consumers live in two SIBLING submodules (`grouped_agg` and `file_resolution`) rather than in one
* *AND* the `GroupKey` and `Aggregate` arms of `empty_grouped_sql` MUST NOT be routed through the helper and SHALL keep their UNCONDITIONAL `CAST(NULL AS <ty>)`, because those arms deliberately cast even to `VARCHAR(2000000)`: a bare `NULL` in a zero-row `SELECT … FROM DUAL WHERE 1=0` carries no `VARCHAR` type and would fail the positional `selectListDataTypes` check the cast exists to satisfy
* *AND* every comment asserting the rule by convention SHALL be replaced by a reference to the helper rather than left standing — in particular `cast_to_declared_type`'s doc sentence naming `cast_merge_items`, `constant_projection_sql`, and the group-key cast as the sites it mirrors, and `constant_projection_sql`'s "mirrors the group-key and aggregate cast discipline" and "matching `group_key_exasol_types`" clauses — because a preserved sentence asserting a convention the code now enforces is the stale note this consolidation deletes
* *AND* the generated SQL for every single-group, grouped, and empty-result pushdown request MUST be byte-identical to its pre-consolidation output EXCEPT for the scan-spec `storage` value, which `storage-access/scan-spec-credential-reference` re-encodes and which this consolidation does not touch, and every existing `dispatch_golden` fixture and grouped-aggregate assertion MUST pass with NO change to any expected value outside that one value — the empty-result response carries no scan spec at all and MUST therefore stay byte-identical with NO exception, which is why the six `empty_*` fixtures are asserted unchanged rather than regenerated

### Scenario: Aggregate function names map to AggKind through two tables

* *GIVEN* `parse_agg_item` (`adapter/pushdown/single_group_agg.rs`), where `SUM`, `MIN`, `MAX`, and `AVG` are four bodies differing only in their `kind:` field, and the six STDDEV/VARIANCE names are six bodies differing only in their `kind:` field
* *WHEN* the adapter resolves a `function_aggregate` select-list item's function name to an `AggKind`
* *THEN* the expression-capable names SHALL be resolved through ONE `[(&str, AggKind)]` table — `SUM`, `MIN`, `MAX`, `AVG` — and the statistical names through a SECOND such table — `STDDEV` and `STDDEV_SAMP` to `StddevSamp`, `STDDEV_POP` to `StddevPop`, `VARIANCE` and `VAR_SAMP` to `VarSamp`, `VAR_POP` to `VarPop`
* *AND* the two tables SHALL stay SEPARATE and MUST NOT be merged into one, because each table's members share an argument-resolution rule the other's do not: the first resolves its argument through `arg_column_or_expr`, admitting either a bare column or a rendered expression, while the second admits a bare column only (see `pushdown-aggregates/pushdown-planning-aggregate-extensions`)
* *AND* `COUNT` SHALL remain its own branch and MUST NOT be folded into either table, because it is the only name whose `AggKind` depends on its ARGUMENT rather than on its name — no argument yields `Count`, an argument yields `CountCol`
* *AND* an unrecognized function name SHALL still decline the aggregate pushdown by returning `None`, and a `distinct: true` item SHALL still decline ahead of any table lookup, so the two tables change how a recognized name is resolved and never which names are recognized
* *AND* the existing test `parse_agg_item` stat-family table (`parse_agg_item_recognises_stat_functions`, `single_group_agg.rs:839-846`) SHALL keep its own LITERAL `[(name, AggKind)]` pairs and MUST NOT be rewritten to iterate the production table, because a test reading the table it verifies asserts the table against itself and would pass any consistent mis-mapping
* *AND* the `AggregatePlan` produced for every function name and argument shape MUST be identical to the pre-refactor plan, and every existing detection test in `single_group_agg.rs`, `grouped_agg.rs`, and `support.rs` MUST pass with NO change to any expected value
