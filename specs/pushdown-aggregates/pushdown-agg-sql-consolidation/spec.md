# Feature: Pushdown Aggregate SQL Consolidation

The adapter builds aggregate-pushdown SQL with one owner each for the STDDEV/VARIANCE merge expressions, the declared-type CAST rule, and the aggregate-function-name to `AggKind` mapping. Committed golden fixtures pin the generated SQL.

## Background

* The three consolidations are independent of each other.
* The statistical merge combines `(cnt, mean, m2)` partials with Chan's parallel formula (`pushdown-aggregates/pushdown-planning-aggregate-extensions`).
* The CAST rule wraps an expression in `CAST(<expr> AS <ty>)` unless `<ty>` is `VARCHAR(2000000)`. The `GroupKey` and `Aggregate` arms of `empty_grouped_sql` keep an unconditional `CAST(NULL AS <ty>)`, because a bare `NULL` has no `VARCHAR` type and fails the positional `selectListDataTypes` check.
* `COUNT` is the only aggregate name whose `AggKind` depends on its argument.
* The `storage` value of the scan-spec literal is the tagged wrapper owned by `storage-access/scan-spec-credential-reference`. The six `empty_*` golden fixtures carry no `storage` value.

## Scenarios

### Scenario: The statistical merge has one owner

* *GIVEN* the six statistical aggregate kinds `VAR_POP`, `VAR_SAMP`, `VARIANCE`, `STDDEV_POP`, `STDDEV_SAMP`, and `STDDEV`
* *WHEN* the adapter builds the outer merge expression for any of them
* *THEN* one function SHALL build the merged count `N`, the merged `M2`, and the three variance forms once per aggregate ordinal, and each standard deviation SHALL be `SQRT` of its matching variance
* *AND* the partial column names the merge references SHALL come from the single name owner recorded in `scan-aggregation/scan-partial-agg-column-contract`, so the statistical merge retains NO `PARTIAL_` string literal of its own

### Scenario: The declared-type CAST rule has one owner

* *GIVEN* the sites that decide whether to wrap an outer-wrapper expression in `CAST(<expr> AS <ty>)`, the constant-projection, group-key, merge-item, and scalar-over-aggregate renderers of the grouped planner, the single-group merge, and the empty-result builder
* *WHEN* any of these sites decides whether to cast an outer-wrapper expression to its Exasol-declared type
* *THEN* exactly ONE helper, `cast_to_declared_type`, SHALL own that decision, taking the rendered expression and an `Option<&str>` declared type and returning the expression cast or uncast, and each site SHALL retain NO `VARCHAR(2000000)` comparison and NO `CAST(` format of its own
* *AND* the helper SHALL take an `Option<&str>` rather than a `&str`, because most sites read their declared type from a `.get(i)` on a possibly-shorter list and treat absence as "no cast"
* *AND* the helper SHALL be declared `pub(super)` in the pushdown module's shared-primitives submodule (`support`), because its consumers live in several sibling submodules rather than in one
* *AND* the `GroupKey` and `Aggregate` arms of the empty grouped-result builder MUST NOT be routed through the helper and SHALL keep their UNCONDITIONAL `CAST(NULL AS <ty>)`, because those arms deliberately cast even to `VARCHAR(2000000)`: a bare `NULL` in a zero-row `SELECT … FROM DUAL WHERE 1=0` carries no `VARCHAR` type and would fail the positional `selectListDataTypes` check the cast exists to satisfy
* *AND* the generated SQL for every single-group, grouped, and empty-result pushdown request MUST match its committed `dispatch_golden` fixture by full-string equality, and the empty-result response carries no scan spec at all, so the six `empty_*` fixtures carry no `storage` value

### Scenario: Aggregate function names map to AggKind through two tables

* *GIVEN* the aggregate-item parser, where `SUM`, `MIN`, `MAX`, and `AVG` would otherwise be four bodies differing only in their `kind:` field, and the six STDDEV/VARIANCE names six bodies differing only in their `kind:` field
* *WHEN* the adapter resolves a `function_aggregate` select-list item's function name to an `AggKind`
* *THEN* the expression-capable names SHALL be resolved through ONE `[(&str, AggKind)]` table, `SUM`, `MIN`, `MAX`, `AVG`, and the statistical names through a SECOND such table, `STDDEV` to `Stddev`, `STDDEV_SAMP` to `StddevSamp`, `STDDEV_POP` to `StddevPop`, `VARIANCE` to `Variance`, `VAR_SAMP` to `VarSamp`, `VAR_POP` to `VarPop`
* *AND* the two tables SHALL stay SEPARATE and MUST NOT be merged into one, because each table's members share an argument-resolution rule the other's do not: the first resolves its argument through `arg_column_or_expr`, admitting either a bare column or a rendered expression, while the second admits a bare column only (see `pushdown-aggregates/pushdown-planning-aggregate-extensions`)
* *AND* `COUNT` SHALL remain its own branch and MUST NOT be folded into either table, because it is the only name whose `AggKind` depends on its ARGUMENT rather than on its name, no argument yields `Count`, an argument yields `CountCol`
* *AND* an unrecognized function name SHALL still decline the aggregate pushdown by returning `None`, and a `distinct: true` item SHALL still decline ahead of any table lookup, so the two tables change how a recognized name is resolved and never which names are recognized
* *AND* the stat-family parser test (`parse_agg_item_recognises_stat_functions`) SHALL keep its own LITERAL `[(name, AggKind)]` pairs and MUST NOT iterate the production table, because a test reading the table it verifies asserts the table against itself and would pass any consistent mis-mapping
* *AND* the `AggregatePlan` produced for a function name and argument shape SHALL depend only on that name and shape, asserted by the detection tests of the single-group, grouped, and shared-support modules
