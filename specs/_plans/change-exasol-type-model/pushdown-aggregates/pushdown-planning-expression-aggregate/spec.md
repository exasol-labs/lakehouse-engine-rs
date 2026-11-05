# Feature: Pushdown Planning, Expression-Argument Aggregates

The adapter pushes an aggregate whose argument is a scalar expression over table columns, such as `SUM(LENGTH(L_COMMENT))`, into the same shard-associative partial/merge plan as a bare-column aggregate.

## Background

* The `vs-expression` translator (`render_expression`) renders the aggregate argument. An argument it cannot render makes the adapter decline the aggregate pushdown and route the request to the qualified single-table wrapper.
* `COUNT(expr)`, `SUM(expr)`, `MIN(expr)`, `MAX(expr)`, and `AVG(expr)` decompose as their bare-column forms do. `COUNT(*)` has no argument.
* An expression has no single source column, so the partial and merge column types come from the aggregate item's declared type in the top-level `selectListDataTypes` array.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Expression-argument partial and merge column types come from the declared aggregate type

* *GIVEN* a `pushdown` request carrying `SUM(expr)`, `MIN(expr)`, `MAX(expr)`, `AVG(expr)`, or `COUNT(expr)` over a renderable expression
* *WHEN* the adapter builds the partial EMITS clause and the outer merge SELECT
* *THEN* the adapter SHALL derive each partial column's Exasol type from the aggregate item's declared type in `selectListDataTypes` at that select-list ordinal, rather than from a source column's type
* *AND* a `SUM(expr)` whose declared result type is a DECIMAL SHALL widen its partial column to `DECIMAL(36,s)` to avoid mid-merge overflow, and a `SUM(expr)` over a floating result SHALL use `DOUBLE PRECISION`, matching the bare-column SUM discipline
* *AND* the outer merge SHALL cast a merged `SUM`, `AVG`, or `COUNT` to the declared type and leave a merged `MIN` or `MAX` uncast, because its partial column already carries the declared type (`pushdown-aggregates/pushdown-planning-single-group-agg`)
<!-- /DELTA:CHANGED -->
