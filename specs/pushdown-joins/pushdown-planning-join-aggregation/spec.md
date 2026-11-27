# Feature: Pushdown Planning, Aggregation over a Join

Plans a join request that requires Exasol postprocessing. A two-leg broadcast join whose aggregation splits is aggregated in the shards, and Exasol merges the partial rows. A broadcast-eligible join whose aggregation does not split and reads only width-bounded dimension columns is aggregated by Exasol over the broadcast fan-out (`pushdown-joins/pushdown-planning-join`), and every other such join is aggregated by Exasol over the unified unaccelerated fallback (`pushdown-joins/pushdown-planning-join-fallback`).

## Background

* The broadcast contract, Exasol postprocessing, and width-bounded columns are defined in `pushdown-joins/pushdown-planning-join`. The qualified wrapper and its entry points are defined in `pushdown-joins/pushdown-planning-join-fallback`.

* An aggregation splits when its select list, `GROUP BY`, `HAVING`, and `ORDER BY` decompose into per-shard partial aggregates and an Exasol merge, as over one table (`pushdown-grouped-agg/pushdown-planning-grouped-agg`, `pushdown-aggregates/pushdown-planning-single-group-agg`). A select list that holds a `COUNT(DISTINCT ...)` does not split over a join.
* Every fact file lies in exactly one shard and every shard joins the whole dimension, so the shards' joined rows partition the join, also when a join key repeats on either side.
* A join of three or more legs does not aggregate in the shards, because Exasol joins the other fan-outs first (`pushdown-joins/pushdown-planning-join-multi-way`).

## Scenarios

### Scenario: A splittable aggregation over a broadcast-eligible join is aggregated in the shards

* *GIVEN* a two-leg inner equi-join `pushdown` request that requires Exasol postprocessing, whose smaller side is at or below the broadcast threshold
* *AND* its aggregation splits, and the scan renders its join keys and every WHERE conjunct
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL emit one broadcast fan-out whose scan spec carries the join block together with the aggregates, and with the group keys when the request groups
* *AND* each group key and each aggregate argument in the scan spec SHALL name its column as a pair qualified by its relation alias, `"<alias>"."<COLUMN>"`, so a column name both legs carry stays one leg's column
* *AND* the fan-out SHALL emit the `GK_*` and `PARTIAL_*` columns that the same aggregation emits over one table, and no joined row
* *AND* one outer Exasol `SELECT` SHALL merge the partial rows and apply `HAVING`, `ORDER BY`, `LIMIT`, and `OFFSET`, as over one table
* *AND* a dimension column that is not width-bounded SHALL NOT keep the request off the broadcast path, because a shard emits it once per group and not once per joined row
* *AND* the returned rows SHALL equal native Exasol evaluation of the same query, for a group key of either table, a single group, an aggregate over an expression that reads both tables, and a self-join whose join key repeats on both sides

### Scenario: An aggregation that does not split over a broadcast-eligible join is aggregated by Exasol over the broadcast fan-out

* *GIVEN* a `pushdown` request that meets the broadcast contract and requires Exasol postprocessing
* *AND* its aggregation does not split, such as a `COUNT(DISTINCT ...)`
* *AND* every dimension column that its select list, `GROUP BY`, `HAVING`, and `ORDER BY` read is width-bounded
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL emit the broadcast fan-out, projecting only the columns that the select list, `GROUP BY`, `HAVING`, and `ORDER BY` read, and one join-key column of the first leg when they read none
* *AND* one outer Exasol `SELECT` SHALL render the select list, `GROUP BY`, `HAVING`, `ORDER BY`, and `LIMIT` over the fan-out's `<alias>_<column>` output names, so no shard aggregates, sorts, or truncates
* *AND* the returned rows SHALL equal native Exasol evaluation of the same query

### Scenario: An aggregation that does not split keeps the N-scan when it reads an unbounded text column of the small side

* *GIVEN* a two-leg inner equi-join `pushdown` request that requires Exasol postprocessing, whose smaller side is at or below the broadcast threshold
* *AND* its aggregation does not split, or the scan declines a WHERE conjunct
* *AND* its select list, `GROUP BY`, `HAVING`, or `ORDER BY` reads a dimension column that is not width-bounded, such as `GROUP BY d.DESCRIPTION` or `COUNT(DISTINCT d.DESCRIPTION)` with no literal condition on it
* *WHEN* the adapter plans the request, after the size check
* *THEN* the adapter SHALL route the request to the unified unaccelerated fallback (`pushdown-joins/pushdown-planning-join-fallback`), because the broadcast fan-out repeats that column once per joined row while the fallback sends each dimension row once
* *AND* a column the request wraps in an expression, such as `SUBSTR(d.DESCRIPTION, 1, 3)`, SHALL count as read, because the fan-out emits only plain columns
* *AND* a fact column of any type SHALL NOT keep the fallback, because both plans send each fact row once
* *AND* the returned rows SHALL equal native Exasol evaluation of the same query

### Scenario: Aggregate over a join routes through the unified qualified wrapper

* *GIVEN* an inner equi-join `pushdown` request outside the broadcast contract that requires Exasol postprocessing, or whose `ORDER BY`/`LIMIT` shape the broadcast path declines, per `pushdown-joins/pushdown-planning-join`
* *WHEN* the adapter plans the request
* *THEN* the adapter SHALL route the request to the unified qualified fallback path
* *AND* a bare `LIMIT` alone, and a bare-projected-column `ORDER BY` alone, SHALL NOT force this route
* *AND* the fallback wrapper SHALL render the aggregate select list as ordinary Exasol SQL over the materialized join (`SELECT <aggregates> FROM (side-0 fan-out) "LHS_T0", (side-1 fan-out) "LHS_T1", … WHERE <conditions> [GROUP BY …] [HAVING …] [ORDER BY …] [LIMIT …]`), splicing Exasol's own aggregate function name verbatim while table-qualifying only its column argument(s)
* *AND* a select-list item that is a SCALAR FUNCTION WRAPPING one or more aggregates (e.g. `ROUND(100.0 * SUM(CASE WHEN … END) / COUNT(*), 2)`) SHALL be rendered by recursing through the `crates/vs-expression` translator, which renders nested `function_aggregate` nodes by splicing the aggregate name verbatim and rendering the argument(s), NOT declined
* *AND* each select-list item SHALL be cast to its `selectListDataTypes` type, except an item whose exact type (`pushdown-aggregates/pushdown-planning-split-aggregate-types`) already equals it, such as `COUNT(*)` or a `MAX` over a CASE of dates
* *AND* the returned result SHALL equal the result of evaluating the same select list over the same inner join on a single node

### Scenario: A scalar function wrapping aggregates in a grouped join select list is rendered, not declined

* *GIVEN* an inner equi-join `pushdown` request outside the broadcast contract whose grouped select list contains a select item that is a scalar function wrapping one or more aggregates, e.g. `ROUND(100.0 * SUM(CASE WHEN l_returnflag = 'R' THEN 1 ELSE 0 END) / COUNT(*), 2)`, alongside plain aggregates such as `SUM(l_quantity)`, `SUM(CASE WHEN … END)`, and `AVG(l_extendedprice)`, with a GROUP BY, a HAVING, an ORDER BY, and a LIMIT
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL NOT decline the request and SHALL NOT return an error for the scalar-over-aggregate select item
* *AND* the adapter SHALL render each such select item by recursing through the `crates/vs-expression` translator, which renders the outer scalar function (`ROUND`, arithmetic, `CASE`, …) around nested `function_aggregate` nodes whose aggregate names (`SUM`, `COUNT`, `AVG`, …) are spliced verbatim and whose column arguments are table-qualified from their `tableName`
* *AND* a top-level bare aggregate and a nested aggregate SHALL be rendered by the same aggregate-rendering path, so the two produce consistent SQL
* *AND* the emitted SQL SHALL be the unified qualified fallback wrapper (`LHS_T0`, `LHS_T1`, … subqueries) with the grouped select list, HAVING, ORDER BY, and LIMIT rendered over the materialized join
* *AND* the returned result SHALL equal the result of evaluating the same grouped, scalar-over-aggregate select list over the same inner join on a single node
