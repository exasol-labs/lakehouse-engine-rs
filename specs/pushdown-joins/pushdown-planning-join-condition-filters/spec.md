# Feature: Pushdown Planning, Join Condition Filters

Splits each inner join's condition into its join equalities and its filters before join planning, so a filter written in `ON` prunes files, reaches its leg's scan, and leaves the broadcast join eligible, exactly as the same filter written in `WHERE` does. Both join paths, the broadcast (`pushdown-joins/pushdown-planning-join`) and the unified unaccelerated fallback (`pushdown-joins/pushdown-planning-join-fallback`), plan the split request.

## Background

* Exasol sends a join's whole `ON` clause as one `condition` node, a `predicate_and` when it holds several conjuncts. A filter written in `ON` stays in that node, and Exasol does not move it to `filter`.
* A join equality is a `predicate_equal` conjunct whose two sides each read the columns of at least one leg, and no leg on both sides. A join key is a join equality whose sides each read exactly one leg (`o.k = c.k`, `o.k = c.k + 1`). The leg binding of `pushdown-joins/pushdown-planning-join-fallback-self-join` decides each column's leg.
* Exasol pushes a join only when its condition holds at least one join equality, and in a join of two legs every join equality is a join key.
* For an inner join, `A JOIN B ON k AND p` and `A JOIN B ON k WHERE p` return the same rows, because `ON` and `WHERE` both drop a row whose predicate is not TRUE. This holds for a whole tree of inner joins.
* A null-supplying side is the side of an outer join whose columns become NULL for an unmatched row: the right side of a left outer join, the left side of a right outer join, and both sides of a full outer join.

## Scenarios

### Scenario: The filters of an inner join's condition are planned as WHERE conjuncts

* *GIVEN* a join `pushdown` request whose inner join condition holds a join equality and at least one other conjunct, such as `ON o.k = c.k AND c.region = 'EU'`, `ON o.k = c.k AND o.d >= c.valid_from`, or `ON o.k = c.k AND (c.x = 'v' OR o.y = 1)`
* *WHEN* the adapter plans the request
* *THEN* before either join path plans it, the adapter SHALL remove every conjunct that is not a join equality from that join's condition and SHALL conjoin it with the request's top-level `filter`, keeping every conjunct the `filter` already held
* *AND* the join's condition SHALL keep its join equalities, as one `predicate_equal` for one equality or one `predicate_and` of them for several
* *AND* every later planning step SHALL read the moved conjuncts as top-level WHERE conjuncts: each leg's file pruning, the broadcast eligibility check, the width-bounded rule for a text column limited to literals, each N-scan leg's filter, and the residual outer `WHERE` (`pushdown-joins/pushdown-planning-join-fallback`)
* *AND* a moved conjunct SHALL keep every `column` node as Exasol sent it, so the leg binding resolves it to the same leg, including one occurrence of a self-join
* *AND* the returned rows SHALL equal native Exasol evaluation of the same query

### Scenario: An ON filter on one leg of a join of three or more legs reaches that leg's scan

* *GIVEN* a `pushdown` request over an inner-join tree of three or more legs, where two different join conditions each hold a join equality and a conjunct that reads one leg only, such as `… JOIN c ON o.k = c.k AND c.name = 'x' JOIN l ON l.o = o.k AND l.flag = 'R' …`
* *WHEN* the adapter plans the request
* *THEN* each such conjunct SHALL appear in the scan filter of its own leg's fan-out subquery, and SHALL prune that leg's files as a WHERE conjunct does (`file-planning/pushdown-file-pruning`)
* *AND* the returned rows SHALL equal native Exasol evaluation of the same query

### Scenario: A join equality that reads several legs on one side stays in the condition

* *GIVEN* a `pushdown` request over an inner-join tree of three legs whose top join condition is `ON a.k + b.k = c.k AND c.r = 'x'`, whose only equality reads two legs on one side
* *WHEN* the adapter plans the request
* *THEN* the join's condition SHALL keep `a.k + b.k = c.k`, which the unified unaccelerated fallback renders in its `INNER JOIN … ON` chain
* *AND* the adapter SHALL move `c.r = 'x'` to the top-level `filter`, so it appears in leg `c`'s scan filter
* *AND* a condition that holds no join equality, which Exasol does not send, SHALL stay unchanged, so no join is left with an empty condition
* *AND* the returned rows SHALL equal native Exasol evaluation of the same query

### Scenario: The filters of a join on an outer join's null-supplying side stay in its condition

* *GIVEN* a join `pushdown` request whose join tree holds a join that is not inner, or a join that lies inside the null-supplying side of an outer join
* *WHEN* the adapter plans the request
* *THEN* the adapter MUST NOT move any conjunct of a join that is not inner, because moving an outer join's `ON` conjunct to `WHERE` drops or keeps the wrong rows
* *AND* the adapter MUST NOT move any conjunct of an inner join that lies inside the null-supplying side of an outer join, because `WHERE` would then also drop the outer join's NULL-extended rows
* *AND* the adapter SHALL still move the filters of an inner join that lies on no outer join's null-supplying side, such as an inner join on the preserved side of a left outer join or an inner join above a right outer join
