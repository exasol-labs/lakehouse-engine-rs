# Feature: Pushdown Planning, Broadcast Inner Equi-Join

Extends pushdown planning (`pushdown/pushdown-planning`) with the broadcast inner equi-join. The adapter broadcasts one eligible pair of join legs: it shards the pair's larger (fact) leg across the scan fan-out and replicates its smaller (dimension) leg into the shard-invariant common spec, and a node-local DataFusion join runs inside the scan UDF (`scan-aggregation/scan-execution-join`). In a two-leg join the pair is the whole join. In a join of three or more legs every other leg keeps its own fan-out, and Exasol joins the fan-outs (`pushdown-joins/pushdown-planning-join-multi-way`). The legs may be one table joined to itself, and they may share column names. A join with no eligible pair is served by the unified unaccelerated fallback (`pushdown-joins/pushdown-planning-join-fallback`).

## Background

* A broadcast pair is two legs of an all-inner join tree that one or more join keys join, each key reading only those two legs (`pushdown-joins/pushdown-planning-join-condition-filters`). Its smaller leg by bytes after its own pruning is the dimension, and the other is the fact.
* A pair is eligible when its dimension is at or below `JOIN_BROADCAST_MAX_BYTES` and, when Exasol processes the pair's rows further (it aggregates them under Exasol postprocessing that the shards do not split, per `pushdown-joins/pushdown-planning-join-aggregation`, joins them with other legs, or applies a filter conjunct to them), every dimension column that the select list, the other join conditions, Exasol's `WHERE`, `GROUP BY`, `HAVING`, and `ORDER BY` read is width-bounded. A request meets the broadcast contract when it is a two-leg join with an eligible pair and the scan renders every WHERE conjunct and every select-list item, so its broadcast fan-out is the whole plan.
* Exasol postprocessing means a non-null `aggregationType`, `group_by` or `single_group`. Exasol sets it on every request that aggregates, groups, or carries a HAVING, including one whose aggregates it stripped from the select list. A pushed `LIMIT` and a pushed `ORDER BY` do not force it.
* A width-bounded column has a fixed-width type (`DECIMAL`, `DOUBLE`, `DATE`, `TIMESTAMP`, or `BOOLEAN`), or is a text column that a top-level WHERE conjunct limits to string literals (`col = 'x'` or `col IN ('x', 'y')`). Every text column the adapter declares has the same declared length, so the type alone cannot tell a short code from a long description.
* A join leg is one occurrence of a table in the FROM tree, and `pushdown-joins/pushdown-planning-join-fallback-self-join` states which leg a `column` node reads. Exasol binds the pushdown result to the select list by position, so the column names the pushdown SQL emits stay inside it.

## Scenarios

### Scenario: Adapter advertises inner equi-join capabilities

* *GIVEN* an Exasol session that has installed the VS adapter script
* *WHEN* Exasol sends a `getCapabilities` request to the adapter
* *THEN* the capabilities list SHALL include `JOIN`, `JOIN_TYPE_INNER`, and `JOIN_CONDITION_EQUI`
* *AND* the capabilities list MUST NOT include `JOIN_TYPE_LEFT_OUTER`, `JOIN_TYPE_RIGHT_OUTER`, `JOIN_TYPE_FULL_OUTER`, `JOIN_CONDITION_ALL`, or any Cartesian-product capability
* *AND* each advertised join capability SHALL be backed by the join-planning path in this feature and its fallback counterpart, so an advertised capability is never a shape the planner cannot serve either by broadcast or by the unified unaccelerated fallback

### Scenario: Broadcast-eligible inner equi-join is planned as a broadcast fan-out

* *GIVEN* a virtual schema over a namespace whose tables are backed by SeaweedFS
* *AND* a `pushdown` request whose `from` clause is a `join` node over exactly two legs joined by an equi-condition
* *AND* the smaller side's table-metadata byte size is at or below the broadcast threshold
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL resolve BOTH sides the way the single-table scan resolves its table (`file-planning/pushdown-format-neutral-resolution`), taking each side's data-file list, per-file byte size, table root, and effective storage from its table's prune and its logical schema from its column notes, loading each distinct table once (`file-planning/pushdown-statement-state`), and recovering each table's original-cased catalog identifier from the schema-metadata mapping by its involved-table name
* *AND* the adapter SHALL designate the larger side as the sharded fact side (its file list partitioned into G byte-balanced work-unit shards and driven through the nested `LAKEHOUSE_DISTRIBUTE_FILES` distributor exactly as the single-table path does) and the smaller side as the replicated dimension side
* *AND* the adapter SHALL carry the dimension side's FULL file list, table root, logical schema, and its OWN storage value (a connection REFERENCE or sealed envelope per `storage-access/scan-spec-credential-reference`) in the shard-invariant common spec's join block, and the fact side's storage and per-shard file subset through the distributor
* *AND* the generated scan-driving SQL SHALL drive the `LAKEHOUSE_SCAN` SCALAR EMIT UDF so that each shard invocation joins its fact-file subset against the full replicated dimension side node-locally, with no cross-shard exchange, and with NO `SELECT * FROM (...)` wrapper for an UNORDERED request; an ORDERED request carries the outer wrapper this feature's ordering scenario specifies
* *AND* the adapter MUST NOT read either side's Parquet row data in the planning layer
* *AND* a join whose two sides are DELTA tables reached through Unity Catalog SHALL take this same broadcast path with no Iceberg-specific step, because the side resolution and the broadcast decision read only format-neutral values

### Scenario: Small-side selection uses table-format metadata and the broadcast threshold

* *GIVEN* an inner equi-join `pushdown` request over two legs
* *WHEN* the adapter evaluates broadcast eligibility
* *THEN* the adapter SHALL compute each side's byte size as the sum of that side's resolved per-file byte sizes, read from the table's own format metadata, an Iceberg manifest's `file_size_in_bytes` for the resolved snapshot, a Delta `add` action's `size` for the resolved version, without opening any Parquet file
* *AND* the adapter SHALL choose the side with the smaller metadata byte size as the broadcast (dimension) side and the other as the sharded (fact) side
* *AND* each side's byte size SHALL be its byte total after that side's own file pruning, so the two occurrences of a self-join can differ in size when a leg-local filter prunes one occurrence's files and not the other's
* *AND* on an exact byte tie the first leg in FROM-tree order SHALL be the broadcast (dimension) side, so the choice of the sharded side is deterministic
* *AND* when the smaller side's byte size is at or below `JOIN_BROADCAST_MAX_BYTES` the adapter SHALL plan the broadcast fan-out
* *AND* when the smaller side's byte size exceeds `JOIN_BROADCAST_MAX_BYTES` the adapter SHALL take the unified unaccelerated fallback instead
* *AND* the threshold SHALL be read from the persisted adapter note `JOIN_BROADCAST_MAX_BYTES`, defaulting to 134217728 when absent or unparseable
* *AND* the sum SHALL saturate, so a side whose byte total overflows `u64` is clamped to `u64::MAX` and is therefore never chosen as the broadcast side

### Scenario: Broadcast join projection and filter are rendered per involved table

* *GIVEN* a `pushdown` request over an inner join with a broadcast pair
* *WHEN* the adapter resolves the projection and renders the WHERE filter
* *THEN* the adapter SHALL resolve each projected column's Exasol output type from the leg it belongs to, matching the column against that leg's involved-table column metadata
* *AND* the scan-driving SQL's declared EMITS column list SHALL match the projected join output columns in order and type
* *AND* each top-level WHERE conjunct whose legs all lie in the pair and that the scan renders against each column's own leg type (`pushdown-types/pushdown-planning-scan-text-conversion`) SHALL be carried in the common spec
* *AND* a conjunct local to a leg outside the pair SHALL be that leg's fan-out filter, as `pushdown-joins/pushdown-planning-join-fallback` states
* *AND* every conjunct that reads legs of more than one fan-out, or that the scan declines, SHALL be applied once in Exasol's `WHERE` over the fan-outs, as `pushdown/pushdown-declined-filter-self-apply` states for a declined conjunct, and the adapter MUST NOT emit a plan that omits a conjunct
* *AND* an ABSENT filter SHALL emit no scan-spec filter and no Exasol `WHERE`
* *AND* every column reference in the scan spec's filter and projection SHALL be qualified with its leg's relation alias `LHS_T{leg}`, the alias the join block carries for that relation, never with Exasol's `tableAlias`

### Scenario: Join condition is rendered via the vs-expression translator

* *GIVEN* a broadcast-eligible inner equi-join carrying an equi-join `condition` expression node
* *WHEN* the adapter renders the join condition
* *THEN* the adapter SHALL render the condition to a DataFusion SQL fragment using the `crates/vs-expression` translator, the same way filter predicates are translated
* *AND* the adapter SHALL qualify every column reference in the condition with its leg's relation alias BEFORE rendering, replacing Exasol's native `tableAlias`, so each operand resolves against its own leg even when both legs carry the same column name
* *AND* a self-join condition such as `a.ID = b.ID` SHALL render as a comparison of `"LHS_T0"."ID"` with `"LHS_T1"."ID"` and MUST NOT render as a comparison of one column to itself, which is trivially true and degenerates the join into a cross product
* *AND* the rendered condition SHALL be carried in the common spec so every shard applies the identical join predicate
* *AND* a condition the translator cannot render SHALL cause the adapter to take the unified unaccelerated fallback rather than emit a broadcast plan with a mistranslated predicate

### Scenario: Legs that share column names are planned as a broadcast fan-out

* *GIVEN* a `pushdown` request that meets the broadcast contract and whose two legs share at least one column name: two tables that both carry the join key, two tables that both carry an unrelated column, or one table joined to itself
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL emit the broadcast fan-out and MUST NOT route the request to the unified unaccelerated fallback because of a shared column name
* *AND* the adapter SHALL give each FROM-tree occurrence its own relation alias through the leg binding, the same `LHS_T{leg}` alias the unified unaccelerated fallback renders (`LHS_T0`, `LHS_T1`), and SHALL carry the fact relation's alias and the dimension relation's alias in the join block (`scan-aggregation/scan-execution-join`)
* *AND* the adapter SHALL handle every column as the pair (relation alias, column), and SHALL render each `column` node of the join condition, the filter, and the select list qualified with its leg's alias, `"LHS_T1"."CUSTOMER_ID"`
* *AND* every check on the broadcast path that compares one column to another, or tests whether a column belongs to a set, SHALL compare the (relation alias, column) pair and MUST NOT compare the bare column name, in the adapter and in the scan alike: so a column name that both legs carry, with equal or different types, never resolves to the other leg's column
* *AND* each projected column SHALL be emitted, in the scan's select list and in the declared EMITS list, under the output name `<alias>_<column>` (`LHS_T0_CUSTOMER_ID`, `LHS_T1_CUSTOMER_ID`), so a select list that names the same column on both legs emits two distinct columns, each carrying its own leg's values
* *AND* an ORDER BY key SHALL bind to the projected column of its own leg by that pair: the join block's post-join ordering SHALL carry the pair, the outer wrapper SHALL order by that column's output name, and a key whose pair the projection does not carry SHALL decline to the unified unaccelerated fallback, as an unprojected sort key already does, even when the other leg projects the same column name (`SELECT a.ID … ORDER BY b.ID`)
* *AND* a `column` node that resolves to no single leg SHALL fail the request before either path renders, as `pushdown/pushdown-planning` "A column the request does not declare fails the request" states, because rendering it against either leg returns wrong rows
* *AND* the adapter SHALL decline the broadcast plan to the unified unaccelerated fallback when a projected column's output name is longer than Exasol's 128-character identifier limit
* *AND* the returned rows SHALL equal native Exasol evaluation of the same join, row for row

### Scenario: A two-leg join on a composite key is planned as a broadcast fan-out

* *GIVEN* a two-leg inner join `pushdown` request whose condition is two or more join keys, such as `ON o.k1 = c.k1 AND o.k2 = c.k2`, whose smaller side is at or below the broadcast threshold, and that meets the rest of the broadcast contract
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL emit the broadcast fan-out and MUST NOT route the request to the unified unaccelerated fallback
* *AND* the join block SHALL carry the condition rendered as the conjunction of every join key, each `column` node qualified with its leg's relation alias
* *AND* the order of the keys, and the side of `=` each leg is written on (`c.k1 = o.k1`), SHALL NOT change the plan's eligibility
* *AND* the returned rows SHALL equal native Exasol evaluation of the same query, for a key of three columns, for a dimension side that holds duplicate composite keys, for a self-join on a key that includes a `DATE` column, and with an aggregate, a `GROUP BY`, or an `ORDER BY … LIMIT`

### Scenario: A join key that compares expressions is planned as a broadcast fan-out

* *GIVEN* a two-leg inner join `pushdown` request whose condition holds a join key with an expression side, such as `ON o.k = c.k + 1`, alone or beside a filter, and that meets the rest of the broadcast contract
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL emit the broadcast fan-out and MUST NOT route the request to the unified unaccelerated fallback
* *AND* the returned rows SHALL equal native Exasol evaluation of the same query

### Scenario: A two-leg join with filters in its condition is planned as a broadcast fan-out

* *GIVEN* a two-leg inner join `pushdown` request whose condition holds its join keys and filters: a conjunct on either leg alone, a comparison across the legs such as `o.x > c.y`, or an `OR` across the legs, with or without a top-level WHERE filter
* *AND* its pair is eligible once those filters are WHERE conjuncts
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL emit the broadcast fan-out and MUST NOT route the request to the unified unaccelerated fallback
* *AND* the common spec's filter SHALL carry each moved conjunct, rendered as a WHERE conjunct is
* *AND* a dimension text column that a moved conjunct limits to literals, such as `GROUP BY c.name` with `ON … AND c.name = 'v'`, SHALL count as width-bounded
* *AND* a moved conjunct on one occurrence of a self-join SHALL prune only that occurrence's files
* *AND* a moved conjunct that the scan translator declines SHALL be applied in Exasol's `WHERE` over the broadcast fan-out, as a declined WHERE conjunct is
* *AND* the returned rows SHALL equal native Exasol evaluation of the same query
