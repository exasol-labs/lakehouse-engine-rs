[lakehouse-engine](../README.md) › [Docs](index.md) › Writing efficient queries

---

# Writing efficient queries

Every query on a lakehouse table returns correct results, but the way you write it decides how
much work runs inside the scan and how much data travels into Exasol. A query is fast when:

- **The scan reads few files.** A filter the adapter receives prunes data files and row groups
  before anything is read.
- **The shards do the work.** Filters, partial aggregates, broadcast joins, and top-N run in
  DataFusion on every node, next to the data.
- **Few rows reach Exasol.** Whatever the adapter does not receive, Exasol computes over the rows
  the scan returns, and it holds all of them in memory first (see
  [Exasol holds every pushdown result in memory](#exasol-holds-every-pushdown-result-in-memory)).

This page lists the rules that follow from this, with the form to prefer and the form to avoid.
[Capabilities](capabilities.md) has the full detail behind each rule. The rules describe the
current release: every new pushed-down function or join shape relaxes one of them, and the
release that does so updates this page. Last verified against lakehouse-engine 0.59.1 on Exasol
2026.2.

## Quick reference

| Prefer | Avoid | Why |
|---|---|---|
| `d >= DATE '2024-01-01' AND d < DATE '2025-01-01'` | `YEAR(d) = 2024` | A function over a column prunes no files. |
| `d = DATE '2024-01-02'` | `d = '2024-01-02'` | Text compared with a date runs in Exasol. |
| Join filters in `WHERE` | Join filters in `ON` | A filter in `ON` loses the broadcast join and prunes nothing. |
| Two tables joined on one column | Two join columns, or three or more tables | Only a two-table, one-column inner join is broadcast. |
| `FROM a JOIN b ON ... JOIN c ON ...` | `FROM a, c, b` where `c` does not join `a` | Exasol then pushes every table separately. |
| Outer-side filters of a `LEFT JOIN` in a `WITH` clause | Outer-side filters in `ON` | Exasol pushes no filter from `ON` to the outer side. |
| `JOIN` on a unique key | `IN (SELECT ...)`, `EXISTS` | A semi-join is never pushed down. |
| Filters on the lakehouse table's own columns | Filters only on the native table it joins | Exasol holds the whole lakehouse side before it joins. |
| `SUM`, `COUNT`, `MIN`, `MAX`, `AVG` | `MEDIAN`, `LISTAGG`, `APPROXIMATE_COUNT_DISTINCT` | Only decomposable aggregates run in the shards. |
| The columns you need | `SELECT *` | The scan reads only the projected columns. |

## Check a query yourself

`EXPLAIN VIRTUAL <query>` shows what the adapter received and generated, without running the
query. [Debugging pushdown](debugging-pushdown.md) explains the output. For the rules on this
page, look at:

- **The number of rows.** Each row is one pushdown. A join that returns one row per table was not
  pushed down as a join.
- **The `PUSHDOWN_SQL` column.** A `"join":{` object in the scan payload is a broadcast join.
  Several `LAKEHOUSE_SCAN` calls in one row are a join that scans each table separately. A
  `FROM DUAL WHERE 1=0` means the filter pruned every file.

## Exasol holds every pushdown result in memory

Exasol writes the complete result of each lakehouse pushdown into a temporary table before it
joins, filters, sorts, or limits it. A query's memory cost is the size of what the pushdowns
return, not of the final result. Each concurrent query holds its own copy. When the copies exceed
DB RAM, Exasol spills them to disk, which makes queries several times slower, and under enough
concurrent load sessions fail with out-of-memory errors.

- **Filters that don't push down don't shrink it.** Exasol holds the whole table, then filters it.
- **`LIMIT` shrinks it only when it pushes down** (see [`ORDER BY ... LIMIT`](#order-by--limit)).
  Over a join with a native table, a join that scans each table on its own, or an unpushed
  filter, Exasol holds every row before it applies the limit.
- **A join with a native Exasol table holds the whole lakehouse side**, reduced only by filters
  on the lakehouse table's own columns. Exasol does not derive them from filters on the native
  table, from the join key, or from an `IN` subquery over a native table, so write them out.
- **A broadcast join holds only the joined rows.** A join that scans each table on its own, and
  tables pushed down separately, hold each table in full, reduced only by its own `WHERE`
  filters.
- **An aggregation that pushes down holds only the groups.**
- **Every selected column is held**, so `SELECT *` costs memory as well as I/O.

| Prefer | Avoid |
|---|---|
| `SELECT ... FROM orders o JOIN native_customers c ON o.customer_id = c.customer_id WHERE o.customer_id IN (1, 2, 3)` | `SELECT ... FROM orders o JOIN native_customers c ON o.customer_id = c.customer_id WHERE c.customer_id IN (1, 2, 3)` |

To see what Exasol held, run the query with `ALTER SESSION SET PROFILE = 'ON'`, then
`FLUSH STATISTICS`, and read `OUT_ROWS` of the `INSERT` parts on `TEMPORARY table` in
`EXA_STATISTICS.EXA_USER_PROFILE_LAST_DAY`.

## Filters

**Compare a bare column with a literal.** Only `column <op> literal` prunes files: `=`, `<>`,
`<`, `<=`, `>`, `>=`, `IN`, `BETWEEN`, `IS [NOT] NULL`, and `LIKE` with a literal prefix,
combined with `AND`, `OR`, and `NOT`. A function over the column, such as `YEAR(d) = 2024` or
`UPPER(code) = 'AB'`, still filters inside the scan but prunes no files. Write a range instead.
See [File pruning](capabilities.md#file-pruning).

**Write literals in the column's type.** `d = '2024-01-02'` on a DATE column converts by the
session's NLS settings, so it runs in Exasol and prunes nothing. Write `DATE '2024-01-02'`,
`TIMESTAMP '...'`, or `TO_DATE(s, 'YYYY-MM-DD')` with a format. See
[Text and session settings](capabilities.md#text-and-session-settings).

**Constant expressions and clock functions are free.** Exasol folds a constant expression into a
literal before the adapter sees it: `DATE '1998-12-01' - INTERVAL '90' DAY` and
`ADD_MONTHS(DATE '2024-01-31', 1)` arrive as date literals and prune. The adapter turns
`CURRENT_DATE`, `CURRENT_TIMESTAMP`, `LOCALTIMESTAMP`, `SYSDATE`, and `SYSTIMESTAMP` into literals
too, once per statement, so `order_date > CURRENT_DATE - 30` and
`created_at > ADD_HOURS(CURRENT_TIMESTAMP, -1)` prune like a filter over a literal.

**Set `SESSION_TIME_ZONE` when your sessions use another zone than the database.** The adapter
cannot read the session's time zone. It folds `CURRENT_DATE`, `CURRENT_TIMESTAMP`, and
`LOCALTIMESTAMP` in the zone of the `SESSION_TIME_ZONE` property ([Tuning](tuning.md)), or in
`DBTIMEZONE` when it is unset. Exasol folds an expression such as `CURRENT_DATE - 30` in the
session's zone. Without the property, a session in another zone can see a different date than
native Exasol around midnight, and the two forms can disagree within one query. See
[Semantic differences](semantic-differences.md#clock-functions-and-timestamp-with-local-time-zone-literals).

**Keep unpushed predicates out of an `OR`.** A predicate the adapter does not handle, such as
`REGEXP_LIKE`, runs in Exasol. Inside an `AND`, the other conditions still reach the scan. Inside
an `OR`, the whole `OR` stays in Exasol.

| Prefer | Avoid |
|---|---|
| `WHERE status = 'F' AND name REGEXP_LIKE 'a.*'` | `WHERE status = 'F' OR name REGEXP_LIKE 'a.*'` |

## Joins

Only one join shape runs inside the scan today: a **broadcast join**. The engine copies the
smaller table into every shard of the larger one and joins there, and the smaller side's join keys
let the larger side skip row groups that cannot match. A join is broadcast when all of these hold:

- it is an inner join of exactly two tables, or of a table with itself;
- it joins on one column equality, `a.k = b.k`;
- the smaller table's files are at most `JOIN_BROADCAST_MAX_BYTES` ([Tuning](tuning.md)),
  128 MiB by default;
- the filters are in `WHERE`, and the scan can evaluate them;
- with an aggregate, `GROUP BY`, or `HAVING`, every column the aggregation reads from the smaller
  table is a number, date, timestamp, or boolean, or a text column that `WHERE` limits to
  literals (`col = 'x'`, `col IN ('x', 'y')`).

Any other inner join still pushes down as one request, but the engine scans each table on its own,
with that table's `WHERE` filters, and Exasol joins the scanned rows. Outer joins and semi-joins
don't push down as joins at all.

### Put join filters in `WHERE`

In an inner join, a filter in `ON` and the same filter in `WHERE` return the same rows, but Exasol
sends an `ON` filter as part of the join condition. The join then loses the broadcast plan, and
that table is read in full.

| Prefer | Avoid |
|---|---|
| `FROM orders o JOIN customers c ON o.customer_id = c.customer_id WHERE c.country = 'DE'` | `FROM orders o JOIN customers c ON o.customer_id = c.customer_id AND c.country = 'DE'` |

### Join on one column

A join on two or more column equalities, such as `ps.partkey = l.partkey AND ps.suppkey =
l.suppkey`, is not broadcast, and moving one equality to `WHERE` does not help, because Exasol
moves it back into the join condition. The same holds for a condition that adds a non-equality,
such as `a.k = b.k AND a.x > b.y`.

### Two tables are broadcast, three or more are not

A join of three or more tables scans every table on its own and joins in Exasol. Filters on each
table still prune that table.

### List comma-joined tables in join order

With a comma-separated `FROM`, Exasol pushes the join down only if every table joins to a table
listed before it. Otherwise it pushes each table down separately, and the adapter is called once
per table. Explicit `JOIN ... ON` syntax, or reordering the list, keeps the join in one request:

| Prefer | Avoid |
|---|---|
| `FROM lineitem l, part p, supplier s WHERE p.partkey = l.partkey AND s.suppkey = l.suppkey` | `FROM part p, supplier s, lineitem l WHERE ...` (`supplier` does not join `part`) |
| `FROM part p JOIN lineitem l ON p.partkey = l.partkey JOIN supplier s ON s.suppkey = l.suppkey` | |

Today the difference is mostly planning time, since the adapter plans once per pushdown. Joins
that arrive as one request are also the ones that future join improvements speed up.

### Outer joins: filter the outer side in a `WITH` clause

A `LEFT`, `RIGHT`, or `FULL` join pushes each table down separately, and Exasol joins them. A
`WHERE` filter on the preserved side still reaches its scan. A filter in `ON` on the outer side
does not reach any scan, and neither does the same filter in an inline subquery, because Exasol
merges the subquery into the join. A `WITH` clause keeps it:

| Prefer | Avoid |
|---|---|
| `WITH o AS (SELECT customer_id, order_id FROM orders WHERE status = 'F') SELECT ... FROM customers c LEFT JOIN o ON c.customer_id = o.customer_id` | `SELECT ... FROM customers c LEFT JOIN orders o ON c.customer_id = o.customer_id AND o.status = 'F'` |
| | `SELECT ... FROM customers c LEFT JOIN (SELECT ... FROM orders WHERE status = 'F') o ON ...` |

A `WHERE` filter on the outer side turns the outer join into an inner join, which Exasol then
pushes down as one. If an inner join is what you mean, write `JOIN`.

### Prefer a join to `IN` and `EXISTS` subqueries

`IN (SELECT ...)`, `EXISTS`, `NOT IN`, and `NOT EXISTS` push each table down separately with its
own filters, and Exasol evaluates the semi-join. Correlated subqueries, such as
`x < (SELECT AVG(x) FROM t WHERE t.k = o.k)`, work the same way. When the subquery selects a
unique key, such as the key of a dimension table, an inner join returns the same rows and can be
broadcast:

| Prefer | Avoid |
|---|---|
| `SELECT o.order_id FROM orders o JOIN customers c ON o.customer_id = c.customer_id WHERE c.country = 'DE'` | `SELECT order_id FROM orders WHERE customer_id IN (SELECT customer_id FROM customers WHERE country = 'DE')` |

Keep the subquery when the key is not unique: the join would repeat rows.

## Aggregation

**Use decomposable aggregates.** `COUNT`, `SUM`, `MIN`, `MAX`, and `AVG` run per shard, and Exasol
merges one partial row per group. `MEDIAN`, `LISTAGG`, `GROUP_CONCAT`,
`APPROXIMATE_COUNT_DISTINCT`, and the `DISTINCT` forms of `SUM` and `AVG` send every scanned row
to Exasol. A `COUNT(DISTINCT col)` runs in the shards only as the single aggregate of a query
without `GROUP BY`. The statistical aggregates (`STDDEV`, `VARIANCE`, and their variants) run in
the shards over a bare column.

**Select a computed group key, or cast it.** `GROUP BY YEAR(d)` with `YEAR(d)` also selected
splits into partials. Another computed key, such as `GROUP BY qty * 2`, needs a `CAST` to a
fixed type. See [Aggregation](capabilities.md#aggregation).

**Aggregate over a broadcast join with narrow columns.** A text column of the smaller table in the
aggregation, such as a name or description, would repeat on every joined row, so the join is not
broadcast. Group by a numeric key instead, or limit the text column with a literal filter.

| Prefer | Avoid |
|---|---|
| `SELECT c.country, SUM(o.amount) FROM orders o JOIN customers c ON ... WHERE c.country IN ('DE', 'FR') GROUP BY c.country` | `SELECT c.name, SUM(o.amount) FROM orders o JOIN customers c ON ... GROUP BY c.name` |

## `ORDER BY ... LIMIT`

`ORDER BY ... LIMIT n` runs as a per-shard top-N, so each shard sends only `n` rows, when the
query reads one table or a broadcast join, has no aggregation and no `OFFSET`, and every sort key
is a column in the select list. Any other shape returns every row to Exasol, which holds them all
before it sorts. A bare `LIMIT` without `ORDER BY` stops each shard early.

## Select list

Select only the columns you need. The scan reads only the projected columns from Parquet, so
`SELECT *` on a wide table reads, decodes, and transfers every column, and Exasol holds every
column of every returned row in memory.

## What always runs in Exasol

Regular expressions, session functions such as `SESSIONTIMEZONE`, geospatial functions, and the unadvertised
capabilities in the [Capability matrix](capability-matrix.md) run in Exasol over the rows the scan
returns. [Handled by Exasol](capabilities.md#handled-by-exasol) explains why each one does.
