[lakehouse-engine](../README.md) › [Docs](index.md) › Writing efficient queries

---

# Writing efficient queries

Every query on a lakehouse table returns correct results. How you write the query decides how
fast it runs and how much memory it uses.

When you query a lakehouse table, Exasol passes the parts of the query it can to the lakehouse
engine. This is called *pushdown*. The engine reads the table's files and runs those parts on all
nodes at once, close to the data. Exasol then runs the rest of the query on the rows the engine
returns.

A query runs well when:

- **The engine skips files.** A filter that reaches the engine lets it skip files, and parts of
  files, that can't contain matching rows.
- **The engine does the work.** Filters, aggregates such as `SUM`, one pair of tables per join,
  and `ORDER BY ... LIMIT` run in the engine, on all nodes at once.
- **Few rows go to Exasol.** Exasol keeps every row the engine returns in memory before it
  continues. See [Exasol keeps every engine result in memory](#exasol-keeps-every-engine-result-in-memory).

This page lists the rules that follow from this. Each rule shows a form to use and a form to
avoid. [Capabilities](capabilities.md) has the technical details. The rules describe the current
release: when the engine learns to run more, a rule gets relaxed and this page is updated. Last
verified against lakehouse-engine 0.61.0 on Exasol 2025.1.

## Quick reference

| Use | Avoid | Why |
|---|---|---|
| `d >= DATE '2024-01-01' AND d < DATE '2025-01-01'` | `YEAR(d) = 2024` | A function around a column can't skip files. |
| `d = DATE '2024-01-02'` | `d = '2024-01-02'` | Comparing a date with text runs in Exasol. |
| `WHERE p.p_brand = 'Brand#23'` on the small table of a join | `WHERE p.p_brand REGEXP_LIKE 'Brand#23'` | The engine joins one pair itself and prefers a small table it can filter. It can't run `REGEXP_LIKE`. |
| `SELECT p.p_partkey, ...` from the small table of a join | `SELECT p.p_comment, ...` | When Exasol works on the joined rows further, a text column of the small table rules the pair out, unless `col = 'x'` or `col IN ('x', 'y')` limits it. |
| `FROM a JOIN b ON ... JOIN c ON ...` | `FROM a, c, b` where `c` doesn't join `a` | Exasol then sends each table to the engine on its own. |
| Filters on the optional side of a `LEFT JOIN` in a `WITH` clause | The same filters in `ON` | A filter in `ON` on that side doesn't reach the engine. |
| `JOIN` on a unique key | `IN (SELECT ...)`, `EXISTS` | The engine can't run these. |
| Filters on the lakehouse table's columns | Filters only on the Exasol table it joins | Exasol keeps the whole lakehouse table in memory before the join. |
| `SUM`, `COUNT`, `MIN`, `MAX`, `AVG` | `MEDIAN`, `LISTAGG`, `APPROXIMATE_COUNT_DISTINCT` | The engine runs the first ones; the others send every row to Exasol. |
| Only the columns you need | `SELECT *` | The engine reads every selected column, and Exasol keeps it in memory. |

## Check a query yourself

Run `EXPLAIN VIRTUAL <query>`. It shows what Exasol sends to the engine, without running the
query. [Debugging pushdown](debugging-pushdown.md) explains the full output. For the rules on this
page, look at two things:

- **The number of rows.** Each row is one request to the engine. If a join of two tables returns
  two rows, Exasol sent each table on its own and joins them itself.
- **The `PUSHDOWN_SQL` column.** `"join":{` marks the pair of tables the engine joins itself.
  Every other `LAKEHOUSE_SCAN` call reads one table, and Exasol joins them. `FROM DUAL WHERE 1=0`
  means the filter ruled out every file.

To see how many rows Exasol kept in memory, use the profile, as described in the next section.

## Exasol keeps every engine result in memory

Exasol stores everything the engine returns in a temporary table, and only then joins, filters,
sorts, or limits it. So the memory a query uses depends on how many rows and columns the engine
returns, not on how many rows the query returns in the end.

Every query that runs at the same time keeps its own copy. When the copies don't fit in
Exasol's memory, Exasol moves them to disk and the queries get much slower. With many large
queries at once, sessions can fail with out-of-memory errors.

This means:

- **A filter the engine can't run doesn't help.** Exasol stores the whole table first, then
  filters it.
- **`LIMIT` helps only when the engine runs it** (see [`ORDER BY ... LIMIT`](#order-by--limit)).
  For example, when you join a lakehouse table with an Exasol table, Exasol stores every row of
  the lakehouse table before it applies `LIMIT 10`.
- **When you join a lakehouse table with an Exasol table, Exasol stores the whole lakehouse
  table.** Only filters on the lakehouse table's own columns make it smaller. A filter on the
  Exasol table doesn't, even on the join column. Write the filter on the lakehouse table.
- **A broadcast join stores only the joined rows of its pair.** Every other table of the join is
  stored in full, minus the rows its own filters remove.
- **An aggregation that runs in the engine stores only one row per group.**
- **Every selected column is stored**, so `SELECT *` uses more memory.

Here `orders` is a lakehouse table and `exasol_customers` is an Exasol table:

| Use | Avoid |
|---|---|
| `SELECT ... FROM orders o JOIN exasol_customers c ON o.customer_id = c.customer_id WHERE o.customer_id IN (1, 2, 3)` | `SELECT ... FROM orders o JOIN exasol_customers c ON o.customer_id = c.customer_id WHERE c.customer_id IN (1, 2, 3)` |

To see how many rows Exasol stored:

1. Turn on profiling: `ALTER SESSION SET PROFILE = 'ON';`
2. Run the query, then `FLUSH STATISTICS;`
3. Query `EXA_STATISTICS.EXA_USER_PROFILE_LAST_DAY` for your session. For each `INSERT` part
   `on TEMPORARY table`, `OUT_ROWS` is the number of rows stored.

## Filters

### Compare a column with a value

The engine can skip files only for filters of the form `column <operator> value`. The operators
are `=`, `<>`, `<`, `<=`, `>`, `>=`, `IN`, `BETWEEN`, `IS [NOT] NULL`, and `LIKE` with a fixed
start such as `'AB%'`, combined with `AND`, `OR`, and `NOT`. With a function around the column,
such as `YEAR(d) = 2024` or `UPPER(code) = 'AB'`, the engine still filters the rows, but it has to
read every file. Write a range instead. See [File pruning](capabilities.md#file-pruning).

### Write values in the column's type

On a `DATE` column, `d = '2024-01-02'` converts the text with the session's date format, so the
filter runs in Exasol and skips no files. Write `DATE '2024-01-02'`, `TIMESTAMP '...'`, or
`TO_DATE(s, 'YYYY-MM-DD')` with a format. See
[Text and session settings](capabilities.md#text-and-session-settings).

On an integer column, a quoted whole number such as `part_nr = '47'` skips files as `part_nr = 47`
does. On a column with decimal places, write the number without quotes.

### Calculated values and the current date work

Exasol calculates constant expressions before it sends the query, so
`DATE '1998-12-01' - INTERVAL '90' DAY` and `ADD_MONTHS(DATE '2024-01-31', 1)` reach the engine
as plain dates. The engine replaces `CURRENT_DATE`, `CURRENT_TIMESTAMP`, `LOCALTIMESTAMP`,
`SYSDATE`, and `SYSTIMESTAMP` with their value, once per query. So
`order_date > CURRENT_DATE - 30` and `created_at > ADD_HOURS(CURRENT_TIMESTAMP, -1)` skip files
just like a filter on a fixed value.

### Set `SESSION_TIME_ZONE` if your sessions use a different time zone

The engine can't see the session's time zone. It calculates `CURRENT_DATE`, `CURRENT_TIMESTAMP`,
and `LOCALTIMESTAMP` in the time zone of the `SESSION_TIME_ZONE` property ([Tuning](tuning.md)),
or in `DBTIMEZONE` if the property isn't set. Exasol calculates an expression such as
`CURRENT_DATE - 30` in the session's time zone. Without the property, a session in another time
zone can get a different date than native Exasol around midnight, and the two forms can disagree
within one query. See
[Semantic differences](semantic-differences.md#clock-functions-and-timestamp-with-local-time-zone-literals).

### Keep filters the engine can't run out of `OR`

Exasol runs the filters the engine doesn't support, such as `REGEXP_LIKE`. When they are combined
with `AND`, the other filters still reach the engine. When they are combined with `OR`, the whole
`OR` runs in Exasol.

| Use | Avoid |
|---|---|
| `WHERE status = 'F' AND name REGEXP_LIKE 'a.*'` | `WHERE status = 'F' OR name REGEXP_LIKE 'a.*'` |

## Joins

The engine runs one kind of join itself, the *broadcast join*. It copies the smaller of two
tables to every node, joins it there with the larger one, and skips parts of the larger table that
can't match. In a join of three or more tables, one pair is a broadcast join and the engine reads
every other table on its own. A pair of tables is a broadcast join when:

- They are inner-joined (`JOIN`). A table joined with itself counts too.
- They join on columns, or on calculations the engine can run: `a.k = b.k`,
  `a.k1 = b.k1 AND a.k2 = b.k2`, `a.k = b.k + 1`.
- The smaller table's files are at most `JOIN_BROADCAST_MAX_BYTES` ([Tuning](tuning.md)),
  128 MiB by default.
- The smaller table passes the column rule below, when that rule applies.

The column rule applies when Exasol works on the joined rows further: the query has an aggregate,
`GROUP BY`, `HAVING`, or a third table, or a filter the engine can't run (see [Filters](#filters)).
Then every column Exasol reads from the smaller table must be one of these:

- a number, date, timestamp, or boolean
- text that a filter limits to fixed values: `col = 'x'` or `col IN ('x', 'y')`

A filter in `ON` works like one in `WHERE`: it skips files, also when it compares the two tables
(`a.x > b.y`). A filter the engine can't run, such as `d LIKE '2024%'` on a date, runs in Exasol
after the join, and the join stays a broadcast join.

Other inner joins go to the engine as one request, but the engine reads each table on its own and
Exasol joins them. Outer joins, `IN`, and `EXISTS` go to the engine one table at a time.

### Join on plain columns

A join on one plain column lets the engine skip the most of the larger table. With two or more
join columns, or a calculation on the larger table's join column, it skips less, but the join is
still a broadcast join. Put a calculation on the smaller table's side.

| Use | Avoid |
|---|---|
| `ON o.customer_id = c.customer_id + 1` | `ON o.customer_id - 1 = c.customer_id` |

### In a join of three or more tables, filter a small table in the engine

The engine joins one pair of tables itself. It prefers a pair whose smaller table has a filter of
its own that the engine runs, because the join then removes rows of the larger table before they
reach Exasol. Two things make the engine pick another pair:

- A filter the engine can't run, such as `REGEXP_LIKE`, doesn't count as the small table's own
  filter.
- A text column that Exasol reads from the small table rules the pair out, unless a filter limits
  it to fixed values (`col = 'x'`, `col IN ('x', 'y')`). Exasol reads a column for the select list,
  for a join with another table, and for a filter it runs itself.

Use:

```sql
SELECT p.p_partkey, SUM(l.l_quantity)
FROM lineitem l
JOIN part p ON p.p_partkey = l.l_partkey
JOIN supplier s ON s.s_suppkey = l.l_suppkey
WHERE p.p_brand = 'Brand#23'
GROUP BY p.p_partkey
```

Avoid these variants of it:

- `WHERE p.p_brand REGEXP_LIKE 'Brand#23'`: the filter doesn't count, and Exasol reads the text
  column `p_brand` to run it.
- `p.p_comment` instead of `p.p_partkey`: Exasol reads the text column `p_comment`.

The query sends Exasol only the `lineitem` rows of brand 23. The variants send every `lineitem`
row.

### List comma-separated tables in join order

With `FROM a, b, c`, Exasol sends the join as one request only if every table joins a table
listed before it. Otherwise it sends each table on its own, and the engine is called once per
table. Use `JOIN ... ON`, or change the order:

| Use | Avoid |
|---|---|
| `FROM lineitem l, part p, supplier s WHERE p.partkey = l.partkey AND s.suppkey = l.suppkey` | `FROM part p, supplier s, lineitem l WHERE ...` (`supplier` doesn't join `part`) |
| `FROM part p JOIN lineitem l ON p.partkey = l.partkey JOIN supplier s ON s.suppkey = l.suppkey` | |

Only a join sent as one request can run a pair of its tables in the engine.

### Outer joins: filter the optional side in a `WITH` clause

For `LEFT`, `RIGHT`, and `FULL` joins, Exasol sends each table on its own and joins them itself.
In `customers c LEFT JOIN orders o`, `customers` keeps all its rows and `orders` is the optional
side. A `WHERE` filter on `customers` still reaches the engine. A filter on `orders` in `ON`
doesn't, and neither does the same filter in a subquery, because Exasol merges the subquery into
the join. A `WITH` clause keeps the filter:

| Use | Avoid |
|---|---|
| `WITH o AS (SELECT customer_id, order_id FROM orders WHERE status = 'F') SELECT ... FROM customers c LEFT JOIN o ON c.customer_id = o.customer_id` | `SELECT ... FROM customers c LEFT JOIN orders o ON c.customer_id = o.customer_id AND o.status = 'F'` |
| | `SELECT ... FROM customers c LEFT JOIN (SELECT ... FROM orders WHERE status = 'F') o ON ...` |

A `WHERE` filter on the optional side turns the outer join into an inner join, which Exasol then
sends as one join. If you want an inner join, write `JOIN`.

### Use a join instead of `IN` and `EXISTS`

For `IN (SELECT ...)`, `EXISTS`, `NOT IN`, and `NOT EXISTS`, Exasol sends each table on its own,
with its own filters, and checks the condition itself. The same holds for subqueries that refer
to the outer query, such as `x < (SELECT AVG(x) FROM t WHERE t.k = o.k)`. When the subquery
selects a unique key, such as the key of a dimension table, a join returns the same rows and can
be a broadcast join:

| Use | Avoid |
|---|---|
| `SELECT o.order_id FROM orders o JOIN customers c ON o.customer_id = c.customer_id WHERE c.country = 'DE'` | `SELECT order_id FROM orders WHERE customer_id IN (SELECT customer_id FROM customers WHERE country = 'DE')` |

Keep the subquery if the key isn't unique, because the join would return some rows more than
once.

## Aggregation

### Use aggregates the engine can split

The engine runs `COUNT`, `SUM`, `MIN`, `MAX`, and `AVG` on every node, and Exasol combines one
row per group from each node. `MEDIAN`, `LISTAGG`, `GROUP_CONCAT`, `APPROXIMATE_COUNT_DISTINCT`,
`SUM(DISTINCT ...)`, and `AVG(DISTINCT ...)` send every row to Exasol. `COUNT(DISTINCT col)` runs
in the engine only when it is the only aggregate and the query has no `GROUP BY`. `STDDEV`,
`VARIANCE`, and their variants run in the engine on a plain column.
An aggregate that appears only in `ORDER BY`, as in
`SELECT k FROM t GROUP BY k ORDER BY SUM(v) DESC LIMIT 10`, splits the same way.

### Select a calculated group key, or cast it

`GROUP BY YEAR(d)` runs in the engine when `YEAR(d)` is also in the select list. Other calculated
keys, such as `GROUP BY qty * 2`, need a `CAST` to a fixed type. See
[Aggregation](capabilities.md#aggregation).

### Aggregate over a join with short columns

If the aggregation uses a text column of the smaller table, such as a name or a description, the
join is not a broadcast join, because that text would be copied to every joined row. Group by a
number column instead, or limit the text column to fixed values with a filter.

| Use | Avoid |
|---|---|
| `SELECT c.country, SUM(o.amount) FROM orders o JOIN customers c ON ... WHERE c.country IN ('DE', 'FR') GROUP BY c.country` | `SELECT c.name, SUM(o.amount) FROM orders o JOIN customers c ON ... GROUP BY c.name` |

## `ORDER BY ... LIMIT`

The engine runs `ORDER BY ... LIMIT n` on every node, and each node returns only `n` rows, when:

- the query reads one table or is a broadcast join,
- it has no aggregation and no `OFFSET`,
- every column in `ORDER BY` is also in the select list.

Otherwise the engine returns every row, and Exasol stores them all before it sorts. A `LIMIT`
without `ORDER BY` makes every node stop reading early.

## Select list

Select only the columns you need. The engine reads only the selected columns from the files. With
`SELECT *` on a wide table, it reads and sends every column, and Exasol stores all of them in
memory.

## What always runs in Exasol

Regular expressions, session functions such as `SESSIONTIMEZONE`, geospatial functions, and
everything the [Capability matrix](capability-matrix.md) lists as not advertised run in Exasol,
on the rows the engine returns. [Handled by Exasol](capabilities.md#handled-by-exasol) explains
why.
