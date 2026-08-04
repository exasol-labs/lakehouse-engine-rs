[lakehouse-engine](../README.md) › [Docs](index.md) › Debugging pushdown

---

# Debugging pushdown

If a query does not push down as [Capabilities](capabilities.md) describes, use `EXPLAIN VIRTUAL`. It shows what the adapter generated for that statement, and it does not run the statement.

## Inspect what the adapter pushes down

Run `EXPLAIN VIRTUAL` against your own Virtual Schema and query:

```sql
EXPLAIN VIRTUAL
SELECT id, name, score FROM MY_LAKEHOUSE.EVENTS WHERE score > 15.0 LIMIT 5;
```

The output includes the scan-spec JSON that the adapter passed to the scan UDF. This JSON gives the literal projection, filter, and limit that the adapter pushed down. Compare the scan spec with the behavior that [Capabilities](capabilities.md) describes:

- A predicate that is absent from the scan spec was not translated. Exasol then filters that predicate after the scan instead of before the scan. Compare the predicate with the [filter capability list](capabilities.md#filtering). The usual cause is an expression shape that the adapter cannot translate, for example a function that the list does not name.
- A projection that is wider than expected contains a column that your `SELECT` does not request. Look for a `WHERE`, `ORDER BY`, or `GROUP BY` reference that requires this column.
- A query that has a `LIMIT`, but whose scan spec has none, is not eligible for the bounded top-N pushdown. See [Capabilities: Ordered top-N](capabilities.md#filtering). The query still returns correct results, through a full scan.

Then run the query. Compare the actual result with the result that the scan spec implies.

## Try it against the bundled local stack first

If you have no Virtual Schema deployed yet, use `scripts/capture-pushdown-payload.sh`. The script runs a query against the bundled local Docker stack (Exasol + MinIO + Iceberg REST) and a small seeded table. It prints the `EXPLAIN VIRTUAL` output and the real result in one step.

```bash
scripts/capture-pushdown-payload.sh 'SELECT COUNT(*) FROM {table} WHERE c_date LIKE '"'"'2024%'"'"''
```

The script replaces `{table}` with the name of the seeded probe table. It builds the UDF `.so`. If the local stack is not running, the script starts it. The script leaves the stack running afterwards, so follow-up queries are cheap. Remove the stack yourself when you are done:

```bash
docker compose down -v
```

### The seeded table

The table has 12 rows and one column for each Arrow/Exasol type pairing. Use it to probe type-specific pushdown behavior, for example decimal precision and date literals:

| Column | Arrow type | Exasol type |
|---|---|---|
| `id` | Int64 | DECIMAL(20,0) |
| `c_decimal_a` | Decimal128(9,2) | DECIMAL(9,2) |
| `c_decimal_b` | Decimal128(20,4) | DECIMAL(20,4) |
| `c_double` | Float64 | DOUBLE PRECISION |
| `c_varchar` | Utf8 | VARCHAR |
| `c_date` | Date32 | DATE |
| `c_ts` | Timestamp(us) | TIMESTAMP |
| `c_bool` | Boolean | BOOLEAN |
| `c_price` | Float64 | DOUBLE PRECISION |
| `c_qty` | Int64 | DECIMAL(20,0) |
