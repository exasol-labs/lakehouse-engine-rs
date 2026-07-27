[lakehouse-engine](../README.md) › [Docs](index.md) › Debugging pushdown

---

# Debugging pushdown

When a query doesn't seem to push down the way [Capabilities](capabilities.md) says it should, `EXPLAIN VIRTUAL` is the tool: it shows exactly what the adapter generated for that statement, without running it.

## Inspect what the adapter pushes down

Run `EXPLAIN VIRTUAL` against your own Virtual Schema and query:

```sql
EXPLAIN VIRTUAL
SELECT id, name, score FROM MY_LAKEHOUSE.EVENTS WHERE score > 15.0 LIMIT 5;
```

The output includes the scan-spec JSON the adapter passed to the scan UDF — the literal projection, filter, and limit it decided to push down. Compare that against what you expected from [Capabilities](capabilities.md):

- A predicate missing from the scan spec means it wasn't translated, so Exasol is filtering it after the scan instead of before. Check it against the [filter capability list](capabilities.md#filtering-) — an untranslatable expression shape (e.g. a function not listed there) is the usual cause.
- A wider projection than expected means a column made it into the scan spec that your `SELECT` didn't ask for — check for a `WHERE`/`ORDER BY`/`GROUP BY` reference that implicitly requires it.
- No `LIMIT` in the scan spec despite one in the query means the shape wasn't eligible for the bounded top-N pushdown (see [Capabilities: Ordered top-N](capabilities.md#filtering-)) — it still returns correct results, just via a full scan.

Then run the query for real and compare the actual result against what the scan spec implies it should return.

## Try it against the bundled local stack first

If you don't yet have your own Virtual Schema deployed, `scripts/capture-pushdown-payload.sh` runs a query against the bundled local Docker stack (Exasol + MinIO + Iceberg REST) and prints both the `EXPLAIN VIRTUAL` output and the real result in one step, against a small seeded table:

```bash
scripts/capture-pushdown-payload.sh 'SELECT COUNT(*) FROM {table} WHERE c_date LIKE '"'"'2024%'"'"''
```

`{table}` is substituted with the seeded probe table's name. The script builds the UDF `.so` and brings up the local stack if it isn't already running, then leaves it running afterward so follow-up queries are cheap — tear it down yourself when done:

```bash
docker compose down -v
```

### The seeded table

12 rows, one column per Arrow/Exasol type pairing, useful for probing type-specific pushdown behavior (e.g. decimal precision, date literals):

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
