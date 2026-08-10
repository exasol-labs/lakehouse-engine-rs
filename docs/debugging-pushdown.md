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

### Declared row cap versus pushdown `limit` (measured)

`e2e_capture_pushdown` reads an optional `CAPTURE_RESULT_SET_MAX_ROWS` env var: unset
means the capture connection declares no cap (`resultSetMaxRows: 0`); set to `n` means it
calls `capped_result_sets(n)` before running the capture. `scripts/capture-pushdown-payload.sh`
needs no flag for this — it inherits whatever `CAPTURE_RESULT_SET_MAX_ROWS` is set in the
environment it runs in. Set it, then diff two captures of the same statement to reproduce
the comparison below for a new shape.

**A declared `resultSetMaxRows` cap DOES reach the adapter as a pushdown `limit` on a real
query execution, for every statement shape tested — but `EXPLAIN VIRTUAL` can never show
it.** `EXPLAIN VIRTUAL` and a real query execution are two different exchanges with the
adapter: `resultSetMaxRows` is an attribute of whichever statement is actually sent, and the
`EXPLAIN VIRTUAL` wrapper is a different statement from the one the cap is declared against —
so its echoed `pushdownRequest` structurally cannot carry a limit that only the real
statement's own request gained. This was confirmed by capturing the adapter's raw incoming
request directly — bypassing `EXPLAIN VIRTUAL` entirely — for all 7 statement shapes below,
against `docker.io/exasol/docker-db:2025.2.1` (WebSocket protocol v3):

| Shape | Declared cap reaches a REAL request as a `limit`? | Adapter behavior |
|---|---|---|
| bare projection | Yes | Applied safely: a per-shard limit plus an outer `LIMIT` wrapper |
| projection + filter | Yes | Applied safely: a per-shard limit plus an outer `LIMIT` wrapper |
| single-group aggregate | Yes | Correctly withheld from beneath the aggregate — outer `LIMIT` only, so the aggregate value itself stays correct |
| `GROUP BY` aggregate | Yes | Correctly withheld from beneath the aggregate — outer `LIMIT` only |
| `COUNT(DISTINCT)` | Yes | Correctly withheld from beneath the per-shard `DISTINCT` row-scan — outer `LIMIT` only |
| `ORDER BY … LIMIT` | Yes, on top of the statement's own SQL `LIMIT` | Applied safely alongside the existing top-N pushdown |
| broadcast-eligible inner equi-join | Yes | **Applied safely; the join stays broadcast.** A bare `LIMIT` becomes a per-shard post-join cap (`JoinSpec::post_join_limit`, `crates/lakehouse-engine/src/scan/spec.rs`) plus an outer `LIMIT` wrapper; a bare-column `ORDER BY` rides an outer wrapper over the broadcast fan-out. Only the surviving forcing conditions — aggregate, `GROUP BY`, group-by-aggregation, `HAVING` — fall back to the unaccelerated two-scan (`LHS_T0`/`LHS_T1`) plan |

An earlier version of this table, built by diffing `EXPLAIN VIRTUAL` output only, concluded
the opposite — that no shape converts a declared cap into a pushdown `limit`. That conclusion
was an artifact of the tool, not a fact about the adapter: `EXPLAIN VIRTUAL`'s echoed
`pushdownRequest` reflects the wrapper statement it runs as, never the statement whose cap is
under test, so it was structurally incapable of showing this regardless of which shape was
captured.

**If you are debugging a capped connection's join plan with
`scripts/capture-pushdown-payload.sh`, its `EXPLAIN VIRTUAL` output never carries the declared
cap, so it cannot show the per-shard post-join `LIMIT` a real capped query applies to a
broadcast join.** The broadcast-vs-two-scan plan shape no longer flips on the cap — a bare
`LIMIT` now stays broadcast — so the captured broadcast shape is accurate, but it omits the
` LIMIT n` and the per-shard cap. Confirm cap-sensitive join behavior only against a real
execution or a direct capture of the adapter's incoming request, never against
`EXPLAIN VIRTUAL` alone.

See `ExaConn::capped_result_sets`'s doc comment
(`crates/lakehouse-engine/tests/common/exasol_ws.rs`) for the calling convention this
table backs, and the `fix-e2e-harness-undeclared-limit` plan's `injection-surface.md` for
the exact statements, the real-execution capture method, the original (superseded)
`EXPLAIN VIRTUAL`-based matrix, and every control.
