# Debugging: capturing what the adapter pushes down for a SQL statement

`scripts/capture-pushdown-payload.sh` runs a caller-supplied SQL statement
against the local Exasol + MinIO + Iceberg REST Docker stack and prints:

1. `EXPLAIN VIRTUAL` output — the SQL the adapter generates, including the
   literal scan-spec JSON (filter/projection/limit) passed to the scan UDF.
2. The real execution result — actual rows, or the actual runtime error text.

This replaces re-deriving throwaway instrumentation each time a pushdown bug
needs a ground-truth payload (see commit `c827d1a` for the last one-off spike
this makes unnecessary).

## Usage

```bash
scripts/capture-pushdown-payload.sh 'SELECT COUNT(*) FROM {table} WHERE c_date LIKE '"'"'2024%'"'"''
```

`{table}` is substituted with the seeded `typed_distinct_probe` Virtual Schema
table name. The script builds the UDF `.so`, brings up `minio`/`iceberg-rest`/
`exasol` (skipping the positional-delete `spark-iceberg-fixtures` job, not
needed by this fixture), then runs the capture test.

It leaves the stack running afterward so follow-up queries are cheap — tear it
down yourself when done:

```bash
docker compose down -v
```

## The seeded table (`typed_distinct_probe`, 12 rows)

See `crates/lakehouse-engine/tests/common/seed.rs` (`seed_typed_distinct_probe`
and the `TYPED_COL_*` constants) for the exact values. Columns:

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

## Adding a new repro table

If a bug needs a shape `typed_distinct_probe` doesn't cover, add a new seed
function to `common/seed.rs` and point `e2e_capture_pushdown.rs` at it — don't
hardcode a one-off query set into the test; keep it driven by `CAPTURE_SQL` so
later issues on this stack can reuse the same tool.
