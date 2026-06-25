# Benchmark

Manually-invoked live benchmark for the lakehouse-engine VS over TPC-H. Exercises
the full query path against a running system and **times it**. Separate from
`make test-e2e` (that stays the CI path).

```bash
make bench                  # build .so → run → write bench/reports/<ts>.txt
./bench/run.sh selftest     # offline self-check of the string logic (no DB)
```

## Modes

Config comes from a gitignored `bench/.env` (copy `bench/.env.example`).
`BENCH_TARGET` picks the mode:

- **`docker` (default)** — self-contained. Brings up the local stack (MinIO +
  Iceberg REST + Exasol via `docker-compose.yml`), loads TPC-H into the local
  catalog, runs the query set + pushdown checks. No AWS; `.env` optional.
- **`remote`** — runs against a real AWS Glue catalog + an external Exasol
  cluster, with a best-effort `PROFILE` dump. Requires `AWS_*` / `GLUE_*` /
  `EXASOL_*` / `BUCKETFS_WRITE_PASS` in `.env`. **You must pre-load TPC-H into
  the Glue namespace yourself** — remote mode does not load data.

## What it does

1. Builds the working-tree `.so` and uploads it + the SLC to BucketFS.
2. Creates the schema, scripts, catalog connection, and `TPCH` virtual schema.
3. **Wiring** (docker): per-table row counts (`REGION`=5, `NATION`=25, rest >0).
4. **Timed queries**: TPC-H-shaped JOIN / filter / GROUP-BY / pricing-summary
   SELECTs — wall-clock is the perf signal.
5. **Pushdown checks** (`EXPLAIN VIRTUAL`): asserts `shard_key` fan-out, `LIMIT`,
   `filter`, and projection actually reach the scan spec.

Each run writes a timestamped report to `bench/reports/` (gitignored).

## Parallelism (docker)

`BENCH_NR_OF_CORES` (default 4) overrides the auto-detected core count and drives
the DataFusion target-partitions / threads-per-UDF defaults; multi-file tables
(`TPCH_FILES`) + `BENCH_PARALLELISM_FACTOR` (default 8) drive the
`GROUP BY shard_key` fan-out. See `../CLAUDE.md` for the engine memory/fan-out model.

## Notes

- Table names assume a flat `tpch` namespace (`LINEITEM`, …); a nested namespace
  flattens to `NS__TABLE` — adjust query names if so.
- The TPC-H loader is a cargo test binary at
  `crates/lakehouse-engine/tests/tpch_loader.rs` (run automatically in docker mode).
- `run.sh` pins `SLC_VERSION` to match the `.so` ABI — don't bump it blindly.
