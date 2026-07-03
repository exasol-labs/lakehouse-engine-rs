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

To stand up a remote AWS cluster + catalog (and to enable co-workers), see
[`../deploy/README.md`](../deploy/README.md); `deploy/scripts/secrets.sh <env>`
generates the `bench/.env` for a deployed cluster.

## What it does

1. Builds the working-tree `.so` and uploads it + the SLC to BucketFS.
2. Creates the schema, scripts, catalog connection, and `TPCH` virtual schema.
3. **Wiring** (docker): per-table row counts (`REGION`=5, `NATION`=25, rest >0).
4. **Timed queries** (Q1-Q9b): TPC-H-shaped JOIN / filter / GROUP-BY / pricing-summary
   SELECTs, plus Q5-Q9b (added to probe specific pushdown strengths/weaknesses: no-filter
   JOIN+GROUP-BY, a ~45M-group high-cardinality GROUP BY, a highly selective single-day
   filter, and narrow-vs-wide column projection) — wall-clock is the perf signal.
5. **Pushdown checks** (`EXPLAIN VIRTUAL`): asserts `shard_key` fan-out, `LIMIT`,
   `filter`, and projection actually reach the scan spec.

Each run writes a timestamped report to `bench/reports/` (gitignored).

## Parallelism (docker)

`BENCH_NR_OF_CORES` (default 4) overrides the auto-detected core count and drives
the DataFusion target-partitions / threads-per-UDF defaults; multi-file tables
(`TPCH_FILES`) + `BENCH_PARALLELISM_FACTOR` (default 8) drive the
`GROUP BY shard_key` fan-out. See `../CLAUDE.md` for the engine memory/fan-out model.

## Companion scripts

- **`import_ceiling.sh`** (remote only) — the VS path vs Exasol's **native
  `IMPORT FROM PARQUET`** reader over the *same* lineitem files, as a goal
  ceiling. Two comparisons: **scan-only** (`COUNT(*)` over both — full read, ~no
  output, so the delta is UDF-layer overhead) and **data-intensive**
  (full-materialization into a real table: native `IMPORT INTO` vs the VS
  `CREATE TABLE AS SELECT *` emit path, 3× each, both landing identical rows).
  Reads the lineitem file list from the newest `reports/bench-report-*.txt`, so
  run `make bench` first. Writes to `bench/reports/` (or a path you pass as `$1`).
  ```bash
  make bench                                    # produces the report it harvests
  ./bench/import_ceiling.sh bench/reports/import-ceiling-$(date +%Y%m%d-%H%M%S).txt
  ```
- **`sweep.sh`** — sweeps the DataFusion threading knobs (`BENCH_DF_*`) across
  `run.sh` invocations to find the best parallelism config.

## Competitive engine comparison (Athena / Trino / Spark)

Beyond the native-`IMPORT` ceiling above, the same TPC-H tables/queries can be run through the
lakehouse engines people put next to a lakehouse: AWS Athena, Trino, and Spark. All three read the
SAME Glue Iceberg catalog + S3 data as `remote` mode above. Manually invoked, not CI — same
convention as the rest of `bench/`.

- **`athena_compare.sh`** — no new infra (the Athena workgroup already exists in
  `deploy/data-stack`). `ATHENA_WORKGROUP=$(cd deploy/data-stack && tofu output -raw
  athena_workgroup) ./athena_compare.sh`.
- **`trino_compare.sh`** — requires an ephemeral Trino cluster stood up first (coordinator +
  workers, sized to match Exasol test1 by default — `r8i.2xlarge` × 2):
  `deploy/scripts/trino-up.sh <env>` → `export TRINO_HOST=<printed coordinator ip>` →
  `./trino_compare.sh`. **Tear it down immediately after**: `deploy/scripts/trino-down.sh <env>`
  (it costs meaningfully more while running than a single small box would). See the "Trino
  (ephemeral, opt-in)" section in [`../deploy/README.md`](../deploy/README.md).
- **`spark_compare.sh`** — requires `deploy/data-stack` applied with `-var
  enable_emr_serverless=true` first (off by default). Export `EMR_SERVERLESS_APP_ID` /
  `EMR_SERVERLESS_ROLE_ARN` / `SPARK_SCRIPT_S3_URI` / `SPARK_LOG_S3_URI` from `tofu output`, plus
  `GLUE_CATALOG_URI` / `GLUE_WAREHOUSE` / `AWS_REGION` (already in `bench/.env` for `remote` mode).
  EMR Serverless is billed only while a job runs — see the "Spark / EMR Serverless" section in
  [`../deploy/README.md`](../deploy/README.md).
- **`compare_all.sh`** — runs `make bench` + `import_ceiling.sh` + `athena_compare.sh`, then
  `trino_compare.sh` / `spark_compare.sh` only if their env vars are set (clean `SKIP` otherwise —
  it never auto-provisions). Writes one aggregated `bench/reports/compare-<ts>.txt`.

**The `TIMING` line convention**: every compare script appends lines of the exact form
`TIMING <engine> <query-name> <seconds>` to its own report. `compare_all.sh` does nothing
engine-specific beyond `grep`-ing `^TIMING ` across the reports it produced into one aligned
table — no CSV/JSON, no dashboard, hand-curate the interesting numbers into
[`../docs/performance.md`](../docs/performance.md) afterward, same as every other bench result.

Query text (Presto/Trino/Spark dialect, identical across all three) is duplicated inline in each
script, translated from `run.sh`'s Q1-Q9b — keep all three in sync if you edit one.

## Synthetic micro-benchmarks (no cluster, no DB)

Two host-runnable micro-benchmarks isolate the two halves of the per-instance
scan path so end-to-end throughput can be attributed (plan tasks 5.1 / 5.2).
They live in `crates/lakehouse-engine/tests/micro_bench.rs` (an `#[ignore]`-gated
test target — no new dependency, no `criterion`) and need neither MinIO nor a
cluster:

- **5.1 emit-only** — the pre-SDK emit work on every batch: `coerce_batch_to_exa_types`
  (the real coercion the emit loop runs) + Arrow IPC `StreamWriter` serialization,
  which is exactly what `ctx.emit_batch` does internally before bytes cross the
  `.so`. It does NOT include the ZMQ `MT_EMIT` round-trip (only measurable on the
  cluster, tasks 6/7). Schemas: BIGINT / DOUBLE / TIMESTAMP / DECIMAL / VARCHAR and
  a TPC-H `lineitem`-shaped mixed row.
- **5.2 scan-only** — Parquet read+decode → DataFusion stream, drained WITHOUT
  emitting, over a self-contained local Parquet file. Reuses the production
  `session_config_for_spec` + `build_raw_scan_physical_plan` seams.

```bash
# full numbers (prints rows/sec, GB/sec, RSS delta per schema):
cargo test -p lakehouse-engine --test micro_bench -- --ignored --nocapture
# release-opt numbers — write to an out-of-tree target dir so the Docker-owned
# target/release tree (and its root-owned .cargo-lock) is never touched:
CARGO_TARGET_DIR=/tmp/lh-bench cargo test -p lakehouse-engine --test micro_bench \
  --release -- --ignored --nocapture
# CI smoke (non-ignored): asserts each path yields a positive GB/sec
cargo test -p lakehouse-engine --test micro_bench
```

## Notes

- Table names assume a flat `tpch` namespace (`LINEITEM`, …); a nested namespace
  flattens to `NS__TABLE` — adjust query names if so.
- The TPC-H loader is a cargo test binary at
  `crates/lakehouse-engine/tests/tpch_loader.rs` (run automatically in docker mode).
- `run.sh` pins `SLC_VERSION` to match the `.so` ABI — don't bump it blindly.

## Remote pre-staged artifacts (when BucketFS write is blocked)

If you can't let the bench PUT to BucketFS (e.g. write is proxied/blocked and you
upload via AdminUI), set in `bench/.env`:

- `BENCH_SKIP_UPLOAD=1` — skip the SLC + `.so` PUTs; the bench still registers the
  RUST alias and builds the VS.
- `BENCH_SO_UDF_OBJECT=buckets/bfsdefault/default/<name>.so` — where the `.so` sits.
- `BENCH_SLC_BUCKET_PATH=bfsdefault/default/<slc-dir>` — the **extracted** SLC dir
  (a `foo.tar.gz` upload extracts to `foo`).

**Always upload a rebuilt `.so` under a NEW filename** (e.g. `…_v2.so`, `…_v3.so`)
and repoint `BENCH_SO_UDF_OBJECT`. Overwriting the same BucketFS path can leave the
UDF node serving a stale cache (`cannot open shared object file`) for many minutes;
a fresh path forces a clean fetch.

## Remote Glue (AWS) gotchas

- `GLUE_WAREHOUSE` must be the REST prefix Glue's `/v1/config` reports —
  `catalogs/<account-id>`, NOT an `s3://` path and NOT the bare account id.
- The S3 data endpoint defaults to `https://s3.$AWS_REGION.amazonaws.com`; the scan
  derives the virtual-hosted bucket URL from the region (no explicit endpoint).
