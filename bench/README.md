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

## Delete-bearing benchmark (`BENCH_WITH_DELETES`)

`BENCH_WITH_DELETES=1` runs the SAME query set (Q1-Q9b/NQ1-NQ5) against Iceberg v2
merge-on-read copies of the TPC-H tables with ~5% of rows position-deleted per table
(deterministic — same deleted set every run), instead of the pristine baseline. This
is a scale measurement of the engine's merge-on-read **read cost**: reconstructing
the live row set by applying Parquet position-delete files during a scan.

- **Default (`BENCH_WITH_DELETES=0`) is byte-for-byte identical to the benchmark's
  existing behavior.** Everything below is inert unless the flag is set.
- **Docker mode**: fully automatic. `make bench` with the flag set authors the
  delete-bearing namespace via `make_deletes_docker.sh` (idempotent — skips if
  already populated) before running the suite. No operator action beyond setting
  the flag.
- **Remote mode**: requires a ONE-TIME prerequisite — run
  `deploy/scripts/make-deletes-remote.sh` (env-var configured — see its header) once per environment before the
  first delete-bench (it submits `deploy/scripts/make_deletes_remote.py` as an EMR
  Serverless job). If skipped, `run.sh` hard-errors pointing at that script.
- `BENCH_DELETE_NAMESPACE` overrides which namespace is used (default
  `${NAMESPACE}_deletes` docker / `tpch_deletes` remote); see
  `bench/.env.example` for the exact knobs.
- A flag-gated sanity check confirms deletes are actually applied on read (not
  ignored, not over-applied): `OK  delete-count LINEITEM: <n> (~95% of baseline
  <m>)`, comparing the delete namespace's LINEITEM count against a second,
  lightweight VS built over the untouched baseline namespace.
- Deletes are authored by Apache Spark, not PyIceberg/iceberg-rust — see
  `scripts/spark-fixtures/create_tpch_deletes.sql`'s header comment for why.
- **The flag also gates the 4 competitor scripts** (`athena_compare.sh`, `trino_compare.sh`,
  `import_jdbc_trino.sh`, `spark_compare.sh` — see "Competitive engine comparison" below): each
  resolves its own override var the same way as `BENCH_DELETE_NAMESPACE` (explicit override always
  wins; otherwise `tpch` / `tpch_deletes` when the flag is on) — `ATHENA_DATABASE` (Athena),
  `TRINO_SCHEMA` (both Trino scripts, shared), `SPARK_NAMESPACE` (Spark, forwarded as
  `spark_queries.py`'s 2nd argument). All 4 only ever run in remote mode, so there is no docker
  variant for them, and they read the SAME `tpch_deletes` Glue database this section's remote
  authoring step already produces — no separate per-engine authoring step needed.

## What it does

1. Builds the working-tree `.so` and uploads it + the SLC to BucketFS.
2. Creates the schema, scripts, catalog connection, and `TPCH` virtual schema.
3. **Wiring** (docker): per-table row counts (`REGION`=5, `NATION`=25, rest >0).
4. **Timed queries** (Q1-Q9b, NQ1-NQ5): TPC-H-shaped JOIN / filter / GROUP-BY / pricing-summary
   SELECTs, plus Q5-Q9b (added to probe specific pushdown strengths/weaknesses: no-filter
   JOIN+GROUP-BY, a ~45M-group high-cardinality GROUP BY, a highly selective single-day
   filter, and narrow-vs-wide column projection) and NQ1-NQ5 (arithmetic aggregate pushdown,
   LIKE/IN filters, a 4-way join, ORDER BY+LIMIT, and GROUP BY+HAVING) — wall-clock is the
   perf signal.
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
- **`import_jdbc_trino.sh`** (remote only, requires `TRINO_HOST`) — the VS path vs Exasol's
  **native `IMPORT FROM JDBC`** reader, pushing Q1-Q9b/NQ1-NQ5 down as sub-selects over a JDBC
  connection to the same ephemeral Trino cluster used by `trino_compare.sh` below. Auto-registers
  the Trino JDBC driver into BucketFS (downloads the matching `trino-jdbc` jar, uploads it + a
  `settings.cfg` via `exapump bucketfs cp`) before running. `SKIP`s cleanly if `TRINO_HOST` is
  unset. Live-verified gotchas on `test1` (2025.2.1): the driver `settings.cfg` MUST include
  `FETCHSIZE`/`INSERTSIZE` or Exasol silently drops the whole registration
  (`ETL-1013: Driver=... is unknown`, with no file/permission error); `NOSECURITY=YES` avoids a
  sandboxed-JVM permission denial the driver hits reaching out over the network; the JDBC user's
  password must be empty (Trino's client refuses a non-empty password without TLS); a `BIGINT`-
  sourced `SUM()` (e.g. `SUM(l_orderkey)`, Q9b) must land in a `DECIMAL` column, not `DOUBLE
  PRECISION` (`ETL-1299`/`ETL-1202` — no BIGINT-to-DOUBLE transformator). The JDBC connection here
  originates from the Exasol cluster itself (Exasol dispatches the JDBC call, not your machine), so
  **`trino-up.sh` must allow the Exasol node IPs too**, not just yours:
  `-var 'allowed_cidrs=["<your-ip>/32","<exasol-node-ip>/32",...]'`.
  ```bash
  deploy/scripts/trino-up.sh myenv && export TRINO_HOST=<printed coordinator ip>
  ./bench/import_jdbc_trino.sh
  deploy/scripts/trino-down.sh myenv
  ```
- **`sweep.sh`** — sweeps the DataFusion threading knobs (`BENCH_DF_*`) across
  `run.sh` invocations to find the best parallelism config.
- **`parallelism_sweep.sh`** — sweeps `BENCH_PARALLELISM_FACTOR` (8/16/24) across
  `run.sh` invocations, capturing Q2/Q3/Q5 (raw-emit-heavy joins) and Q9b (non-join
  regression check). Tests whether oversubscribing shards beyond 1/core hides
  the synchronous per-shard `MT_EMIT` ack latency — see
  `specs/_plans/add-arithmetic-aggregate-pushdown-and-benchmark-suite/decision-log.md`.

## Competitive engine comparison (Athena / Trino / Spark)

Beyond the native-`IMPORT` ceiling above, the same TPC-H tables/queries can be run through the
lakehouse engines people put next to a lakehouse: AWS Athena, Trino, and Spark. All three read the
SAME Glue Iceberg catalog + S3 data as `remote` mode above. Manually invoked, not CI — same
convention as the rest of `bench/`.

All four scripts below (`athena_compare.sh`, `trino_compare.sh`, `import_jdbc_trino.sh`,
`spark_compare.sh`) support `BENCH_WITH_DELETES=1` (see the "Delete-bearing benchmark" section
above for the override vars and namespace resolution). When on, each appends `-deletes` to its
`TIMING <engine> <query> <seconds>` engine label (e.g. `athena` → `athena-deletes`) so a
with-deletes run's results can be told apart from a without-deletes run in your own report —
`compare_all.sh`'s aggregation is line-based (`grep '^TIMING '`) and unaffected by the extra
suffix. `compare_all.sh` itself stays single-variant per invocation: run
it once with `BENCH_WITH_DELETES=0` and once with `=1` to get both tables — it does not
auto-provision both, consistent with never auto-double-provisioning Trino/EMR.

- **`athena_compare.sh`** — no new infra (the Athena workgroup already exists in
  `deploy/data-stack`). `ATHENA_WORKGROUP=$(cd deploy/data-stack && tofu output -raw
  athena_workgroup) ./athena_compare.sh`.
- **`trino_compare.sh`** — requires an ephemeral Trino cluster stood up first (coordinator +
  workers, sized to match Exasol test1 by default — `r8i.2xlarge` × 2):
  `deploy/scripts/trino-up.sh <env>` → `export TRINO_HOST=<printed coordinator ip>
  TRINO_WORKER_HOST=<a worker ip, e.g. tofu output -json trino_worker_hosts>` → `./trino_compare.sh`.
  **Tear it down immediately after**: `deploy/scripts/trino-down.sh <env>` (it costs meaningfully
  more while running than a single small box would). See the "Trino (ephemeral, opt-in)" section
  in [`../deploy/README.md`](../deploy/README.md).
  Methodology: ONE persistent Trino CLI session for the whole 15-query batch (a single `docker run
  --execute "<all queries>; --ignore-errors`), launched via SSH onto a **worker** node — not your
  machine, not the coordinator — so this script pays the same client-overhead profile (no per-query
  container/JVM cold start) and network-hop shape (your machine → the cluster's own node, over the
  internet; that node → the thing being measured, intra-VPC) as `bench/run.sh` (VS) and
  `import_jdbc_trino.sh`, both of which measure via a single `exapump` process (no JVM) talking to
  Exasol. An earlier version spun up a fresh Docker container + JVM **per query** from the operator's
  machine, reaching Trino over the public internet — that made native Trino look slower than it is,
  which is why IMPORT FROM JDBC appeared to beat it on every query, purely from measurement bias.
  Requires the Trino EC2 key pair's private key locally to SSH into the worker (`KEY_FILE`,
  default `~/.ssh/spot-strata-rsa` — apply the stack with a `key_pair_name` you actually hold);
  resolves the coordinator's private ip itself via the AWS CLI (connecting from the worker to the
  coordinator's *public* ip does not reliably pass the security group's internode rule).
- **`spark_compare.sh`** — requires `deploy/data-stack` applied with `-var
  enable_emr_serverless=true` first (off by default). Export `EMR_SERVERLESS_APP_ID` /
  `EMR_SERVERLESS_ROLE_ARN` / `SPARK_SCRIPT_S3_URI` / `SPARK_LOG_S3_URI` from `tofu output`, plus
  `GLUE_CATALOG_URI` / `GLUE_WAREHOUSE` / `AWS_REGION` (already in `bench/.env` for `remote` mode).
  EMR Serverless is billed only while a job runs — see the "Spark / EMR Serverless" section in
  [`../deploy/README.md`](../deploy/README.md).
- **`compare_all.sh`** — runs `make bench` + `import_ceiling.sh` + `athena_compare.sh` always, then
  `spark_compare.sh` if its env vars are set (clean `SKIP` otherwise). Trino is the one exception to
  "never auto-provisions": set `RUN_TRINO_COMPARISON=1` and it stands up an ephemeral Trino cluster,
  runs `trino_compare.sh` against it, tears it down, stands up a **fresh** cluster, runs
  `import_jdbc_trino.sh` against that one, tears it down. Native and JDBC each get their own cold,
  never-before-queried cluster — sharing one cluster between them would let whichever ran first
  JIT-warm Trino and cache Iceberg metadata for the other, skewing the comparison. This means Trino
  gets provisioned **twice** per full run — real AWS spend, hence the explicit opt-in. Writes one
  aggregated `bench/reports/compare-<ts>.txt`.

**The `TIMING` line convention**: every compare script appends lines of the exact form
`TIMING <engine> <query-name> <seconds>` to its own report. `compare_all.sh` does nothing
engine-specific beyond `grep`-ing `^TIMING ` across the reports it produced into one aligned
table — no CSV/JSON, no dashboard; reports land in `bench/reports/` for you to read directly.

Query text (Presto/Trino/Spark dialect, identical across all three) is duplicated inline in each
script, translated from `run.sh`'s Q1-Q9b and NQ1-NQ5 — keep all four in sync if you edit one.

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

- `GLUE_WAREHOUSE` is the bare account id (matches `.env.example` and `install.md`), NOT
  an `s3://` path. The adapter derives the `catalogs/<account-id>` REST prefix internally.
- The S3 data endpoint defaults to `https://s3.$AWS_REGION.amazonaws.com`; the scan
  derives the virtual-hosted bucket URL from the region (no explicit endpoint).
