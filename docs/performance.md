[lakehouse-engine](../README.md) › [Docs](index.md) › Performance

---

# Performance

Optimizations, live-cluster numbers, and tuning headroom. Numbers from the last recorded
benchmark (`specs/_recorded/2026-06-27-change-engine-throughput/`) plus a larger-scale,
not-yet-recorded validation run (`bench/reports/bench-report-20260701-123648.txt`,
`bench/reports/import-ceiling-20260701-124836.txt`) — see
[Larger-scale validation](#larger-scale-validation-180m-row-lineitem-60-files) below.

> **Benchmark caveat — pre-0.20.1 node-count bug:** every benchmark below that predates the
> `exasol-udf-sdk`/`exasol-udf-macros` `0.20.1` bump (`add-scan-connection-concurrency`, closes
> #43) ran on live clusters where `ctx.node_count()` always returned `0` at
> `createVirtualSchema` time; `resolve_cluster_nodes` maps that to `1`. Any such run whose
> `adapterNotes` recorded `CLUSTER_NODES=1` on what was actually a multi-node cluster therefore
> silently computed shard count as `G = 1 × parallelism_factor` instead of
> `G = node_count × parallelism_factor` — collapsing cluster-wide sharding to single-node
> sharding. Single-node timings are unaffected; multi-node scaling claims recorded before the
> fix should be treated with suspicion. Full detail:
> `specs/_recorded/2026-07-02-add-scan-connection-concurrency/decision-log.md` (Design Decisions [1] and [5]).

## Optimizations delivered

- **Parquet predicate pushdown** (`datafusion.execution.parquet.pushdown_filters`) — filters
  push into the Parquet reader so non-matching rows are never decoded. (DataFusion defaults
  this off; the engine turns it on.) Distinct from Iceberg file pruning.
- **Row-group & page pruning** (`parquet.pruning`, `enable_page_index`) — skip whole row
  groups / pages whose statistics exclude the predicate.
- **Repartition-free raw-scan plan** — the single-partition raw-scan physical plan carries no
  `RepartitionExec`, `CoalescePartitionsExec`, or global sort/aggregate stage; filter and
  projection fuse into the data source.
- **Configurable per-instance threading** — `DATAFUSION_THREADING_MODE` (`AUTO`/`FIXED`),
  tunable without recompiling. See [Tuning](tuning.md).
- **Configurable object-store connection concurrency** — `S3_MAX_CONNECTIONS` sizes the
  per-instance S3 HTTP connection pool, oversubscribed relative to CPU threads on IO-bound
  scans. See [Tuning](tuning.md#s3_max_connections) and
  [Native `IMPORT` parity goal](#native-import-parity-goal) below.
- **Projection + partial-aggregate pushdown** — only projected columns are read; aggregates
  ship one partial row per group instead of raw rows.

## Native `IMPORT` parity goal

`S3_MAX_CONNECTIONS` targets closing the gap this doc measures below: native
`IMPORT FROM PARQUET` exposes an explicit `MaxConnections` knob for per-instance object-store
fetch concurrency, while the engine previously left that axis entirely to library defaults.
This knob is deliberately orthogonal to the two existing levers — `PARALLELISM_FACTOR` controls
*how many shards* run, `DATAFUSION_THREADING_MODE` controls *how many CPU threads* decode a
shard, and `S3_MAX_CONNECTIONS` controls *how many S3 fetches* a shard's threads can keep in
flight while they wait on the network. Raising it is the next lever toward approaching native
`IMPORT` throughput on IO-bound scans (the common case per the phase telemetry below —
`import ≫ emit`), layered on top of threading and sharding rather than replacing them. See
`specs/_recorded/2026-07-02-add-scan-connection-concurrency/decision-log.md` (Design Decisions [2]-[4]) for
the knob's design rationale, and Task 3.1 (benchmark sweep) for the validating measurement.

> **Sweep outcome (2026-07-02):** the validating sweep found `S3_MAX_CONNECTIONS` had **no
> measurable effect** on full-scan throughput on the tested 2-node cluster (< 2 % across 4→128,
> at either shard shape) — the object-store connection pool was not the limiter there; the S3
> read itself is network-distance bound (~0.17 GB/s). The knob is correctly wired and remains a
> supported lever for genuinely connection-starved deployments, but it did not close the gap
> here. Full results in [Connection-concurrency & shard-shape sweep](#connection-concurrency--shard-shape-sweep-2026-07-02-post-0201).

## Current benchmark results

**Cluster:** 3 nodes, Exasol DB 2025.1.11, AWS Glue catalog `tpch`, SLC lc-rs 0.19.1.
**Data:** `lineitem` ≈ 1.7 GB across 20 Parquet files on S3.
**Config:** `DATAFUSION_THREADING_MODE=FIXED`, threads = partitions = 4, telemetry off.

| Query | Time | Notes |
|---|---|---|
| Q1 (wiring) | 3.75 s | — |
| Q2 (3-way join) | 9.89 s | join handled by Exasol over per-table scans |
| Q3 (filter + GROUP BY) | 8.20 s | partial-agg pushdown |
| **Q4 (full `lineitem` scan)** | **8.94 s ≈ 0.19 GB/s** | reproduces prior baseline |
| `COUNT(*)` via metadata pushdown | 1.60–2.67 s | no row scan |

### Thread sweep (NR_OF_CORES = 4)

| threads / partitions | Q4 full scan | vs. 1/1 |
|---|---|---|
| 1 / 1 (what `AUTO` derives here) | 12.45 s | baseline |
| 2 / 2 | 10.52 s | +18 % |
| **4 / 4** | **8.94 s** | **+39 %** |
| 8 / 8 | 10.02 s | regresses |

⇒ **For read-bound scans, set `DATAFUSION_THREADING_MODE=FIXED`, threads = partitions =
`NR_OF_CORES`.** Single-thread leaves ~39 % unused; threads help by overlapping read latency,
not CPU.

### The engine is not the bottleneck — object-storage read is

Three independent confirmations:

1. **Native baseline** — Exasol's own `IMPORT FROM PARQUET` over the same files hits the same
   ~0.17 GB/s ceiling (≈ 10.07 s). The VS path is competitive or faster (pushdown). Same
   storage, same ceiling ⇒ the limit is the read, not the UDF layer.
2. **Phase telemetry** (single `COUNT` shard): startup ≈ 110 ms, import ≈ **650 ms**, emit
   ≈ 2 ms — overwhelmingly import-bound.
3. **Thread sweep** — added threads only help by overlapping read waits (above).

Memory stayed bounded across the sweep — no OOM, no VM crash.

> **Benchmark caveat:** this run's S3 was in a *different VPC* from the cluster, so the
> ~0.17 GB/s ceiling reflects network distance, not the engine. Co-locating storage would lift
> it — but that's a deployment property of the benchmark, not a knob the engine owns. Many
> deployments legitimately query distant storage, so the durable wins are the engine levers
> below.

Reproduce: `bench/run.sh`, `bench/sweep.sh` ([`bench/README.md`](../bench/README.md)).

## Larger-scale validation (180M-row lineitem, 60 files)

A 2026-07-01 run against the full TPC-H `lineitem` table — **60 Parquet files, 179,998,372
rows** on Glue (`eu-west-1`), SLC lc-rs 0.19.1, `NR_OF_CORES=8`, `PARALLELISM_FACTOR=8` — ~30×
the row count of the 20-file sample above. Not yet formally recorded as a spec benchmark; raw
output in `bench/reports/bench-report-20260701-123648.txt` and
`bench/reports/import-ceiling-20260701-124836.txt`.

| Query | Result | Time |
|---|---|---|
| Q1 (wiring check) | 25 rows | 2.12 s |
| Q2 (3-way join) | 179,998,372 rows joined | 22.17 s |
| Q3 (filter + GROUP BY) | 5 groups | 20.54 s |
| Q4 (lineitem pricing summary) | 4 groups | 28.51 s |

**Native `IMPORT` vs. VS, same 60 files (avg of 3 runs each):**

| Path | Avg time |
|---|---|
| Native `IMPORT` — `COUNT(*)` ceiling over all 60 files | ~28.8 s |
| VS — `SELECT COUNT(*)` (metadata pushdown) | ~1.3 s |
| Native `IMPORT INTO` (full load, 180M rows materialized) | **~80.4 s** |
| VS — `CREATE TABLE AS SELECT *` (full emit, 180M rows) | **~151 s** |

Metadata-only `COUNT(*)` still favors the VS path by ~20× (answered from Iceberg/row-group
stats, no row materialization). But **full materialization flips the earlier finding**: native
`IMPORT INTO` is ~1.9× faster than the VS full-emit CTAS at this scale, unlike the small-scale
run where the VS aggregate path was competitive with native IMPORT.

> **Confound resolved (re-gate, 2026-07-02, post-0.20.1):** the run above recorded
> `CLUSTER_NODES=1` despite `NR_OF_CORES=8` / `PARALLELISM_FACTOR=8` — the pre-0.20.1
> `ctx.node_count()==0` handshake bug (#43). After the 0.20.1 bump landed, the same cluster's
> `adapterNotes` now reports the **real `CLUSTER_NODES=2`**, so at the default
> `PARALLELISM_FACTOR=8` the shard count is `G = 2 × 8 = 16` (was `1 × 8 = 8`). Re-gate outcome:
>
> - **Scan/aggregate path improved.** Q4 (full-`lineitem` pricing summary) dropped from **28.5 s
>   → 20.5 s** (≈ −28 %) purely from the corrected shard count — no knob change.
> - **Full-emit gap persists and is *not* under-sharding.** The exact 60-file / 180M-row
>   materialization re-run could not be reproduced (the shared cluster's 10 GiB raw-size license
>   is exceeded by a single 180M-row `lineitem` table ≈ 24 GiB raw), so the emit path was
>   re-measured at reduced scale (≈ 30–33 M rows, same files, corrected `CLUSTER_NODES=2`):
>   native `IMPORT INTO` **2.07 M rows/s** vs. VS `CREATE TABLE AS SELECT *` **1.19 M rows/s** —
>   a **~1.74×** gap, essentially unchanged from the pre-fix **~1.88×**. Decisively, the VS
>   full-emit throughput (**1.19 M rows/s**) is *identical* to the pre-fix full-scale run
>   (180M / 151 s = 1.19 M rows/s): **doubling `G` (8 → 16) did not move full-emit throughput at
>   all**, so the emit gap is bottlenecked *downstream* of sharding, not by cluster parallelism.
>
> The confound is therefore resolved: the 1.9× emit gap was **not** primarily under-sharding.
> See the emit-path isolation under [Tuning levers & outlook](#tuning-levers--outlook) for why
> it is also **not** primarily the `Int64→Decimal128` coercion, and
> `specs/_recorded/2026-07-02-add-scan-connection-concurrency/decision-log.md` (Design Decision
> [5] + the 2026-07-02 validation addendum) for the full methodology and verdict.

## Competitive engine comparison

**Status: live-verified 2026-07-03 against `test1`** (2-node Exasol cluster, `r8i.2xlarge` × 2,
180M-row `lineitem`, 60 files, same Glue catalog + S3 data for every engine below). Latest pass is
a single fresh full re-run across all four engines with the extended Q1-Q9b query set (below);
Trino now defaults to a 2-node `r8i.2xlarge` cluster matching `test1` (was a single `r6i.xlarge` in
the first round — see [Infrastructure comparison](#infrastructure-comparison)).

Beyond the native `IMPORT` ceiling above, the same TPC-H tables are queried through the lakehouse
engines people put next to a lakehouse today, all reading the SAME Glue Iceberg catalog + S3 data:

- **AWS Athena** (`bench/athena_compare.sh`) — serverless, no infra to stand up; timed via the
  Athena API's `Statistics.EngineExecutionTimeInMillis` (engine time only, excludes queue wait).
- **Trino** (`bench/trino_compare.sh`) — an ephemeral 2-node cluster by default
  (`deploy/trino-stack/`, `deploy/scripts/trino-up.sh`/`trino-down.sh`), wall-clock timed via the
  Trino CLI. Opt-in and torn down explicitly — see the cost/teardown callout in
  [`deploy/README.md`](../deploy/README.md).
- **Spark** (`bench/spark_compare.sh`) — AWS EMR Serverless (`deploy/data-stack`'s
  `enable_emr_serverless` toggle, off by default), billed only while a job runs. Timed from
  `elapsed:` lines the submitted job prints to its driver log.

`bench/compare_all.sh` runs all of the above (skipping Trino/Spark cleanly if not provisioned)
and aggregates every engine's timings into one report with a summary table.

### Query set

Q1-Q4 are the original TPC-H-shaped set (wiring check, 3-way join, filtered JOIN+GROUP-BY,
pricing summary). Q5-Q9b were added to probe specific pushdown strengths/weaknesses:

| Query | What it tests |
|---|---|
| Q5 | Q3 with the `WHERE` dropped — unfiltered JOIN + GROUP BY |
| Q6 | Q4 with the `WHERE` dropped — unfiltered pricing summary |
| Q7 | High-cardinality `GROUP BY L_ORDERKEY` (~45M distinct groups, vs. Q3/Q4's 4-5) |
| Q8 | Single-day filter (`L_SHIPDATE = DATE '1995-06-15'`, <0.05% of rows) — selective-pushdown/pruning |
| Q9a | Narrow projection — `SUM` over one column, full scan |
| Q9b | Wide projection — aggregates touching all 16 `lineitem` columns, full scan |

### Results (2026-07-03, single fresh pass, all four engines)

| Query | lakehouse-engine-rs | AWS Athena | Trino (2-node) | Spark (EMR Serverless) |
|---|---|---|---|---|
| Q1 (wiring) | **1.79 s** | 1.87 s | 7.05 s | 16.80 s |
| Q2 (3-way join) | 16.90 s | **2.54 s** | 12.19 s | 43.59 s |
| Q3 (filter+groupby) | 14.48 s | **2.99 s** | 8.37 s | 31.51 s |
| Q4 (pricing summary) | 19.17 s | **3.19 s** | 5.56 s | 21.46 s |
| Q5 (Q3, no filter) | 18.23 s | **2.50 s** | 10.25 s | 38.07 s |
| Q6 (Q4, no filter) | 19.25 s | **2.71 s** | 4.98 s | 18.77 s |
| Q7 (high-card. GROUP BY) | **FAILED** (bug, see below) | **1.62 s** | 5.20 s | 12.84 s |
| Q8 (selective filter) | 1.51 s | **0.93 s** | 4.25 s | 5.62 s |
| Q9a (narrow projection) | **2.18 s** | 2.39 s | 3.78 s | 5.64 s |
| Q9b (wide projection) | 67.08 s | 44.98 s | **15.19 s** | 59.19 s |

Native `IMPORT` (goal ceiling, not a competing "engine" — no Q5-Q9b equivalent, unaffected by this
round): scan-only `COUNT(*)` ~28.8–30.9 s vs. the VS's metadata-pushdown ~0.8–2.0 s; full
materialization native `IMPORT INTO` ~80.4–81.0 s vs. VS `CREATE TABLE AS SELECT *`
~124.5–125.2 s (see [Larger-scale validation](#larger-scale-validation-180m-row-lineitem-60-files)
above).

**Reading these numbers**:

- **Athena wins 7 of the 10 queries outright** (everything except Q1, Q9a, and Q9b) — it's the
  cleanest comparison point (zero infra-sizing decisions) and clearly the most consistent
  performer across small/medium queries.
- **Trino wins decisively on Q9b** (15.19 s vs. everyone else's 45-67 s) — the wide-projection
  full scan is where matching `test1`'s hardware pays off most clearly. It's competitive but not
  fastest on the other heavy scans (Q2, Q5): faster than lakehouse-engine-rs and Spark, but Athena
  still wins on raw time.
- **lakehouse-engine-rs wins Q1 and Q9a** (small/single-column queries — competitive when its
  pushdown paths apply cleanly and the result is tiny), but is consistently the slowest on
  unfiltered/wide-scan queries (Q5, Q6, Q9b) — the per-row Arrow→Value emit-path overhead
  documented earlier in this doc compounds most visibly exactly where no pushdown can shrink the
  work. Even on Q8 (its best-case selective-filter scenario) it trails Athena (1.51 s vs. 0.93 s).
- **Q7 is a genuine bug, not a slowness finding**: `SELECT COUNT(*) FROM (SELECT L_ORDERKEY,
  COUNT(*) FROM lineitem GROUP BY L_ORDERKEY) t` fails outright against lakehouse-engine-rs —
  `DataFusion SQL error: Schema error: No field named "NULL"` — while Athena, Trino, and Spark all
  handle the identical nested-aggregate query in 1.6-12.8 s. Filed as
  [#52](https://github.com/exasol-labs/lakehouse-engine-rs/issues/52): the adapter's
  aggregate-pushdown translation appears to substitute a literal `NULL` where a field reference is
  expected when composing an outer `COUNT(*)` (no underlying column) over an already-pushed-down
  inner `GROUP BY`.
- **Spark is consistently the slowest** across nearly every query — expected, given the numbers
  include EMR Serverless's per-job executor allocation on top of query time, not just the query
  itself.

**Reproducibility (earlier check, Q1-Q4 only, first Trino-resize round)**: run twice on
independent fresh Trino clusters — Q1/Q3/Q4 were tight (<2% spread), Q2 (the heaviest query then)
showed the most run-to-run variance (15.74 s → 13.16 s, ~16%), plausibly cold caches or a noisy
neighbor on shared cloud hardware. Not repeated for the full Q1-Q9b set this round; treat single-
digit-percent differences between engines as noise-level rather than a hard ranking.

### Infrastructure comparison

Athena has **no equivalent sizing control** to match here: the standard Athena SQL engine (what
`bench/athena_compare.sh` exercises via `StartQueryExecution`) is fully serverless — AWS
auto-scales compute per query with no user-facing vCPU, RAM, or node-count knob. (*Athena for
Apache Spark* has a configurable Data Processing Unit count, but that's a different, session-based
execution mode — using it would change what's being measured, not just how big it is.) Athena is
included because it's what a customer actually gets, not because it's size-matched.

Trino and lakehouse-engine-rs, as of this round, run on identical VM shape and count:

| | Instance type | Nodes | vCPU (total) | RAM (total) |
|---|---|---|---|---|
| Exasol `test1` (runs lakehouse-engine-rs) | `r8i.2xlarge` | 2 | 16 | 128 GB |
| Trino | `r8i.2xlarge` | 2 | 16 | 128 GB |
| AWS Athena | fully managed — no sizing control | n/a | n/a | n/a |

Same hardware, different consumption model, though: lakehouse-engine-rs doesn't get a dedicated
box — it shares each Exasol node with the DB engine itself. Live-checked on `test1`: the Exasol DB
is configured for **97.05 GiB total across both nodes** (`MemSize` in `EXAConf`, ≈48.5 GiB/node out
of each node's 64 GiB nameplate RAM — the rest is OS/COS/UDF headroom via `c4`'s memory-sizing
formula), and the UDF's own DataFusion pool is a further fraction of whatever per-instance memory
limit the script metadata reports, fanned out via `PARALLELISM_FACTOR`/`NR_OF_CORES`. Trino, by
contrast, dedicates the whole node to query execution with a directly-configured 48 GB JVM heap
per node. "Same VM shape and count" is the fairest comparison available, but it isn't "identical
resources actually available to the query engine."

### Bugs found and fixed during first live verification (2026-07-03)

Standing up the Athena/Trino/Spark comparison against a real cluster for the first time surfaced
ten real bugs — none were visible from code review or `tofu validate` alone:

1. **`bench/.env`'s `BENCH_SLC_VERSION` was stale** (`0.19.1`) vs. the crate's actual
   `exasol-udf-sdk` (`0.20.1`) — `make bench` failed with `F-UDF-CL-RUST-9001: Fingerprint
   mismatch`. Worked around with `BENCH_SLC_VERSION=0.20.1`; `secrets.sh`/`run.sh` defaults should
   be bumped to track the crate's pinned SDK version going forward.
2. **`import_ceiling.sh`'s file-harvest regex** assumed full per-file `s3://` URLs, but the
   scan-spec-files-payload change (#48) made the report embed a `table_root` + paths *relative* to
   it. Fixed in `bench/import_ceiling.sh` to reconstruct full URLs from both parts.
3. **`bench/.env`'s engine-reader AWS creds clobbered `AWS_PROFILE`** in `athena_compare.sh` /
   `spark_compare.sh` — sourcing `.env` exports `AWS_ACCESS_KEY_ID`/`SECRET` for the Glue/S3-only
   `engine-reader` user (needed by the Exasol CONNECTION), which then shadows the operator's own
   broader identity for `aws` CLI calls, causing `AccessDeniedException` on Athena/EMR Serverless
   APIs. Fixed by unsetting those three vars right after sourcing `.env` in both scripts.
4. **Trino data-dir permission**: the official `trinodb/trino` image runs as uid/gid 1000, but the
   bind-mounted host directory was root-owned → `Permission denied: '/data/trino/var'` on startup.
   Fixed in `trino-userdata.sh.tftpl` with `chown -R 1000:1000`.
5. **Trino's Iceberg REST catalog config was invalid** — `iceberg.rest-catalog.security=SIGV4`
   doesn't exist in Trino 465 (`IcebergRestCatalogConfig$Security` only has `NONE`/`OAUTH2`,
   confirmed via `javap` on the connector jar). Switched to `iceberg.catalog.type=glue` (native AWS
   Glue Data Catalog integration — no REST/SigV4 config needed at all, and it uses the same
   instance-profile credential chain already in place).
6. **Trino JVM heap too small**: the default `-Xmx8G` OOM'd on Q2's 3-way join over 180M lineitem
   rows (`Query exceeded per-node memory limit of 2.40GB`). Bumped to `-Xmx24G` +
   `query.max-memory-per-node=18GB` for the `r6i.xlarge` (30 GiB RAM) node.
7. **`set -euo pipefail` in the three new compare scripts** aborted the whole run on a single
   failing query instead of reporting `FAILED` and continuing (a `var=$(cmd)` assignment under
   `-e` exits immediately on a nonzero `cmd`, before the script's own `rc=$?` check runs). Dropped
   `-e` to match `bench/import_ceiling.sh`'s existing, deliberate convention.
8. **EMR Serverless application creation needs `iam:CreateServiceLinkedRole`** for
   `AWSServiceRoleForAmazonEMRServerless` — not covered by `emr-serverless:*` (a separate IAM
   action namespace). Added a scoped statement to `deploy/iam/deployer-policy.json`.
9. **EMR Serverless has no internet egress by default** — `spark.jars.packages` (Maven Central via
   Ivy) timed out resolving `iceberg-spark-runtime-3.5_2.12`. Switched to the release's *locally
   bundled* jar (`/usr/share/aws/iceberg/lib/iceberg-spark3-runtime.jar`) and Spark's Iceberg
   `GlueCatalog` impl (`catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog`) — the same
   "talk to Glue directly, not via REST" pattern as the Trino fix above.
10. **`emr_serverless_max_capacity` default (4 vCPU/16 GB) was too small even for one executor** —
    every job failed with `ApplicationMaxCapacityExceededException`, zero executors ever allocated
    (the driver alone consumes most of a 4 vCPU/16 GB ceiling). Bumped the default to 16 vCPU/64 GB
    in `deploy/data-stack/variables.tf`. Also found: updating `maximumCapacity` (or deleting the
    application) requires the app to be `STOPPED` first — documented in `deploy/README.md`'s
    "Known seams".

All fixes are on `feat/competitive-engine-benchmark` (PR #51).

### Bugs found resizing Trino to a 2-node cluster (2026-07-03, second round)

Converting `deploy/trino-stack/` from a single node to a real coordinator + worker cluster
surfaced two more, both live-only:

11. **A worker referencing the coordinator's `private_ip` from within the same `count`-based
    `aws_instance` resource is a self-cycle for index 0** — Terraform's dependency graph sees the
    static reference `aws_instance.trino[0]` inside the resource's own config and treats every
    instance in that `count`, including index 0 itself, as depending on index 0 (a
    `ternary`-conditioned expression doesn't change this — the graph is built from the referenced
    addresses, not evaluated branches). Caught at `tofu validate`/`plan` time, not live, but
    worth recording: split into separate `aws_instance.trino_coordinator` (singular) and
    `aws_instance.trino_worker` (`count = node_count - 1`) resources instead — a worker
    referencing a *different* resource's attribute is a normal, acyclic dependency.
12. **Trino's `/v1/node` endpoint needs an `X-Trino-User` header** even with no authentication
    configured (`curl -sf` alone gets `Basic authentication or X-Trino-User ... must be sent`),
    **and it doesn't list the coordinator itself** — only nodes reached via the
    announcement/discovery protocol. `trino-up.sh`'s readiness check was waiting on the wrong
    count (`node_count`, unreachable since the coordinator never appears) with the wrong request
    (no header, always failing). Fixed to add the header and wait on `node_count - 1` (workers
    only) — the coordinator's own liveness is separately confirmed via `/v1/info`.

## Tuning levers & outlook

The engine controls these regardless of where storage lives — apply them first:

- **Threading** — `DATAFUSION_THREADING_MODE=FIXED`, threads = partitions = `NR_OF_CORES` for
  read-bound scans (~+39 % vs. the `AUTO` single-thread default).
- **`PARALLELISM_FACTOR`** — more shards oversubscribe the cluster and overlap more reads;
  balances stragglers without raising peak memory.
- **`DATAFUSION_BATCH_SIZE`** — bounds the per-batch decode working set.
- **`MEMORY_POOL_FRACTION`** — pool size vs. the per-instance limit (default 0.6).

See [Tuning](tuning.md) for ranges and defaults.

### Connection-concurrency & shard-shape sweep (2026-07-02, post-0.20.1)

A sweep on the 2-node cluster (`CLUSTER_NODES=2` confirmed in `adapterNotes`), 60-file / 180M-row
`lineitem`, tested the hypothesis that *serial / under-concurrent fetching* — not the engine —
caps throughput, via three levers: (1) `PARALLELISM_FACTOR=1` (one shard per node), (2)
`DATAFUSION_THREADING_MODE=AUTO` (that single instance gets all the node's cores), and (3) a
swept `S3_MAX_CONNECTIONS`. **All three levers were refuted; the shipped default wins.**

| Config (all `CLUSTER_NODES=2`) | shards `G` | Q4 full scan | Q2 join | Q3 grp |
|---|---|---|---|---|
| **Default `PARALLELISM_FACTOR=8`, `S3_MAX_CONNECTIONS` AUTO (=4)** | **16** | **20.5 s** | 18.7 s | 16.0 s |
| `PARALLELISM_FACTOR=8`, `S3_MAX_CONNECTIONS` = 16 / 32 / 64 | 16 | 20.1 / 20.1 / 20.4 s | 17.8 s | 15.6–16.3 s |
| `PARALLELISM_FACTOR=1`, AUTO threads, `S3_MAX_CONNECTIONS` AUTO (=32) | 2 | 63.7 s | 30.2 s | 34.1 s |
| `PARALLELISM_FACTOR=1`, AUTO threads, `S3_MAX_CONNECTIONS` = 64 / 128 | 2 | 63.9 / 64.4 s | 29.8–30.1 s | 33.4–33.7 s |

- **Lever 1 (fewer, bigger shards) — refuted.** One shard per node (`G=2`) is **~3.1× slower** on
  the full-scan Q4 (63.7 s vs. 20.5 s) and ~2× slower on Q2/Q3. Inter-instance oversubscription
  (`G=16`, multiplexed onto each node's core pool) decisively beats a single big instance per node
  — DataFusion's intra-instance threading does not substitute for it.
- **Lever 2 (one AUTO instance/node) — refuted** (coupled with lever 1): AUTO correctly gave the
  single instance 8 threads / 8 partitions, yet the shape was still ~3× worse.
- **Lever 3 (`S3_MAX_CONNECTIONS`) — refuted.** Sweeping it changed Q4 by **< 2 %** at *either*
  shard shape (`PARALLELISM_FACTOR=8`: 20.1–20.4 s across 4→64; `PARALLELISM_FACTOR=1`:
  63.7–64.4 s across 32→128). The object-store HTTP connection pool is not the throughput limiter
  on this deployment. The knob remains a supported, correctly-wired lever (verified end-to-end via
  `adapterNotes`), just not the one that helps here.
- **The real win was the 0.20.1 `CLUSTER_NODES` fix**, not a new knob: correcting the node count
  from the buggy `1` to the real `2` doubled `G` at the default `PARALLELISM_FACTOR=8` (8 → 16),
  which is what cut Q4 from ~28.5 s to ~20.5 s.

**Native-`IMPORT` comparison (same 60 files, 5.40 GB / 180M rows, S3 in `eu-west-1`):** native
`IMPORT FROM PARQUET` `COUNT(*)` (full read) ≈ **30.6 s (0.176 GB/s)**; the VS scan/aggregate path
(Q4, with projection + predicate + Iceberg pruning) is **faster** at 20.5 s; VS metadata
`COUNT(*)` ≈ 1.6 s. The mission's ~1 GB/s target is **not reachable on this cluster** — the S3
read ceiling here is ~0.17 GB/s (storage network distance, a deployment property, consistent with
the earlier different-VPC caveat), not an engine limit. The durable engine wins are the sharding
correctness and pushdown, which already make the *scan* path competitive with (or faster than)
native; only the *full raw-emit* path trails native's bulk loader (~1.74×, see re-gate above).

Reproduce: `bench/sweep.sh` (shard/connection sweep) + `bench/import_ceiling.sh` (native ceiling).

### Emit-path batch-size & connection sweep (2026-07-02, post-0.20.1)

The `S3_MAX_CONNECTIONS` sweep above tested the *aggregate* Q4 (few rows over the wire). This
pass isolates the two remaining untested emit-path levers on the **raw full-emit** workload — the
same reduced-scale `CREATE TABLE AS SELECT *` filtered to `L_ORDERKEY < 33007128` (33,006,459
rows, same 60 files, `CLUSTER_NODES=2`, `PARALLELISM_FACTOR=8` ⇒ `G=16`, threading AUTO) that the
re-gate above used, against the same native `IMPORT INTO` ceiling of **2.07 M rows/s** (best of 2
passes per config; VS reset + `FLUSH STATISTICS` between runs for the 10 GiB license).

**Lever A — `DATAFUSION_BATCH_SIZE` (raw-emit round-trip count).** The emit loop
(`scan/emit.rs::emit_stream`) fetches one DataFusion `RecordBatch`, calls `ctx.emit_batch` once,
drops it, and fetches the next — so batch size sets the rows per `MT_EMIT` and therefore the
round-trip count (≈ 4,030 round-trips cluster-wide at 8192, ≈ 252 at 131072, a **16× reduction**).

| `DATAFUSION_BATCH_SIZE` | round-trips (approx) | VS raw-emit rows/s | gap vs native |
|---|---|---|---|
| **8192 (default)** | ~4,030 | 1,222,009 | 1.70× |
| 32768 | ~1,007 | 1,278,824 | 1.62× |
| **65536 (best)** | ~504 | 1,306,669 | 1.59× |
| 131072 | ~252 | 1,272,416 | 1.63× |

⇒ **Round-trip count is a minor cost, not the bottleneck.** A 16× reduction in `MT_EMIT`
round-trips bought only **~7 %** throughput (1.22 M → 1.31 M rows/s), plateauing at 32k–65k and
falling back slightly at 131k. The gap stays **~1.6×**; larger batches do **not** close it. This
directly **refines the earlier attribution** — the residual emit gap is *not* dominated by the
`MT_EMIT` round-trip **count** (if it were, 16× fewer would have moved it far more than 7 %). What
scales with the workload is the **per-row cost**: Arrow→`Value` row materialization plus the
DB-side ingest of each emitted row, both proportional to row count regardless of how rows are
batched. The synchronous send/ack **per-round-trip latency** is real but its aggregate is small
because the count is already low relative to 33 M rows.

**Lever B — `S3_MAX_CONNECTIONS` on the raw-emit path.** Prior work refuted this knob on Q4
(aggregate). Re-tested here specifically on the raw-emit path (30 M+ rows streamed via `MT_EMIT`),
in case a wider fetch pipeline keeps more decoded batches ready to emit and hides emit-wait, at the
winning `DATAFUSION_BATCH_SIZE=65536`:

| `S3_MAX_CONNECTIONS` | VS raw-emit rows/s | gap vs native |
|---|---|---|
| AUTO (resolved 4) | 1,309,261 | 1.58× |
| 8 | 1,310,820 | 1.58× |
| 32 | 1,297,934 | 1.60× |
| 64 | 1,311,341 | 1.58× |
| 128 | 1,315,522 | 1.58× |

⇒ **Refuted on the emit path too** — a **~1.4 %** spread across AUTO→128, gap fixed at ~1.58×. The
emit path is no more fetch-concurrency-bound than the aggregate path was; the wider pipeline does
not hide emit-wait here.

**Aggregate-path regression check** (Q1–Q4, `DATAFUSION_BATCH_SIZE` 8192 vs. 65536): 65536 is **not
a regression** — Q1 1.94→1.96 s (flat), Q2 18.53→17.03 s (−8 %), Q3 15.75→15.30 s (−3 %), Q4
20.35→18.71 s (−8 %). Larger batches marginally help the aggregate/join path (fewer per-batch
overheads), and every timing stays within the prior sweep's recorded ranges.

**Verdict:** neither round-trip count (batch size) nor S3 fetch concurrency moves the ~1.6× raw-emit
gap. Combined with the earlier findings that doubling `G` (8→16) did nothing and the Int64→Decimal
coercion is only ~1.27× (~5–6 % of emit time), the residual gap is an **architectural floor**: the
VS materializes each row (Arrow→`Value`) and streams it through the UDF emit protocol for the DB to
ingest row-wise, whereas native `IMPORT INTO` uses Exasol's own bulk Parquet loader writing directly
into columnar storage — bypassing UDF row-materialization and the emit protocol entirely. No tunable
knob crosses that boundary. Per the evidence-gated convention (ADR-055), **no shipped-crate change
was made**: `DATAFUSION_BATCH_SIZE=65536` is documented as an operator tuning hint for emit-bound /
wide `SELECT *` workloads (~7 % emit gain, no aggregate regression, memory-safe on an 8-core /
4 GiB-per-instance node), but the shipped default stays **8192** (matches DataFusion's own default;
keeps the per-batch decode working set — and out-of-pool RSS — small for memory-constrained
deployments, where the ~7 % is not worth an 8× larger in-flight batch).

Reproduce: `bench/batch_size_sweep.sh` (batch-size emit sweep) + `bench/emit_s3conn_sweep.sh`
(connections on the emit path) + `bench/batch_size_aggcheck.sh` (Q1–Q4 regression check).

Future engine work (deferred, evidence-gated):

- **I/O-aware `AUTO` threading** — `AUTO` stays the safe CPU/memory-bound default; a future
  variant could detect a read-bound scan and oversubscribe threads automatically (today that's
  the manual `FIXED` lever).
- **Decode-emit overlap buffer** — gate-failed here (emit ≈ 2 ms, nothing to overlap); revisit
  for an emit-bound workload (wide `SELECT *`).
- **Emit-path Arrow cast** — **evaluated 2026-07-02 and NOT pursued.** The `BIGINT`
  (Int64 → Decimal128) coercion measured 50–200× slower than zero-copy types *in a synthetic
  micro-bench*, but a column-isolation experiment on the real `lineitem` full-emit workload
  (33 M rows, four columns per class, corrected `CLUSTER_NODES=2`) does **not** reproduce that:
  the four Int64→Decimal columns emit at **4.63 M rows/s** vs. **5.89 M rows/s** for four
  zero-copy `Decimal128(15,2)` columns and **6.82 M rows/s** for four `Utf8` columns — the
  coercion is the slowest column class but only **~1.27×** slower, contributing **~5–6 %** of
  the full 16-column emit time. Eliminating it would move the native-vs-VS full-emit gap only
  from ~1.74× to ~1.65×. The gap is dominated by general per-row Arrow→`Value` conversion, **not**
  the Int64 coercion. Per the project's evidence-gated convention (ADR-055), no emit-path coercion
  code was written — the micro-bench figure is not a real-workload bottleneck. Revisit only if a
  future profile isolates a workload where the coercion dominates.
  (**Refinement, 2026-07-02:** the `DATAFUSION_BATCH_SIZE` sweep above shows the `MT_EMIT`
  round-trip *count* is **not** a co-dominant factor — a 16× reduction in round-trips moved
  throughput only ~7 %. The dominant residual cost is per-*row* work (Arrow→`Value` materialization
  + DB-side row ingest), which scales with row count independent of batching.)
