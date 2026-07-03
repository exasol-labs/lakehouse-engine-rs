# Decision Log: add-scan-connection-concurrency

Date: 2026-07-02

GitHub issues: https://github.com/exasol-labs/lakehouse-engine-rs/issues/47 (this plan) and https://github.com/exasol-labs/lakehouse-engine-rs/issues/43 (pre-existing bug report for the exact `ctx.node_count()==0` handshake symptom the dependency bump fixes). Reference both as `Closes #47, Closes #43` in the implementing commit.

## Interview

**Q1 (plan shape):** Two of the three hypothesis bullets (#shards=#nodes, one DataFusion instance per node getting max resources) are already expressible today via existing VS properties (`PARALLELISM_FACTOR=1` → `G = node_count`; `DATAFUSION_THREADING_MODE=AUTO`/`FIXED` gives that single instance all the node's cores). Only "max file connections per node to saturate network/IO" has no existing knob. Options: (a) benchmark-first, gate the new knob on results [recommended]; (b) build the knob now and benchmark alongside it; (c) benchmark only, no new knob.
**A1:** "Bump to lc-rs v0.20.1 first because the number of nodes/cores was wrongfully exposed; add the knob and benchmark; goal of the plan shall be to get close to the performance of the native IMPORT (see benchmark results) → benchmarks/e2e/benches shall not be part of the spec deltas." → Resolved as option (b): build the knob now AND benchmark, gated on the 0.20.1 dependency-version prerequisite, with benchmark/e2e work excluded from spec deltas (supporting verification only, referenced in plan.md's validation/ADR narrative).

**Q2 (knob placement):** VS property like `PARALLELISM_FACTOR` [recommended], or a fixed hardcoded default with no operator-facing property?
**A2:** VS property, like `PARALLELISM_FACTOR`.

### Amendment interview (2026-07-02, new 180M-row benchmark evidence)

**Q3 (evidence incorporation):** A 2026-07-01 full-`lineitem` run (60 files, 180M rows) found the VS full-emit `CREATE TABLE AS SELECT *` (~151 s) is ~1.9× slower than native `IMPORT INTO` (~80.4 s) — flipping the original aggregate-path finding — but the run recorded the confounded `CLUSTER_NODES=1` (the pre-0.20.1 `node_count()==0` bug that Task 1 fixes). Should we (a) expand this plan to build emit-path optimization now, (b) incorporate the evidence as rationale only + add a named re-gate task + document emit-path work as evidence-gated deferred work, or (c) ignore it?
**A3:** No user response received within the interview timeout. Proceeded on the project's established "evidence-gated future work" convention already codified in `docs/performance.md` §"Future engine work (deferred, evidence-gated)" → resolved as option (b): evidence-only, plus a named re-gate task (plan.md Task 10) and a documented, evidence-gated deferred-work item (plan.md §Deferred work). No code/scope expansion.

## Design Decisions

### [1] Prerequisite dependency bump 0.20.0 → 0.20.1 ships without a spec delta

- **Decision:** Bump `exasol-udf-sdk`/`exasol-udf-macros` `0.20.0 → 0.20.1` (fixes upstream `language-container-rs` issue #41: `ctx.node_count()` returned `0` on the single-call VS-adapter path). Ship it as a task only, with no Given/When/Then scenario. Closes this repo's own bug report of the same symptom, **issue #43** ("`CLUSTER_NODES` always 1 on multi-node clusters"), which had left the fix undecided pending #41 and flagged `create_vs_records_cluster_nodes_property` as an assertion to revisit.
- **Alternatives:** Author a `resolve_cluster_nodes` / `create-virtual-schema-adapter-notes` CHANGED delta (as the prior `2026-07-01-fix-createvs-cores-nodecount` plan did for its 0.19.1→0.20.0 bump, because that plan rewrote `resolve_cluster_nodes`).
- **Rationale:** This repo's `resolve_cluster_nodes` (`adapter/mod.rs:693-702`) is unchanged — it already maps `node_count()==0 → 1` and passes positive counts through verbatim. The observable contract is already fully covered by `cluster_nodes_passes_through_reported_node_count` and the `0→1` fallback scenarios; the fix lives entirely in the upstream SDK handshake threading. Inventing a scenario for a pure version pin with zero contract change would be spec noise. Issue #43's own "not yet verified" list resolves cleanly to option (a) it posed ("fix consumed via a new SLC release") now that #41 shipped.
- **Promotes to ADR:** no

### [2] One `S3_MAX_CONNECTIONS` knob, not a dual per-file/per-node pair

- **Decision:** Expose a single operator VS property `S3_MAX_CONNECTIONS` (mirroring the native `IMPORT FROM PARQUET` `MaxConnections` vocabulary) rather than mirroring the native importer's full dual model (`MaxConnections` = parallel reads within a file + `MaxConcurrentReads` = files in parallel per node).
- **Alternatives:** Ship both axes as two properties now.
- **Rationale:** The user's hypothesis is a single lever ("max file connections per node to saturate"). One knob covers it; a second axis is unproven complexity. Defer the split until the benchmark shows one knob is insufficient (YAGNI). Establishes the project convention that new tuning axes ship as one operator knob until a benchmark proves a second is needed.
- **Promotes to ADR:** yes

### [3] Explicit-wins-else-AUTO, with no separate MODE property

- **Decision:** Resolve `S3_MAX_CONNECTIONS` as: explicit positive integer verbatim (FIXED-like), else AUTO-derive from `nr_of_cores` and the per-node UDF-instance share (mirroring `auto_threads_per_udf`), with `0` cores → built-in default. No `..._MODE` property.
- **Alternatives:** A full `DATAFUSION_THREADING_MODE`-style AUTO/FIXED mode property.
- **Rationale:** The threading MODE property exists only because partitions and threads are two coupled fields needing a shared selector. Connection concurrency is one field, so a mode property is redundant machinery — the `PARALLELISM_FACTOR` single-property-with-computed-default pattern fits better.
- **Promotes to ADR:** no

### [4] Apply the budget via object_store ClientOptions, not DataFusion target_partitions

- **Decision:** Size the budget onto the S3 client through `AmazonS3Builder::with_client_options(ClientOptions)` (object_store 0.13.2, method confirmed present), targeting the HTTP connection pool — the axis independent of CPU thread count.
- **Alternatives:** DataFusion `target_partitions` file-group splitting (rejected: that is the CPU/threading axis, already a knob); `datafusion.execution.meta_fetch_concurrency` (rejected: only affects schema/stats reads, not data-scan throughput).
- **Rationale:** The object-store HTTP client pool is what genuinely maps to "how many concurrent fetches from S3 per UDF instance". The exact pooling call (`with_pool_max_idle_per_host` and/or companions) and whether it must be paired with file-group splitting is the expert-tagged mechanism decision (Task 5); the AUTO-derivation formula is the expert-tagged decision in Task 3. Records that object-store connection concurrency is a first-class tuning axis distinct from the DataFusion thread/partition budget.
- **Promotes to ADR:** yes

### [5] Incorporate 180M-row full-emit evidence as rationale + re-gate task + deferred-work doc; do NOT expand scope to build emit-path work

- **Decision:** Fold the 2026-07-01 180M-row / 60-file finding (native `IMPORT INTO` ~80.4 s vs. VS full-emit CTAS ~151 s, ~1.9× — a full raw-row `SELECT *` workload, differently shaped from the original aggregate-path benchmark; `docs/performance.md` §"Larger-scale validation") into plan.md as reinforcing rationale for *both* existing deliverables. Add one named validation task (plan.md Task 10) to re-run that exact 60-file comparison *after* Task 1's dep bump lands and confirm whether the gap narrows once `CLUSTER_NODES` is real. Document the emit-path `Int64→Decimal128` coercion optimization as evidence-gated deferred work (plan.md §Deferred work), conditioned on Task 10's outcome. Do NOT add code beyond the plan's existing scope (0.20.1 dep bump + `S3_MAX_CONNECTIONS` knob). The feature spec (`datafusion-scan/scan-execution-connection-concurrency/spec.md`) is unchanged — the knob's contract is unaffected; only the surrounding rationale/validation narrative grows.
- **Alternatives:** (a) Expand this plan to build the emit-path `Int64→Decimal128` optimization now — rejected. (c) Ignore the new evidence — rejected (it materially reshapes the rationale and surfaces a real open question).
- **Rationale:** The 151 s/80.4 s gap is confounded by the pre-0.20.1 `CLUSTER_NODES=1` under-sharding bug that Task 1 already fixes, so it is unknown whether the gap is under-sharding (would close on the dep bump) or a genuine emit-path bottleneck. Building emit-path coercion work now would be YAGNI — there is no confirmed emit-bound root cause, only a confounded measurement. Task 10 supplies the isolating measurement; the deferred-work doc records the optimization so it is not lost, gated on that measurement. Keeping benchmark/re-gate work out of Given/When/Then deltas follows the same rule as the existing Task 9 (bench work is validation, not spec contract). Resolution followed the project's "evidence-gated future work" convention after the amendment interview (Q3) received no response within the timeout.
- **ADR note:** Codifies the project rule — new benchmark evidence that is confounded by an in-flight fix is incorporated as rationale + a named post-fix re-gate task + evidence-gated deferred-work docs, never as immediate scope expansion, until the confound is isolated.
- **Promotes to ADR:** yes

## Validation addendum (2026-07-02): sweep methodology, hypothesis verdict, re-gate outcome

Live validation of Tasks 3.1 (sweep) and 3.2 (180M re-gate) against benchmark cluster **test1**
(2-node Exasol, AWS Glue `eu-west-1`, SLC lc-rs **0.20.1**, `NR_OF_CORES=8`, 60-file / 179,998,372-row
`lineitem` = 5.398 GB Parquet). Raw logs under `bench/reports/` and the run's scratch outputs.

### Lever-1 premise confirmed: `CLUSTER_NODES` now real

Post-0.20.1, `adapterNotes` reports **`CLUSTER_NODES=2`** (was the buggy `1`). At the default
`PARALLELISM_FACTOR=8` this makes `G = 2 × 8 = 16` shards (was `1 × 8 = 8`). This single fix cut Q4
(full-`lineitem` scan) from **28.5 s → 20.5 s** with no knob change. (Also surfaced and fixed a stale
`bench/run.sh` DDL bug: the scan SET SCRIPT was still declared single-arg `(spec …)` while the
current adapter emits the two-arg `(common, files)` pushdown — corrected to match `e2e_scan_test.rs`.)

### Methodology

`bench/sweep.sh` extended to drive `PARALLELISM_FACTOR` + `DATAFUSION_THREADING_MODE` +
`S3_MAX_CONNECTIONS` per config row; `bench/run.sh` `.env` sourcing fixed so caller-exported
`BENCH_*` overrides win over `.env` defaults (previously `.env`'s `BENCH_PARALLELISM_FACTOR=8`
silently clobbered the sweep's `PARALLELISM_FACTOR=1` — first sweep pass was invalid; re-run after
fix). Each config verified against the resolved `adapterNotes` before trusting its timings. Native
ceiling via `bench/import_ceiling.sh`.

### Hypothesis verdict — all three levers refuted

| Lever | Config | Result | Verdict |
|---|---|---|---|
| 1 — #shards = #nodes | `PARALLELISM_FACTOR=1` → `G=2` | Q4 63.7 s vs. 20.5 s at `G=16` (**3.1× slower**); Q2/Q3 ~2× slower | **Refuted** |
| 2 — one AUTO instance/node | `PARALLELISM_FACTOR=1` + AUTO (8 threads/8 parts) | same losing shape; intra-instance threading ≠ inter-instance sharding | **Refuted** |
| 3 — max S3 connections | sweep 4→128 at both shard shapes | Q4 varied **< 2 %** (`PF=8`: 20.1–20.4 s; `PF=1`: 63.7–64.4 s) | **Refuted** |

**Best config = the shipped default** (`PARALLELISM_FACTOR=8`, `G=16`, threading AUTO/FIXED,
`S3_MAX_CONNECTIONS` AUTO). No swept knob beat it. The genuine improvement was the 0.20.1
`CLUSTER_NODES` correctness fix, not any new tuning value. `S3_MAX_CONNECTIONS` is correctly wired
(end-to-end verified via `adapterNotes`) but is not the throughput limiter on this deployment — the
S3 read is network-distance bound (~0.176 GB/s native ceiling; the ~1 GB/s mission target is a
deployment/co-location property, not reachable here). No aggregate-query regression: Q1–Q4 at the
winning `PF=8` shape are all ≤ their prior times.

### Task 3.2 re-gate outcome — emit gap persists, but is NOT under-sharding

The exact 60-file / 180M-row materialization could **not** be reproduced: the shared cluster's
**10 GiB raw-size license** is exceeded by a single 180M-row `lineitem` table (≈ 24 GiB raw), so any
full materialization is rejected. Re-measured at reduced scale (≈ 30–33 M rows, same files, corrected
`CLUSTER_NODES=2`): native `IMPORT INTO` **2.07 M rows/s** vs. VS `CREATE TABLE AS SELECT *`
**1.19 M rows/s** = **~1.74×** (pre-fix full-scale was ~1.88×). The VS full-emit throughput
(1.19 M rows/s) is *identical* to the pre-fix full-scale run — **doubling `G` (8→16) did not move it**,
so the emit gap is bottlenecked downstream of sharding. Confound from Design Decision [5] is resolved:
the 1.9× gap was **not** primarily under-sharding.

### Emit-path optimization decision — NOT pursued (evidence-gated, per ADR-055)

Column-isolation on the real workload (33 M rows, 4 columns per class): Int64→`Decimal128(20,0)`
coercion **4.63 M rows/s** vs. zero-copy `Decimal128(15,2)` **5.89 M rows/s** vs. `Utf8`
**6.82 M rows/s**. The coercion is the slowest class but only **~1.27×** slower — contributing
~5–6 % of full-emit time (eliminating it moves the native gap only ~1.74× → ~1.65×), nowhere near the
synthetic micro-bench's 50–200×. The gap is dominated by general per-row Arrow→`Value` conversion and
synchronous `MT_EMIT` round-trips. **No emit-path code was written** — the micro-bench figure does not
reproduce as a real-workload bottleneck; the deferred item is downgraded to "revisit only if a future
profile isolates a coercion-dominated workload" rather than an actionable optimization.

## Validation addendum 2 (2026-07-02): emit-path batch-size & connection sweep

Follow-up to the re-gate above, isolating the two emit-path levers the prior pass left untested,
on the **same** reduced-scale raw-emit workload (`CREATE TABLE AS SELECT * FROM TPCH.LINEITEM WHERE
L_ORDERKEY < 33007128` = 33,006,459 rows; 60-file `lineitem`; cluster **test1**, `CLUSTER_NODES=2`,
`PARALLELISM_FACTOR=8` ⇒ `G=16`, threading AUTO; native `IMPORT INTO` ceiling **2.07 M rows/s** from
the re-gate, reused since the row-set is unchanged). Best of 2 passes/config; VS recreated per config
(property resolved at `createVirtualSchema`, verified via `adapterNotes`); `FLUSH STATISTICS` between
runs for the 10 GiB license.

**Methodology note (honesty):** the first attempt ran two sweep instances concurrently against the
same VS + `BENCH.LI_BS` table — they clobbered each other's batch-size setting and contended for the
cluster, producing two mutually-contradictory datasets (0.89–1.32 M rows/s for the *same* config).
Both were discarded. The authoritative numbers below are from a single sequential foreground run with
nothing else touching the cluster.

### Lever A — `DATAFUSION_BATCH_SIZE` (raw-emit `MT_EMIT` round-trip count): refuted as the bottleneck

| batch size | ~round-trips | VS rows/s | gap |
|---|---|---|---|
| 8192 (default) | ~4,030 | 1,222,009 | 1.70× |
| 32768 | ~1,007 | 1,278,824 | 1.62× |
| 65536 (best) | ~504 | 1,306,669 | 1.59× |
| 131072 | ~252 | 1,272,416 | 1.63× |

A **16× reduction** in `MT_EMIT` round-trips (8192→131072) bought only **~7 %** throughput, plateauing
at 32k–65k. The gap stays ~1.6×. **Refines** the re-gate's attribution: the round-trip *count* is a
minor cost, not co-dominant with per-row conversion — if it were, 16× fewer would have moved far more
than 7 %. The residual gap is dominated by per-*row* work (Arrow→`Value` materialization + DB-side row
ingest), which scales with row count regardless of batching.

### Lever B — `S3_MAX_CONNECTIONS` on the raw-emit path (bs=65536): refuted (as on Q4)

AUTO(=4) 1,309,261 / 8: 1,310,820 / 32: 1,297,934 / 64: 1,311,341 / 128: 1,315,522 rows/s — a **~1.4 %**
spread, gap fixed at ~1.58×. A wider fetch pipeline does not hide emit-wait; the emit path is no more
fetch-concurrency-bound than the aggregate path was.

### Aggregate-path regression check (Q1–Q4, bs 8192 vs 65536): no regression

Q1 1.94→1.96 s, Q2 18.53→17.03 s (−8 %), Q3 15.75→15.30 s (−3 %), Q4 20.35→18.71 s (−8 %). 65536
marginally *helps* the aggregate/join path; all within the prior sweep's ranges.

### Decision — NOT pursued (evidence-gated, per ADR-055): shipped default stays 8192

Neither lever closes the ~1.6× raw-emit gap; it is an architectural floor (UDF per-row
materialization + emit protocol vs. native `IMPORT`'s bulk columnar loader), not a tunable. No
shipped-crate change was made. `DATAFUSION_BATCH_SIZE=65536` is documented as an operator hint for
emit-bound / wide `SELECT *` workloads (~7 % emit gain, no aggregate regression, memory-safe here),
but the default remains **8192** — it matches DataFusion's own default and keeps the per-batch decode
working set (out-of-pool RSS) small for memory-constrained deployments, where ~7 % does not justify an
8× larger in-flight batch. Bench scaffolding added (not spec deltas): `bench/batch_size_sweep.sh`,
`bench/emit_s3conn_sweep.sh`, `bench/batch_size_aggcheck.sh`, and a `BENCH_DF_BATCH_SIZE` knob in
`bench/run.sh` (selftest extended, still passing).

## Review Findings

<!-- Populated by speq-implement after code review. -->
