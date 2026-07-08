[lakehouse-engine](../README.md) › [Docs](index.md) › Performance

---

# Performance

TPC-H sf=30 (8-table schema, `lineitem` 180M rows, 60 Parquet files, AWS Glue Iceberg catalog), same data for every engine. Live-verified 2026-07-06; lakehouse-engine-rs column re-verified 2026-07-07 post lc-rs 0.20.3 (see §3 below for the A/B), then fully re-run again 2026-07-07 against `test1` on `feat/fix-join-decline-hard-fail` (PR #78) to confirm the #76 join-pushdown-decline fix, and once more 2026-07-08 against a freshly-recreated `test1` cluster on `feat/add-delete-benchmark-flag` (PR #81) as a post-merge regression check — see §7. Competitor columns (Trino/Athena/Spark) are unchanged from 2026-07-06 — not re-run this pass.

Q1, Q2, and NQ3 (previously broken by [#76](https://github.com/exasol-labs/lakehouse-engine-rs/issues/76), fixed in PR #78) re-verified passing as of 2026-07-07 — see §5. The full table below is a fresh clean re-run of all 15 queries on that date, superseding the prior 2026-07-06 lakehouse-engine-rs numbers, then superseded again by the 2026-07-08 re-run (§7).

| Engine | Resources |
|---|---|
| lakehouse-engine-rs | Exasol `test1`, 2× `r8i.2xlarge` |
| Trino (native) | 2× `r8i.2xlarge`, ephemeral, fresh cluster |
| Trino (IMPORT FROM JDBC) | 2× `r8i.2xlarge`, ephemeral, fresh cluster (via Exasol `test1`) |
| Athena | on-demand workgroup |
| Spark | EMR Serverless |

Fastest time per query in **bold**.

| Query | lakehouse-engine-rs | Trino (2-node) | Athena | Spark (EMR Serverless) | IMPORT FROM JDBC (Trino) |
|---|---|---|---|---|---|
| Q1 (3-way join, wiring) | **1.73 s** | 2.81 s | 2.43 s | 18.89 s | 3.13 s |
| Q2 (3-way join, big scan) | 18.03 s | 9.71 s | **2.14 s** | 42.89 s | 8.77 s |
| Q3 (join + filter + GROUP BY) | 15.06 s | 5.12 s | **2.37 s** | 29.20 s | 4.91 s |
| Q4 (pricing summary, filter) | 5.70 s | **2.09 s** | 3.43 s | 18.94 s | 3.22 s |
| Q5 (Q3, no filter) | 18.44 s | 6.63 s | **2.16 s** | 35.38 s | 7.57 s |
| Q6 (Q4, no filter) | 4.40 s | **1.34 s** | 2.52 s | 16.52 s | 2.62 s |
| Q7 (high-cardinality GROUP BY) | 7.27 s | **2.04 s** | 2.30 s | 12.15 s | 2.71 s |
| Q8 (selective filter) | 3.27 s | **0.73 s** | 0.99 s | 4.38 s | 2.22 s |
| Q9a (narrow projection) | 2.67 s | **0.65 s** | 1.57 s | 4.51 s | 2.15 s |
| Q9b (wide projection) | **11.54 s** | 11.91 s | 27.93 s | 57.31 s | 12.78 s |
| NQ1 (arithmetic aggregate: `SUM(price*discount)`) | 5.22 s | **1.91 s** | 2.27 s | 10.72 s | 3.26 s |
| NQ2 (LIKE + IN filter pushdown) | 5.18 s | **2.38 s** | 2.71 s | 11.45 s | 3.67 s |
| NQ3 (4-way join, part/partsupp) | 5.52 s | **1.55 s** | 4.45 s | 3.88 s | 2.47 s |
| NQ4 (ORDER BY + LIMIT top-N) | 3.53 s | **1.62 s** | 2.34 s | 11.18 s | 2.88 s |
| NQ5 (tuple GROUP BY + HAVING + AVG) | 2.07 s | **0.54 s** | 2.06 s | 5.39 s | 2.18 s |

*(lakehouse-engine-rs column: 2026-07-08 numbers, §7. Prior 2026-07-07 column preserved in git history if needed for the #76-fix-specific comparison.)*

Reproduce: `RUN_TRINO_COMPARISON=1 bench/compare_all.sh` ([`bench/README.md`](../bench/README.md)).

## Bottleneck analysis (2026-07-06)

Grounded in live `EXA_USER_PROFILE_LAST_DAY` telemetry captured against `test1`, not assumed.

**Methodology fix first**: `bench/run.sh` turned Exasol's native `PROFILE` on *after* every timed
query had already run, so `EXA_USER_PROFILE_LAST_DAY` held nothing for them. Moved the
`ALTER SYSTEM SET PROFILE = 'ON'` to before Q1 — see the script for the corrected placement.

### 1. JOIN queries (Q2, Q3, Q5, NQ3) are the biggest gap — and telemetry shows the mechanism

`customer × orders × lineitem` is not pushed into DataFusion (out of scope per
[`specs/backlog.md`](../specs/backlog.md) `BL-001`): each table gets its own independent VS
row-scan, and Exasol's own core engine executes the join over the fully materialized results.
PROFILE shows individual `JOIN` operators costing 1.5–3.5s CPU apiece over up to 180M rows, on
top of multi-second `GROUP BY` cost reassembling each table's sharded scan output before the join
can start. This lines up with the wall-clock gap: Q2 17.1s here vs Trino 9.7s / Athena 2.1s; Q3
15.4s vs 5.1s / 2.4s; Q5 18.3s vs 6.6s / 2.2s.

→ **Primary lever**: implement `BL-001` Phase 1 (broadcast inner equi-join pushdown).

### 2. Shard-count-to-selectivity is already correct — checked in code, not a gap

`shard_count()` clamps `G = node_count × parallelism_factor` to the file count returned by
`resolve_file_list`, which prunes via the pushed filter *before* `shard_count` runs. A selective
query already gets fewer shards. Verified by reading `handle_pushdown` in
`crates/lakehouse-engine/src/adapter/pushdown.rs`; ruled out as an improvement area rather than
assumed to be one.

### 3. String-block wire encoding for DATE/DECIMAL/NUMERIC columns: adopted, but no measurable end-to-end win (2026-07-07)

`lineitem` carries 3 DATE + 4 DECIMAL columns, encoded as string blocks over the UDF ABI
regardless of their final SQL column type. lc-rs v0.20.3 (released 2026-07-07, PR #44)
shipped exactly this optimization — hand-rolled fixed-format Decimal/Date/Timestamp
formatters replacing `chrono`'s/`Decimal`'s generic `Display` — measuring **+28% to +46%**
on its own isolated `benches/emit-bench` "wide" shape (a pure emit-throughput microbenchmark:
`BIGINT, DECIMAL(18,2), DATE, TIMESTAMP, VARCHAR(100)`, no network/S3/DataFusion/Exasol-join
cost in the loop). Adopted here by bumping `exasol-udf-sdk`/`exasol-udf-macros`/the SLC from
0.20.2 to 0.20.3 (no code change — ships unconditionally).

**End-to-end, it doesn't move the needle.** A same-session, same-cluster A/B on `test1`
(0.20.2 rebuilt and re-benchmarked back-to-back against 0.20.3, isolating the SDK/SLC version
as the only variable — cross-session comparisons carry too much cloud-environment noise to
trust) shows every query within roughly ±10-15% of its 0.20.2 time, with no consistent
direction:

| Query | 0.20.2 | 0.20.3 (avg of 2 runs) | Δ |
|---|---|---|---|
| Q3 | 16.31 s | 15.51 s | -4.9% |
| Q4 | 4.77 s | 4.90 s | +2.7% |
| Q5 | 19.26 s | 17.92 s | -7.0% |
| Q6 | 4.18 s | 4.18 s | 0% |
| Q7 | 6.59 s | 7.01 s | +6.4% |
| Q8 | 3.31 s | 3.06 s | -7.6% |
| Q9a | 2.08 s | 2.55 s | +22.6%* |
| Q9b | 11.00 s | 11.11 s | +1.0% |
| NQ1 | 4.56 s | 4.77 s | +4.6% |
| NQ2 | 4.38 s | 5.01 s | +14.4% |
| NQ4 | 2.79 s | 3.57 s | +27.9%* |
| NQ5 | 2.43 s | 2.10 s | -13.6% |

\* Q9a/NQ4 are short (2-4s) queries where ~0.5-1s of absolute run-to-run cloud noise is a
large fraction of the total — the two individual 0.20.3 runs disagreed with each other by
more than they disagreed with 0.20.2 (e.g. Q9a: 3.22s then 1.87s).

This is the expected outcome once you look at where the wall-clock actually goes for a
TPC-H-shaped query here: string-block formatting is one line item inside a scan/emit path
dominated by S3/Parquet I/O, DataFusion execution, and (for join queries) Exasol-side
`JOIN`/`GROUP BY` reassembly. A 28-46% win on the formatter alone doesn't surface above
noise at the full-query level for this workload. The optimization is real and worth having
(it's unconditional and free), it's just not the lever for *this* bottleneck — §1 (join
pushdown) remains the primary one.

**Correctness note** (from when this was still an unreleased branch, kept for the record):
the SLC's own new ingest-throughput test harness had a bug — it read back a parallel SET
UDF's per-shard partial row counts with no `GROUP BY`/`SUM()`, undercounting (e.g. 64,696
counted vs 1,000,000 expected). Fixed upstream; unrelated to the runtime encode/decode logic,
which passed all unit tests unchanged.

### 4. Join-pushdown-broadcast (PR #70) regressed 3+ table joins — separate from this pass

Found while re-running this benchmark on 2026-07-07: Q1, Q2, and NQ3 (all 3+ table joins)
now hard-fail with `F-UDF-CL-RUST-9001: UDF error: join pushdown declined: ... Exasol will
retry the query natively` — except no native retry actually happens; the error propagates to
the client as a failed statement. Reproduces identically on `main` before the lc-rs 0.20.3
bump, so it's unrelated to this pass — a regression from the broadcast join-pushdown feature
merged just before. Filed as [#76](https://github.com/exasol-labs/lakehouse-engine-rs/issues/76);
their previous timings (Q1 1.67s, Q2 17.09s, NQ3 4.40s, table below) are stale until fixed.

**Fixed 2026-07-07 in PR #78** — see §5 for the re-verification run.

### 5. Join-pushdown-decline hard-fail (#76) fixed and re-verified (2026-07-07)

PR #78 (`feat/fix-join-decline-hard-fail`) fixes the hard-fail described in §4. Re-verified
against a fresh start/stop cycle of the `test1` cluster on that branch with a full clean
15-query re-run (numbers folded into the results table above, superseding the prior
2026-07-06 lakehouse-engine-rs column): Q1 now completes in 2.31s, Q2 in 18.45s, and NQ3 in
5.12s — all three previously hard-failing with `F-UDF-CL-RUST-9001`, now returning correct
results with no error. All other queries and every `PUSHDOWN` wiring check in the suite also
passed clean.

Also fixed in passing: `deploy/scripts/secrets.sh` still hardcoded the pre-0.20.3
`BENCH_SLC_VERSION=0.20.2`, which produced an unrelated `F-UDF-CL-RUST-9001` fingerprint
mismatch (expected 0.20.2, `.so` built against 0.20.3) before the join fix could even be
exercised on a freshly-provisioned `bench/.env`. Bumped to `0.20.3` to match the Makefile
default and the SDK version this branch actually builds against.

### 7. Post-merge regression re-run on a freshly-recreated `test1` cluster (2026-07-08)

Full clean 15-query re-run against `test1` on `feat/add-delete-benchmark-flag` (PR #81, which merged
`main` — including the join-pushdown-rendering change in #76's follow-up PR #78/e66b95a — mid-branch).
Purpose was a general post-merge health check, not an investigation of a specific optimization: confirm
the engine still performs consistently after the intervening `main` commits and this branch's own
(bench-harness-only) changes, none of which touch query execution code.

**Numbers are within normal cloud run-to-run noise of the 2026-07-07 baseline** — the same ±10-30%
spread already characterized in §3's A/B methodology, no query regressed or improved outside that band
except NQ1/NQ2/NQ4 (~20-32% slower) and NQ5 (~28% faster), all short (2-5s) queries where absolute noise
dominates the percentage, same caveat as §3's Q9a/NQ4 footnote. No action taken; not a real signal.

**Operational context for these numbers**: this was the first `test1` run after the cluster's EC2 key
pair private key was discovered lost (nobody had a shared copy — see
[#89](https://github.com/exasol-labs/lakehouse-engine-rs/issues/89)). The cluster was destroyed and
recreated with a freshly generated key pair, this time stored in AWS SSM Parameter Store
(SecureString, `/spot-strata/deploy/ssh_key/spot-strata-key`) so any deployer-credentialed teammate can
access it going forward — no IAM change needed, `ssm:*` was already granted. Data (the persistent
`tpch` Glue tables) was untouched by the cluster recreate.

Also found and fixed in the same session, a SEPARATE recurrence of §5's stale-SLC-version bug: this
time in `bench/run.sh`'s own hardcoded default (`0.16.0`, unrelated to `secrets.sh`'s already-fixed
default), causing the same `F-UDF-CL-RUST-9001` fingerprint mismatch on any run not explicitly
overriding `BENCH_SLC_VERSION`. Fixed to `0.20.3` (commit `fe6827b`,
[#91](https://github.com/exasol-labs/lakehouse-engine-rs/issues/91)) — two independent hardcoded copies
of this version number drifting independently suggests it may be worth a follow-up to derive both from
one source instead.

The new `BENCH_WITH_DELETES` flag (this branch's actual feature) was thoroughly verified live in
**docker mode** (`~95% of baseline` delete-count sanity check passing, full query suite + pushdown
checks green — see `specs/_plans/add-delete-benchmark-flag/verification-report.md`) but was **not**
exercised against `test1` in this run — the one-time remote delete-authoring prerequisite
(`deploy/scripts/make-deletes-remote.sh`) hasn't been run against this cluster's Glue tables yet. Left
as a follow-up, not a completion gate for PR #81.

### 6. Small/narrow-result queries (Q8, Q9a, NQ4, NQ5) still lose 2–4x despite trivial output

PROFILE shows a recurring ~0.3–1.0s `PUSHDOWN` / `COMPILE / EXECUTE` cost per query — Exasol
planning and compiling the (per-shard) pushdown SQL literal — that doesn't shrink with query
selectivity, unlike Trino's persistent worker fleet. Partly inherent to the "stateless UDF, no
caching" mission constraint. Not further decomposed this pass: the existing per-scan
`phase_startup_ms` / `phase_import_ms` / `phase_emit_ms` telemetry (see
[Tuning](tuning.md#telemetry)) would attribute this precisely for a single-leg repro, but
capturing it needs a redeployed debug-level scan script and more cluster time than this pass's
AWS budget allowed — left as a follow-up.
