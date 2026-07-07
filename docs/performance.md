[lakehouse-engine](../README.md) › [Docs](index.md) › Performance

---

# Performance

TPC-H sf=30 (8-table schema, `lineitem` 180M rows, 60 Parquet files, AWS Glue Iceberg catalog), same data for every engine. Live-verified 2026-07-06.

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
| Q1 (3-way join, wiring) | **1.67 s** | 2.81 s | 2.43 s | 18.89 s | 3.13 s |
| Q2 (3-way join, big scan) | 17.09 s | 9.71 s | **2.14 s** | 42.89 s | 8.77 s |
| Q3 (join + filter + GROUP BY) | 15.10 s | 5.12 s | **2.37 s** | 29.20 s | 4.91 s |
| Q4 (pricing summary, filter) | 3.89 s | **2.09 s** | 3.43 s | 18.94 s | 3.22 s |
| Q5 (Q3, no filter) | 18.06 s | 6.63 s | **2.16 s** | 35.38 s | 7.57 s |
| Q6 (Q4, no filter) | 3.56 s | **1.34 s** | 2.52 s | 16.52 s | 2.62 s |
| Q7 (high-cardinality GROUP BY) | 5.98 s | **2.04 s** | 2.30 s | 12.15 s | 2.71 s |
| Q8 (selective filter) | 2.34 s | **0.73 s** | 0.99 s | 4.38 s | 2.22 s |
| Q9a (narrow projection) | 1.17 s | **0.65 s** | 1.57 s | 4.51 s | 2.15 s |
| Q9b (wide projection) | **11.36 s** | 11.91 s | 27.93 s | 57.31 s | 12.78 s |
| NQ1 (arithmetic aggregate: `SUM(price*discount)`) | 3.96 s | **1.91 s** | 2.27 s | 10.72 s | 3.26 s |
| NQ2 (LIKE + IN filter pushdown) | 4.19 s | **2.38 s** | 2.71 s | 11.45 s | 3.67 s |
| NQ3 (4-way join, part/partsupp) | 4.40 s | **1.55 s** | 4.45 s | 3.88 s | 2.47 s |
| NQ4 (ORDER BY + LIMIT top-N) | 2.13 s | **1.62 s** | 2.34 s | 11.18 s | 2.88 s |
| NQ5 (tuple GROUP BY + HAVING + AVG) | 2.63 s | **0.54 s** | 2.06 s | 5.39 s | 2.18 s |

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

### 3. String-block wire encoding for DATE/DECIMAL/NUMERIC columns is a real, separate cost

`lineitem` carries 3 DATE + 4 DECIMAL columns, encoded as string blocks over the UDF ABI
regardless of their final SQL column type. A local, cluster-free microbenchmark
(`language-container-rs` `benches/emit-bench`) of the unreleased `feat/add-emit-transfer-spikes`
branch — which pre-sizes the emit/ingest `Vec` buffers (avoiding `Vec::new()`+`push`'s doubling
reallocation curve) and replaces `chrono`/`Decimal`-`Display` DATE/TIMESTAMP/DECIMAL formatting
with hand-rolled fixed-format byte parsers — measured a mixed-shape (no temporal/decimal columns)
throughput baseline of ~720k–810k rows/s Rust vs ~140k–210k rows/s Python3 (4–5x) pre-patch.
Cannot be adopted yet: it lives in an unreleased `exasol-udf-sdk`/SLC version ("only use local
artifacts for testing" — the branch will be released later). Logged as a backlog item
(`BL-002`) to revisit once released.

**Correctness note, found via an actual failing run, not assumed**: the branch's own new
ingest-throughput test harness had a bug — it read back a parallel SET UDF's per-shard partial
row counts with no `GROUP BY`/`SUM()`, undercounting (e.g. 64,696 counted vs 1,000,000 expected).
Fixed locally (one-line `SUM()`). The *runtime* encode/decode logic itself is unrelated to that
harness bug and passes all 31 existing unit tests unchanged on the patched branch.

### 4. Small/narrow-result queries (Q8, Q9a, NQ4, NQ5) still lose 2–4x despite trivial output

PROFILE shows a recurring ~0.3–1.0s `PUSHDOWN` / `COMPILE / EXECUTE` cost per query — Exasol
planning and compiling the (per-shard) pushdown SQL literal — that doesn't shrink with query
selectivity, unlike Trino's persistent worker fleet. Partly inherent to the "stateless UDF, no
caching" mission constraint. Not further decomposed this pass: the existing per-scan
`phase_startup_ms` / `phase_import_ms` / `phase_emit_ms` telemetry (see
[Tuning](tuning.md#telemetry)) would attribute this precisely for a single-leg repro, but
capturing it needs a redeployed debug-level scan script and more cluster time than this pass's
AWS budget allowed — left as a follow-up.
