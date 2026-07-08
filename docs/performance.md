[lakehouse-engine](../README.md) › [Docs](index.md) › Performance

---

# Performance

## What this is

TPC-H sf=30 (8-table schema, `lineitem` 180M rows, 60 Parquet files, AWS Glue Iceberg catalog), same
data for every engine, run against the `test1` Exasol cluster (2× `r8i.2xlarge`). Two variants of the
same 15-query set (Q1-Q9b, NQ1-NQ5):

- **(a) Without deletes** — the pristine baseline `tpch` Iceberg tables.
- **(b) With deletes** — Iceberg v2 merge-on-read position-delete copies of the same tables
  (`BENCH_WITH_DELETES=1`; ~5% of rows deleted per table, deterministic `key % 20 = 0`, authored via
  Apache Spark since neither PyIceberg nor iceberg-rust can write MOR position deletes — see
  `scripts/spark-fixtures/create_tpch_deletes.sql` for the mechanism). Confirms the merge-on-read read
  path's cost at scale, not just correctness (already covered by `make test-e2e`).

Both variants were re-run together on 2026-07-08 on a freshly-recreated `test1` cluster (the prior
cluster's EC2 key pair had been lost with no shared copy — see
[#89](https://github.com/exasol-labs/lakehouse-engine-rs/issues/89) — so this run is also the first
against the new cluster and the new SSM-backed shared-key setup). Competitor engine columns
(Trino/Athena/Spark) are from the 2026-07-06 run — not re-run this pass; they don't have a with-deletes
variant since delete-flag benchmarking is a lakehouse-engine-rs-specific feature.

Reproduce: `make bench` (without deletes) / `BENCH_WITH_DELETES=1 make bench` (with deletes) — see
[`bench/README.md`](../bench/README.md). Remote (`test1`): `deploy/scripts/bench-remote.sh test1`
(optionally prefixed with `BENCH_WITH_DELETES=1`) — see [`deploy/README.md`](../deploy/README.md).

| Resource | |
|---|---|
| lakehouse-engine-rs | Exasol `test1`, 2× `r8i.2xlarge` |
| Trino (native) | 2× `r8i.2xlarge`, ephemeral, fresh cluster |
| Trino (IMPORT FROM JDBC) | 2× `r8i.2xlarge`, ephemeral, fresh cluster (via Exasol `test1`) |
| Athena | on-demand workgroup |
| Spark | EMR Serverless |

## (a) Without deletes

Fastest lakehouse-engine-rs-vs-competitor time per query in **bold**.

| Query | lakehouse-engine-rs | Trino (native) | Trino (IMPORT FROM JDBC) | Athena | Spark |
|---|---|---|---|---|---|
| Q1 (3-way join, wiring) | **1.73 s** | 2.81 s | 3.13 s | 2.43 s | 18.89 s |
| Q2 (3-way join, big scan) | 18.03 s | 9.71 s | 8.77 s | **2.14 s** | 42.89 s |
| Q3 (join + filter + GROUP BY) | 15.06 s | 5.12 s | 4.91 s | **2.37 s** | 29.20 s |
| Q4 (pricing summary, filter) | 5.70 s | **2.09 s** | 3.22 s | 3.43 s | 18.94 s |
| Q5 (Q3, no filter) | 18.44 s | 6.63 s | 7.57 s | **2.16 s** | 35.38 s |
| Q6 (Q4, no filter) | 4.40 s | **1.34 s** | 2.62 s | 2.52 s | 16.52 s |
| Q7 (high-cardinality GROUP BY) | 7.27 s | **2.04 s** | 2.71 s | 2.30 s | 12.15 s |
| Q8 (selective filter) | 3.27 s | **0.73 s** | 2.22 s | 0.99 s | 4.38 s |
| Q9a (narrow projection) | 2.67 s | **0.65 s** | 2.15 s | 1.57 s | 4.51 s |
| Q9b (wide projection) | **11.54 s** | 11.91 s | 12.78 s | 27.93 s | 57.31 s |
| NQ1 (arithmetic aggregate: `SUM(price*discount)`) | 5.22 s | **1.91 s** | 3.26 s | 2.27 s | 10.72 s |
| NQ2 (LIKE + IN filter pushdown) | 5.18 s | **2.38 s** | 3.67 s | 2.71 s | 11.45 s |
| NQ3 (4-way join, part/partsupp) | 5.52 s | **1.55 s** | 2.47 s | 4.45 s | 3.88 s |
| NQ4 (ORDER BY + LIMIT top-N) | 3.53 s | **1.62 s** | 2.88 s | 2.34 s | 11.18 s |
| NQ5 (tuple GROUP BY + HAVING + AVG) | 2.07 s | **0.54 s** | 2.18 s | 2.06 s | 5.39 s |

## (b) With deletes (`BENCH_WITH_DELETES=1`)

lakehouse-engine-rs only — no competitor engine has a comparable delete-flag variant in this suite.
Delete-count sanity check confirms position deletes are applied on read:
`LINEITEM 170997641 (~95.0% of baseline 179998372)`. The delete-bearing `tpch_deletes` tables were
authored once via the one-time remote EMR Serverless job (`deploy/scripts/make-deletes-remote.sh`);
`LINEITEM`'s copy has 30 Parquet data files (vs. the baseline's 64 — a smaller file count from the
Spark CTAS write, not a correctness issue: shard fan-out and per-file position-delete pairing were
independently re-verified live via `EXPLAIN VIRTUAL` after this run).

**Merge-on-read read overhead is substantial at this scale — 3-11x slower for most queries**, with
join-heavy queries much less affected (Q1, NQ3). This is the actual signal this benchmark variant
exists to surface (confirming the read path's *cost*, not just its correctness — already covered by
`make test-e2e`).

| Query | lakehouse-engine-rs (with deletes) | vs. without deletes (§a) |
|---|---|---|
| Q1 (3-way join, wiring) | 1.94 s | 1.1x |
| Q2 (3-way join, big scan) | 52.05 s | 2.9x |
| Q3 (join + filter + GROUP BY) | 50.41 s | 3.3x |
| Q4 (pricing summary, filter) | 33.63 s | 5.9x |
| Q5 (Q3, no filter) | 56.05 s | 3.0x |
| Q6 (Q4, no filter) | 32.45 s | 7.4x |
| Q7 (high-cardinality GROUP BY) | 34.09 s | 4.7x |
| Q8 (selective filter) | 31.31 s | 9.6x |
| Q9a (narrow projection) | 30.29 s | 11.3x |
| Q9b (wide projection) | 38.04 s | 3.3x |
| NQ1 (arithmetic aggregate: `SUM(price*discount)`) | 32.57 s | 6.2x |
| NQ2 (LIKE + IN filter pushdown) | 31.76 s | 6.1x |
| NQ3 (4-way join, part/partsupp) | 9.77 s | 1.8x |
| NQ4 (ORDER BY + LIMIT top-N) | 30.45 s | 8.6x |
| NQ5 (tuple GROUP BY + HAVING + AVG) | 11.26 s | 5.4x |

Not further root-caused this pass (out of scope for the `add-delete-benchmark-flag` plan, which targets
*measuring* this cost, not optimizing it) — a natural follow-up given the magnitude here.

**Note on this run's pushdown-check exit status**: the same live run's trailing `EXPLAIN VIRTUAL`
pushdown assertions (`shard_key`, `filter`, `aggregates`, etc.) all failed uniformly in a way consistent
with a single transient `EXPLAIN VIRTUAL` call erroring rather than a real regression — re-querying the
same `EXPLAIN VIRTUAL SELECT COUNT(*) FROM TPCH.LINEITEM` moments later on the same cluster showed the
expected `... AS shards(shard_key, files) GROUP BY shard_key` shape with position-delete files correctly
paired per data file. The 15 query results and timings above are unaffected — only the harness's
own trailing pushdown-check block flaked. `bench/run.sh`'s `pushdown_check` has no retry, so one
transient EXPLAIN failure fails the whole run; worth hardening separately.
